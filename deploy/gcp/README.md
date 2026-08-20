# vibevmm orchestration layer on GCP

Pulumi (TypeScript, bun) stack deploying the `vmon` v2 orchestration layer:

```
clients ──▶ sched VM (vmon sched, static IP) ──direct gRPC──▶ worker MIG (vmon serve, nested virt)
                 │ in-memory worker table                            │ heartbeats (self-expiring key + stream)
                 └───────────── stream follow ──────▶ state VM ◀─────┘
                                              (Redis :6379 + Postgres :5432)
```

Deliberately cheap: **no Memorystore, no Cloud SQL, no Cloud NAT, no LB**. One
small VM runs both Redis (the orch state bus — reconstructible cache, not a
source of truth) and Postgres (vmond's cluster substrate). Redundancy for the
state box is explicitly out of scope; losing it degrades scheduling freshness
and durable metadata, not running sandboxes.

Unlike EC2, GCE exposes `/dev/kvm` on ordinary VMs: nested virtualization is a
template flag on **Intel** machine series (N1/N2/C2/C3, …) — E2 and AMD series
do not support it, and Arm has no nested virtualization at all, so `arm64`
requires an explicit `-metal` machine type. Default fleet arch is therefore
`x86_64`, which makes GCP the cheap place to run this stack.

## Autoscaling (the part that actually moves)

`vmon sched`'s leader computes desired capacity (HPA-like, memory-utilization
target) and drives the worker MIG through hooks installed by this stack:

- `scale-up.sh` → `gcloud compute instance-groups managed resize --size $VMON_SCALE_DESIRED`
- `scale-down.sh` → for each `$VMON_IDLE_WIDS` entry (workers that are drained
  **and** empty): `delete-instances --instances $wid` (decrements target size
  atomically)

Worker ids are instance names (`VMON_ORCH_ID` = metadata `instance/name`), so
the delete mapping is direct. No GCP autoscaler is attached and instance
redistribution is `NONE`: GCP never picks victims — the scheduler drains a
worker (placement stops), waits for it to empty, then deletes it by name. A
drained worker that never empties is deleted only after its sandboxes finish;
if the fleet stays over target it is re-selected on a later tick. Sandboxes on
a worker that dies anyway are marked `lost` by the controller.

## Deploy

Prerequisites: `pulumi`, `bun`, GCP credentials (`gcloud auth application-default login`),
and a static musl `vmon` binary reachable over HTTPS or GCS (see `release.yml`
artifacts).

```bash
cd deploy/gcp
bun install
pulumi stack init prod
pulumi config set gcp:project my-project
pulumi config set gcp:region  europe-west4
pulumi config set binaryUrl   https://…/vmon-x86_64-unknown-linux-musl
pulumi config set assetsUrl   https://…/vmon-assets-x86_64.tar.gz    # optional: kernel + agent
# Or use IAM-authenticated GCS artifacts and private image sources:
# pulumi config set binaryGcsUri gs://artifacts/vmon-x86_64-unknown-linux-musl
# pulumi config set assetsGcsUri gs://artifacts/vmon-assets-x86_64.tar.gz
# pulumi config set rootfsGcsPrefix gs://disk-exports/published/
# pulumi config set artifactRegistryRepository projects/my-project/locations/us-central1/repositories/vmon-images
pulumi config set allowedCidr 203.0.113.7/32                         # your egress IP; default 0.0.0.0/0
pulumi config set workerMin 1
pulumi config set workerMax 4
pulumi up
```

| Key | Default | Meaning |
|---|---|---|
| `binaryUrl` | — (required) | musl `vmon` binary URL, must match `arch` |
| `binaryGcsUri` | none | `gs://` alternative; grants the worker and scheduler identities read access to that object only |
| `assetsUrl` / `assetsGcsUri` | none | tarball extracted to `/var/lib/vmon/assets`; a GCS URI grants the worker identity read access to that object only |
| `rootfsGcsPrefix` | none | `gs://bucket/prefix/` for cloud disk sources and published rootfs objects; grants workers publish and read access only below that prefix |
| `artifactRegistryRepository` | none | full `projects/PROJECT/locations/LOCATION/repositories/REPOSITORY` resource name for private pulls; omitted means no Artifact Registry grant |
| `arch` | `x86_64` | `x86_64` or `arm64`; `arm64` demands a `-metal` worker type |
| `workerMin` / `workerMax` | 1 / 4 | MIG bounds; the vmon autoscaler moves target size within them |
| `workerMachineType` | `n2-standard-32` | **must support nested virt** (Intel) or be `-metal` |
| `workerDiskGb` | 500 | worker boot disk capacity in GiB (`pd-balanced`) |
| `schedulerCount` | 1 | scheduler instances (each gets a static IP); N×M needs no LB |
| `schedulerMachineType` | `e2-small` / `t2a-standard-1` | |
| `stateMachineType` | `e2-small` / `t2a-standard-1` | Redis + Postgres box |
| `maxSandboxesPerWorker` | 0 | optional worker admission hard ceiling; 0 disables the count cap |
| `memoryReserveMiB` | 32768 | refuse creates when actual host-available memory reaches this reserve |
| `netSlots` | 256 | preallocated TAP/network slots per worker (0 disables pooling) |
| `targetUtil` | 0.7 | autoscaler target memory utilization |
| `allowedCidr` | `0.0.0.0/0` | who may reach sched :8100 and worker :8000 |

The 32 GiB reserve matches one fleet-standard guest: it leaves enough headroom for a guest's
shared pages to become private between admission samples, as well as for the worker daemon,
kernel, and page cache. Admission uses current host-available memory rather than charging
every sandbox its configured guest RAM, so idle forked sandboxes can share pages without
hiding memory dirtied under load.

The scheduler's service account carries a custom role with exactly the
resize/delete permissions. Launch artifact grants are conditioned to the
configured object rather than its entire bucket.

## Cloud image permissions

`rootfsGcsPrefix` must end in `/`; `gs://bucket/` deliberately selects the
whole bucket. Before deployment, create the bucket and verify that each source
export's metadata has a positive object generation and an ETag; lazy rootfs
requires both and pins reads to that identity. These are account-controlled
prerequisites. The referenced bucket and objects are not created or relocated
by this stack and may use any GCS location.

When configured, the deployment grants the generated worker service account
`roles/storage.objectUser` on the bucket with an IAM condition matching only
`projects/_/buckets/<bucket>/objects/<prefix>`. That supplies the
`storage.objects.get`, `storage.objects.create`, and
`storage.objects.delete` operations required to inspect the source and
sidecar, read the published rootfs, and create or replace the rootfs and
sidecar. Prefix-conditioned object listing is neither granted nor needed.
GCS resumable publication uses `storage.objects.create`; it does not use the
XML multipart API or require a separate multipart-abort permission.

If `vmon image publish-rootfs` runs somewhere other than a deployed worker,
grant its Google identity those same three object permissions below the same
prefix. The deployment identity running `pulumi up` needs
`storage.buckets.getIamPolicy` and `storage.buckets.setIamPolicy` on each
referenced bucket. Those deployment-generated IAM bindings do not create the
bucket or source objects. Metadata OAuth credentials used for remote reads are
sent only to provider-trusted HTTPS GCS object endpoints.

`artifactRegistryRepository` is opt-in and includes the repository's project
and location, so regional and multi-region repository resources are not
guessed from `gcp:region`. Configuring it grants
`roles/artifactregistry.reader` on that repository to the worker service
account. Omitting it grants no Artifact Registry access. The deployment
identity needs `artifactregistry.repositories.getIamPolicy` and
`artifactregistry.repositories.setIamPolicy` on that repository, including
when it belongs to another project. Registry creation and cross-project
network policy are intentionally outside this stack.

## Connect

```bash
pulumi stack output schedulerEndpoints
pulumi stack output apiTokenOut --show-secrets
vmon context add prod --server http://<sched-ip>:8100 --token <apiToken> --save-token
vmon run alpine -- echo hello   # placed on a nested-virt worker by the scheduler
```

Sandbox views carry `node` (worker id) and `endpoint` (worker URL); clients
may dial the worker directly for exec-heavy traffic — worker :8000 is open to
`allowedCidr` and authenticated by `workerTokenOut`.

## Rollout

`deploy/gcp/rollout.sh` builds vmon (cargo-zigbuild), discovers instances by
their `vibevmm-role` label, opens SSH to this host only for the duration (a
temporary firewall rule), and atomically swaps the binary with health-checked
rollback. Pass `--gcs-uri` to also refresh the MIG launch artifact.

## Teardown

`pulumi destroy`. The worker MIG's target size is `ignoreChanges`-guarded, so
a later `pulumi up` never fights the live autoscaler.
