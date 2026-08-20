# vibevmm orchestration layer on AWS

Pulumi (TypeScript, bun) stack deploying the `vmon` v2 orchestration layer:

```
clients ──▶ sched EC2 (vmon sched, EIP) ──direct gRPC──▶ worker ASG (vmon serve, *.metal)
                 │ in-memory worker table                        │ heartbeats (self-expiring key + stream)
                 └───────────── stream follow ──────▶ state VM ◀─┘
                                              (Redis :6379 + Postgres :5432)
```

Deliberately cheap: **no RDS, no ElastiCache, no NAT gateways, no NLB**. One
small VM runs both Redis (the orch state bus — reconstructible cache, not a
source of truth) and Postgres (vmond's cluster substrate). Redundancy for the
state box is explicitly out of scope; losing it degrades scheduling freshness
and durable metadata, not running sandboxes.

The one thing you cannot cheap out on: **workers must be `*.metal` instances**
— EC2 exposes `/dev/kvm` only on bare metal. That is the dominant line item;
size `workerMin`/`workerMax` accordingly.

## Autoscaling (the part that actually moves)

`vmon sched`'s leader computes desired capacity (HPA-like, memory-utilization
target) and drives the worker ASG through hooks installed by this stack:

- `scale-up.sh` → `aws autoscaling set-desired-capacity $VMON_SCALE_DESIRED`
- `scale-down.sh` → for each `$VMON_IDLE_WIDS` entry (workers that are drained
  **and** empty): `terminate-instance-in-auto-scaling-group
  --should-decrement-desired-capacity`

Worker ids are EC2 instance ids (`VMON_ORCH_ID` = instance-id), so the
terminate mapping is direct. The ASG has scale-in protection enabled and
AZRebalance suspended: AWS never picks victims — the scheduler drains a worker
(placement stops), waits for it to empty, then terminates it by id. A drained
worker that never empties is terminated only after its sandboxes finish; if the
fleet stays over target it is re-selected on a later tick. Sandboxes on a
worker that dies anyway are marked `lost` by the controller.

## Deploy

Prerequisites: `pulumi`, `bun`, AWS credentials, and a static musl `vmon`
binary reachable over HTTPS (see `release.yml` artifacts).

```bash
cd deploy/aws
bun install
pulumi stack init prod
pulumi config set aws:region us-east-1
pulumi config set binaryUrl  https://…/vmon-aarch64-unknown-linux-musl
pulumi config set assetsUrl  https://…/vmon-assets-aarch64.tar.gz   # optional: kernel + agent
pulumi config set allowedCidr  203.0.113.7/32                       # your egress IP; default 0.0.0.0/0
pulumi config set workerMin 1
pulumi config set workerMax 4
pulumi up
```

| Key | Default | Meaning |
|---|---|---|
| `binaryUrl` | — (required) | musl `vmon` binary URL, must match `arch` |
| `assetsUrl` | none | tarball extracted to `/var/lib/vmon/assets` |
| `arch` | `arm64` | `arm64` or `x86_64`; picks AMI + instance-type defaults |
| `workerMin` / `workerMax` | 1 / 4 | ASG bounds; the vmon autoscaler moves desired within them |
| `workerInstanceType` | `c7g.metal` / `c5.metal` | **must be metal** |
| `schedulerCount` | 1 | scheduler instances (each gets an EIP); N×M needs no LB |
| `schedulerInstanceType` | `t4g.small` / `t3.small` | |
| `stateInstanceType` | `t4g.small` / `t3.small` | Redis + Postgres box |
| `maxSandboxesPerWorker` | 0 | worker admission cap (0 = memory-bound) |
| `netSlots` | 256 | preallocated TAP/network slots per worker (0 disables pooling) |
| `targetUtil` | 0.7 | autoscaler target memory utilization |
| `allowedCidr` | `0.0.0.0/0` | who may reach sched :8100 and worker :8000 |

## Connect

```bash
pulumi stack output schedulerEndpoints
pulumi stack output apiTokenOut --show-secrets
vmon context add prod --server http://<sched-eip>:8100 --token <apiToken> --save-token
vmon run alpine -- echo hello   # placed on a metal worker by the scheduler
```

Sandbox views carry `node` (worker id) and `endpoint` (worker URL); clients
may dial the worker directly for exec-heavy traffic — worker :8000 is open to
`allowedCidr` and authenticated by `workerTokenOut`.

## Teardown

`pulumi destroy`. The worker ASG's desired capacity is `ignoreChanges`-guarded,
so a later `pulumi up` never fights the live autoscaler.
