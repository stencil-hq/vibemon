# Storage, object store, and volumes

Where a node keeps templates, sandbox disks, and paused state; what the optional object store (`--object-store`) adds; and how E2B volumes and bring-your-own-bucket mounts (`vmonS3Mounts`) are stored. Read this when sizing node disks, enabling portable pauses, or choosing a volume backend.

Flag reference: [`vmon serve` options](../reference/serve.md#object-store-and-volumes), [object store credentials](../reference/serve.md#object-store-credentials).

## Data directory

`vmon serve --data-dir` (`VMON_HOME`) defaults to `/var/lib/vmon` when running as root on Linux, else `~/.vmon`. Per-VM Unix sockets live separately under `--run-dir` (`VMON_RUN_DIR`, default `<runtime dir>/vmon` or `/tmp/vmon-<uid>`), one short directory per VM, to stay under the 104-byte macOS socket path limit.

```text
<data-dir>/
├── node-id
├── cluster-secret              generated when --cluster-secret is unset
├── raft/                       raft log and state (drivers)
│   └── journal/                history journal: sandbox events, build logs, webhook deliveries (30 days)
├── files/<templateID>/<hash>.tar   build file-context uploads (stored on nodes)
├── cache/
│   ├── oci/blobs/sha256/<hex>  pulled image layers, shared by builds
│   ├── steps/<layerKey>/       build step cache layers
│   └── context/<templateID>/<hash>/  extracted COPY contexts
├── builds/<buildID>/           template artifacts: rootfs, snapshot/, meta.json, build.log
├── sandboxes/<sandboxID>/      running sandbox: disk overlay, logs, metrics, metadata
├── paused/<sandboxID>/         paused sandbox: memory snapshot + disk overlay
├── volumes/<volumeID>/         node-local volumes (unless --volumes-dir)
├── assets/                     fallback guest kernel download
└── egress/                     egress interception CA (Linux)
```

| Path | Contents | Lifetime |
|---|---|---|
| `builds/<buildID>/` | `rootfs.ext4` (raw ext4 base), `snapshot/` (VMM memory snapshot with `startCmd` running), `meta.json`, `manifest.json`, `build.log`, `console-*.log` | Until the template is deleted and no running or paused sandbox uses it. A failed or cancelled build, or one cut off by a node restart, keeps only its logs |
| `cache/steps/<layerKey>/` | `rootfs.ext4` and `layer.json` of a cached build layer | At most 48 layers within `--step-cache-mb` (default 10% of the data directory's filesystem); least recently used evicted first |
| `sandboxes/<id>/` | `rootfs.img` + `rootfs.img.base`, `console.log`, `console.vmm.log`, `metrics.jsonl`, `meta.json` (0600), `snap/`, `net-policy.json` (user-net) | `console.log` and `metrics.jsonl` stay 7 days after the sandbox ends, then the directory is deleted |
| `paused/<id>/` | `snapshot/` (+ `base/` for a delta), `rootfs.img` + `rootfs.img.base`, `committed` | Until the next pause or kill |
| `paused/<id>.upload/` | Staging of a running object-store upload | Transient |

Other nodes fetch a build's artifacts from a node that holds it when they first create a sandbox from it; transfers send only data extents, so destinations stay sparse.

## Sandbox disks

A sandbox disk is a block-level copy-on-write overlay over the template's raw rootfs, independent of the host filesystem:

- `rootfs.img` is a sparse overlay: written 4 KiB clusters live at their own offset, everything else is read from the base. An allocation bitmap and footer sit past the disk end.
- `rootfs.img.base` is a hard link to the template's `rootfs.ext4`. Bases are never written. The link count keeps a base alive after its template is deleted.
- An overlay of an overlay (resume, creates from snapshot templates) clones the source's data and bitmap onto the same raw base, so chains never form. Clones reflink when the filesystem supports it (XFS, btrfs on Linux; `fclonefileat` on macOS), else copy only the written extents.
- Guest flushes skip the host fsync (the disk is private to its VMM). The node fdatasyncs a paused disk before marking the pause durable.

Local raw-base overlay operations do not copy the template rootfs, including on ext4. Remote resume and materializing a lazy disk can fetch and write the full disk. Hard links require `builds/`, `sandboxes/`, and `paused/` to be on one filesystem; they all live under the data directory.

| Platform | Disk overlay |
|---|---|
| Linux | Block overlay + hard-linked base; overlay clones reflink on XFS/btrfs |
| macOS | Block overlay + hard-linked base; clones via `fclonefileat` |

### Sizing

A template built from an image gets its rootfs as a sparse ext4 image of the building team's `max_disk_mb` limit (default 20480 MiB; set with [`vmon admin set-limits --max-disk-mb`](../reference/admin.md)). Its sandboxes see that size; host usage grows with written data only: the template's allocated blocks once, plus each sandbox's written clusters. Delta paused memory snapshots add the pages written since the fork (below). Nodes report the free space of the data directory in their capacity (`disk_free_mb`).

## Pause artifacts

A pause snapshots the VM and moves its state into `paused/<id>/`:

- **Memory**: `snapshot/`. When the sandbox has a local RAM base (for example, after a template fork or a non-lazy memory resume), the snapshot is a delta against that base (the memory file it was forked from, hard-linked as `base/`): only pages that differ from the base are stored, so the cost scales with memory written since the fork, not guest RAM. Lazily restored Linux memory has no local RAM base, so its next pause writes a full memory snapshot.
- **Disk**: the sandbox's `rootfs.img` overlay and its base link.
- **Commit**: the node fdatasyncs the overlay, fsyncs the directory entries, then creates `committed`. Restart recovery keeps a paused sandbox only with that marker, so memory and disk are always a durable pair.

A resume forks from `paused/<id>/snapshot` with a new overlay cloned from the paused disk. `pause` with `memory=false` keeps only the disk; the resume cold-boots it.

## Object store

`vmon serve --object-store` (`VMON_OBJECT_STORE`) points the cluster at an S3-compatible bucket. It enables portable paused sandboxes, template publishing, and object-store volumes. Configure the same store on every node and driver: nodes advertise the `object-store` feature, and drivers use the store for volumes, the volume-content API, and cleanup on kill.

| Form | Example |
|---|---|
| `s3://bucket[/prefix]` | `s3://vmon-prod/cluster-a` (AWS, or `VMON_S3_ENDPOINT`) |
| `https://endpoint/bucket[/prefix]` | `https://minio.internal:9000/vmon/cluster-a` |
| `http://endpoint/bucket[/prefix]` | Plain HTTP, accepted for the operator store |

The location cannot contain credentials, a query, or a fragment; `s3://` locations cannot carry a port.

| Variable | Purpose |
|---|---|
| `VMON_S3_ENDPOINT` | Endpoint (`http(s)://host[:port]`, no path) serving `s3://` locations instead of AWS |
| `VMON_S3_REGION`, `AWS_REGION`, `AWS_DEFAULT_REGION` | Signing region, first non-empty in the listed order; then EC2 metadata. Custom endpoints fall back to `us-east-1` if metadata is unavailable |
| `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY` (+ `AWS_SESSION_TOKEN`) | Static credentials |
| `VMON_S3_ACCESS_KEY` + `VMON_S3_SECRET_KEY` (+ `VMON_S3_SESSION_TOKEN`) | Static credentials, used when the `AWS_*` pair is unset |

Without static keys the node uses the ECS task role or the EC2 instance role and refreshes the credentials before they expire.

```sh
VMON_S3_ENDPOINT=https://minio.internal:9000 \
VMON_S3_ACCESS_KEY=vmon VMON_S3_SECRET_KEY=… \
vmon serve --object-store s3://vmon/cluster-a
```

Object layout under the store prefix:

| Prefix | Contents |
|---|---|
| `builds/<buildID>/` | Published template builds: every artifact file as packed sparse extents, `manifest.json` last |
| `paused/<sandboxID>/<uuid>/` | One generation of a paused sandbox: `meta.json` (without the envd token), `boot-init.json`, `rootfs.blk` + `rootfs.blk.json` (disk as a block image of 1 MiB zstd frames), `snapshot/…`, `base/…`, `manifest.json` last |
| `volumes/<volumeID>/` | Object-store volumes |

### Published templates

After every build commit (build ready, peer fetch, snapshot template, fork template) the holder publishes the build to `builds/<buildID>/` unless it is already there. A node that cannot fetch a build from a live peer fetches it from the store with the same verification, and the driver accepts creates for a build with no live holder when its published manifest exists. Templates therefore survive the loss of every node that held them. Deleting a template also deletes its published build.

### Portable paused sandboxes

After a pause commits, the node uploads one generation in the background. The RAM base is uploaded with it unless it is the template's own snapshot and that build is already published. Only after `manifest.json` is written does the sandbox's KV record point at the generation. A failed upload leaves the local pause usable; restart recovery re-uploads a committed pause that has no record. Kill deletes the generation and its record.

Resume prefers the node that holds the pause while it is alive. Otherwise the driver picks an `object-store` node of the same architecture:

- same `backend` and CPU baseline: memory and disk are restored, processes keep running;
- otherwise: rootfs-only cold boot on a same-arch node; guest processes are lost, the disk is kept.

Sandboxes with volumes on node-local storage stay pinned to that node. Without an object store, pauses stay node-local and a sandbox whose node is gone cannot be resumed.

### Lazy resume

`--lazy-resume` (`VMON_LAZY_RESUME`, default `true`) lets a node resume another node's paused sandbox without downloading it first. The node fetches only metadata, the snapshot state, and the block index, pins the `rootfs.blk` ETag, and boots with a remote disk: the VMM fetches each 1 MiB frame by HTTP range on first touch into a local sparse cache, and guest writes go to a private overlay.

| | Linux | macOS |
|---|---|---|
| Disk | Fetched on demand | Fetched on demand |
| Memory | Paged in on demand from a per-VM loopback page server | Downloaded before the fork |

The next pause materializes the full disk while the VM is paused, then deletes the lazy files and the consumed generation. Lazy preparation or boot failures fall back to the full download; later remote I/O failures do not. `--lazy-resume false` downloads the required paused state eagerly before boot; cold boots omit saved memory.

> [!WARNING]
> Lazy resume limits: the pre-signed block URL expires after 7 days, after which blocks touched for the first time fail with I/O errors; a node restart orphans a Linux page server, so the adopted VM dies on its next remote page fault; block fetches stall the VM's device worker while they run.

## Volumes

The E2B Volumes API (`/volumes`, volume-content API under `/volumecontent/{id}/…`) works on every deployment. Where a volume lives is fixed when it is created:

| Deployment | New volume | Mounting | Volume-content API |
|---|---|---|---|
| No object store | Directory `volumes/<id>/` (or `--volumes-dir`) on the node with the most free disk | Sandboxes are placed on that node; mounted over virtio-fs | Driver proxies to the owning node |
| `--volumes-dir` + `--shared-volumes` | Directory in the shared `--volumes-dir` | Any node | Driver proxies to an alive, non-draining node |
| `--object-store` | Prefix `volumes/<id>/` in the store; no node owns it | Any node with feature `object-store`, as a write-back S3 mount | Driver serves it directly against S3 |

`--shared-volumes` (`VMON_SHARED_VOLUMES`) declares that `--volumes-dir` (`VMON_VOLUMES_DIR`) is shared storage visible to every node at the same path; set `--volumes-dir` on every node and `--shared-volumes` on every node and driver. Volumes created before an object store was configured keep their node directory.

Volume mount paths must be absolute and clean, not `/`, and unique per sandbox. Node-local volumes in one sandbox must all live on the same node. A create with node-local volumes cold-boots the template rootfs instead of forking its memory snapshot, because template snapshots carry no shared-directory devices.

Deleting a volume removes its directory or its whole object-store prefix. Deleting a node-local volume whose owning node is gone drops only the record.

## Bring-your-own-bucket mounts

`vmonS3Mounts` is a vibemon extension on `POST /sandboxes` and `POST /v2/sandboxes` that mounts a user's bucket into the sandbox. The request shape is in [E2B compatibility](../reference/e2b-compat.md); this section covers the node side.

S3 mounts and object-store volumes are VMM remote virtio-fs devices backed by one host proxy per sandbox, listening on `<run-dir>/<id>-s3/fs<n>.sock`. The proxy talks to S3; credentials never enter the guest.

| Mode | Guest view | Writes |
|---|---|---|
| `ro` (default) | Read-only mount; only touched chunks are fetched | Rejected |
| `overlay` | Bucket as the lower layer of a guest overlayfs (`/var/lib/vmon/s3/<tag>/` on the sandbox disk) | Kept on the sandbox disk; survive pause, gone at kill |
| `writeback` | Read-write mount | Staged in `sandboxes/<id>/s3fs/`; `close`/`fsync` upload the whole file (multipart above 8 MiB) |

Operator-relevant behavior:

- **Limits**: at most 8 S3-backed mounts per sandbox, counting object-store volumes. Paths must not overlap each other or `volumeMounts`.
- **Caching**: each mount keeps a 32 MiB LRU of 1 MiB chunks keyed by ETag; attributes are cached ≤ 1 s (`writeback`) or 5 s (`ro`, `overlay`).
- **Consistency**: last writer wins per object; no cross-node locking. Directory renames are copy + delete, at most 10 000 keys (`EXDEV` above). Symlinks and hard links are `EPERM`.
- **Lifecycle**: pause flushes all pending uploads before the snapshot, so a resume on another node sees them. Kill and VMM exit flush for up to 5 minutes, then drop staging. A create with S3 mounts forks the template snapshot only when the snapshot was captured with the same mount devices, else cold-boots.
- **Credentials**: inline credentials are stored only encrypted in the cluster KV (and dropped from the record when the sandbox is killed); `credentialsSecret` names a team secret that is re-read on every resume and fork, so a rotated secret applies at the next resume. Credentials are never logged or returned. The node keeps the mount list with credentials at mode 0600 in the sandbox directory so a restarted node can re-serve adopted VMMs; staged writes not yet uploaded by the previous process are lost.
- **Anonymous access**: mounts without credentials send unsigned requests, never the node's identity.
- **Private endpoints**: a node refuses endpoints that resolve to loopback, private, link-local, CGNAT (`100.64.0.0/10`), IPv6 unique-local, unspecified, or multicast addresses, so users cannot reach the node's own network. `vmon serve --s3-mounts-allow-private` (`VMON_S3_MOUNTS_ALLOW_PRIVATE`) lifts this, for example for a MinIO on the node's network.
- **Egress**: mount traffic is made by the host and is not subject to the sandbox's egress policy.
