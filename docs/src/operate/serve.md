# Running vmon serve

`vmon serve` is the long-running server process. This page covers what each role needs, which ports must be reachable, what the server keeps on disk, and how to run it on a production host. Every flag is listed in [vmon serve options](../reference/serve.md).

## Roles and topologies

A `vmon serve` process runs one or both roles, selected with `--role` (default `node,driver`):

| Role | Does | Needs |
|---|---|---|
| `driver` | E2B REST API, sandbox ingress, auth, placement, lifecycle and build supervision. Drivers are the raft voters of the cluster KV. | `--mysql-url` (the driver refuses to start without it). Optional: `--admin-token` for the admin endpoints, and `--tls-*` for HTTPS. |
| `node` | Runs microVMs (`vmon vmm` children), builds templates, stores artifacts and volumes, serves the node API and guest tunnels. | A guest kernel and a static guest agent, the hypervisor (`/dev/kvm` on Linux, Hypervisor.framework on macOS), and TAP privileges on Linux. Template builds also need e2fsprogs (`mkfs.ext4`, `e2fsck`, `resize2fs`). On macOS vmon also looks for these in the Homebrew `e2fsprogs` keg. |

Both topologies use the same binary and flags:

- **Monolithic:** one `vmon serve --role node,driver` process with a MySQL database. No gossip seeds are needed.
- **Distributed:** any number of `--role driver` and `--role node` processes (or mixed ones) that share one MySQL database and one cluster secret, and find each other through gossip seeds (`--join`). See [Clusters](cluster.md).

The node role runs on Linux with KVM (x86_64 and aarch64 guests) and on macOS 15+ on Apple Silicon with HVF (aarch64 guests). The macOS binary must be ad-hoc signed with `hvf.entitlements`. The driver role runs on both. Windows builds contain only `vmon vmm`. See [Installation](../get-started/installation.md).

## Listeners

| Listener | Default | Opened by | Must be reachable by |
|---|---|---|---|
| `--listen` | `0.0.0.0:3000` | drivers | SDK and CLI clients, and browsers that open sandbox URLs. Plain HTTP/1.1 and h2c. |
| `--tls-listen` | off | drivers | Same clients over HTTPS (`h2`/`http/1.1` via ALPN). See [Domains, TLS, and ingress](ingress.md). |
| `--cluster-listen` | `0.0.0.0:7947` | every process | Drivers, which call other drivers for raft and KV and call nodes for the node API and tunnels. Nodes, which forward KV calls to drivers and fetch artifacts and build file contexts from other nodes. |
| `--gossip-listen` | `0.0.0.0:7946` | every process | Every other process, on both UDP and TCP. |

Peers dial the advertised addresses (`--cluster-advertise`, `--gossip-advertise`), not the bind addresses. When unset, both are derived from the IP of `--cluster-listen`, or from the host's outbound IPv4 address when it binds `0.0.0.0` (details in [Cluster and gossip](../reference/serve.md#cluster-and-gossip)).

Cluster HTTP requires `Authorization: Bearer <cluster secret>`. Gossip packets carry an HMAC tag keyed from the same secret, and unauthenticated packets are dropped. Even so, keep both internal listeners on a private network, because the node API controls every VM.

`GET /health` on the public listener needs no authentication. It returns 204 while MySQL answers and the local KV replica is readable, so load balancers can use it as a health check.

## Data and run directories

`--data-dir` defaults to `/var/lib/vmon` for root on Linux and to `~/.vmon` otherwise. It holds:

```text
node-id                     random 64-bit member ID, written on first start
cluster-secret              generated secret (0600) when --cluster-secret is unset
raft/                       drivers: raft log, vote, snapshots (fsync'd)
assets/                     nodes: pinned fallback kernel, when downloaded
egress/                     Linux nodes: node-local MITM CA for egress header rules
files/<templateID>/<hash>.tar   nodes: build file-context archives
cache/oci/                  nodes: pulled image layers
cache/steps/, cache/context/    nodes: build step cache, extracted COPY contexts
builds/<buildID>/           nodes: template artifacts (rootfs, snapshot, logs)
sandboxes/<sandboxID>/      nodes: disk overlay, console and metrics logs, live snapshot dir
paused/<sandboxID>/         nodes: paused sandbox state
volumes/<volumeID>/         nodes: volume contents (unless --volumes-dir)
```

Keep the data directory across restarts. Its `node-id` is the process's identity in gossip, raft, and `GET /nodes`, and a restarted node adopts its running sandboxes only from the same directory. Node free disk is measured on the data directory. For the storage layout of disks, pauses, and volumes, see [Storage, object store, and volumes](storage.md).

`--run-dir` holds one short subdirectory per VM with its `api.sock` and `agent.sock`. It defaults to `$XDG_RUNTIME_DIR/vmon`, or to `/tmp/vmon-<uid>`. The path must stay short because of the 104-byte macOS `sun_path` limit.

## Guest kernel and agent

At startup the node resolves the guest kernel and the guest agent:

- **Kernel:** `--kernel`, else `vmon-kernel-<arch>` next to the `vmon` binary (release assets), else `target/kernel/` of a dev tree (`just kernel`). As a last resort the node downloads a pinned cloud-hypervisor kernel into `<data-dir>/assets/`. That kernel lacks netfilter, IPv6, bridge, and NFS, which breaks Docker-in-sandbox and templates that rely on those features, so vmon logs a warning when it falls back to it.
- **Agent:** `--agent`, else `vmon-agent-<arch>` next to the binary, else `target/test-assets/` of a dev tree (`just agent-musl`). It must be a static ELF for the guest architecture. If no agent is found, the node does not start.

Keep the release assets next to `vmon`, or pass both flags explicitly. See [Guest kernel](kernel.md).

## Cluster secret

The cluster secret authenticates cluster HTTP and gossip. It is also the root of every derived key: JWTs, envd and traffic access tokens, signed upload URLs, and the AES key for API keys, env vars, secrets, and webhook secrets stored in MySQL.

- When `--cluster-secret` (`VMON_CLUSTER_SECRET`) is unset, `vmon serve` uses `<data-dir>/cluster-secret`, and generates the file on first start (64 hex characters, mode `0600`) with a warning.
- Every process in a cluster must use the same secret. Generate it once and distribute it, for example by copying the first process's `cluster-secret` file into `VMON_CLUSTER_SECRET` for the others.
- `vmon admin` needs the same secret to encrypt the API keys it creates. It reads `--cluster-secret`, or else `<data-dir>/cluster-secret`. See [Administration](admin.md).
- Changing the secret invalidates stored ciphertexts, JWTs, and derived envd and traffic tokens; database-backed refresh tokens are not derived from it.

> [!TIP]
> Pass secrets through `VMON_CLUSTER_SECRET`, `VMON_MYSQL_URL`, and `VMON_ADMIN_TOKEN` instead of flags. `--help` hides these values, but command-line arguments show up in process listings.

## Logging

Logs go to stderr. `--log` (`VMON_LOG`) takes a `tracing` env-filter, by default `info,openraft=warn`, and an invalid filter aborts startup. `--log-json` (`VMON_LOG_JSON=true`) switches to one JSON object per line.

```sh
VMON_LOG='info,openraft=warn,vmon_driver=debug' vmon serve --log-json
```

Each sandbox's guest console and VMM log are kept under `sandboxes/<id>/` in the data directory, not in the server log.

## Shutdown and restart

`vmon serve` installs its SIGINT and SIGTERM handlers before it starts the roles, so a signal during startup still shuts down cleanly. On either signal:

1. The listeners stop accepting. In-flight public connections get up to 10 s to drain.
2. The node role stops its background tasks and removes idle pre-created TAPs. Sandbox VMMs keep running: they live in their own process group, and the next start on the same data directory adopts them.
3. Raft shuts down, and the process leaves gossip (state `Left`), waiting up to 2 s for the announcement.

If a listener fails, for example because its port is taken, the process exits with the error without running the graceful node and cluster shutdown sequence.

A node restarted with SIGTERM can adopt its surviving VMMs when it comes back within the leader's 60 s grace period for `Left` nodes. On Linux, a sandbox with lazily restored memory loses its node-local page server and fails its next remote page fault after restart. A node that dies without leaving (SIGKILL, crash, network loss) goes `Dead`, and its sandboxes are marked killed. See [Failure handling](cluster.md#failure-handling).

## Linux privileges

- **KVM:** VMM processes need read-write access to `/dev/kvm`, including the uid:gid selected by `--vmm-user`; they open it after dropping privileges.
- **TAP networking** (`--net tap`, the Linux default via `auto`) needs root or `CAP_NET_ADMIN`. The node creates TAP devices with `ip` and installs NAT and egress rules with `iptables`, `iptables-restore`, `ip6tables`, and `ip6tables-restore`. With `--net tap` and no such privilege, startup fails. With `auto`, the node starts but rejects creates. See [Networking and egress](networking.md).
- **Hugepages:** `--hugepage-pool-mb` writes `/proc/sys/vm/nr_hugepages`, which needs root.
- **VMM privilege drop:** when `vmon serve` runs as root, set `--vmm-user <uid>:<gid>`. VMMs then drop to that uid and gid and run under their Landlock and seccomp filters. The node makes each VM's run directory, disk directory, and snapshot directory, as well as its TAP device, accessible to that user. Without `--vmm-user`, a root node starts VMMs with `--no-sandbox`, so they keep root and run without filters.

## Capacity

A node advertises its capacity in gossip, and drivers place sandboxes from it:

| Setting | Effect |
|---|---|
| `--max-vcpus` (default: host CPUs) | Advertised vCPUs. Placement admits allocated vCPUs up to 4× this value. |
| `--max-memory-mb` (default: host memory) | Advertised memory. Placement admits allocated guest memory up to 2× this value, because forks of a template share memory copy-on-write. |
| Free disk | A node with less than 512 MiB free on its data directory gets no new sandboxes. |
| `--boot-concurrency` (default `0` = 4 × CPUs) | Concurrent VM boots. Also sets the size of the pre-created TAP pool on Linux (one per boot slot, at most 32). |
| `--max-builds` (default `0` = 2) | Concurrent template builds. Further builds queue. |

When no node has room, creates fail with 503 `sandbox_capacity_unavailable`. When no node matches the request's architecture, features, or pin, they fail with 503 `sandbox_no_compatible_node`. Per-team quotas are separate; see [Authentication and teams](../concepts/auth.md#limits). Lower `--max-vcpus` and `--max-memory-mb` to reserve host resources for other workloads.

## Hugepages

On Linux, sandbox RAM restored from a snapshot (create, resume, fork) can be backed by 2 MiB hugetlb pages. This requires room in the host hugetlb pool, userfaultfd `WP_ASYNC` and `PAGEMAP_SCAN` support (6.7+), permission to create userfaultfd, readable pagemap state, and a 2 MiB-aligned memory layout. If these prerequisites are unavailable, restore falls back to 4 KiB pages. Pages are filled on first touch from the memory file, and KVM maps them with 2 MiB stage-2 entries.

`--hugepage-pool-mb N` sets `vm.nr_hugepages` to `N / 2` pages when the node starts. This sets the pool size, not an increment. The kernel can grant fewer pages on a fragmented host, and vmon logs what it got. The default `0` leaves the host setting alone, so a pool configured by other means is used as is.

| | Hugepages | 4 KiB pages (default without a pool) |
|---|---|---|
| First command after create | ~4.5 ms | ~23 ms |
| Resume to first command | ~25 ms faster | baseline |
| Fork | ~+150 ms (fills every unpopulated chunk first) | baseline |
| Pause | ~+12 ms (2 MiB-granular dirty tracking) | baseline |
| Memory sharing | none: each sandbox reserves its whole RAM in huge pages | forks share the template's page cache |

The latency figures were measured under nested KVM (CHANGELOG, ARCHITECTURE.md §8.2).

Size the pool for the total RAM of the restored sandboxes you expect to run at once. A VM that the pool cannot cover falls back to 4 KiB pages. Surplus pages (`vm.nr_overcommit_hugepages`) also work, but can stall a create for seconds in direct compaction on a fragmented host.

## Example: monolithic Linux host

One root process with both roles, HTTPS ingress on a wildcard domain, internal listeners on loopback, and VMMs dropped to an unprivileged uid. Place `vmon-kernel-<arch>` and `vmon-agent-<arch>` next to `vmon`, create the `vmon` database, and point `*.sbx.example.com` and `api.sbx.example.com` at the host.

```sh
export VMON_MYSQL_URL='mysql://vmon:<password>@db.internal:3306/vmon'
export VMON_ADMIN_TOKEN='<random admin token>'

vmon serve \
  --role node,driver \
  --data-dir /var/lib/vmon \
  --domain sbx.example.com \
  --listen 127.0.0.1:3000 \
  --tls-listen 0.0.0.0:443 \
  --tls-cert /etc/vmon/tls/fullchain.pem \
  --tls-key /etc/vmon/tls/privkey.pem \
  --cluster-listen 127.0.0.1:7947 \
  --gossip-listen 127.0.0.1:7946 \
  --vmm-user 64000:64000 \
  --hugepage-pool-mb 32768 \
  --registry-mirror https://mirror.gcr.io \
  --log-json
```

On first start this generates `/var/lib/vmon/cluster-secret`. Run `vmon admin` on the same host (it reads that file by default) to create the first team. With `--tls-listen`, `--public-url` defaults to `https://api.sbx.example.com`. Clients need only `E2B_API_KEY` and `E2B_DOMAIN=sbx.example.com`; see [Connecting SDKs and the CLI](../get-started/clients.md).
