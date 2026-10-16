# vmon serve options

Every flag of `vmon serve`, grouped by topic. Every configuration flag can also be set through the listed `VMON_*` environment variable. When both are given, the flag wins. Behaviour that `vmon serve --help` leaves out comes from `src/serve.rs` and the node and driver crates. For how to run the server, see [Running vmon serve](../operate/serve.md).

```text
vmon serve [OPTIONS]
```

Boolean switches (`--log-json`, `--s3-mounts-allow-private`, `--shared-volumes`) take no value on the command line. Enable them through the environment with `true`, for example `VMON_LOG_JSON=true`. `--lazy-resume` is different: it always takes a value, `true` or `false`.

> [!NOTE]
> `--help` does not print the values of `VMON_CLUSTER_SECRET`, `VMON_MYSQL_URL`, and `VMON_ADMIN_TOKEN`.

## Roles and logging

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--role <ROLE>` | `VMON_ROLE` | `node,driver` | Roles to run, comma separated. `node` hosts microVMs. `driver` serves the E2B API and sandbox ingress and schedules work. At least one role is required. The driver role refuses to start without `--mysql-url`. |
| `--data-dir <DATA_DIR>` | `VMON_HOME` | see below | State directory. Created if missing. |
| `--run-dir <RUN_DIR>` | `VMON_RUN_DIR` | see below | Directory for per-VM Unix sockets (`api.sock`, `agent.sock`), one short subdirectory per VM. |
| `--log <LOG>` | `VMON_LOG` | `info,openraft=warn` | Log filter in `tracing` env-filter syntax, for example `info,vmon_node=debug`. An invalid filter aborts startup. |
| `--log-json` | `VMON_LOG_JSON` | off | Emit JSON log lines instead of text. Logs always go to stderr. |
| `-h`, `--help` | none | | Print help (`-h` prints a summary). |

Default directories:

- `--data-dir` is `/var/lib/vmon` when `vmon` runs as root (effective uid 0) on Linux. Otherwise it is `$HOME/.vmon`, falling back to `/var/lib/vmon` when `HOME` is unset. `vmon admin` uses the same default when it looks for `cluster-secret`.
- `--run-dir` is `$XDG_RUNTIME_DIR/vmon` when `XDG_RUNTIME_DIR` is set and not empty. Otherwise it is `/tmp/vmon-<uid>`. Socket paths must stay under the 104-byte macOS `sun_path` limit, so keep this path short.

The files kept in both directories are listed in [Data and run directories](../operate/serve.md#data-and-run-directories).

## Cluster and gossip

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--cluster-secret <CLUSTER_SECRET>` | `VMON_CLUSTER_SECRET` | `<data-dir>/cluster-secret` | Shared cluster secret. Must not be empty. See below. |
| `--cluster-listen <CLUSTER_LISTEN>` | `VMON_CLUSTER_LISTEN` | `0.0.0.0:7947` | Internal HTTP listener: raft and KV (`/cluster/v1/*`, drivers) and the node API and guest tunnels (`/node/v1/*`, nodes). Every request needs `Authorization: Bearer <cluster secret>`. |
| `--cluster-advertise <CLUSTER_ADVERTISE>` | `VMON_CLUSTER_ADVERTISE` | `http://<advertise-ip>:<cluster-listen port>` | URL that peers use to reach `--cluster-listen`. Published in gossip metadata. |
| `--gossip-listen <GOSSIP_LISTEN>` | `VMON_GOSSIP_LISTEN` | `0.0.0.0:7946` | Gossip listener. UDP carries probes and updates, TCP on the same port carries push-pull state exchange. |
| `--gossip-advertise <GOSSIP_ADVERTISE>` | `VMON_GOSSIP_ADVERTISE` | `<advertise-ip>:<gossip-listen port>` | Address that peers use to reach `--gossip-listen`. Must be an `ip:port` socket address, not a host name. |
| `--join <JOIN>` | `VMON_JOIN` | none | Gossip seeds as `host:port`, comma separated. Host names are resolved through DNS. If no seed answers at startup, the process logs a warning and retries every 2 s in the background. |
| `--bootstrap-expect <BOOTSTRAP_EXPECT>` | `VMON_BOOTSTRAP_EXPECT` | `1` | Drivers only: how many alive, uninitialized drivers gossip must show before raft bootstraps. The driver with the smallest node ID among them then initializes raft with exactly those voters. |
| `--raft-reap-after <RAFT_REAP_AFTER>` | `VMON_RAFT_REAP_AFTER` | `600` | Seconds a driver can stay `Dead` or `Left` in gossip before the leader removes it from the raft voters. The voter set never drops below 1. |

**Cluster secret.** When `--cluster-secret` is unset, `vmon serve` reads `<data-dir>/cluster-secret`. If that file does not exist, it generates 32 random bytes and writes them hex-encoded (64 characters) to the file with mode `0600`. It then logs a warning to share the secret with every process of the cluster. The secret authenticates cluster HTTP (bearer token) and gossip (HMAC-SHA256 keyed by `HKDF(secret, "vmon-gossip")`). The keys for JWTs, envd and traffic tokens, signed upload URLs, and encryption of stored data (API keys, env vars, secrets, webhook secrets) are all derived from it as well. Changing the secret invalidates the stored ciphertexts. For how to share it, see [Cluster secret](../operate/serve.md#cluster-secret).

**Advertise address.** `<advertise-ip>` is the IP of `--cluster-listen` when that IP is specific. When `--cluster-listen` binds an unspecified address (`0.0.0.0`), vmon uses the host's outbound IPv4 address: the source address the kernel picks for a route to `192.0.2.1`. If no route exists, it uses `127.0.0.1`. The default `--gossip-advertise` and the default plain-HTTP `--public-url` use the same IP. On hosts with several interfaces, or behind NAT, set the advertise flags explicitly.

**Node identity.** On first start each process writes a random 64-bit ID to `<data-dir>/node-id` and keeps it from then on. This ID is the gossip member ID, the raft node ID, and the `id` shown by `GET /nodes`. A restarted node therefore keeps its identity only if it reuses the same data directory.

## Public listener and TLS

These flags apply to the driver role. Node-only processes do not open a public listener.

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--listen <LISTEN>` | `VMON_LISTEN` | `0.0.0.0:3000` | Public listener for the E2B API and sandbox ingress. Plain HTTP/1.1 and h2c (prior knowledge). |
| `--tls-listen <TLS_LISTEN>` | `VMON_TLS_LISTEN` | off | Optional HTTPS listener serving the same router. ALPN offers `h2` and `http/1.1`. Requires `--tls-cert` and `--tls-key`. |
| `--tls-cert <TLS_CERT>` | `VMON_TLS_CERT` | none | PEM certificate chain, a wildcard for `*.<domain>` plus `api.<domain>`. Read once when the listener starts. |
| `--tls-key <TLS_KEY>` | `VMON_TLS_KEY` | none | PEM private key. |
| `--domain <DOMAIN>` | `VMON_DOMAIN` | `localhost` | Sandbox domain. Sandboxes are served at `<port>-<id>.<domain>`, and sandbox API responses return it as `domain`. |
| `--public-url <PUBLIC_URL>` | `VMON_PUBLIC_URL` | see below | Public base URL of this driver. |

**Public URL default.** With `--tls-listen` set, the default is `https://api.<domain>`, with no port. Without it, the default is `http://<advertise-ip>:<listen port>`. The driver uses this URL in signed template file upload URLs (`PUT /templates/{id}/files/{hash}/upload?expires=…&signature=…`) and in the `tokenEndpoint` and `revokeEndpoint` it hands to `e2b auth login`. It is also published in gossip. If clients reach the TLS listener on a port other than 443, or through a proxy, set it explicitly.

Routing rules, certificates, and DNS are covered in [Domains, TLS, and ingress](../operate/ingress.md).

## Database and admin

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--mysql-url <MYSQL_URL>` | `VMON_MYSQL_URL` | none | Database URL (driver), for example `mysql://user:pass@host:3306/vmon`. Required by the driver role. The database must already exist. The driver applies its embedded migrations at startup and keeps a pool of up to 32 connections. |
| `--admin-token <ADMIN_TOKEN>` | `VMON_ADMIN_TOKEN` | none | Token accepted in `X-Admin-Token` on the admin endpoints (`/nodes`, `/admin/*`). When it is unset, every admin request gets 401. |

Admin workflows are covered in [Administration](../operate/admin.md).

## Guest assets

These flags apply to the node role. Both assets are resolved once, at node startup.

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--kernel <KERNEL>` | `VMON_KERNEL` | see below | Guest kernel image. When given, the file must exist. |
| `--agent <AGENT>` | `VMON_AGENT` | see below | Static guest agent binary, installed in guests as `/usr/bin/envd`. Must be a static ELF (no dynamic loader) for the guest architecture. |

Kernel lookup when `--kernel` is unset:

1. `vmon-kernel-<arch>` in the directory of the `vmon` binary (release assets).
2. `target/kernel/Image-aarch64` or `target/kernel/bzImage-x86_64` under the nearest ancestor of the binary's directory (a dev tree after `just kernel`).
3. A pinned cloud-hypervisor kernel, downloaded into `<data-dir>/assets/` and verified by SHA-256. vmon logs a warning when it falls back to this kernel, because the kernel lacks netfilter (nftables/iptables, Docker), IPv6, bridge, and NFS.

Agent lookup when `--agent` is unset: `vmon-agent-<arch>` next to the `vmon` binary, then `../../target/test-assets/vmon-agent-<arch>` relative to it (`just agent-musl`). If neither exists, the node fails to start. See [Guest kernel](../operate/kernel.md).

## Networking and egress

These flags apply to the node role.

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--net <NET>` | `VMON_NET` | `auto` | Guest networking. `auto`: TAP on Linux, user-mode NAT elsewhere. `tap`: host TAP + NAT (Linux only, needs root or `CAP_NET_ADMIN`; otherwise startup fails). `user`: user-mode NAT inside the VMM (not supported on Linux; startup fails there). With `auto` on Linux but without `CAP_NET_ADMIN`, the node starts, logs a warning, and rejects sandbox creates. |
| `--net-pool <NET_POOL>` | `VMON_NET_POOL` | `172.20.0.0/16` | Address pool for Linux TAP leases. Each sandbox gets a /30. |
| `--vmm-user <VMM_USER>` | `VMON_VMM_USER` | none | `uid:gid` (numeric) that VMM processes drop to. Linux root only. See below. |
| `--egress-internal-allow <EGRESS_INTERNAL_ALLOW>` | `VMON_EGRESS_INTERNAL_ALLOW` | none | CIDRs that sandboxes may reach despite the always-denied private ranges, comma separated. |

**VMM privilege drop.** The node launches each VMM as `vmon vmm`:

- **Root on Linux, `--vmm-user` set:** the VMM gets `--sandbox-uid`/`--sandbox-gid`. It drops to that uid and gid and applies its Landlock and seccomp filters. TAP devices are created owned by that uid so the unprivileged VMM can open them.
- **Root on Linux, `--vmm-user` unset:** VMMs are started with `--no-sandbox`. They keep root and run without the Landlock and seccomp filters.
- **Not root:** VMMs run as the invoking user, with the default filters on Linux. macOS has no host process sandbox.

Policy semantics per platform are covered in [Networking and egress](../operate/networking.md).

## Templates and registries

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--max-builds <MAX_BUILDS>` | `VMON_MAX_BUILDS` | `0` | Concurrent template builds on this node. `0` means 2. Further builds wait for a slot. |
| `--step-cache-mb <STEP_CACHE_MB>` | `VMON_STEP_CACHE_MB` | `0` | Disk the build step cache (`cache/steps/`) may occupy, in MiB, counting allocated blocks. Least recently used layers are evicted beyond it (and beyond 48 layers). `0` means 10% of the filesystem holding the data directory. |
| `--registry-mirror <REGISTRY_MIRROR>` | `VMON_REGISTRY_MIRROR` | none | Docker Hub pull-through mirror, for example `https://mirror.gcr.io`. Tried first for anonymous `docker.io` template base images, then falls back to Docker Hub. |

See [Templates](../concepts/templates.md).

## Object store and volumes

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--object-store <OBJECT_STORE>` | `VMON_OBJECT_STORE` | none | Operator object store, as `s3://bucket/prefix` or `https://endpoint/bucket/prefix` (`http://` is accepted too). Used for portable paused sandboxes, published template builds, and S3 volumes. Configured once at startup and shared by the node and driver roles of the process. A configuration error aborts startup. An empty value means unset. Nodes with a store advertise the `object-store` feature. |
| `--lazy-resume <LAZY_RESUME>` | `VMON_LAZY_RESUME` | `true` | Resume other nodes' paused sandboxes lazily from the object store: rootfs blocks on demand, and on Linux guest memory too. `false` downloads the required paused state eagerly before boot; cold boots omit saved memory. Lazy preparation or boot failures fall back to the full download; later remote I/O failures do not. |
| `--s3-mounts-allow-private` | `VMON_S3_MOUNTS_ALLOW_PRIVATE` | off | Let `vmonS3Mounts` endpoints resolve to loopback, private, or link-local addresses. When off, these are refused, because the node would otherwise reach its own network on behalf of users. |
| `--volumes-dir <VOLUMES_DIR>` | `VMON_VOLUMES_DIR` | `<data-dir>/volumes` | Directory for node-local volumes. |
| `--shared-volumes` | `VMON_SHARED_VOLUMES` | off | The volume directory is shared storage visible to every node, so volumes do not pin sandboxes to one node. Set it on drivers and nodes alike. |

### Object store credentials

With `--object-store` set, these environment variables configure the S3 client. They are not flags.

| Variable | Use |
|---|---|
| `VMON_S3_ENDPOINT` | Endpoint (`scheme://host[:port]`, no path) serving an `s3://` location instead of AWS. Setting it together with an `http(s)://` location is an error. |
| `VMON_S3_REGION`, `AWS_REGION`, `AWS_DEFAULT_REGION` | Signing region, first non-empty wins. If none is set, vmon asks EC2 instance metadata. For custom endpoints (`VMON_S3_ENDPOINT` or an `http(s)://` location), it falls back to `us-east-1` when metadata is unavailable. |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` | Static credentials, checked first. The key ID and secret key must be set together. |
| `VMON_S3_ACCESS_KEY`, `VMON_S3_SECRET_KEY`, `VMON_S3_SESSION_TOKEN` | Static credentials, used when the `AWS_*` keys are unset. The access key and secret key must be set together. |
| `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`, `AWS_CONTAINER_CREDENTIALS_FULL_URI`, `AWS_CONTAINER_AUTHORIZATION_TOKEN`, `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE` | ECS task role credentials, used when no static keys are set. |
| `AWS_EC2_METADATA_SERVICE_ENDPOINT`, `AWS_EC2_METADATA_DISABLED` | EC2 instance role (IMDS), the last resort for credentials and region. `AWS_EC2_METADATA_DISABLED=true` turns it off. |

Role credentials are refreshed before they expire. Data layout and portability are covered in [Storage, object store, and volumes](../operate/storage.md).

## Capacity and memory

These flags apply to the node role.

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--boot-concurrency <BOOT_CONCURRENCY>` | `VMON_BOOT_CONCURRENCY` | `0` | Concurrent VM boots. `0` means 4 × host CPUs. On Linux it also sets the number of pre-created TAPs (one per boot slot, at most 32). |
| `--max-vcpus <MAX_VCPUS>` | `VMON_MAX_VCPUS` | host CPUs | vCPUs offered to sandboxes, as advertised in gossip. Placement admits allocated vCPUs up to 4× this value. |
| `--max-memory-mb <MAX_MEMORY_MB>` | `VMON_MAX_MEMORY_MB` | host memory | Memory offered to sandboxes in MiB. Placement admits allocated guest memory up to 2× this value. |
| `--hugepage-pool-mb <HUGEPAGE_POOL_MB>` | `VMON_HUGEPAGE_POOL_MB` | `0` | Linux, node role: at startup, write `<value> / 2` to `/proc/sys/vm/nr_hugepages`, which reserves that many MiB of 2 MiB hugetlb pages for sandbox RAM restored from snapshots (create, resume, fork). `0` leaves the host setting alone. |

The hugepage write sets the absolute size of the host pool; it does not add to the current size. It needs root. The kernel can grant fewer pages than requested when contiguous memory is short. vmon logs the result either way. On other platforms a non-zero value logs a warning and is otherwise ignored. For the trade-offs, see [Hugepages](../operate/serve.md#hugepages).
