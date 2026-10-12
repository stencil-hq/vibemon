# Vibemon

Vibemon (`vmon`) is a KVM/HVF microVM platform that speaks the E2B API. Point
the official E2B SDKs (`e2b`, `@e2b/code-interpreter` / `e2b_code_interpreter`,
`@e2b/desktop` / `e2b_desktop`) or the `e2b` CLI at it and they work unmodified.

One Rust binary, three entry points:

| Command | What it does |
|---|---|
| `vmon serve --role node,driver` | Runs the **node** role (hosts microVMs), the **driver** role (E2B REST API, sandbox ingress, scheduling), or both. |
| `vmon vmm …` | The per-VM monitor (KVM / Hypervisor.framework / WHP). Spawned by nodes, never by hand. |
| `vmon admin …` | Teams, users, API keys, limits, system teams (talks to MySQL directly). |

```mermaid
graph LR
  SDK[E2B SDKs / CLI] -->|HTTPS api.domain, port-id.domain| D[driver]
  D <-->|gossip + raft KV| D2[driver …]
  D -->|SQL| M[(MySQL)]
  D -->|node API, tunnels| N[node]
  N -->|spawn| V[vmon vmm] --> G[guest: systemd + envd]
```

- **Monolithic**: one process with both roles.
- **Distributed**: any number of driver and node processes. They discover each other through
  a gossip memberlist (`--join`). Drivers form a raft KV for live cluster
  state (sandbox routes, admission, locks), and MySQL holds durable state.

Design: [`ARCHITECTURE.md`](ARCHITECTURE.md). E2B coverage, operation by operation:
[`docs/e2b-compat.md`](docs/e2b-compat.md). Running a cluster:
[`docs/cluster.md`](docs/cluster.md).

## Quickstart (monolithic, macOS Apple Silicon or Linux/KVM)

```sh
just mysql                       # MySQL 8.4 in Docker on :13306
just serve                       # builds vmon + the guest agent, serves the API on 127.0.0.1:3000
vmon admin --mysql-url mysql://root:vmon@127.0.0.1:13306/vmon create-team dev   # prints an e2b_… key
just templates base              # builds the default `base` template as a public system template
```

```python
# E2B_API_KEY=e2b_… E2B_DOMAIN=localhost E2B_API_URL=http://127.0.0.1:3000 \
# E2B_SANDBOX_URL=http://127.0.0.1:3000
from e2b import Sandbox

sbx = Sandbox.create()                              # forks the template snapshot
print(sbx.commands.run("uname -a").stdout)
sbx.files.write("/home/user/hello.txt", "hi")
sbx.pause(); sbx = Sandbox.connect(sbx.sandbox_id)  # snapshot to disk, then resume
sbx.kill()
```

`E2B_SANDBOX_URL` sends sandbox traffic to the API listener, which routes it
by the `E2b-Sandbox-*` headers. In production, run `--tls-listen` with a
wildcard certificate for `*.<domain>` and `api.<domain>`. The SDKs then use
`https://<port>-<sandboxId>.<domain>` directly and need only `E2B_DOMAIN`.

## Templates

- **Building:** templates are built with the E2B v2 `Template` builder, or with `e2b template create` from a
  Dockerfile. A node pulls the OCI image, unpacks it inside a build VM,
  provisions it the way upstream E2B does (systemd/OpenRC, user `user` with
  sudo, envd as a service), runs the steps, then snapshots the VM with
  `startCmd` running. A sandbox create is a copy-on-write fork of that snapshot.
- **Defaults:** `templates/` holds the definitions the SDKs expect: `base`,
  `code-interpreter-v1`, `desktop` and `mcp-gateway`, adapted to build on
  aarch64 and x86_64 guests. `just templates` builds them as public templates of
  a system team, so unqualified aliases resolve for every team.

## Object storage (optional)

`--object-store s3://bucket/prefix` (or `https://endpoint/bucket/prefix`,
credentials from `VMON_S3_*` / the AWS environment) makes state portable
across nodes:

- **Templates**: finished builds are published to the bucket, so any node can
  start a sandbox from a template even when the node that built it is gone.
- **Paused sandboxes**: a pause uploads the memory delta and the disk in the
  background. `connect` resumes on any compatible node if the owner is gone or
  draining. The disk is fetched lazily block by block, and so is memory on
  Linux; macOS fetches memory in full.
- **Volumes**: E2B Volumes live in the bucket and mount on any node
  (write-back).
- **Bucket mounts**: `vmonS3Mounts` on sandbox create mounts your own buckets
  in `ro`, `overlay` (volatile writes) or `writeback` mode. This is a vibemon
  extension that needs the raw API; see `docs/e2b-compat.md`.

Credentials stay on the host. The guest only sees virtio-fs mounts.

## Platforms

| | Linux / KVM | macOS 15+ / HVF | Windows / WHP |
|---|---|---|---|
| Guest arch | x86_64, aarch64 | aarch64 | x86_64 |
| `vmon vmm` | ✅ | ✅ | ✅ |
| node role | ✅ TAP + NAT, transparent egress proxy (domains, SOCKS5, HTTPS header rules) | ✅ user-mode NAT, in-VMM egress filter (CIDRs, DNS-learned domains) | — |
| driver role | ✅ | ✅ | — |

macOS binaries must be ad-hoc signed with `hvf.entitlements` (`just build` does it).
On Linux the node needs `/dev/kvm`, and TAP networking needs root or `CAP_NET_ADMIN`.

The guest kernel is the vmon kernel ([`kernel/`](kernel/README.md), `just
kernel`). It is shipped next to `vmon` in release assets, and found under
`target/kernel/` in a dev tree. It has the netfilter, IPv6, cgroup v2, overlay
and NFS features that templates and Docker-in-sandbox rely on.

## Development

```sh
just build | just lint | just test     # workspace build, clippy -D warnings, unit tests
just test-db                           # driver tests against MySQL (`just mysql`)
just agent-musl                        # static guest agent → target/test-assets/
just integration                       # VMM integration tests (VMON_E2E=1)
just cluster-e2e                       # 3 drivers + 2 nodes failover test
just e2e all                           # upstream E2B Python + JS SDK suites (see e2e/README.md)
```

| Path | Crate | Purpose |
|---|---|---|
| `vmm/` | `vmm` | The VMM: KVM/HVF/WHP, virtio devices, snapshot/restore/fork. |
| `agent/` | `vmon-agent` | Guest agent: GC4 control channel, tunnels, OCI unpack, and an envd reimplementation. |
| `cluster/` | `vmon-cluster` | Gossip memberlist, raft KV (openraft), `Cluster` facade. |
| `node/` | `vmon-node` | VM runtime, sandboxes, template builds, artifacts, egress, node API. |
| `driver/` | `vmon-driver` | E2B API, MySQL store, auth, scheduler, lifecycle, ingress. |
| `src/` | `vmon` | The binary: `serve`, `admin`, `vmm` dispatch. |

## License

MIT (see [`LICENSE`](LICENSE)). E2B-derived specs, templates and provisioning
scripts keep their Apache-2.0 license; see `spec/e2b/SOURCES.md`,
`templates/SOURCES.md` and [`THIRD-PARTY-NOTICES.txt`](THIRD-PARTY-NOTICES.txt).
