# Repository Guidelines

## Project Overview

Vibemon (`vmon`) is a KVM/HVF microVM platform with an E2B-compatible control
plane. One Rust binary:

- `vmon vmm …`: the per-VM monitor (`vmm` crate). Spawned by nodes.
- `vmon serve --role node,driver`: the node role (`vmon-node`: hosts VMs,
  builds templates) and/or the driver role (`vmon-driver`: E2B REST API,
  sandbox ingress, scheduling). A process runs either role or both.
- `vmon admin …`: operator tooling (teams, users, API keys, system teams) against MySQL.

The official E2B SDKs and the `e2b` CLI are the clients. There is no vibemon
SDK and no other API.

- **Platforms:** Linux + KVM (`x86_64`, `aarch64`); macOS 15+ Apple Silicon +
  HVF (`aarch64` guests). Windows/WHP builds only `vmon vmm`. The backend is
  selected at compile time.

## Architecture & Data Flow

Normative design: `ARCHITECTURE.md`. Keep it in sync with behavior changes.
E2B coverage per operation: `docs/e2b-compat.md`.

```
E2B SDKs / e2b CLI
   │ HTTPS api.<domain>, <port>-<sandboxID>.<domain> (or E2b-Sandbox-* headers)
driver (vmon-driver): E2B API, auth, scheduler, lifecycle, build supervision, ingress proxy
   │ MySQL (durable state)        raft KV (live routes, admission, locks)
   │ node API /node/v1 + upgrade tunnels on the cluster listener
node (vmon-node): VM runtime, sandboxes, template builds, artifacts, egress, volumes
   │ spawns `vmon vmm …` per VM; GC4 frames over virtio-console (agent.sock)
guest: systemd/OpenRC + vmon-agent installed as /usr/bin/envd (envd on :49983)
```

- **Cluster** (`cluster/`): an in-house SWIM gossip memberlist (`gossip/`) handles membership and
  metadata (roles, URLs, capacity). openraft (`kv/`) provides a KV with txn/TTL/watch.
  Drivers are raft voters; node-only processes forward KV calls to drivers.
  `Cluster` is the facade.
- **Guest channel**: GC4 frames (`agent/src/proto.rs`) carry control ops,
  exec, file ops, OCI unpack/provision, and flow-controlled TCP tunnels
  (`tcp_connect` + `DATA`/`WINDOW`/`CLOSE`). Ingress reaches guest ports
  only through these tunnels.
- **VMM** (`vmm/`): `Config::from_args()` → `vmm::run()` → `Vmm::build()`
  (boot/restore/fork) → vCPU threads + device worker threads; synchronous,
  `EventFd` + `poll()`; control socket with newline JSON
  (`ping/info/pause/resume/snapshot/quit/metrics/extend/set_net_policy`).

## Key Directories

- `vmm/`: the core VMM (`hv/{kvm,hvf,whp}`, `arch/`, `virtio/`, `snapshot/`,
  `os/`). Change it minimally, with tests.
- `agent/`: `vmon-agent`. `main.rs` (PID 1 / service, GC4 loop), `envd/` (envd
  reimplementation: Connect process/filesystem, `/files`, `/init`, …),
  `tunnel.rs`, `unpack.rs`, `provision.rs`, `proto.rs` (shared with the host).
- `cluster/`: `vmon-cluster` (gossip, raft KV, `Cluster`).
- `node/`: `vmon-node`. `api.rs` (node API DTOs + KV records), `client.rs`
  (`NodeClient`), `vm/`, `gc4/`, `sandbox/`, `build/`, `oci/`,
  `artifacts/`, `egress/`, `net/`, `server.rs`.
- `driver/`: `vmon-driver`. `api/` (E2B routes), `store/` + `migrations/`
  (MySQL; migrations are registered by hand in `store/mod.rs`), `lifecycle/`,
  `scheduler.rs`, `builds/`, `ingress/`, `auth.rs`, `crypto.rs`, `admin.rs`.
- `src/`: the binary (`main.rs`, `serve.rs`, `admin.rs`).
- `templates/`: default template definitions (E2B v2 builder scripts) and `build.py`.
- `kernel/`: guest kernel config fragments and `build.sh`.
- `e2e/`: harness that runs the upstream E2B SDK suites against a live server.
- `spec/e2b/`: vendored upstream specs (OpenAPI, envd protos, provisioning
  references). Provenance is in `SOURCES.md`.
- `tests/`: VMM integration tests (`VMON_E2E=1`) and `cluster.rs` (`VMON_CLUSTER_E2E=1`).

## Development Commands

`just` is the task runner (`just --list`).

```bash
just build | just release        # build (+ HVF codesign on macOS)
just format | just lint | just check
just test                        # unit tests (HV-gated cases skip)
just mysql && just test-db       # driver tests against MySQL (VMON_MYSQL_TEST_URL)
just agent-musl                  # static guest agent → target/test-assets/vmon-agent-<arch>
just kernel                      # guest kernel → target/kernel/ (Docker on macOS, capped CPUs)
just serve                       # monolithic dev server on 127.0.0.1:3000
just templates [names]           # build default templates as public system templates
just integration                 # VMM integration tests
just cluster-e2e                 # distributed failover test (HVF/KVM)
just e2e [python|js|all]         # upstream E2B SDK suites (e2e/README.md)
```

macOS: the `vmon` binary must be ad-hoc codesigned with `hvf.entitlements`
before it runs VMs. `just build` does this; with a custom `CARGO_TARGET_DIR`,
run `codesign --sign - --entitlements hvf.entitlements --force <vmon>`.

## Code Conventions

- **Rust:** edition 2024; hard tabs, width 3, `max_width 100`, field alignment
  (`rustfmt.toml`). Always format with rustfmt. Workspace clippy: deny
  correctness/suspicious, warn pedantic/nursery; every `unsafe` needs
  `// SAFETY:`; every `#[allow]` needs a `reason`. Dependency versions live in
  root `[workspace.dependencies]`.
- **Runtime:** tokio/axum/hyper in `cluster`, `node`, `driver`, and envd. The
  `vmm` crate stays synchronous (no async runtime).
- **Errors:** E2B-shaped API errors (`driver/src/error.rs`:
  `{code, message, error_code?}`); node API errors are `{code, message}` with
  stable codes (`not_found`, `busy`, `port_closed`, …); the VMM uses `vmm/src/result.rs`.
- **sqlx:** no `macros` feature (its proc-macro dylib fails to load on macOS 27).
  Use runtime queries plus `#[derive(sqlx::FromRow)]`, and register each new
  migration in `store/mod.rs` `MIGRATIONS`. Never edit an applied migration.
- **E2B fidelity:** match upstream wire behavior, using `spec/e2b/` and the
  upstream sources cited in `docs/e2b-compat.md`. Protocol identifiers (`E2B_*`
  env vars, `/.e2b`, `/run/e2b`, aliases) stay as upstream. User-visible
  branding (hostname, wallpaper, noVNC title) is `vibemon`.
- **Platform code:** keep OS/arch differences behind `vmm/src/{os,hv,arch}`,
  `node/src/net`, `node/src/egress` with `#[cfg]`, not scattered through call sites.

## Testing

- Unit tests per crate (`cargo test -p <crate>`). Driver store tests need
  `VMON_MYSQL_TEST_URL`.
- Real-VM tests are gated (`VMON_E2E=1`, `VMON_CLUSTER_E2E=1`,
  `VMON_BUILD_E2E=1`). On macOS, run them through `demo/hvf-test-runner.sh` (see justfile).
- E2B compatibility is proven by the upstream SDK suites (`just e2e`). Record
  pass/fail counts in `e2e/README.md` when they change.
- Linux-only code (agent envd/unpack, egress proxy, TAP) must type-check for
  Linux: `cargo zigbuild --target aarch64-unknown-linux-musl -p vmon-agent`,
  `cargo zigbuild --target aarch64-unknown-linux-gnu -p vmon-node`.

## Runtime/Tooling Preferences

- Rust toolchain pinned in `rust-toolchain.toml` (nightly, with musl/gnu
  Linux targets for the agent and cross checks).
- Python (templates, e2e harness): `uv`. JS (e2e): `bun`.
- Env vars: `VMON_*` mirror every `vmon serve` flag (`vmon serve --help`).
  Common ones are `VMON_MYSQL_URL`, `VMON_CLUSTER_SECRET`, `VMON_JOIN`, `VMON_DOMAIN`,
  `VMON_KERNEL`, `VMON_AGENT`, `VMON_ADMIN_TOKEN`.
