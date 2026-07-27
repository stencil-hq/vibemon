# Repository Guidelines

## Project Overview

Vibemon (`vmon`) is a small KVM/HVF-based Linux microVM monitor. One Rust binary owns the user CLI, `vmon serve` HTTP/WebSocket server (`vmond` crate), and low-level `vmon vmm` per-VM monitor (`vmm` crate). Python and TypeScript are thin client SDKs for the Rust API.

- **Platforms:** Linux + KVM (`x86_64`, `aarch64`); macOS 15+ Apple Silicon + HVF (`aarch64` guests only). Backend is selected at compile time — there is no runtime switch, and `x86_64` macOS is unsupported.
- **Capabilities:** direct-kernel and UEFI boot, virtio devices (blk/net/console/fs), snapshot / restore / fork with copy-on-write, in-process sandboxing (seccomp + Landlock + jailer), warm pools, secrets, volumes, lazy S3 mounts, egress control, PTY exec, and metrics.

## Architecture & Data Flow

Three runtime layers. The Rust server owns the registry and spawns one `vmon vmm` child per microVM; the guest agent runs inside the VM and talks back over a virtio-console channel.

```
Web UI / Rust CLI / Python SDK / TypeScript SDK / Go SDK
   │ gRPC (h2c over TCP or UDS); browsers/TS ride a gRPC-over-WebSocket
   │ bridge (`GET /grpc`, proto/vmon/v1/bridge.proto)
vmon serve (Rust axum + tonic, vmond crate)
   │ Engine registry, image pipeline, pools, mesh, volumes
   │ spawns `vmon vmm ... --api-sock <sock>` per VM
vmon vmm (Rust VMM crate)
   │ virtio-console, length-prefixed binary frames (GC4 / proto.rs)
vmon-agent (guest agent, Linux guest only)
```

**Rust boot path:** `Config::from_args()` → `vmm::run()` → `Vmm::build()` (boot or restore/fork) → allocate guest memory, instantiate virtio device backends, register on the device `Bus` → `Vmm::start()` spawns one thread per vCPU and one worker thread per device. vCPU threads run the hypervisor loop (`KVM_RUN` / HVF), trap MMIO/PortIO to the `Bus`, and notify virtio queues; device workers `poll()` queue/backend/control eventfds and signal completion interrupts.

**Control plane:** Unix-socket JSON protocol (`ping`, `info`, `pause`, `resume`, `snapshot`, `quit`, `metrics`, `extend`). The socket thread never touches the `Vmm` directly — requests cross a `flume` channel to the owner thread. `PauseGate` quiesces vCPUs via an RT signal without `SA_RESTART` on Linux (handler is a no-op; `EINTR` rechecks run state) and via a backend kicker callback on HVF.

**Orchestration (v2, `vmond/src/orch/`):** a horizontally-scalable scheduling layer beside the mesh. `vmon sched` servers keep an in-memory worker table fed from Redis (self-expiring `vmon:o:w:<wid>` keys + `vmon:o:workers` stream, followed by a hand-rolled RESP client in `orch/redis.rs`), place creates with power-of-two-choices, and forward directly to the owning `vmon serve` worker's gRPC endpoint — no datastore on the create path. Workers publish batched heartbeats (`orch/worker.rs`) and reject creates with `busy` when full; schedulers penalize and retry elsewhere. Sandbox routes (`vmon:o:sb:<sid>`) are written asynchronously; created/fetched views gain `node` + `endpoint` (the owning worker's URL). A `SET NX PX` leader lease gates the controller janitor (marks sandboxes of dead workers `lost`) and the HPA-like autoscaler (drain keys + `sh -c` scale hooks with `VMON_SCALE_*`/`VMON_DRAIN_WIDS`/`VMON_IDLE_WIDS`). `--redis` omitted embeds the dev-only mini-redis (`orch/miniredis.rs`); the hermetic end-to-end lives in `orch/e2e_tests.rs` (no hypervisor needed).

## Key Directories

- `src/` — Rust top-level `vmon` binary: CLI (`cli.rs`), local/remote tonic transport (`transport.rs`), and context storage (`contexts.rs`).
- `vmm/` — Rust VMM crate used by `vmon vmm`.
  - `vmm/src/hv/` — hypervisor seam; `kvm/` and `hvf/` backends selected by `#[cfg(target_os)]`.
  - `vmm/src/arch/` — architecture-specific boot/setup (`x86_64/`: MP table, GDT, MSR; `aarch64/`: FDT, GIC).
  - `vmm/src/virtio/` — virtio device model: `mod.rs` (trait + worker loop), `mmio.rs`, `pci.rs` (x86_64-only), `net.rs`, `block.rs`, `fs.rs`, `console.rs`.
  - `vmm/src/os/` — OS primitives (`EventFd`: real `eventfd(2)` on Linux, pipe-backed shim on macOS).
  - `vmm/src/devices/`, `vmm/src/snapshot/`.
- `proto/` — `vmon-proto` crate and the API contract: `vmon/v1/api.proto` (five gRPC services; the ONLY API) and `vmon/v1/bridge.proto` (browser WS bridge framing). Rust code generates at build time via protox + tonic; client codegen is checked in.
- `vmond/` — Rust server/engine crate used by `vmon serve`: gRPC services (`api/grpc.rs`), WS bridge (`api/bridge.rs`), remaining HTTP surfaces (healthz, metrics, ports proxy, static UI), registry, image pipeline, mesh, pools, volumes, lazy S3 access (`s3.rs`, `engine/s3proxy.rs`), VM spawn/control, and the v2 orchestration layer (`orch/`: scheduler + worker publisher + controller + autoscaler, served by `vmon sched`).
- `agent/` — `vmon-agent` guest agent crate (Linux guest only).
- `tests/` — Rust integration tests; shared helpers in `tests/common/mod.rs`.
- `sdk/py/vmon/` — thin Python SDK only (`_endpoint.py`, `client.py`, `driver.py`, `process.py`, `sandbox.py`, `remote.py`, `_remote_source.py`, `_remote_runner.py`, `cls.py`, `volume.py`, `secret.py`, `context.py`, `wsframe.py`, `v1/` generated protobuf/gRPC code, `__init__.py`).
- `sdk/py/tests/`, `sdk/py/e2e.py` — Python SDK unit tests and real-VM SDK driver.
- `sdk/ts/` — TypeScript SDK (bun).
- `sdk/go/` — Go SDK (`go test`, `google.golang.org/grpc`; `github.com/coder/websocket` only for the ports tunnel).
- `ui/` — React + Vite + TypeScript web panel; **builds into `vmond/web/`** for Rust embedding.
- `demo/` — runnable demo and asset-fetch scripts (Ubuntu/arm64 boots, OCI→ext4, Lima bridge for macOS).
- `deploy/aws/` — Pulumi (TypeScript/bun) stack: scheduler EC2 + bare-metal worker ASG (EC2 exposes `/dev/kvm` only on `*.metal`) + one state VM running Redis and Postgres; the vmon autoscaler drives the ASG through `scale-up.sh`/`scale-down.sh` hooks (worker ids are EC2 instance ids).
- `deploy/gcp/` — Pulumi (TypeScript/bun) stack mirroring `deploy/aws`: scheduler VMs + worker MIG (GCE grants `/dev/kvm` via the nested-virtualization flag on Intel series; Arm needs `-metal`) + one state VM running Redis and Postgres; the vmon autoscaler drives the MIG through `resize`/`delete-instances` hooks (worker ids are instance names).

## Development Commands

`just` is the canonical task runner. Recipes are OS-conditional (Linux uses `sudo` for `/dev/kvm` + TAP; macOS auto-codesigns).

```bash
just build           # debug build (auto-codesigns on macOS)
just release         # release build (+ codesign on macOS)
just run *args       # build then run vmon (sudo on Linux)
just format          # format Rust, Python SDK, and web UI
just lint            # lint Rust, Python SDK, and web UI
just check           # type-check Rust, Python SDK, and web UI
just test            # cargo test (unit + integration; KVM-gated cases auto-skip)
just integration     # VMON_E2E=1 cargo test --tests -- --test-threads=1
just cluster-e2e     # VMON_CLUSTER_E2E=1 VMON_E2E=1 cargo test --test cluster_e2e -- --test-threads=1
just soak            # VMON_E2E=1 VMON_SOAK=1 cargo test --test soak -- --test-threads=1
just fetch-assets    # ./demo/fetch-test-assets.sh  (kernels/images → target/test-assets/)
just ui              # cd ui && bun install && bun run build  → vmond/web/
just proto           # bunx @bufbuild/buf generate  (regenerate Go/Py/TS/UI client code from proto/)
just agent-musl      # build static vmon-agent → target/test-assets/vmon-agent-<arch>
just sdk-ts          # cd sdk/ts && bun install && bun run typecheck
just sdk-ts-smoke    # cd sdk/ts && bun install && VMON_TS_SMOKE=1 bun test
just sdk-go          # cd sdk/go && go test ./...
```

macOS HVF requires the `vmon` binary to be ad-hoc codesigned with `hvf.entitlements` (`com.apple.security.hypervisor`) before running — `just codesign` / `just build` handle this. Hypervisor.framework needs no root; only vmnet networking needs `sudo`.

A repo-root uv **workspace** (root `pyproject.toml` with `[tool.uv.workspace] members = ["sdk/py"]`) makes the `sdk/py` SDK package resolve from the repository root, so a single root `.venv` + root `uv.lock` serve both `uv run <cmd>` from the repo root (e.g. `VMON_BIN=$PWD/target/debug/vmon VMON_E2E=1 uv run python sdk/py/e2e.py`) and `cd sdk/py && uv run <cmd>`. The package's own `pyproject.toml` stays in `sdk/py`. Python tooling for the SDK: `uv run ruff check`, `uv run mypy`, `uv run pytest` — from the root or `sdk/py`. UI dev server: `cd ui && bun run dev` (proxies API to `:8000`). Per-language recipes are suffixed `-rust`/`-py`/`-ui` (e.g. `just lint-py`, `just fmt-ui`, `just check-rust`, `just test-py`); SDK recipes are `just sdk-ts`, `just sdk-ts-smoke`, and `just sdk-go`.

## Code Conventions & Common Patterns

**Rust**
- **Formatting:** edition 2024; hard tabs, tab width 3, `max_width 100`; `group_imports = "StdExternalCrate"`, `imports_granularity = "Crate"`. Always run `just fmt` — never hand-format.
- **Lints:** workspace clippy `deny` correctness/suspicious, `warn` pedantic/nursery/perf/style; `undocumented_unsafe_blocks` and `allow_attributes_without_reason` are warnings. Every `unsafe` block needs a `// SAFETY:` comment; every `#[allow]` needs a reason.
- **Errors:** root CLI code uses `src/error.rs::CliError`; the VMM crate uses `vmm/src/result.rs`; server/engine failures use stable `vmond::ErrorCode` values from `vmond/src/error.rs` that the API serializes. Keep error codes stable and map lower-level errors at crate boundaries.
- **Concurrency:** the VMM crate has no async runtime: blocking syscalls + `EventFd` wakeups + `poll()` loops, with cross-thread control over `flume`. The `vmond` API layer uses tokio/axum only at the HTTP/WebSocket edge and reaches the synchronous engine with blocking tasks.
- **Platform abstraction:** isolate OS/arch differences behind `vmm/src/os/`, `vmm/src/hv/`, `vmm/src/arch/`, `vmm/src/tap.rs`, and `vmond/src/net.rs` with `#[cfg(target_os/target_arch)]` — do not scatter `cfg` through call sites.

**Python**
- `PascalCase` classes, `snake_case` functions, `_leading_underscore` privates; `ruff` (line-length 100, target py314, select E/F/W/I/UP/B/C4) and `mypy` (typed, `ignore_missing_imports` + `warn_redundant_casts`/`warn_unused_ignores`; not `--strict`).
- Project Python is 3.14+. Write for that target instead of preserving older
  compatibility, and verify syntax claims with `cd python && uv run python -m
  compileall ...` or targeted `py_compile` before changing working code.
- PEP 758 is in scope here: `except ValueError, AttributeError:` is valid
  parenthesis-free multiple-exception syntax because this project targets Python
  3.14+. Do not call it Python 2 syntax, do not rewrite it only to add
  parentheses, and do not generalize this to older Python or to `as exc` forms
  unless the configured interpreter, formatter, or linter verifies the syntax.
- Prefer modern annotations: built-in generics (`dict[str, Any]`), PEP 604
  unions (`Path | str`), `collections.abc` protocols for inputs, concrete
  containers for owned return values, `Self` for fluent APIs, `type` statements
  for aliases, and PEP 695 generic syntax where it improves clarity. Avoid
  `typing.List`/`Dict`/`Tuple`/`Set`, `Optional`, `Union`, and compatibility
  aliases.
- Keep annotation runtime behavior intentional. Add or remove
  `from __future__ import annotations` only after checking code that inspects
  annotations; Python 3.14's default annotation semantics are not the same as
  stringized future annotations.
- Prefer modern stdlib idioms: `pathlib.Path`/`PathLike`, `Path.open()`,
  explicit `encoding="utf-8"` for text files, `time.monotonic()` for deadlines,
  `contextlib.suppress()` for deliberately ignored cleanup errors, and
  `hashlib.file_digest()` for file hashes.
- After changing `requires-python` or dependency constraints, regenerate the
  root workspace lockfile `uv.lock` with `uv lock` (from the repo root); never
  hand-edit generated lockfile markers.
- **Thin client boundary:** the Python package is a client SDK only. Keep gRPC channel/HTTP/ports-tunnel mechanics in `_endpoint.py`/`wsframe.py`, mesh failover in `driver.py`, context persistence in `context.py`, and user objects in `sandbox.py`, `volume.py`, and `secret.py`. Do not reintroduce SDK-side command entry points, daemon/server code, mesh control, image building, VMM orchestration, or asset bundling.
- **Errors:** `DaemonError` carries stable server error codes across the transport boundary (`vmon-code` gRPC trailing metadata; gRPC status code as fallback); SDK convenience layers may add narrow client-side exceptions such as `RemoteFunctionError`. Server/engine errors live in Rust (`vmond::error`) and map onto gRPC statuses in `vmond/src/api/grpc.rs`.
- **State:** contexts live under `$VMON_HOME` (`contexts.json` plus optional private token files). Secrets remain in memory and are sent only in create/exec requests; never persist them in SDK metadata.

**UI** — React function components + hooks; gRPC client (`api.ts`) over the WebSocket bridge transport (`grpc-ws.ts`, one RPC per socket) with bearer auth; the terminal drives `SandboxService.Exec` bidi; polling via hooks (`useSandboxes`); OKLCH dark-theme design tokens in `styles.css`. TypeScript strict, `verbatimModuleSyntax`, `noUnusedLocals/Parameters`.

## Important Files

- `src/main.rs` — single-binary dispatch; `vmon vmm` jumps into the VMM crate, every other subcommand uses the Rust CLI.
- `src/cli.rs`, `src/transport.rs`, `src/contexts.rs` — user CLI, local/remote tonic transport (background runtime thread + UDS connector), and context storage.
- `vmm/src/vmm.rs` — VMM lifecycle (build/start/pause/snapshot); owns vCPUs, devices, `PauseGate`.
- `vmm/src/config.rs` — manual VMM CLI parser and all launch-time flags + hard caps.
- `vmm/src/control.rs` — Unix-socket JSON control plane and `PauseGate`.
- `vmond/src/lib.rs`, `vmond/src/api/` (`grpc.rs` services, `bridge.rs` WS bridge, `routes.rs` remaining HTTP), `vmond/src/engine/`, `vmond/src/image/`, `vmond/src/mesh/` — Rust server core, gRPC + HTTP surfaces, engine facade/spawn/control, OCI image pipeline, and cluster mesh.
- `agent/src/main.rs`, `agent/src/proto.rs` — guest agent and its frame protocol.
- `sdk/py/vmon/sandbox.py`, `_endpoint.py`, `client.py`, `driver.py`, `process.py`, `remote.py`, `_remote_source.py`, `_remote_runner.py`, `cls.py`, `context.py`, `secret.py`, `volume.py`, `wsframe.py` — thin Python SDK.
- `sdk/ts/package.json`, `sdk/ts/src/` — TypeScript SDK.
- `sdk/go/go.mod`, `sdk/go/*.go` — Go SDK.
- `proto/vmon/v1/api.proto`, `proto/vmon/v1/bridge.proto`, `buf.gen.yaml` — the API contract and client codegen config.
- `Cargo.toml` (workspace + lints + profiles), `justfile`, `rust-toolchain.toml`, `rustfmt.toml`, `sdk/py/pyproject.toml`, `ui/vite.config.ts`, `hvf.entitlements`.

## Runtime/Tooling Preferences

- **Rust:** pinned `nightly-2026-04-29` (`rust-toolchain.toml`, with rustfmt/clippy/rust-analyzer). Release profile: `opt-level = 2`, `lto = "thin"`, `codegen-units = 1`, `strip = true`.
- **Python:** `>=3.14`; **`uv`** for everything, run from the repo root or `sdk/py` (`uv run`, `uv sync`). Build backend is `setuptools`; dev deps live in `[dependency-groups]`. Runtime dependencies are the gRPC stack (`grpcio`, `protobuf`) plus `httpx` for the ports proxy.
- **UI + TS SDK:** **bun** for everything (`bun.lock`; no `package-lock.json`). React/Vite/TS power the UI, which builds into `vmond/web/`; the TypeScript SDK lives in `sdk/ts` with `bun run typecheck` and `bun test`.
- **Go SDK:** Go 1.23+ with standard `go fmt`/`go vet`/`go test`; runtime dependencies are `google.golang.org/grpc`/`google.golang.org/protobuf` plus `github.com/coder/websocket` for the ports tunnel.
- **Env vars:** `VMON_HOME`, `VMON_BIN`, `VMON_KERNEL`, `VMON_AGENT`, `VMON_API_TOKEN`, `VMON_CLIENT_TOKEN`, `VMON_CONFIG`, `VMON_CONTEXT`, `VMON_REPLICATE_SEC`, `VMON_RESTORE_QUORUM`, `VMON_ORCH_REDIS`, `VMON_ORCH_URL`, `VMON_ORCH_ID`, `VMON_ORCH_HEARTBEAT_SEC`, `VMON_ORCH_DEAD_AFTER_SEC`, `VMON_ORCH_MAX_SANDBOXES`, `VMON_BOOT_CONCURRENCY` (concurrent local VM boots; 0 = 4× host CPUs), `VMON_WORKER_TOKEN` (sched → worker bearer). The Rust CLI/server locates the `vmon` binary from cargo target dirs, `$VMON_BIN`, or `PATH`.

## Testing & QA

**Rust** — `cargo test` runs unit tests plus integration tests in `tests/`. Most integration tests boot a real VM and are gated by `VMON_E2E=1` (see `tests/common/mod.rs::require_hv`, which also checks `/dev/kvm` on Linux / `kern.hv_support` on macOS); soak tests additionally need `VMON_SOAK=1`. `tests/cli_matrix.rs` validates flag rejection with no hypervisor needed.

- `boot.rs`, `blk.rs`, `lifecycle.rs`, `net.rs`, `pager.rs`, `snapshot.rs`, `timeout.rs`, `uefi.rs`, `server_e2e.rs`, `cluster_e2e.rs`, `soak.rs` — one concern each (boot markers, block I/O, control protocol, networking, pager eviction, snapshot/fork, timeout self-kill, UEFI boot, server API, cluster failover, stability).
- Integration runs single-threaded (`--test-threads=1`). Boot tests require assets from `just fetch-assets` (cached in `target/test-assets/`). macOS uses `demo/hvf-test-runner.sh` to codesign spawned test binaries.

**Python** — thin SDK unit tests live under `sdk/py/tests/` and run with `cd sdk/py && uv run pytest`. The real-VM SDK driver is `VMON_BIN=$PWD/target/debug/vmon VMON_E2E=1 uv run python sdk/py/e2e.py`, including the Python remote-function path.

**TypeScript and Go** — package tests run with `cd sdk/ts && bun test` and `cd sdk/go && go test ./...`. Real-VM remote-function tests require a running server plus `VMON_TS_REMOTE_SMOKE=1` or `VMON_GO_REMOTE_SMOKE=1`, `VMON_SERVER_URL`, and `VMON_API_TOKEN`.

**CI** — `ci.yml` (ubuntu): Rust fmt/check/clippy `-D warnings`, AArch64 check/clippy, `cargo test`, cargo-audit; Python SDK `ruff check`/`mypy`/`pytest`; Go SDK formatting/vet/race tests; web UI checks; TypeScript SDK typecheck + ungated `bun test`; macOS builds + codesigns + `cargo test --no-run`. `integration.yml` runs gated Rust e2e, Python SDK e2e, and Rust cluster e2e on KVM/HVF; `mesh-soak.yml` loops the Rust cluster suite under host-level tc-netem; `release.yml` builds musl binaries plus an SDK-only Python wheel/sdist.

When changing exported Rust symbols, check call sites with the language server (`lsp references`) before editing. Verify behavioral changes with the specific gated test rather than relying on `cargo check` alone.
