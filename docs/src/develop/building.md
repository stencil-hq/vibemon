# Building and testing

How to build vibemon from source, run its test tiers, and follow the code conventions CI enforces. For contribution licensing see [`CONTRIBUTING.md`](https://github.com/stencil-hq/vibemon/blob/main/CONTRIBUTING.md); the normative design is [`ARCHITECTURE.md`](https://github.com/stencil-hq/vibemon/blob/main/ARCHITECTURE.md).

## Workspace

| Path | Crate | Purpose |
|---|---|---|
| `vmm/` | `vmm` | The VMM: KVM/HVF/WHP, virtio devices, snapshot/restore/fork. |
| `agent/` | `vmon-agent` | Guest agent: GC4 control channel, tunnels, OCI unpack, envd reimplementation. |
| `cloud/` | `vmon-cloud` | Cloud workload identity, request signing, and the S3 client. |
| `cluster/` | `vmon-cluster` | Gossip memberlist, raft KV (openraft), `Cluster` facade. |
| `node/` | `vmon-node` | VM runtime, sandboxes, template builds, artifacts, egress, node API. |
| `driver/` | `vmon-driver` | E2B API, KV-backed store, auth, scheduler, lifecycle, ingress. |
| `src/` | `vmon` | The binary: `serve`, `admin`, `vmm` dispatch. |

Other directories: `templates/` (default templates), `kernel/` (guest kernel), `e2e/` (upstream SDK suites), `spec/e2b/` (vendored upstream specs), `tests/` (VMM integration tests and the distributed test).

## Toolchain

- **Rust:** pinned in [`rust-toolchain.toml`](https://github.com/stencil-hq/vibemon/blob/main/rust-toolchain.toml) to `nightly-2026-09-14` with `rustfmt`, `clippy`, `rust-analyzer`, and the targets `aarch64`/`x86_64-unknown-linux-gnu`, `aarch64`/`x86_64-unknown-linux-musl`, `aarch64-apple-darwin`, and `x86_64-pc-windows-msvc`. `rustup show` installs it.
- **[`just`](https://github.com/casey/just):** the task runner; `just --list` shows every recipe.
- **macOS native deps:** `brew install libslirp pkg-config` (as in CI).
- **Windows:** only `vmon vmm` builds; CI installs `libslirp` through vcpkg.
- **`cargo-zigbuild` + zig:** static guest agent and Linux cross checks. CI installs `ziglang==0.13.0` and `cargo-zigbuild` with pip.
- **Docker:** `just kernel` on macOS.
- **Python `uv`:** `just templates` and the e2e harness; the JS e2e suite also needs `pnpm` and `node`.
- **`cargo-deny`:** `just license-check`.
- **Lima:** for the `lima-*` recipes and `seccomp-audit` on macOS.

## macOS signing

Hypervisor.framework refuses to run a binary that is not signed with the `com.apple.security.hypervisor` entitlement. `just build` and `just release` ad-hoc sign `vmon` with `hvf.entitlements`. A plain `cargo build`, or a binary in a custom `CARGO_TARGET_DIR` that `just` did not sign, needs:

```sh
codesign --sign - --entitlements hvf.entitlements --force <path/to/vmon>
```

`just bin` prints the path `just` uses (it honors `CARGO_TARGET_DIR`). `cargo test` replaces the signed binary with a fresh unsigned copy, so the VM test recipes run tests through `demo/hvf-test-runner.sh`, a cargo runner that signs the `vmon` the tests spawn.

## Recipes

Host recipes pick the platform implementation automatically: KVM on Linux, HVF on macOS. Set `PROFILE=release` (or `just profile=release …`) to build release binaries.

| Recipe | What it does |
|---|---|
| `build` | `cargo build`; on macOS also `codesign`. |
| `release` | `build` with the release profile. |
| `codesign` | Ad-hoc sign the built binary with `hvf.entitlements` (macOS; no-op on Linux). |
| `bin` | Print the path of the built `vmon`. |
| `run *args` | Build and run `vmon` (under `sudo` on Linux for `/dev/kvm` and TAP). |
| `serve *args` | `build` + `agent-musl`, then a monolithic `vmon serve` on `127.0.0.1:3000`, domain `localhost`; extra args are forwarded. |
| `test *args` | `cargo test`, including the driver store and API tests (each on a private one-voter KV cluster); hypervisor-gated cases skip. |
| `format` / `fmt-check` | `cargo fmt --all`, or check only (CI gate). |
| `lint` | `cargo clippy --workspace --all-targets -- -D warnings`. |
| `check` | `cargo check --workspace --all-targets`, plus `cargo zigbuild --release -p vmon-agent` for both musl targets. |
| `fetch-assets` | Download the pinned UEFI firmware and guest images for the integration tests (`demo/fetch-test-assets.sh`). |
| `agent-musl` | Static `vmon-agent` for the host arch → `target/test-assets/vmon-agent-<arch>` (uses `cargo zigbuild` when installed). |
| `integration` | VMM integration tests: `VMON_E2E=1 cargo test --tests -- --test-threads=1`. |
| `smoke` | `integration` with the musl agent built, so the agent rows run too: every VMM flag and control verb at least once. Linux-only rows skip on macOS. |
| `smoke-jail` | Root-only jail rows (`VMON_JAIL=1`, separate `target/sudo`). Linux only. |
| `soak` | Long-running soak test (`VMON_SOAK=1`). |
| `seccomp-audit` | Integration suite with `VMON_SECCOMP_ACTION=log`; prints instructions for scanning kernel audit records for denied syscalls. On macOS it relays to `lima-seccomp-audit`. |
| `e2e suite *args` | Upstream E2B SDK suites against a fresh monolithic server; `suite` is `python`, `js`, `all`, `serve`, `shell`, or `none`. See [Upstream SDK suites](#upstream-sdk-suites). |
| `cluster-e2e` | Distributed test: 3 drivers + 2 nodes, failover (`VMON_CLUSTER_E2E=1`). See [Distributed, KVM, and TLS verification](cluster-verification.md). |
| `templates *args` | Build the default templates as public templates of the system team against a running server (`templates/build.py`), e.g. `just templates -j 2 base desktop`. See [Templates](../concepts/templates.md). |
| `ui` | Rebuild the admin dashboard (`ui/`, React + Vite, `bun`) into `driver/web/`, which the driver embeds; commit the output with the sources. `cd ui && bun run dev` serves it with hot reload on `:5173`, proxying the API to `just serve`. See [Admin dashboard](../operate/admin.md#admin-dashboard). |
| `kernel *args` | Build the guest kernels into `target/kernel/` (`kernel/build.sh`; in Docker on macOS). See [Guest kernel](../operate/kernel.md). |
| `gpu-driver *args` | Build the guest kernels plus the NVIDIA GPU driver bundle `target/kernel/vmon-nvidia-<arch>/` (`kernel/build.sh --nvidia`). See [Guest kernel](../operate/kernel.md#nvidia-gpu-driver-bundle). |
| `clean` | `cargo clean`. |
| `license-check` | `cargo deny check licenses`. |
| `local-release target` | Package release binaries and guest assets for `x86_64-unknown-linux-musl` or `aarch64-unknown-linux-musl` without uploading (`scripts/package-local-release.sh`). |
| `lima-build`, `lima-run`, `lima-test`, `lima-integration`, `lima-smoke`, `lima-soak`, `lima-seccomp-audit`, `lima-shell` | Linux tasks inside a Lima VM with nested KVM, from macOS. `lima-build` always builds release; `lima-run` runs `vmon vmm`; `lima-smoke` does not build the musl agent. |

The Lima recipes use the instance `kvm` (`VMON_LIMA_VM`) and expect this checkout at the same path inside the guest (`LIMA_REPO`). Create the instance with:

```sh
limactl start --vm-type=vz --set='.nestedVirtualization=true' --name=kvm template:default
```

## Local development loop

```sh
just serve                                  # API on 127.0.0.1:3000
vmon admin create-team dev                  # via the server's cluster listener
just templates base
```

Use the printed key with the E2B SDKs as described in [Quickstart](../get-started/quickstart.md).

## Test tiers

| Tier | Command | Gate |
|---|---|---|
| Unit tests | `just test`, `cargo test -p <crate>` | none; hypervisor-dependent cases skip |
| Driver store and API tests | `just test`, `cargo test -p vmon-driver` | none; each test starts a private one-voter KV cluster (`store::testing::TestStore`) |
| VMM integration (`tests/`) | `just integration`, `just smoke` | `VMON_E2E=1`, a usable hypervisor |
| Jail rows | `just smoke-jail` | `VMON_E2E=1 VMON_JAIL=1`, root, Linux |
| Soak | `just soak` | `VMON_E2E=1 VMON_SOAK=1` |
| Template build e2e (`node/src/build/e2e.rs`) | `VMON_BUILD_E2E=1 cargo test -p vmon-node` | `VMON_BUILD_E2E=1`, plus `VMON_BIN` (signed `vmon`), `VMON_KERNEL`, `VMON_AGENT` (static agent), Docker Hub access; `VMON_BUILD_E2E_ONLY=alpine\|python\|ubuntu` runs one case |
| Distributed (`tests/cluster.rs`) | `just cluster-e2e` | `VMON_CLUSTER_E2E=1`, hypervisor |
| Upstream SDK suites | `just e2e` | `uv`, `pnpm`, `node` |

Without their gate variables the gated tests skip. On macOS, run gated VM tests through `demo/hvf-test-runner.sh` (set `CARGO_TARGET_AARCH64_APPLE_DARWIN_RUNNER` and `VMON_ENTITLEMENTS`, as the recipes do).

CI (`.github/workflows/ci.yml`) runs on Linux: license check, `cargo fmt --check`, `cargo check` and `cargo clippy -D warnings` for the host and `aarch64-unknown-linux-gnu`, `cargo test` (driver store tests included), static agent builds for both musl targets, and `cargo audit`. On macOS it builds, signs, and compiles the tests; on Windows it checks `vmon` and lints and unit-tests the `vmm` crate. A separate workflow runs `VMON_E2E=1` on a KVM runner.

## Cross checks for Linux-only code

Agent envd and unpack, the egress proxy, and TAP networking compile only for Linux. From macOS, type-check them before sending a change:

```sh
cargo zigbuild --target aarch64-unknown-linux-musl -p vmon-agent
cargo zigbuild --target aarch64-unknown-linux-gnu -p vmon-node
```

## Upstream SDK suites

`just e2e` runs upstream's unmodified `packages/python-sdk/tests` and `packages/js-sdk/tests`, fetched at a pinned E2B commit, against a fresh monolithic `vmon serve`, the way upstream CI runs them. Each run creates a `vmon_<name>` database, mints a team key with `vmon admin create-team`, starts the server with data dir `/tmp/vmon-<name>`, builds the test templates with the SDK, runs the suites, and tears everything down (`--keep` retains the data directory and database after shutdown; `--reuse` reuses retained state). Arguments after `--` go to pytest or vitest:

```sh
just e2e python -- tests/sync/sandbox_sync
python3 e2e/run.py all --ingress tls      # upstream's TLS topology with a throwaway CA
```

`--ingress plain` (default) routes SDK traffic through `E2B_API_URL`/`E2B_SANDBOX_URL`; `--ingress tls` serves `*.<name>.localhost` on port 443. Use your own `--name` and `--slot` when several plain-HTTP stacks share a host; TLS stacks all bind port 443, so only one can run per host. The harness options, the latest pass/fail counts, and the known failures are in [`e2e/README.md`](https://github.com/stencil-hq/vibemon/blob/main/e2e/README.md); record new counts there when they change. Per-operation coverage is in [E2B compatibility](../reference/e2b-compat.md).

## Documentation

This site is an mdBook (0.5 or newer) in `docs/`: pages in `docs/src/`, the table of contents in `docs/src/SUMMARY.md`, the theme in `docs/theme/custom.css`.

```sh
mdbook serve docs --port 3100    # live preview on http://localhost:3100
mdbook build docs                # static site in docs/book/ (gitignored)
```

The `Documentation` workflow (`.github/workflows/docs.yml`) builds the book and checks local Markdown links on pull requests that touch `docs/`, and publishes it to GitHub Pages on pushes to `main`. Run it by hand with `workflow_dispatch` to republish.

## Code conventions

The full list is in [`AGENTS.md`](https://github.com/stencil-hq/vibemon/blob/main/AGENTS.md).

- **Formatting:** edition 2024, `rustfmt.toml` with hard tabs (tab width 3), `max_width = 100`, aligned struct fields, and crate-grouped imports. Always run `just format`.
- **Clippy:** workspace lints deny `correctness` and `suspicious` and warn on `pedantic`, `nursery`, `perf`, and `style`; CI runs clippy with `-D warnings`. Every `unsafe` block needs a `// SAFETY:` comment and every `#[allow]` a `reason`.
- **Dependencies:** versions live in the root `[workspace.dependencies]`.
- **Async:** tokio/axum/hyper in `cluster`, `node`, `driver`, and envd; the `vmm` crate stays synchronous.
- **Errors:** E2B-shaped API errors (`{code, message, error_code?}`, `driver/src/error.rs`); node API errors `{code, message}` with stable codes; the VMM uses `vmm/src/result.rs`.
- **Driver store:** records are serde structs under `db/<table>/…` keys in the raft KV (ARCHITECTURE.md §7). Read through `Store::read()`; write in a `Store::tx()` loop (guarded reads, staged writes, rerun when `commit()` returns `false`). There are no migrations: new record fields need `#[serde(default)]`.
- **E2B fidelity:** match upstream wire behavior using `spec/e2b/`; protocol identifiers (`E2B_*`, `/.e2b`, `/run/e2b`, aliases) stay as upstream, user-visible branding is vibemon.
- **Platform code:** keep OS and arch differences behind `vmm/src/{os,hv,arch}`, `node/src/net`, and `node/src/egress` with `#[cfg]`.
- **Design changes:** keep `ARCHITECTURE.md` in sync with behavior.
