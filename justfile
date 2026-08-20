# vmon — build, sign, run, and test across the three supported environments:
#
#   * macOS host        Hypervisor.framework (HVF). The binary must be ad-hoc
#                       codesigned with `hvf.entitlements` before it can run.
#   * Lima (Linux/KVM)  A Linux guest with nested KVM, driven from macOS via
#                       `limactl shell`; the `lima-*` recipes relay into it.
#   * Linux host        KVM directly. Integration/soak/seccomp tests run here.
#
# Host recipes (`build`, `run`, `test`, `integration`, ...) auto-pick the right
# implementation for the current OS: Linux runs KVM directly; macOS runs HVF
# natively, ad-hoc codesigning the spawned binary via demo/hvf-test-runner.sh.
# The `lima-*` recipes additionally drive the Linux/KVM path from a Mac.
# Override behaviour with variables, e.g. `just profile=release run --restore D`.

set shell := ["bash", "-eu", "-o", "pipefail", "-c"]

# Build profile: debug | release.
profile := env_var_or_default("PROFILE", "debug")
# macOS HVF entitlements file used for ad-hoc codesigning.
entitlements := "hvf.entitlements"
# Lima instance name for the Linux-in-macOS path.
lima_vm := env_var_or_default("VMON_LIMA_VM", "kvm")
# vmon checkout inside the Lima guest (the macOS checkout is not mounted there).
lima_repo := env_var_or_default("LIMA_REPO", "~/vmon")

# `--release` when profile is release, empty otherwise.
_prof_flag := if profile == "release" { "--release" } else { "" }
# Guest bash preamble: $1 = repo dir -> cd into it (expanding a leading ~),
# drop it from $@, and put a rustup-installed cargo on PATH.
lima_sh := 'cd "${1/#\~/$HOME}" 2>/dev/null || { echo "guest: vmon not found at $1 (clone it there or set LIMA_REPO)" >&2; exit 1; }; shift; . ~/.cargo/env 2>/dev/null || true; '

# List available recipes.
default:
    @{{just_executable()}} --list

# ---------------------------------------------------------------- build / sign

_compile:
    cargo build {{_prof_flag}}

# Debug build; ad-hoc HVF-codesigned on macOS, plain on Linux.
[macos]
build: _compile codesign

[linux]
build: _compile

# Release build (+ codesign on macOS).
release:
    @{{just_executable()}} profile=release build

# Resolve the host path to the vmon binary for a profile (debug|release).
_bin prof:
    @printf '%s/%s/vmon\n' "${CARGO_TARGET_DIR:-$(cargo metadata --no-deps --format-version 1 | uv run --no-project python -c 'import json,sys;print(json.load(sys.stdin)["target_directory"])')}" "{{prof}}"

# Print the path to the built vmon binary (honors `profile`).
bin:
    @{{just_executable()}} _bin {{profile}}

# Ad-hoc codesign the built binary with HVF entitlements (macOS only).
[macos]
codesign:
    codesign --sign - --entitlements {{entitlements}} --force "$({{just_executable()}} _bin {{profile}})"

[linux]
codesign:
    @echo "codesign: not required on Linux (KVM)"

# ------------------------------------------------------------------------- run

# macOS HVF needs no root for the hypervisor itself; vmnet networking does
# (run the whole command under sudo).
# Build, sign, then run vmon, e.g. `just run --restore /tmp/snaps/snap1`.
[macos]
[positional-arguments]
run *args: build
    exec "$({{just_executable()}} _bin {{profile}})" "$@"

# Linux/KVM needs root for /dev/kvm + TAP networking.
[linux]
[positional-arguments]
run *args: build
    exec sudo "$({{just_executable()}} _bin {{profile}})" "$@"

# ----------------------------------------------------------------------- tests

# Host unit + integration tests (KVM-gated cases auto-skip off Linux/KVM).
[positional-arguments]
test *args:
    cargo test "$@"

# e2e suite on Linux/KVM (TAP/PCI/pager cases run; macOS-only ones auto-skip).
[linux]
integration: fetch-assets
    VMON_E2E=1 cargo test --tests -- --test-threads=1

# e2e suite on macOS/HVF (user-net runs; TAP/PCI/pager auto-skip). The runner
# ad-hoc signs the spawned vmon with the hypervisor entitlement per test.
[macos]
integration: fetch-assets
    CARGO_TARGET_AARCH64_APPLE_DARWIN_RUNNER="{{justfile_directory()}}/demo/hvf-test-runner.sh" \
    VMON_ENTITLEMENTS="{{justfile_directory()}}/{{entitlements}}" \
    VMON_E2E=1 cargo test --tests -- --test-threads=1

# Full VMM smoke matrix: every CLI flag and control verb exercised at least
# once (`integration` + the musl guest agent baked into the initramfs, so the
# agent rows run instead of skipping). Linux-only rows (TAP/pager/jail)
# auto-skip on macOS — run `just lima-smoke` for those.
[linux]
smoke: agent-musl fetch-assets
    rm -rf target/test-runs
    VMON_E2E=1 cargo test --tests -- --test-threads=1

[macos]
smoke: agent-musl fetch-assets
    rm -rf target/test-runs
    CARGO_TARGET_AARCH64_APPLE_DARWIN_RUNNER="{{justfile_directory()}}/demo/hvf-test-runner.sh" \
    VMON_ENTITLEMENTS="{{justfile_directory()}}/{{entitlements}}" \
    VMON_E2E=1 cargo test --tests -- --test-threads=1

# Root-gated jail smoke rows (cgroup limits, cgroup-mode off, netns). The
# isolated target dir keeps root-owned artifacts out of target/.
[linux]
smoke-jail: fetch-assets
    sudo env VMON_E2E=1 VMON_JAIL=1 CARGO_TARGET_DIR=target/sudo HOME="$HOME" PATH="$PATH" cargo test --test jail -- --test-threads=1

[macos]
smoke-jail:
    @echo "smoke-jail: --jail is Linux-only; run it on a Linux host or inside Lima"

# Long-running soak test on Linux/KVM.
[linux]
soak: fetch-assets
    VMON_E2E=1 VMON_SOAK=1 cargo test --test soak -- --test-threads=1

# Long-running soak test on macOS/HVF.
[macos]
soak: fetch-assets
    CARGO_TARGET_AARCH64_APPLE_DARWIN_RUNNER="{{justfile_directory()}}/demo/hvf-test-runner.sh" \
    VMON_ENTITLEMENTS="{{justfile_directory()}}/{{entitlements}}" \
    VMON_E2E=1 VMON_SOAK=1 cargo test --test soak -- --test-threads=1

# Run the integration suite with seccomp in log mode to audit the syscall allowlist.
[linux]
seccomp-audit: fetch-assets
    VMON_E2E=1 VMON_SECCOMP_ACTION=log cargo test --tests -- --test-threads=1
    @echo
    @echo "seccomp-audit: scan kernel audit records for denied syscalls, e.g.:"
    @echo "  journalctl -k | grep -i SECCOMP   # or: dmesg | grep -i seccomp"

# Seccomp allowlist audit, relayed into the Lima guest.
[macos]
seccomp-audit:
    @{{just_executable()}} lima-seccomp-audit

# ------------------------------------------------------- format / lint / check
#
# Umbrella recipes fan out across the web UI (biome), the Python SDK
# (ruff + mypy), and the Rust workspace (cargo fmt/clippy/check). Dedicated
# TypeScript SDK recipes live under `sdk-ts` and `sdk-ts-smoke`.

# Format every language in place.
format: fmt-rust fmt-py fmt-ui fmt-ts fmt-go

# Verify formatting across every language without writing (CI gate).
fmt-check: fmt-check-rust fmt-check-py fmt-check-ui fmt-check-ts fmt-check-go

# Lint every language (biome | ruff | clippy | go vet).
lint: lint-rust lint-py lint-ui lint-ts lint-go

# Static/type-check every language (tsc | mypy | cargo check | go build).
check: check-rust check-py check-ui check-ts check-go

# -- Rust --
fmt-rust:
    cargo fmt --all

fmt-check-rust:
    cargo fmt --all -- --check

lint-rust:
    cargo clippy --workspace --all-targets -- -D warnings

check-rust:
    cargo check --workspace --all-targets

# -- Python (sdk/py) --
fmt-py:
    cd sdk/py && uv run ruff format .

fmt-check-py:
    cd sdk/py && uv run ruff format --check .

lint-py:
    cd sdk/py && uv run ruff check .

check-py:
    cd sdk/py && uv run mypy

# Python SDK test suite.
test-py:
    cd sdk/py && uv run pytest

# Gated cluster e2e (real hypervisor required; skips otherwise).
[linux]
cluster-e2e: fetch-assets
    VMON_CLUSTER_E2E=1 VMON_E2E=1 cargo test --test cluster_e2e -- --test-threads=1

[macos]
cluster-e2e: fetch-assets
    CARGO_TARGET_AARCH64_APPLE_DARWIN_RUNNER="{{justfile_directory()}}/demo/hvf-test-runner.sh" \
    VMON_ENTITLEMENTS="{{justfile_directory()}}/{{entitlements}}" \
    VMON_CLUSTER_E2E=1 VMON_E2E=1 cargo test --test cluster_e2e -- --test-threads=1

# -- Web UI (ui/) --
fmt-ui:
    cd ui && bun run format

fmt-check-ui:
    cd ui && bun run format:check

lint-ui:
    cd ui && bun run lint

check-ui:
    cd ui && bun run typecheck

# -- TypeScript SDK (sdk/ts) --
fmt-ts:
    cd sdk/ts && bun run format

fmt-check-ts:
    cd sdk/ts && bun run format:check

lint-ts:
    cd sdk/ts && bun run lint

check-ts:
    cd sdk/ts && bun run typecheck

sdk-ts:
    cd sdk/ts && bun install && bun run typecheck

sdk-ts-smoke:
    cd sdk/ts && bun install && VMON_TS_SMOKE=1 bun test

# -- Go SDK (sdk/go) --
fmt-go:
    gofmt -s -w sdk/go

fmt-check-go:
    @test -z "$(gofmt -l sdk/go)" || { echo "Go files need formatting. Run 'just format' to fix."; gofmt -d sdk/go; exit 1; }

lint-go:
    cd sdk/go && go vet ./...

check-go:
    cd sdk/go && go build ./...

sdk-go:
    cd sdk/go && go test ./...

# ----------------------------------------------------------------------- assets

# Download pinned UEFI firmware + guest images used by the integration suite.
fetch-assets:
    ./demo/fetch-test-assets.sh

# Build the React/Vite web UI into vmond/web for embedding in vmon serve.
ui:
    cd ui && bun install && bun run build

# Regenerate protobuf client code (Go/Py/TS/UI) from proto/. Needs network (buf remote plugins).
proto:
    bunx @bufbuild/buf generate

# Build the statically linked (musl) guest agent for the host arch into
# target/test-assets so e2e initramfs builders can embed it when present.
agent-musl:
    #!/usr/bin/env bash
    set -euo pipefail
    arch="$(uname -m | sed 's/arm64/aarch64/')"
    triple="${arch}-unknown-linux-musl"
    rustup target add "$triple"
    if command -v cargo-zigbuild >/dev/null 2>&1; then
        cargo zigbuild --release -p vmon-agent --target "$triple"
    else
        cargo build --release -p vmon-agent --target "$triple"
    fi
    target_dir="${CARGO_TARGET_DIR:-$(cargo metadata --no-deps --format-version 1 | uv run --no-project python -c 'import json,sys;print(json.load(sys.stdin)["target_directory"])')}"
    dest="{{justfile_directory()}}/target/test-assets"
    mkdir -p "$dest"
    cp "$target_dir/$triple/release/vmon-agent" "$dest/vmon-agent-$arch"
    echo "guest agent -> $dest/vmon-agent-$arch"

# Remove build artifacts.
clean:
    cargo clean

# ----------------------------------------------- Lima (Linux/KVM inside macOS)

# Fail early unless limactl is installed and the VM exists.
_lima-check:
    #!/usr/bin/env bash
    set -euo pipefail
    command -v limactl >/dev/null 2>&1 || { echo "error: limactl not found — install Lima with 'brew install lima'" >&2; exit 1; }
    limactl list -q 2>/dev/null | grep -qx '{{lima_vm}}' || {
        echo "error: lima VM '{{lima_vm}}' not found. Create one with nested KVM:" >&2
        echo "  limactl start --vm-type=vz --set='.nestedVirtualization=true' --name={{lima_vm}} template:default" >&2
        exit 1
    }

# Relay a fixed shell snippet into the guest (cwd = lima_repo, cargo on PATH).
_lima cmd: _lima-check
    @exec limactl shell '{{lima_vm}}' -- bash -lc '{{lima_sh}}{{cmd}}' lima '{{lima_repo}}'

# Build the release binary inside the Lima guest.
lima-build: _lima-check
    @{{just_executable()}} _lima 'cargo build --release'

# Run vmon inside the guest (sudo for /dev/kvm + TAP); forwards args verbatim.
[positional-arguments]
lima-run *args: lima-build
    @exec limactl shell '{{lima_vm}}' -- bash -lc '{{lima_sh}}exec sudo target/release/vmon vmm "$@"' lima '{{lima_repo}}' "$@"

# Cargo tests inside the guest.
lima-test:
    @{{just_executable()}} _lima 'cargo test'

# KVM integration suite inside the guest (fetches assets first).
lima-integration:
    @{{just_executable()}} _lima './demo/fetch-test-assets.sh && VMON_E2E=1 cargo test --tests -- --test-threads=1'

# Smoke matrix inside the Lima guest (nested KVM): covers the Linux-only rows
# (TAP, pager, remote-pager, jail gates) that auto-skip on the macOS host.
lima-smoke:
    @{{just_executable()}} _lima 'rm -rf target/test-runs && ./demo/fetch-test-assets.sh && VMON_E2E=1 cargo test --tests -- --test-threads=1'

# Soak test inside the guest.
lima-soak:
    @{{just_executable()}} _lima './demo/fetch-test-assets.sh && VMON_E2E=1 VMON_SOAK=1 cargo test --test soak -- --test-threads=1'

# Seccomp allowlist audit inside the guest.
lima-seccomp-audit:
    @{{just_executable()}} _lima './demo/fetch-test-assets.sh && VMON_E2E=1 VMON_SECCOMP_ACTION=log cargo test --tests -- --test-threads=1'

# Open an interactive shell in the Lima guest.
lima-shell: _lima-check
    @exec limactl shell '{{lima_vm}}'

# Check the locked, all-feature/all-target license policy.
license-check:
    cargo deny check licenses

# Validate systemd, Compose, Helm templates, and Kubernetes schemas.
validate-deploy:
    shellcheck deploy/single-node/*.sh
    VMON_API_TOKEN=validation-token-000000000000 docker compose -f deploy/single-node/docker-compose.yml config --quiet
    docker run --rm -v "$PWD:/src" alpine/helm:3.17.3 lint /src/deploy/helm/vmon -f /src/deploy/helm/vmon/ci-values.yaml
    docker run --rm -v "$PWD:/src" alpine/helm:3.17.3 template vmon /src/deploy/helm/vmon -f /src/deploy/helm/vmon/ci-values.yaml | docker run --rm -i ghcr.io/yannh/kubeconform:v0.6.7 -strict -summary

# Package every local release artifact for an explicit Linux musl target.
# This builds and smoke-checks binaries but never uploads or publishes.
local-release target:
    ./scripts/package-local-release.sh --target "{{target}}"

# Package the deployment assets into a release tarball
package-deploy:
    ./scripts/package-release.sh
