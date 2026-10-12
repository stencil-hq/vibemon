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
# Override behaviour with variables, e.g. `just profile=release run vmm --restore D`.

set shell := ["bash", "-eu", "-o", "pipefail", "-c"]

# Build profile: debug | release.
profile := env_var_or_default("PROFILE", "debug")
# macOS HVF entitlements file used for ad-hoc codesigning.
entitlements := "hvf.entitlements"
# Lima instance name for the Linux-in-macOS path.
lima_vm := env_var_or_default("VMON_LIMA_VM", "kvm")
# vmon checkout inside the Lima guest (default: this checkout, mounted at the same path).
lima_repo := env_var_or_default("LIMA_REPO", justfile_directory())

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
    @printf '%s/%s/vmon\n' "${CARGO_TARGET_DIR:-$(dirname "$(cargo locate-project --workspace --message-format plain)")/target}" "{{prof}}"

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
# Build, sign, then run vmon, e.g. `just run vmm --restore /tmp/snaps/snap1`.
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

# Format the Rust workspace in place.
format:
    cargo fmt --all

# Verify formatting without writing (CI gate).
fmt-check:
    cargo fmt --all -- --check

# Clippy across the workspace (warnings are errors, as in CI).
lint:
    cargo clippy --workspace --all-targets -- -D warnings

# Type-check the workspace, plus the Linux-only crates for both guest arches.
check:
    cargo check --workspace --all-targets
    cargo zigbuild --release -p vmon-agent --target aarch64-unknown-linux-musl
    cargo zigbuild --release -p vmon-agent --target x86_64-unknown-linux-musl

# --------------------------------------------------------------- serve / e2e

# Start MySQL 8.4 for local development (docker container `vmon-mysql`, port 13306).
mysql:
    docker start vmon-mysql 2>/dev/null || docker run -d --name vmon-mysql -e MYSQL_ROOT_PASSWORD=vmon -e MYSQL_DATABASE=vmon -p 13306:3306 mysql:8.4

# Monolithic dev server (node + driver) against `just mysql`; extra args are forwarded.
[positional-arguments]
serve *args: build agent-musl
    "$({{just_executable()}} _bin {{profile}})" serve \
        --mysql-url mysql://root:vmon@127.0.0.1:13306/vmon \
        --agent "{{justfile_directory()}}/target/test-assets/vmon-agent-$(uname -m | sed 's/arm64/aarch64/')" \
        --listen 127.0.0.1:3000 --public-url http://127.0.0.1:3000 --domain localhost "$@"

# Official E2B SDK test suites against a fresh monolithic `vmon serve`
# (python | js | all | serve | shell); see e2e/README.md. Extra args go to
# e2e/run.py, e.g. `just e2e python -- tests/sync/sandbox_sync`.
[positional-arguments]
e2e suite="all" *args: build agent-musl fetch-assets mysql
    VMON_BIN="$({{just_executable()}} _bin {{profile}})" \
    VMON_AGENT="{{justfile_directory()}}/target/test-assets/vmon-agent-$(uname -m | sed 's/arm64/aarch64/')" \
    python3 e2e/run.py "$@"

# Gated distributed e2e (tests/cluster.rs): 3 drivers + 2 nodes on loopback,
# leader/node failover, graceful node restart; see docs/src/develop/cluster-verification.md.
cluster-e2e: agent-musl fetch-assets mysql
    CARGO_TARGET_AARCH64_APPLE_DARWIN_RUNNER="{{justfile_directory()}}/demo/hvf-test-runner.sh" \
    VMON_ENTITLEMENTS="{{justfile_directory()}}/{{entitlements}}" \
    VMON_CLUSTER_E2E=1 cargo test --test cluster -- --nocapture

# Build the default templates (base, code-interpreter-v1, desktop, mcp-gateway)
# as public templates of the system team against a running `vmon serve`
# (default: `just serve`); extra args go to templates/build.py, e.g.
# `just templates -j 2 base desktop`. See templates/SOURCES.md.
[positional-arguments]
templates *args: build
    VMON_BIN="$({{just_executable()}} _bin {{profile}})" uv run templates/build.py "$@"

# Rebuild the admin dashboard (ui/, React + Vite) into driver/web, which the
# driver embeds at /dashboard/; commit the output with the sources.
# `cd ui && bun run dev` serves it on :5173 against `just serve`.
ui:
    cd ui && bun install --frozen-lockfile && bun run check && bun run build

# Driver tests that need MySQL (`just mysql` first).
test-db:
    VMON_MYSQL_TEST_URL=mysql://root:vmon@127.0.0.1:13306/vmon cargo test -p vmon-driver

# ----------------------------------------------------------------------- assets

# Download pinned UEFI firmware + guest images used by the integration suite.
fetch-assets:
    ./demo/fetch-test-assets.sh

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
    target_dir="${CARGO_TARGET_DIR:-$(dirname "$(cargo locate-project --workspace --message-format plain)")/target}"
    dest="{{justfile_directory()}}/target/test-assets"
    mkdir -p "$dest"
    cp "$target_dir/$triple/release/vmon-agent" "$dest/vmon-agent-$arch"
    echo "guest agent -> $dest/vmon-agent-$arch"

# Build the vmon guest kernels (kernel/README.md) into target/kernel/; args go
# to kernel/build.sh, e.g. `just kernel --arch aarch64`.
[linux]
[positional-arguments]
kernel *args:
    ./kernel/build.sh "$@"

# macOS: builds in one Ubuntu 24.04 container (kernel/Dockerfile) capped at 4
# CPUs and niced. Source and build trees live in the `vmon-kernel-cache`
# volume (the kernel tree needs a case-sensitive filesystem); outputs land in
# this checkout's target/kernel/.
[macos]
[positional-arguments]
kernel *args:
    docker build --quiet --tag vmon-kernel-build kernel >/dev/null
    docker run --rm --cpus 4 \
        --volume "{{justfile_directory()}}:/src" --volume vmon-kernel-cache:/root/.cache/vmon-kernel \
        --workdir /src vmon-kernel-build nice -n 10 ./kernel/build.sh --jobs 4 "$@"

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

# Package release binaries and guest assets for an explicit Linux musl target
# (builds and smoke-checks; never uploads).
local-release target:
    ./scripts/package-local-release.sh --target "{{target}}"
