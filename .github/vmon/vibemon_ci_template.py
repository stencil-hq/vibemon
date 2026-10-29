# /// script
# requires-python = ">=3.10"
# dependencies = ["e2b>=2.20.3,<3"]
# ///
"""Build the `vibemon-ci` vmon template: the sandbox vibemon's Linux CI jobs boot.

Workflows select it with `runs-on: vmon-large/from=vibemon-ci` (see
docs/src/concepts/templates.md, "The `vibemon-ci` template"). It derives from
vmon's public `ci` runner template (Ubuntu 24.04, systemd, Docker, actions/runner,
rustup with stable, cargo-zigbuild/nextest/audit/deny, just, zig, mdbook, uv,
node, go, the aarch64 cross gcc; templates/ci/template.py) and adds what
vibemon's jobs would otherwise fetch or build on every run:

- apt: what the KVM integration suite, the guest kernel build and the release
  smoke need (cpio, e2fsprogs, flex, bison, bc, libelf-dev, dwarves,
  qemu-user);
- the nightly toolchain rust-toolchain.toml pins, with its components and
  targets (the musl targets the guest agent is built for included);
- a git checkout of the pinned commit at the path actions/checkout uses
  (/home/user/actions-runner/_work/vibemon/vibemon, `origin` = the GitHub
  URL, so a `clean: false` checkout fetches into it instead of wiping it) with
  - the Cargo registry and git sources warmed (`cargo fetch --locked`),
  - a pre-built `target/` for exactly the commands CI runs (check, clippy,
    aarch64 check and clippy, test and integration-test binaries, debug
    build, the static guest agents), so the first job build only recompiles
    the workspace crates a newer commit touched,
  - `target/test-assets` from `just agent-musl fetch-assets` (sha256-pinned
    kernel, busybox and UEFI firmware, plus the initramfs and rootfs built from
    them);
- the RustSec advisory database (`cargo audit`'s ~/.cargo/advisory-db).

Rebuild after bumping rust-toolchain.toml or Cargo.lock, or when the pre-built
`target/` has drifted far from main (it only goes stale: jobs rebuild the
difference, slower but correct):

    E2B_API_URL=<vmon API URL> E2B_API_KEY=<`github-ci` team API key> \\
        uv run .github/vmon/vibemon_ci_template.py [--ref main|<sha>] [--skip-cache]

The build runs at 8 vCPU / 16384 MB, the `vmon-large` size, so
`vmon-large/from=vibemon-ci` jobs fork its snapshot (other sizes cold-boot its
disk). It is tagged `default` and `vibemon-<sha12>`; roll back with
`vmon-large/from=vibemon-ci:vibemon-<sha12>` or by pointing `default` at an
older build (`POST /templates/tags`).
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time

REPO_URL = "https://github.com/stencil-hq/vibemon"
NAME = "vibemon-ci"
CPU_COUNT = 8
MEMORY_MB = 16384

# actions/checkout's workspace for this repository; Cargo fingerprints are
# path-sensitive, so the pre-built target/ only helps here.
WORKSPACE = "/home/user/actions-runner/_work/vibemon/vibemon"

APT_PACKAGES = [
	# KVM integration suite (.github/actions/kvm-host): initramfs and rootfs
	# fixtures (`ci` has iproute2 and iptables).
	"cpio",
	"e2fsprogs",
	# Guest kernel build (kernel/build.sh, release.yml `kernel`).
	"flex",
	"bison",
	"bc",
	"libelf-dev",
	"dwarves",
	# Release smoke of the aarch64 binaries on x86_64.
	"qemu-user",
]

# CARGO_INCREMENTAL=0 as the jobs build (ci.yml sets it, Swatinem/rust-cache
# sets it in integration.yml): a different value changes the workspace
# crates' fingerprints.
ENV = (
	"export RUSTUP_HOME=/home/user/.rustup CARGO_HOME=/home/user/.cargo "
	"PATH=/home/user/.cargo/bin:$PATH CARGO_INCREMENTAL=0"
)


def checkout(sha: str) -> str:
	return f"""set -eux
rm -rf {WORKSPACE}
mkdir -p {WORKSPACE}
cd {WORKSPACE}
git init -q
git remote add origin {REPO_URL}
git fetch -q --depth 1 origin {sha}
git checkout -q --detach FETCH_HEAD
test "$(git rev-parse HEAD)" = {sha}
"""


# User: the pinned nightly (components and targets from rust-toolchain.toml)
# and the crate sources.
INSTALL_RUST = f"""set -eux
{ENV}
cd {WORKSPACE}
rustup toolchain install
rustup show active-toolchain
cargo fetch --locked
"""

# User: the builds CI runs (.github/workflows/ci.yml `rust`, integration.yml
# via .github/actions/kvm-host), in the same profiles and flags, so their
# fingerprints match. `cargo test --no-run` builds what `cargo test` would
# run. Keep in step with the workflows. The plain `cargo check --workspace`
# (dependency features without dev-dependencies) is for ad-hoc steps.
PREBUILD = f"""set -eux
{ENV}
cd {WORKSPACE}
cargo check --workspace --locked
cargo check --workspace --all-targets --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo check --workspace --all-targets --locked --target aarch64-unknown-linux-gnu
cargo clippy --workspace --all-targets --locked --target aarch64-unknown-linux-gnu -- -D warnings
cargo build --locked
cargo test --locked --no-run
cargo test --tests --locked --no-run
cargo zigbuild --locked --release -p vmon-agent --target x86_64-unknown-linux-musl
cargo zigbuild --locked --release -p vmon-agent --target aarch64-unknown-linux-musl
just agent-musl
just fetch-assets
# The advisory database `cargo audit` clones on first use; its exit status
# only reports advisories, which jobs check themselves.
cargo audit || true
test -d "$CARGO_HOME/advisory-db"
du -sh target "$CARGO_HOME/registry" "$CARGO_HOME/git" "$RUSTUP_HOME" 2>/dev/null || true
"""


def make_template(sha: str):
	from e2b import Template

	return (
		Template()
		.from_template("ci")
		.apt_install(APT_PACKAGES)
		.run_cmd(checkout(sha), user="user")
		.run_cmd(INSTALL_RUST, user="user")
		.run_cmd(PREBUILD, user="user")
		# Same as `ci`: snapshot with dockerd up, so forks start ready.
		.set_start_cmd("sudo systemctl start docker.service", "sudo docker info >/dev/null 2>&1")
	)


def resolve(ref: str) -> str:
	"""Commit SHA of `ref` (branch, tag, or SHA) on the public repo."""
	if len(ref) == 40 and all(c in "0123456789abcdef" for c in ref):
		return ref
	out = subprocess.run(
		["git", "ls-remote", REPO_URL, ref, f"refs/heads/{ref}", f"refs/tags/{ref}"],
		check=True,
		capture_output=True,
		text=True,
	).stdout.split()
	if not out:
		sys.exit(f"vibemon_ci_template.py: {ref!r} not found on {REPO_URL}")
	return out[0]


def main() -> int:
	parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
	parser.add_argument("--ref", default="main", help="branch, tag, or commit to warm from (default: main)")
	parser.add_argument("--skip-cache", action="store_true", help="rebuild every layer")
	args = parser.parse_args()

	for var in ("E2B_API_URL", "E2B_API_KEY"):
		if not os.environ.get(var):
			sys.exit(f"vibemon_ci_template.py: set {var} (vmon API URL / API key of the `github-ci` team)")

	from e2b import Template

	sha = resolve(args.ref)
	tag = f"vibemon-{sha[:12]}"
	print(f"building {NAME} from {REPO_URL}@{sha} (tags: default, {tag})", flush=True)
	started = time.monotonic()
	info = Template.build(
		make_template(sha),
		NAME,
		tags=[tag],
		cpu_count=CPU_COUNT,
		memory_mb=MEMORY_MB,
		skip_cache=args.skip_cache,
		on_build_logs=lambda entry: print(entry, flush=True),
	)
	print(
		f"{NAME} ready in {time.monotonic() - started:.0f}s: template {info.template_id}, "
		f"build {info.build_id}, tags default + {tag}",
		flush=True,
	)
	return 0


if __name__ == "__main__":
	sys.exit(main())
