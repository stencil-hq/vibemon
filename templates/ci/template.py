"""`ci`: the GitHub Actions runner image behind `runs-on: vmon…` (vmon CI).

Every CI job runs in a fresh sandbox of this template (or of a template derived
from it with `Template().from_template("ci")`, selected by the label option
`from=`). The layout is a contract with the driver's CI controller and with
workflows (docs/src/operate/ci.md):

- Ubuntu 24.04 with systemd as PID 1; default user `user` (uid/gid 1000,
  passwordless sudo, groups `docker` and `kvm`), default workdir `/home/user`.
- actions/runner (pinned version and sha256 below) unpacked, never configured,
  at `/home/user/actions-runner` (owned by `user`), its dependencies installed
  (`bin/installdependencies.sh`). The controller starts
  `run.sh --jitconfig <encoded>` there as `user`; jobs run in `_work/`.
- The runner's `.env` sets `VMON_CI=1` (how workflows detect vmon runners),
  `ImageOS=ubuntu24`, the tool cache `/opt/hostedtoolcache` (owned by `user`,
  as `actions/setup-*` expect), `LANG=C.UTF-8`, and `PATH`, `RUSTUP_HOME`,
  `CARGO_HOME` for the toolchains below (the same values are the template's
  environment, so sandbox commands see them too).
- Docker CE with the buildx and compose plugins; dockerd is started by the
  start command, so the template snapshot (and every fork of it) has the engine
  up. Plus build-essential, git, git-lfs, gh, curl, wget, jq, unzip, zip,
  xz-utils, zstd, python3 (venv, pip), rsync, openssh-client, pkg-config.
- `/dev/kvm` usable by `user` when the guest kernel provides it (udev rule
  `KERNEL=="kvm", GROUP="kvm", MODE="0666"`).

Toolchains, so jobs don't install them per run (what GitHub's ubuntu-24.04
image gives Rust, JS, Python and Go users; prebuilt downloads are pinned by
version and sha256 below, APT packages are Ubuntu's):

- APT: clang, lld, cmake, ninja-build, musl-tools, protobuf-compiler,
  libssl-dev, and the cross C toolchain for the other architecture
  (gcc-aarch64-linux-gnu + libc6-dev-arm64-cross on x86_64,
  gcc-x86-64-linux-gnu + libc6-dev-amd64-cross on aarch64).
- Rust: rustup in `~/.rustup` + `~/.cargo` (owned by `user`), stable as the
  default toolchain (minimal profile + clippy, rustfmt, rust-src; targets
  x86_64-unknown-linux-musl, aarch64-unknown-linux-gnu,
  aarch64-unknown-linux-musl). Repositories with a `rust-toolchain.toml`
  still get their pin from rustup on first use. In `~/.cargo/bin`:
  cargo-binstall, cargo-nextest, cargo-zigbuild, cargo-audit, cargo-deny,
  just, mdbook (replaceable with `cargo binstall`/`cargo install`).
- The tool cache `/opt/hostedtoolcache`, in the layout `actions/setup-node`,
  `actions/setup-python`, `actions/setup-go` and `goto-bus-stop/setup-zig`
  look up before downloading: `node/<v>/<arch>`, `Python/<v>/<arch>` (the
  actions/python-versions builds), `go/<v>/<arch>`, `zig/<v>/<arch>`.
  node, npm, npx, go, gofmt and zig are linked into
  `/usr/local/bin`; `npm install -g` goes to `~/.local` (`~/.npmrc`).
- bun in `~/.bun/bin`, uv/uvx in `~/.local/bin`, both linked into
  `/usr/local/bin`. python3 is Ubuntu's 3.12.

Build size: 4 vCPUs / 8192 MiB, the `vmon` label's size, so `runs-on: vmon`
jobs fork the template snapshot; other sizes cold-boot its disk. The disk is
the building team's `max_disk_mb`; `MIN_FREE_DISK_MB` asks for 64 GiB, so the
build is refused until the system team's limit is raised:
`vmon admin set-limits system --max-disk-mb 65536`.

Needs the `kernel/` guest kernel (Docker engine in the guest; `/dev/kvm` needs
its nested-KVM options).
"""

from pathlib import Path

from e2b import Template

CPU_COUNT = 4
MEMORY_MB = 8192
# Doubles as a guard: the build is refused unless the team's max_disk_mb
# (which sizes every build disk) is at least 64 GiB.
MIN_FREE_DISK_MB = 65536

# https://github.com/actions/runner/releases (sha256 from the release notes,
# checked against the downloaded tarballs).
RUNNER_VERSION = "2.338.2"
RUNNER_SHA256 = {
	"x64": "baebf204307bac6193480852732f0aba2ae97b6c5597c244f4d5558390147c40",
	"arm64": "670adb2f268fe21dcbb58b3db0e0aa9d5b3bddfe2c651481336e561ff86de945",
}
# APT signing keys, pinned by content. Docker: fingerprint
# 9DC8 5822 9FC7 DD38 854A E2D8 8D81 803C 0EBF CD88. GitHub CLI: keyring with
# 2C61 0620 1985 B60E 6C7A C873 23F3 D4EA 7571 6059 (expired 2026-09) and
# 7F38 BBB5 9D06 4DBC B3D8 4D72 5612 B364 6231 3325. A rotated key fails the
# build here; re-pin after checking the new key.
DOCKER_KEY_URL = "https://download.docker.com/linux/ubuntu/gpg"
DOCKER_KEY_SHA256 = "1500c1f56fa9e26b9b8f42452a553675796ade0807cdce11975eb98170b3a570"
GH_KEY_URL = "https://cli.github.com/packages/githubcli-archive-keyring.gpg"
GH_KEY_SHA256 = "6084d5d7bd8e288441e0e94fc6275570895da18e6751f70f057485dc2d1a811b"

RUNNER_DIR = "/home/user/actions-runner"
TOOL_CACHE = "/opt/hostedtoolcache"

RUSTUP_HOME = "/home/user/.rustup"
CARGO_HOME = "/home/user/.cargo"
# Runner steps (via `.env`) and sandbox commands (template environment).
PATH = (
	f"{CARGO_HOME}/bin:/home/user/.local/bin:"
	"/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
)
RUST_TARGETS = ["x86_64-unknown-linux-musl", "aarch64-unknown-linux-gnu", "aarch64-unknown-linux-musl"]

# Per-architecture names used in the download URLs, keyed by `dpkg --print-architecture`.
ARCHES = {
	"amd64": {
		"rt": "x64",  # actions/runner, tool cache, node, python-versions
		"cpu": "x86_64",
		"gnu": "x86_64-unknown-linux-gnu",
		"musl": "x86_64-unknown-linux-musl",
		# cargo-zigbuild and cargo-audit publish no aarch64 musl build.
		"static": "x86_64-unknown-linux-musl",
		"bun": "x64",
		"go": "amd64",
	},
	"arm64": {
		"rt": "arm64",
		"cpu": "aarch64",
		"gnu": "aarch64-unknown-linux-gnu",
		"musl": "aarch64-unknown-linux-musl",
		"static": "aarch64-unknown-linux-gnu",
		"bun": "aarch64",
		"go": "arm64",
	},
}

# Prebuilt downloads: name -> (version, URL, sha256 per architecture). Every
# sha256 matches the publisher's own (release .sha256 / SHASUMS256.txt /
# ziglang.org index.json / go.dev JSON / GitHub asset digest). Bump the
# version and both sums together.
TOOLS = {
	"rustup-init": (
		"1.29.2",
		"https://static.rust-lang.org/rustup/archive/{v}/{gnu}/rustup-init",
		{
			"amd64": "5be191e8c70a5007074254cb74d78dbfdd25cd5a61c27de7b8e5410122ec24f4",
			"arm64": "a9c168dbea81a84671c6b7a5c1795cb0eb699020110d107ddfca427bd797e697",
		},
	),
	"cargo-binstall": (
		"1.26.1",
		"https://github.com/cargo-bins/cargo-binstall/releases/download/v{v}/cargo-binstall-{musl}.tgz",
		{
			"amd64": "43d379fef8cb450961a14ff113e7bce7b5e1e52db709c776d186f928d8a20a40",
			"arm64": "243dbea9976495f0a5640f856283b5b3d934113381c9a1fba347c9e4b1fea142",
		},
	),
	"cargo-nextest": (
		"0.9.150",
		"https://github.com/nextest-rs/nextest/releases/download/cargo-nextest-{v}/cargo-nextest-{v}-{musl}.tar.gz",
		{
			"amd64": "84f0f9ba75d0b6fa117bc418d49747ba78e20e4583057f6fa628f1b17c17234d",
			"arm64": "5a7fe62b64a9006e1e47422021df7b1d308cc76c528c9eaf4649a488615ed6fb",
		},
	),
	"cargo-zigbuild": (
		"0.24.0",
		"https://github.com/rust-cross/cargo-zigbuild/releases/download/v{v}/cargo-zigbuild-{static}.tar.xz",
		{
			"amd64": "52be2cb85ad3875682564a29bac1832b2ab3db68b1c504765c92d5249cc9b96f",
			"arm64": "de42626985f428033078dfc12ac8a59ee713f8e5627235ec883b1c899bee895c",
		},
	),
	"cargo-audit": (
		"0.22.2",
		"https://github.com/rustsec/rustsec/releases/download/cargo-audit%2Fv{v}/cargo-audit-{static}-v{v}.tgz",
		{
			"amd64": "7fb9497f8594b389e5fce5ef9b92db08432996895b2e0c5a0167a69ed445c428",
			"arm64": "c6603814ddaa45e51263dafd31c0ac98808f688d26f7395804f9670b0fd599dd",
		},
	),
	"cargo-deny": (
		"0.20.2",
		"https://github.com/EmbarkStudios/cargo-deny/releases/download/{v}/cargo-deny-{v}-{musl}.tar.gz",
		{
			"amd64": "9f12ed4c49936e09b48bf862b595cde2fe64fcbd9d74dfacac6131ca824c8d5f",
			"arm64": "995c82be0defc7a025cae49a2aa2644ce8245c9a3318fc4103907c6a285e8c7d",
		},
	),
	"just": (
		"1.59.0",
		"https://github.com/casey/just/releases/download/{v}/just-{v}-{musl}.tar.gz",
		{
			"amd64": "409044617b6f06517c1035e55ca38c8c856ff8b650e41bc2aa1ef82d5903a23e",
			"arm64": "23871b1702bfd550fb5f901778fd4290ecf2ee034a45a889fbe96d89b188086d",
		},
	),
	"mdbook": (
		"0.5.4",
		"https://github.com/rust-lang/mdBook/releases/download/v{v}/mdbook-v{v}-{musl}.tar.gz",
		{
			"amd64": "5222beabd3e37dc5be0d18ff99b79058469354db5c220153a1b92db5ba12be89",
			"arm64": "753e5c5c363ee8a56972344dcf91466f005a51db84a7aeffe427ae3ef83d6d44",
		},
	),
	"zig": (
		"0.16.0",
		"https://ziglang.org/download/{v}/zig-{cpu}-linux-{v}.tar.xz",
		{
			"amd64": "70e49664a74374b48b51e6f3fdfbf437f6395d42509050588bd49abe52ba3d00",
			"arm64": "ea4b09bfb22ec6f6c6ceac57ab63efb6b46e17ab08d21f69f3a48b38e1534f17",
		},
	),
	# Node.js LTS (26 "Lithium").
	"node": (
		"26.13.0",
		"https://nodejs.org/dist/v{v}/node-v{v}-linux-{rt}.tar.xz",
		{
			"amd64": "d28a96af6c38596f6d9d97c0ee531c13d238d757742387d1928de822d959e997",
			"arm64": "406fe3fbde1c054adff8b297c16fdb5af6659db327c1ac8acd46b56ef1af9a92",
		},
	),
	"bun": (
		"1.4.4",
		"https://github.com/oven-sh/bun/releases/download/bun-v{v}/bun-linux-{bun}.zip",
		{
			"amd64": "bf8ccbfb2b0b80159e0f9a6d15c55317add8077534dbc6130a3edb52fccb395c",
			"arm64": "baa6bb36f51d6e58561e02bbea825038a9df454f673d6d776e8bbf86f8c7850e",
		},
	),
	"uv": (
		"0.13.6",
		"https://github.com/astral-sh/uv/releases/download/{v}/uv-{gnu}.tar.gz",
		{
			"amd64": "f29c3e94ffabd7e6f3ba521dba169a9b7ec12100bf3257087f0f24f6123f3645",
			"arm64": "229e194255c7958d86d56e8790d969a6f74736592133f0dfd0076f754c642c1d",
		},
	),
	"go": (
		"1.27.3",
		"https://go.dev/dl/go{v}.linux-{go}.tar.gz",
		{
			"amd64": "65efbc80fdc1fa2cd2631cad5b481777a68e8725f34e89f50fc6ac714e379b40",
			"arm64": "a06e99309dbcbdca1a8ccd2282c3ee146e55930886e26b4f1193269587165f49",
		},
	),
}
# actions/python-versions builds for `actions/setup-python`'s tool cache:
# version -> (release tag, sha256 per architecture).
PYTHONS = {
	"3.12.16": (
		"3.12.16-40260402378",
		{
			"amd64": "dec61ed20cd3e71143f1642ef9966945dea8ae4f3e96b306205a710da4f2191a",
			"arm64": "86c16c393f74e4d814f9606da32aed57be85a951e772bf1c7eb80a48ef3cb664",
		},
	),
	"3.13.18": (
		"3.13.18-40336726450",
		{
			"amd64": "683005a1465d94d40045f6a1a6eb316885b5f08d3360f7922b44ef58a14a9eed",
			"arm64": "8336b5b058474b8967155a24a5dd164cd6fa14866c009cce599f751e878dfc71",
		},
	),
	"3.14.10": (
		"3.14.10-40336797752",
		{
			"amd64": "d67d217db8480fd474bac19db80f6b956362432cca30ed6ac6fbe5a2bba6b4cd",
			"arm64": "d33ee2c4300ab6fd8fc0fd796e78276061588e5fc9f1f4d68d3961f9c1587b16",
		},
	),
}
PYTHON_URL = (
	"https://github.com/actions/python-versions/releases/download/{tag}/python-{v}-linux-24.04-{rt}.tar.gz"
)

# Where the downloads wait (root-owned, world-readable) for the install steps.
DOWNLOADS = "/tmp/vmon-ci-tools"

# `ubuntu:24.04` ships user `ubuntu` with uid/gid 1000, so the platform's
# default user `user` gets 1001. Drop `ubuntu` and move `user` to 1000.
FIX_USER = r"""set -eu
if id ubuntu >/dev/null 2>&1; then userdel --remove ubuntu 2>/dev/null || userdel ubuntu; fi
if getent group ubuntu >/dev/null; then groupdel ubuntu; fi
if ! id user >/dev/null 2>&1; then
  useradd --create-home --shell /bin/bash --uid 1000 --user-group user
fi
old_uid="$(id -u user)"
old_gid="$(id -g user)"
if [ "$old_gid" != 1000 ]; then
  groupmod -g 1000 user
  find / -xdev -gid "$old_gid" -exec chgrp -h 1000 {} +
fi
if [ "$old_uid" != 1000 ]; then
  usermod -u 1000 user
  find / -xdev -uid "$old_uid" -exec chown -h 1000 {} +
fi
usermod -g 1000 user
id user
"""

INSTALL_PACKAGES = rf"""set -eu
export DEBIAN_FRONTEND=noninteractive
arch="$(dpkg --print-architecture)"
case "$arch" in
  amd64) cross="gcc-aarch64-linux-gnu libc6-dev-arm64-cross" ;;
  arm64) cross="gcc-x86-64-linux-gnu libc6-dev-amd64-cross" ;;
  *) echo "unsupported architecture: $arch" >&2; exit 1 ;;
esac
. /etc/os-release
apt-get update
apt-get install -y --no-install-recommends ca-certificates curl
install -m 0755 -d /etc/apt/keyrings
curl -fsSL -o /etc/apt/keyrings/docker.asc "{DOCKER_KEY_URL}"
echo "{DOCKER_KEY_SHA256}  /etc/apt/keyrings/docker.asc" | sha256sum -c -
curl -fsSL -o /etc/apt/keyrings/githubcli-archive-keyring.gpg "{GH_KEY_URL}"
echo "{GH_KEY_SHA256}  /etc/apt/keyrings/githubcli-archive-keyring.gpg" | sha256sum -c -
chmod a+r /etc/apt/keyrings/docker.asc /etc/apt/keyrings/githubcli-archive-keyring.gpg
echo "deb [arch=$arch signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu $UBUNTU_CODENAME stable" \
  > /etc/apt/sources.list.d/docker.list
echo "deb [arch=$arch signed-by=/etc/apt/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" \
  > /etc/apt/sources.list.d/github-cli.list
apt-get update
apt-get install -y --no-install-recommends \
  build-essential pkg-config git git-lfs gh curl wget jq unzip zip xz-utils zstd \
  python3 python3-venv python3-pip rsync openssh-client gnupg lsb-release file \
  udev tzdata dbus \
  clang lld cmake ninja-build musl-tools protobuf-compiler libssl-dev $cross \
  docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
rm -rf /var/lib/apt/lists/*
mkdir -p /etc/docker
printf '{{\n  "storage-driver": "overlay2",\n  "log-driver": "local"\n}}\n' > /etc/docker/daemon.json
systemctl enable docker.service containerd.service
# Keep the kernel's interface names: udev must not rename the sandbox NIC.
ln -sf /dev/null /etc/udev/rules.d/80-net-setup-link.rules
"""

SETUP_HOST = rf"""set -eu
groupadd -f --system kvm
groupadd -f --system docker
usermod -aG docker,kvm user
cat > /etc/udev/rules.d/99-vmon-kvm.rules <<'EOF'
KERNEL=="kvm", GROUP="kvm", MODE="0666", OPTIONS+="static_node=kvm"
EOF
install -d -o user -g user -m 0755 {TOOL_CACHE}
"""

INSTALL_RUNNER = rf"""set -eu
case "$(dpkg --print-architecture)" in
  amd64) arch=x64; sum={RUNNER_SHA256["x64"]} ;;
  arm64) arch=arm64; sum={RUNNER_SHA256["arm64"]} ;;
  *) echo "unsupported architecture: $(dpkg --print-architecture)" >&2; exit 1 ;;
esac
curl -fsSL -o /tmp/actions-runner.tgz \
  "https://github.com/actions/runner/releases/download/v{RUNNER_VERSION}/actions-runner-linux-$arch-{RUNNER_VERSION}.tar.gz"
echo "$sum  /tmp/actions-runner.tgz" | sha256sum -c -
install -d -o user -g user -m 0755 {RUNNER_DIR}
tar -xzf /tmp/actions-runner.tgz -C {RUNNER_DIR}
rm /tmp/actions-runner.tgz
{RUNNER_DIR}/bin/installdependencies.sh
rm -rf /var/lib/apt/lists/*
cat > {RUNNER_DIR}/.env <<'EOF'
VMON_CI=1
ImageOS=ubuntu24
AGENT_TOOLSDIRECTORY={TOOL_CACHE}
RUNNER_TOOL_CACHE={TOOL_CACHE}
LANG=C.UTF-8
PATH={PATH}
RUSTUP_HOME={RUSTUP_HOME}
CARGO_HOME={CARGO_HOME}
EOF
install -d -o user -g user -m 0755 {RUNNER_DIR}/_work
chown -R user:user {RUNNER_DIR}
"""

# --- vmon CI cache: Runner.Worker patch (ARCHITECTURE.md §9.7) -------------
# Node and container actions get the job's results service as
# ACTIONS_RESULTS_URL from Runner.Worker (NodeScriptActionHandler /
# ContainerActionHandler, v2.338.2), overriding the process environment. The
# cache toolkit (@actions/cache v4+, buildx type=gha) talks to that URL, so the
# worker's one UTF-16LE literal is renamed to the same-length
# ACTIONS_RESULTS_ORL: steps keep GitHub's URL under that name and inherit
# ACTIONS_RESULTS_URL from the runner process, which vmon's start command points
# at the node's cache service. sha256 of the patched DLL, per architecture:
RUNNER_WORKER_PATCHED_SHA256 = {
	"x64": "b7c08b2d51f9836b4fa42a1926125e5d10027d8ad8ab159ac994313b5761c91a",
	"arm64": "a3107de73d53f01c2425ffc12c08d713795679bb88ff737616436afb8368c130",
}
PATCH_RUNNER = rf"""set -eu
case "$(dpkg --print-architecture)" in
  amd64) sum={RUNNER_WORKER_PATCHED_SHA256["x64"]} ;;
  arm64) sum={RUNNER_WORKER_PATCHED_SHA256["arm64"]} ;;
  *) echo "unsupported architecture" >&2; exit 1 ;;
esac
dll={RUNNER_DIR}/bin/Runner.Worker.dll
python3 - "$dll" <<'EOF'
import sys
path = sys.argv[1]
data = open(path, "rb").read()
old = "ACTIONS_RESULTS_URL".encode("utf-16-le")
new = "ACTIONS_RESULTS_ORL".encode("utf-16-le")
if data.count(old) != 1:
	sys.exit("expected exactly one ACTIONS_RESULTS_URL literal in " + path)
open(path, "wb").write(data.replace(old, new))
EOF
echo "$sum  $dll" | sha256sum -c -
"""
# --- end vmon CI cache ------------------------------------------------------


def _download_script() -> str:
	"""Root: fetch every pinned download into DOWNLOADS, checking its sha256."""
	cases = []
	for deb, names in ARCHES.items():
		lines = [f"  {deb})"]
		for name, (version, url, sums) in TOOLS.items():
			lines.append(f'    fetch {name} "{url.format(v=version, **names)}" {sums[deb]}')
		for version, (tag, sums) in PYTHONS.items():
			url = PYTHON_URL.format(tag=tag, v=version, rt=names["rt"])
			lines.append(f'    fetch python-{version} "{url}" {sums[deb]}')
		lines.append("    ;;")
		cases.append("\n".join(lines))
	cases_sh = "\n".join(cases)
	return f"""set -eu
rm -rf {DOWNLOADS}
install -d -m 0755 {DOWNLOADS}
fetch() {{  # fetch <name> <url> <sha256>
  curl -fsSL --retry 3 -o "{DOWNLOADS}/$1" "$2"
  echo "$3  {DOWNLOADS}/$1" | sha256sum -c -
}}
case "$(dpkg --print-architecture)" in
{cases_sh}
  *) echo "unsupported architecture: $(dpkg --print-architecture)" >&2; exit 1 ;;
esac
chmod -R a+rX {DOWNLOADS}
chmod a+x {DOWNLOADS}/rustup-init
"""


def _v(name: str) -> str:
	return TOOLS[name][0]


# User: everything lands in `user`-owned directories (home, tool cache), so
# jobs can update or add versions without sudo.
INSTALL_TOOLS = rf"""set -eu
D={DOWNLOADS}
TC={TOOL_CACHE}
export RUSTUP_HOME={RUSTUP_HOME} CARGO_HOME={CARGO_HOME}
case "$(dpkg --print-architecture)" in
  amd64) rt=x64 ;;
  arm64) rt=arm64 ;;
esac
pick() {{  # pick <archive> <dest dir> <binary>...: install named files from an archive
  src=$1 dest=$2
  shift 2
  t=$(mktemp -d)
  if unzip -tq "$src" >/dev/null 2>&1; then unzip -q "$src" -d "$t"; else tar -xf "$src" -C "$t"; fi
  mkdir -p "$dest"
  for b in "$@"; do
    f=$(find "$t" -type f -name "$b" | head -n 1)
    [ -n "$f" ] || {{ echo "$b not found in $src" >&2; exit 1; }}
    install -m 0755 "$f" "$dest/$b"
  done
  rm -rf "$t"
}}
cache() {{  # cache <tool> <version> <archive>: unpack into the tool cache layout
  dir="$TC/$1/$2/$rt"
  rm -rf "$dir"
  mkdir -p "$dir"
  tar -xf "$3" -C "$dir" --strip-components=1
  touch "$TC/$1/$2/$rt.complete"
}}

"$D/rustup-init" -y --no-modify-path --profile minimal --default-toolchain stable \
  --component clippy,rustfmt,rust-src --target {",".join(RUST_TARGETS)}
for tool in cargo-binstall cargo-nextest cargo-zigbuild cargo-audit cargo-deny just mdbook; do
  pick "$D/$tool" "$CARGO_HOME/bin" "$tool"
done

pick "$D/uv" "$HOME/.local/bin" uv uvx
pick "$D/bun" "$HOME/.bun/bin" bun
ln -sf bun "$HOME/.bun/bin/bunx"

cache node {_v("node")} "$D/node"
cache go {_v("go")} "$D/go"
cache zig {_v("zig")} "$D/zig"
for v in {" ".join(PYTHONS)}; do
  t=$(mktemp -d)
  tar -xzf "$D/python-$v" -C "$t"
  (cd "$t" && AGENT_TOOLSDIRECTORY="$TC" bash ./setup.sh >/dev/null)
  rm -rf "$t"
done
# Global npm installs go to ~/.local (its bin is on PATH), not the tool cache.
printf 'prefix=%s\n' "$HOME/.local" > "$HOME/.npmrc"
"""

# Root: the cached node/go/zig and the home-installed bun/uv on every PATH,
# then drop the downloads.
LINK_TOOLS = rf"""set -eu
case "$(dpkg --print-architecture)" in
  amd64) rt=x64 ;;
  arm64) rt=arm64 ;;
esac
TC={TOOL_CACHE}
for b in node npm npx; do ln -sf "$TC/node/{_v("node")}/$rt/bin/$b" /usr/local/bin/$b; done
for b in go gofmt; do ln -sf "$TC/go/{_v("go")}/$rt/bin/$b" /usr/local/bin/$b; done
ln -sf "$TC/zig/{_v("zig")}/$rt/zig" /usr/local/bin/zig
ln -sf /home/user/.local/bin/uv /home/user/.local/bin/uvx /home/user/.bun/bin/bun /home/user/.bun/bin/bunx /usr/local/bin/
rm -rf {DOWNLOADS}
"""

# User, with exactly the runner's `.env` environment: every baked tool runs.
CHECK_TOOLS = rf"""set -eu
set -a
. {RUNNER_DIR}/.env
set +a
cd /home/user
rustup show
for c in "rustc -V" "cargo -V" "cargo clippy -V" "cargo fmt --version" \
  "cargo binstall -V" "cargo nextest --version" "cargo-zigbuild --version" \
  "cargo audit --version" "cargo deny --version" "just --version" "mdbook --version" \
  "zig version" "node --version" "npm --version" "bun --version" "uv --version" \
  "python3 --version" "go version" "clang --version" "ld.lld --version" "cmake --version" \
  "ninja --version" "protoc --version" "musl-gcc --version" "docker --version" "gh --version"; do
  $c >/tmp/version.txt 2>&1 || {{ echo "$c failed:" >&2; cat /tmp/version.txt >&2; exit 1; }}
  printf '%s: %s\n' "$c" "$(head -n 1 /tmp/version.txt)"
done
rm -f /tmp/version.txt
(cd {RUNNER_DIR} && ./config.sh --version)
case "$(dpkg --print-architecture)" in
  amd64) aarch64-linux-gnu-gcc --version | head -n 1 ;;
  arm64) x86_64-linux-gnu-gcc --version | head -n 1 ;;
esac
ls {TOOL_CACHE}/*
"""


def make_template():
	return (
		Template(file_context_path=Path(__file__).parent)
		.from_image("ubuntu:24.04")
		.run_cmd(FIX_USER, user="root")
		.run_cmd(INSTALL_PACKAGES, user="root")
		.run_cmd(SETUP_HOST, user="root")
		.run_cmd(INSTALL_RUNNER, user="root")
		.run_cmd(PATCH_RUNNER, user="root")
		.run_cmd(_download_script(), user="root")
		.run_cmd(INSTALL_TOOLS, user="user")
		.run_cmd(LINK_TOOLS, user="root")
		.set_envs({"PATH": PATH, "RUSTUP_HOME": RUSTUP_HOME, "CARGO_HOME": CARGO_HOME})
		.run_cmd(CHECK_TOOLS, user="user")
		.set_user("user")
		.set_workdir("/home/user")
		# The template snapshot is taken with the Docker engine already up.
		.set_start_cmd("sudo systemctl start docker.service", "sudo docker info >/dev/null 2>&1")
	)
