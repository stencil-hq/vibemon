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
  as `actions/setup-*` expect), and `LANG=C.UTF-8`.
- Docker CE with the buildx and compose plugins; dockerd is started by the
  start command, so the template snapshot (and every fork of it) has the engine
  up. Plus build-essential, git, git-lfs, gh, curl, wget, jq, unzip, zip,
  xz-utils, zstd, python3 (venv, pip), rsync, openssh-client, pkg-config.
- `/dev/kvm` usable by `user` when the guest kernel provides it (udev rule
  `KERNEL=="kvm", GROUP="kvm", MODE="0666"`).

Unlike GitHub-hosted runners there are no preinstalled language toolchains:
workflows install what they need (rustup, setup-node, setup-go, ...).

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
EOF
install -d -o user -g user -m 0755 {RUNNER_DIR}/_work
chown -R user:user {RUNNER_DIR}
"""


def make_template():
	return (
		Template(file_context_path=Path(__file__).parent)
		.from_image("ubuntu:24.04")
		.run_cmd(FIX_USER, user="root")
		.run_cmd(INSTALL_PACKAGES, user="root")
		.run_cmd(SETUP_HOST, user="root")
		.run_cmd(INSTALL_RUNNER, user="root")
		.run_cmd(f"{RUNNER_DIR}/config.sh --version && docker --version && gh --version", user="user")
		.set_user("user")
		.set_workdir("/home/user")
		# The template snapshot is taken with the Docker engine already up.
		.set_start_cmd("sudo systemctl start docker.service", "sudo docker info >/dev/null 2>&1")
	)
