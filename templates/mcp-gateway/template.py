"""`mcp-gateway`: the template behind `Sandbox.create(mcp={...})`.

Upstream E2B's `mcp-gateway` template is not public; this one implements the
contract the SDKs rely on (local://e2b-api.md Part E §4):

- `mcp-gateway --config '<json>'` (run by the SDK as root with
  `GATEWAY_ACCESS_TOKEN`) starts a detached gateway and exits 0 once it
  serves MCP Streamable HTTP on `:50005/mcp` behind
  `Authorization: Bearer <token>`; the token is kept in
  `/etc/mcp-gateway/.token` for `getMcpToken()` after `connect()`.
- Catalog servers (ids of the SDK's `McpServer` type) run from the Docker MCP
  Catalog through docker/mcp-gateway on a Docker engine inside the sandbox;
  `github/<owner>/<repo>` servers are cloned and run over stdio.
- `mcp-gateway pull <ids…>` pre-pulls catalog images; it is what
  `Template().from_template("mcp-gateway").add_mcp_server([...])` emits.

Needs a guest kernel with Docker's requirements (overlayfs, cgroup v2,
netfilter/nat, bridge, veth). The gateway lives in `gateway/`.
"""

from pathlib import Path

from e2b import Template

CPU_COUNT = 2
MEMORY_MB = 2048
# Room for MCP server images pulled at runtime.
MIN_FREE_DISK_MB = 8192

# docker/mcp-gateway (MIT): the `docker mcp` CLI plugin running catalog servers.
DOCKER_MCP_VERSION = "v0.44.1"
DOCKER_MCP_SHA256 = {
	"arm64": "75e9f9df92355bab788a057b4d59308abd73473f7db209cd22df03456978b70c",
	"amd64": "8cdd121b87e1afe11e66dfa89ef3bd05433c0389cf27399de70fd3ef0c6434e9",
}
# The SDK's catalog id table, pinned to the E2B SDK commit we target.
E2B_SPEC_URL = (
	"https://raw.githubusercontent.com/e2b-dev/E2B/df6c368152b067512702fc677952c0fc4e13c2f0/spec/mcp-server.json"
)
E2B_SPEC_SHA256 = "3d9a8ff53c88dbd225062e8abd520ebd6aa253c4d3047d288bcef75726e1bc93"
# Docker MCP Catalog v3 (superset of v2; images pinned by digest).
DOCKER_CATALOG_URL = "https://desktop.docker.com/mcp/catalog/v3/catalog.yaml"

INSTALL_DOCKER = r"""set -eu
. /etc/os-release
case "$ID ${ID_LIKE:-}" in
  *ubuntu*) distro=ubuntu ;;
  *debian*) distro=debian ;;
  *) echo "unsupported distro: $ID" >&2; exit 1 ;;
esac
codename="${UBUNTU_CODENAME:-${VERSION_CODENAME:-}}"
arch="$(dpkg --print-architecture)"
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y --no-install-recommends ca-certificates curl gnupg iptables git python3 python3-venv
install -m 0755 -d /etc/apt/keyrings
curl -fsSL "https://download.docker.com/linux/$distro/gpg" -o /etc/apt/keyrings/docker.asc
chmod a+r /etc/apt/keyrings/docker.asc
echo "deb [arch=$arch signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/$distro $codename stable" \
  > /etc/apt/sources.list.d/docker.list
apt-get update
apt-get install -y --no-install-recommends docker-ce docker-ce-cli containerd.io
rm -rf /var/lib/apt/lists/*
mkdir -p /etc/docker
printf '{\n  "storage-driver": "overlay2",\n  "log-driver": "local"\n}\n' > /etc/docker/daemon.json
if command -v systemctl >/dev/null; then systemctl enable docker.service containerd.service; fi
"""

INSTALL_DOCKER_MCP = rf"""set -eu
arch="$(dpkg --print-architecture)"
case "$arch" in
  arm64) sum={DOCKER_MCP_SHA256["arm64"]} ;;
  amd64) sum={DOCKER_MCP_SHA256["amd64"]} ;;
  *) echo "unsupported architecture: $arch" >&2; exit 1 ;;
esac
curl -fsSL -o /tmp/docker-mcp.tgz \
  "https://github.com/docker/mcp-gateway/releases/download/{DOCKER_MCP_VERSION}/docker-mcp-linux-$arch.tar.gz"
echo "$sum  /tmp/docker-mcp.tgz" | sha256sum -c -
mkdir -p /usr/libexec/docker/cli-plugins
tar -xzf /tmp/docker-mcp.tgz -C /usr/libexec/docker/cli-plugins docker-mcp
rm /tmp/docker-mcp.tgz
docker mcp version
"""

INSTALL_GATEWAY = rf"""set -eu
python3 -m venv /opt/mcp-gateway/venv
/opt/mcp-gateway/venv/bin/pip install --no-cache-dir --disable-pip-version-check /opt/mcp-gateway/src
ln -sf /opt/mcp-gateway/venv/bin/mcp-gateway /usr/local/bin/mcp-gateway
mkdir -p /opt/mcp-gateway/share /root/.docker/mcp/catalogs /etc/mcp-gateway /var/log/mcp-gateway
chmod 700 /etc/mcp-gateway
curl -fsSL -o /opt/mcp-gateway/share/mcp-server.json "{E2B_SPEC_URL}"
echo "{E2B_SPEC_SHA256}  /opt/mcp-gateway/share/mcp-server.json" | sha256sum -c -
curl -fsSL -o /root/.docker/mcp/catalogs/docker-mcp.yaml "{DOCKER_CATALOG_URL}"
mcp-gateway --help >/dev/null
"""


def make_template():
	return (
		Template(
			file_context_path=Path(__file__).parent,
			file_ignore_patterns=["**/__pycache__", "**/.venv", "**/*.egg-info", "**/build", "**/uv.lock"],
		)
		.from_base_image()
		.run_cmd(INSTALL_DOCKER, user="root")
		.run_cmd(INSTALL_DOCKER_MCP, user="root")
		.copy("gateway", "/opt/mcp-gateway/src", user="root")
		.run_cmd(INSTALL_GATEWAY, user="root")
		# The template snapshot is taken with the Docker engine already up.
		.set_start_cmd("sudo systemctl start docker.service", "sudo docker info >/dev/null 2>&1")
	)
