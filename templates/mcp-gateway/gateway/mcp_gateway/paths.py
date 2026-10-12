"""Filesystem layout of the gateway inside the sandbox."""

from pathlib import Path

# `getMcpToken()` reads this as root after `Sandbox.connect()`.
TOKEN_FILE = Path("/etc/mcp-gateway/.token")
CONFIG_DIR = Path("/etc/mcp-gateway")
CONFIG_FILE = CONFIG_DIR / "config.json"

# Catalog data installed by the template build.
SHARE_DIR = Path("/opt/mcp-gateway/share")
E2B_SPEC = SHARE_DIR / "mcp-server.json"
DOCKER_HOME = "/root"
DOCKER_CATALOG_NAME = "docker-mcp.yaml"
DOCKER_CATALOG = Path(DOCKER_HOME) / ".docker/mcp/catalogs" / DOCKER_CATALOG_NAME

RUN_DIR = Path("/run/mcp-gateway")
PID_FILE = RUN_DIR / "gateway.pid"
STATUS_FILE = RUN_DIR / "status.json"
DOCKER_SECRETS = RUN_DIR / "docker-secrets.env"
DOCKER_CONFIG = RUN_DIR / "docker-config.yaml"

LOG_DIR = Path("/var/log/mcp-gateway")
LOG_FILE = LOG_DIR / "gateway.log"
DOCKERD_LOG = LOG_DIR / "dockerd.log"

SANDBOX_USER = "user"
PORT = 50005
MCP_PATH = "/mcp"
