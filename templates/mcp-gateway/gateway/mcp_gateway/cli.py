"""`mcp-gateway` command line.

    mcp-gateway --config '<json>'   validate, (re)start the daemon, wait ≤ 55 s
    mcp-gateway pull <server…>      pre-pull catalog server images (template builds)
    mcp-gateway serve               the daemon itself (started by --config)
    mcp-gateway status              print /run/mcp-gateway/status.json

`--config` is what the E2B SDKs run after `Sandbox.create(mcp=…)` (as root,
`GATEWAY_ACCESS_TOKEN` in the environment, 60 s command timeout); a non-zero
exit makes the SDK kill the sandbox and raise "Failed to start MCP gateway:
<stderr>".
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
import subprocess
import sys
import time
import uuid
from pathlib import Path

from . import paths
from .catalog import Catalog, CatalogError
from .config import ConfigError, GatewayConfig, parse

START_DEADLINE = 55.0


def _fail(msg: str, code: int = 1) -> int:
	print(f"mcp-gateway: {msg}", file=sys.stderr)
	return code


def load_catalog() -> Catalog | None:
	if not paths.E2B_SPEC.is_file() or not paths.DOCKER_CATALOG.is_file():
		return None
	return Catalog.load(paths.E2B_SPEC, paths.DOCKER_CATALOG)


def load_config(raw_json: str) -> GatewayConfig:
	try:
		raw = json.loads(raw_json)
	except json.JSONDecodeError as e:
		raise ConfigError(f"--config is not valid JSON: {e}") from None
	return parse(raw, load_catalog())


def _write_private(path: Path, text: str) -> None:
	path.parent.mkdir(parents=True, exist_ok=True)
	tmp = path.with_name(path.name + ".tmp")
	fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
	with os.fdopen(fd, "w") as f:
		f.write(text)
	os.replace(tmp, path)


def _stop_running() -> None:
	"""Terminate a previously started daemon (and its process group)."""
	try:
		pid = int(paths.PID_FILE.read_text())
	except (OSError, ValueError):
		return
	try:
		os.killpg(pid, signal.SIGTERM)
	except ProcessLookupError:
		return
	except PermissionError:
		os.kill(pid, signal.SIGTERM)
	for _ in range(200):
		try:
			os.kill(pid, 0)
		except ProcessLookupError:
			return
		time.sleep(0.1)
	try:
		os.killpg(pid, signal.SIGKILL)
	except ProcessLookupError:
		pass


def cmd_start(raw_json: str) -> int:
	if os.geteuid() != 0:
		return _fail("must run as root")
	try:
		cfg = load_config(raw_json)
	except ConfigError as e:
		return _fail(str(e))
	if not cfg.github and not cfg.catalog:
		return _fail("no MCP servers configured")
	token = os.environ.get("GATEWAY_ACCESS_TOKEN") or str(uuid.uuid4())

	_stop_running()
	paths.CONFIG_DIR.mkdir(mode=0o700, parents=True, exist_ok=True)
	os.chmod(paths.CONFIG_DIR, 0o700)
	_write_private(paths.TOKEN_FILE, token)
	_write_private(paths.CONFIG_FILE, raw_json)
	paths.RUN_DIR.mkdir(parents=True, exist_ok=True)
	paths.STATUS_FILE.unlink(missing_ok=True)
	paths.LOG_DIR.mkdir(parents=True, exist_ok=True)

	env = {k: v for k, v in os.environ.items() if k != "GATEWAY_ACCESS_TOKEN"}
	env.setdefault("HOME", "/root")
	with open(paths.LOG_FILE, "ab") as log_f:
		proc = subprocess.Popen(
			[sys.executable, "-m", "mcp_gateway", "serve"],
			stdin=subprocess.DEVNULL,
			stdout=log_f,
			stderr=log_f,
			start_new_session=True,
			close_fds=True,
			cwd="/",
			env=env,
		)
	paths.PID_FILE.write_text(f"{proc.pid}\n")
	return _wait_started(proc)


def _wait_started(proc: subprocess.Popen) -> int:
	deadline = time.monotonic() + START_DEADLINE
	status: dict = {}
	while time.monotonic() < deadline:
		if proc.poll() is not None:
			return _fail(f"gateway exited with status {proc.returncode}; see {paths.LOG_FILE}\n{_log_tail()}")
		try:
			status = json.loads(paths.STATUS_FILE.read_text())
		except (OSError, ValueError):
			status = {}
		backends = status.get("backends", {})
		failed = {n: b.get("error") for n, b in backends.items() if b.get("state") == "failed"}
		if failed:
			_stop_running()
			lines = "\n".join(f"  {n}: {err}" for n, err in failed.items())
			return _fail(f"failed to start MCP server(s):\n{lines}")
		if status.get("http") and backends and all(b.get("state") == "ready" for b in backends.values()):
			names = ", ".join(backends)
			print(f"MCP gateway listening on :{paths.PORT}{paths.MCP_PATH} ({names})")
			return 0
		time.sleep(0.2)
	if status.get("http"):
		pending = [n for n, b in status.get("backends", {}).items() if b.get("state") != "ready"]
		print(
			f"MCP gateway listening on :{paths.PORT}{paths.MCP_PATH}; still starting: {', '.join(pending)}",
			file=sys.stderr,
		)
		return 0
	_stop_running()
	return _fail(f"gateway did not start within {START_DEADLINE:.0f}s; see {paths.LOG_FILE}\n{_log_tail()}")


def _log_tail(lines: int = 15) -> str:
	try:
		return "\n".join(paths.LOG_FILE.read_text(errors="replace").splitlines()[-lines:])
	except OSError:
		return ""


def cmd_serve() -> int:
	from .backends import DockerCatalogBackend, GitHubBackend
	from .server import Gateway

	logging.basicConfig(
		level=logging.INFO,
		format="%(asctime)s %(levelname)s %(name)s: %(message)s",
		stream=sys.stderr,
	)
	logging.getLogger("mcp").setLevel(logging.ERROR)
	token = paths.TOKEN_FILE.read_text().strip()
	cfg = load_config(paths.CONFIG_FILE.read_text())
	backends = [GitHubBackend(s) for s in cfg.github]
	if cfg.catalog:
		backends.append(DockerCatalogBackend(cfg.catalog))
	paths.PID_FILE.write_text(f"{os.getpid()}\n")
	asyncio.run(Gateway(backends, token).serve())
	return 0


def cmd_pull(names: list[str]) -> int:
	from .backends import BackendError, docker_ready, ensure_docker, init_system, start_dockerd_detached, wait_docker

	catalog = load_catalog()
	if catalog is None:
		return _fail("the MCP server catalog is not installed")
	images: list[str] = []
	try:
		for n in names:
			image = catalog.image(n)
			if image is None:
				print(f"{n}: remote server, nothing to pull")
			else:
				images.append(image)
	except CatalogError as e:
		return _fail(str(e))
	if not images:
		return 0

	started = None
	try:
		if not asyncio.run(docker_ready()):
			if init_system() is not None:
				asyncio.run(ensure_docker(60))
			else:
				# Template build steps run without an init system: run an
				# engine just for the pull.
				started = start_dockerd_detached()
				asyncio.run(wait_docker(60))
		for image in images:
			print(f"pulling {image}", flush=True)
			if subprocess.run(["docker", "pull", "--quiet", image]).returncode != 0:
				return _fail(f"pulling {image} failed")
	except BackendError as e:
		return _fail(str(e))
	finally:
		if started is not None:
			started.terminate()
			try:
				started.wait(30)
			except subprocess.TimeoutExpired:
				started.kill()
	return 0


def cmd_status() -> int:
	try:
		print(paths.STATUS_FILE.read_text())
	except OSError:
		return _fail("gateway is not running")
	return 0


def main(argv: list[str] | None = None) -> int:
	argv = sys.argv[1:] if argv is None else argv
	if argv and argv[0] in ("pull", "serve", "status"):
		sub = argv[0]
		if sub == "pull":
			p = argparse.ArgumentParser(prog="mcp-gateway pull")
			p.add_argument("servers", nargs="+", help="catalog server ids (e.g. exa, githubOfficial)")
			return cmd_pull(p.parse_args(argv[1:]).servers)
		if sub == "serve":
			return cmd_serve()
		return cmd_status()
	p = argparse.ArgumentParser(prog="mcp-gateway", description="E2B-compatible MCP gateway")
	p.add_argument("--config", required=True, help="JSON object of MCP servers (Sandbox.create(mcp=…))")
	args = p.parse_args(argv)
	return cmd_start(args.config)
