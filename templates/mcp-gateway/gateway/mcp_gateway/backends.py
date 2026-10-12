"""MCP servers behind the gateway, each reached over stdio.

- `GitHubBackend`: a `github/<owner>/<repo>` custom server. The repository is
  cloned into `/home/user/mcp-servers/<owner>/<repo>`, `installCmd` runs once
  per clone, and `runCmd` is the stdio server. Both run through `bash -l -c`
  as the sandbox user `user` (so `sudo …` in a command works as in upstream
  examples), with the entry's `envs` added.
- `DockerCatalogBackend`: every Docker MCP Catalog server, behind one
  `docker mcp gateway run --transport stdio` (docker/mcp-gateway), which
  owns the catalog semantics (images, secrets → env, config templating,
  volumes, remote servers, signature verification).
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import pwd
import shutil
import subprocess
import sys
from contextlib import AsyncExitStack
from pathlib import Path

import yaml
from mcp import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client

from . import paths
from .catalog import Resolved
from .config import GitHubServer

log = logging.getLogger("mcp-gateway")

# Environment the gateway itself was started with that servers must not see.
_PRIVATE_ENV = ("GATEWAY_ACCESS_TOKEN",)


class BackendError(RuntimeError):
	"""A backend could not be prepared or started."""


class Backend:
	"""One stdio MCP server connection."""

	name: str

	def __init__(self) -> None:
		self.session: ClientSession | None = None
		self.state = "starting"
		self.error: str | None = None
		self._stop = asyncio.Event()

	async def prepare(self) -> None:
		"""Blocking setup before the server starts (clone, install, …)."""

	def params(self) -> StdioServerParameters:
		raise NotImplementedError

	async def run(self, on_change) -> None:
		"""Prepare, connect, and keep the session open until `stop()`."""
		try:
			await self.prepare()
			async with AsyncExitStack() as stack:
				read, write = await stack.enter_async_context(
					stdio_client(self.params(), errlog=sys.stderr)
				)
				session = await stack.enter_async_context(ClientSession(read, write))
				await session.initialize()
				self.session = session
				self.state = "ready"
				log.info("%s: ready", self.name)
				on_change()
				await self._stop.wait()
		except asyncio.CancelledError:
			raise
		except BaseException as e:  # noqa: BLE001 — report every failure, incl. ExceptionGroup
			self.state = "failed"
			self.error = _describe(e)
			log.error("%s: %s", self.name, self.error)
		finally:
			self.session = None
			if self.state == "ready":
				self.state = "stopped"
			on_change()

	def stop(self) -> None:
		self._stop.set()


def _describe(e: BaseException) -> str:
	if isinstance(e, BaseExceptionGroup):
		return "; ".join(_describe(x) for x in e.exceptions)
	text = str(e) or type(e).__name__
	return text


def _child_env(extra: dict[str, str] | None = None) -> dict[str, str]:
	env = {k: v for k, v in os.environ.items() if k not in _PRIVATE_ENV}
	env.update(extra or {})
	return env


async def _run(
	cmd: list[str], *, cwd: str | None = None, env: dict[str, str] | None = None, what: str, tag: str = ""
) -> None:
	"""Run a setup command, its output going to the gateway log as `[tag: what]`."""
	proc = await asyncio.create_subprocess_exec(
		*cmd,
		cwd=cwd,
		env=env,
		stdin=subprocess.DEVNULL,
		stdout=subprocess.PIPE,
		stderr=subprocess.STDOUT,
	)
	assert proc.stdout is not None
	tail: list[str] = []
	async for line in proc.stdout:
		text = line.decode(errors="replace").rstrip()
		sys.stderr.write(f"[{tag + ': ' if tag else ''}{what}] {text}\n")
		tail = [*tail[-19:], text]
	if await proc.wait() != 0:
		detail = "\n".join(tail[-5:])
		raise BackendError(f"{what} failed (exit {proc.returncode})" + (f": {detail}" if detail else ""))


class GitHubBackend(Backend):
	def __init__(self, server: GitHubServer) -> None:
		super().__init__()
		self.server = server
		self.name = server.key
		self.user = _sandbox_user()
		home = Path(self.user.pw_dir) if self.user else Path("/root")
		self.dir = home / "mcp-servers" / server.owner / server.repo

	def _as_user(self, script: str) -> list[str]:
		if self.user is None or os.geteuid() != 0 or self.user.pw_uid == 0:
			return ["/bin/bash", "-l", "-c", script]
		return ["runuser", "-u", self.user.pw_name, "--", "/bin/bash", "-l", "-c", script]

	def _env(self) -> dict[str, str]:
		env = _child_env(self.server.envs)
		if self.user is not None:
			env.update(HOME=self.user.pw_dir, USER=self.user.pw_name, LOGNAME=self.user.pw_name)
		return env

	async def prepare(self) -> None:
		s = self.server
		if not (self.dir / ".git").is_dir():
			parent = self.dir.parent
			parent.mkdir(parents=True, exist_ok=True)
			if self.user is not None:
				for p in (parent.parent, parent):
					os.chown(p, self.user.pw_uid, self.user.pw_gid)
			tmp = parent / f".{s.repo}.clone"
			shutil.rmtree(tmp, ignore_errors=True)
			await _run(
				self._as_user(f"git clone --depth 1 {_q(s.clone_url)} {_q(str(tmp))}"),
				env={**self._env(), "GIT_TERMINAL_PROMPT": "0"},
				what="git clone",
				tag=self.name,
			)
			tmp.rename(self.dir)
		if s.install_cmd:
			marker = self.dir / ".mcp-gateway-installed"
			digest = hashlib.sha256(s.install_cmd.encode()).hexdigest()
			if not marker.is_file() or marker.read_text().strip() != digest:
				await _run(
					self._as_user(s.install_cmd),
					cwd=str(self.dir),
					env=self._env(),
					what="installCmd",
					tag=self.name,
				)
				marker.write_text(digest + "\n")

	def params(self) -> StdioServerParameters:
		cmd = self._as_user(self.server.run_cmd)
		return StdioServerParameters(command=cmd[0], args=cmd[1:], env=self._env(), cwd=str(self.dir))


class DockerCatalogBackend(Backend):
	name = "docker-mcp-catalog"

	def __init__(self, servers: list[Resolved]) -> None:
		super().__init__()
		self.servers = servers

	async def prepare(self) -> None:
		await ensure_docker()
		paths.RUN_DIR.mkdir(parents=True, exist_ok=True)
		secrets: dict[str, str] = {}
		config: dict[str, dict] = {}
		for r in self.servers:
			secrets.update(r.secrets)
			for block, values in r.config.items():
				_merge(config.setdefault(block, {}), values)
		_write_private(paths.DOCKER_SECRETS, "".join(f"{k}={v}\n" for k, v in secrets.items()))
		_write_private(paths.DOCKER_CONFIG, yaml.safe_dump(config, sort_keys=True))

	def params(self) -> StdioServerParameters:
		args = [
			"mcp",
			"gateway",
			"run",
			"--transport=stdio",
			"--servers=" + ",".join(r.name for r in self.servers),
			"--catalog=" + paths.DOCKER_CATALOG_NAME,
			"--secrets=" + str(paths.DOCKER_SECRETS),
			"--config=" + str(paths.DOCKER_CONFIG),
			"--watch=false",
			"--long-lived",
			"--verbose",
		]
		return StdioServerParameters(command="docker", args=args, env=_child_env({"HOME": paths.DOCKER_HOME}))


def _merge(dst: dict, src: dict) -> None:
	for k, v in src.items():
		if isinstance(v, dict) and isinstance(dst.get(k), dict):
			_merge(dst[k], v)
		else:
			dst[k] = v


def _write_private(path: Path, text: str) -> None:
	tmp = path.with_suffix(path.suffix + ".tmp")
	fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
	with os.fdopen(fd, "w") as f:
		f.write(text)
	os.replace(tmp, path)


def _q(s: str) -> str:
	return "'" + s.replace("'", "'\\''") + "'"


def _sandbox_user() -> pwd.struct_passwd | None:
	try:
		return pwd.getpwnam(paths.SANDBOX_USER)
	except KeyError:
		return None


def init_system() -> str | None:
	"""`systemd`, `openrc`, or None when PID 1 is no service manager (the
	agent during template build steps; `/run` may still hold the markers of
	the booted template, so PID 1 itself decides)."""
	try:
		pid1 = Path("/proc/1/comm").read_text().strip()
	except OSError:
		return None
	if pid1 == "systemd":
		return "systemd"
	if pid1 == "init" and Path("/run/openrc").is_dir() and shutil.which("rc-service"):
		return "openrc"
	return None


def _running_commands() -> set[str]:
	names = set()
	for comm in Path("/proc").glob("[0-9]*/comm"):
		try:
			names.add(comm.read_text().strip())
		except OSError:
			continue
	return names


async def docker_ready() -> bool:
	"""Whether the Docker engine answers (bounded: a wedged socket is "no")."""
	proc = await asyncio.create_subprocess_exec(
		"docker", "info", "--format", "{{.ServerVersion}}",
		stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
	)
	try:
		return await asyncio.wait_for(proc.wait(), 10) == 0
	except asyncio.TimeoutError:
		proc.kill()
		await proc.wait()
		return False


async def wait_docker(timeout: float) -> None:
	loop = asyncio.get_running_loop()
	deadline = loop.time() + timeout
	while loop.time() < deadline:
		if await docker_ready():
			return
		await asyncio.sleep(0.5)
	try:
		tail = "\n".join(paths.DOCKERD_LOG.read_text(errors="replace").splitlines()[-10:])
	except OSError:
		tail = ""
	raise BackendError(f"the Docker engine did not become ready within {timeout:.0f}s" + (f":\n{tail}" if tail else ""))


async def ensure_docker(timeout: float = 30.0) -> None:
	"""Start the Docker engine if it is not running and wait for it."""
	if shutil.which("docker") is None:
		raise BackendError("Docker is not installed in this template")
	if await docker_ready():
		return
	log.info("starting the Docker engine")
	init = init_system()
	if init == "systemd":
		cmd = ["systemctl", "start", "docker.service"]
	elif init == "openrc":
		cmd = ["rc-service", "docker", "start"]
	else:
		cmd = None
	if cmd is not None:
		await _run(cmd, what="start docker")
	else:
		start_dockerd_detached()
	await wait_docker(timeout)


def start_dockerd_detached() -> subprocess.Popen:
	"""Run `dockerd` without an init system (template build steps).

	The rootfs comes from a booted template, so `/run` may still hold the
	pid files of that boot's engine; with no engine running they are stale,
	and dockerd refuses to start when the stale pid now names a live process
	(or waits forever on a stale `/run/containerd/containerd.sock`).
	"""
	cg = Path("/sys/fs/cgroup")
	if not (cg / "cgroup.controllers").exists() and not any(cg.iterdir() if cg.is_dir() else []):
		subprocess.run(["mount", "-t", "cgroup2", "cgroup2", str(cg)], check=False)
	running = _running_commands()
	stale = ["/run/docker.pid", "/run/docker.sock"] if "dockerd" not in running else []
	if "containerd" not in running:
		stale += ["/run/containerd/containerd.sock", "/run/containerd/containerd.pid", "/run/docker/containerd/containerd.pid"]
	for path in stale:
		Path(path).unlink(missing_ok=True)
	paths.LOG_DIR.mkdir(parents=True, exist_ok=True)
	log_f = open(paths.DOCKERD_LOG, "ab")
	return subprocess.Popen(
		["dockerd"],
		stdin=subprocess.DEVNULL,
		stdout=log_f,
		stderr=log_f,
		start_new_session=True,
		env=_child_env(),
	)
