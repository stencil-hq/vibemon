"""The gateway daemon: one MCP Streamable HTTP endpoint aggregating backends.

Serves `http://0.0.0.0:50005/mcp`; every request needs
`Authorization: Bearer <token>` (`/etc/mcp-gateway/.token`). Tools, prompts,
resources and resource templates of all ready backends are merged; a name
offered by two backends goes to the one configured first. Backend progress
is published in `/run/mcp-gateway/status.json` for `mcp-gateway --config`.
"""

from __future__ import annotations

import asyncio
import contextlib
import functools
import hmac
import json
import logging
import os
import signal
import socket
import weakref
from typing import Any

import mcp.types as types
import uvicorn
from mcp.server.lowlevel import NotificationOptions, Server
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
from mcp.shared.exceptions import McpError

from . import __version__, paths
from .backends import Backend

log = logging.getLogger("mcp-gateway")


class Gateway:
	def __init__(self, backends: list[Backend], token: str) -> None:
		self.backends = backends
		self.token = token
		self.http_ready = False
		self._shadowed: set[tuple[str, str]] = set()
		# Client sessions that listed something; told when the lists change.
		self._sessions: weakref.WeakSet = weakref.WeakSet()
		self._notify_tasks: set[asyncio.Task] = set()
		self.server = self._build_server()
		self.server.create_initialization_options = functools.partial(
			Server.create_initialization_options,
			self.server,
			NotificationOptions(prompts_changed=True, resources_changed=True, tools_changed=True),
		)

	def on_backend_change(self) -> None:
		"""A backend became ready or went away: publish it and notify clients."""
		self.write_status()
		task = asyncio.get_running_loop().create_task(self._notify_list_changed())
		self._notify_tasks.add(task)
		task.add_done_callback(self._notify_tasks.discard)

	async def _notify_list_changed(self) -> None:
		for session in list(self._sessions):
			for send in (
				session.send_tool_list_changed,
				session.send_prompt_list_changed,
				session.send_resource_list_changed,
			):
				try:
					await send()
				except Exception:  # noqa: BLE001 — the client went away
					self._sessions.discard(session)
					break

	def _remember_session(self) -> None:
		with contextlib.suppress(LookupError):
			self._sessions.add(self.server.request_context.session)

	# ---- status -----------------------------------------------------------

	def write_status(self) -> None:
		status = {
			"pid": os.getpid(),
			"http": self.http_ready,
			"backends": {b.name: {"state": b.state, "error": b.error} for b in self.backends},
		}
		tmp = paths.STATUS_FILE.with_suffix(".tmp")
		tmp.write_text(json.dumps(status))
		os.replace(tmp, paths.STATUS_FILE)

	# ---- aggregation ------------------------------------------------------

	def _ready(self) -> list[Backend]:
		return [b for b in self.backends if b.session is not None]

	async def _collect(self, method: str, attr: str) -> list[tuple[Backend, Any]]:
		"""Call a paginated list method on every ready backend."""

		async def one(b: Backend) -> list[tuple[Backend, Any]]:
			if b.session is None:
				return []
			caps = b.session.get_server_capabilities() if hasattr(b.session, "get_server_capabilities") else None
			cap_name = {"list_tools": "tools", "list_prompts": "prompts"}.get(method, "resources")
			if caps is not None and getattr(caps, cap_name, None) is None:
				return []
			items: list[tuple[Backend, Any]] = []
			cursor = None
			try:
				while True:
					res = await getattr(b.session, method)(cursor) if cursor else await getattr(b.session, method)()
					items += [(b, x) for x in getattr(res, attr)]
					cursor = res.nextCursor
					if not cursor:
						break
			except Exception as e:  # noqa: BLE001 — one broken backend must not hide the others
				log.warning("%s: %s failed: %s", b.name, method, e)
			return items

		results = await asyncio.gather(*(one(b) for b in self._ready()))
		return [x for r in results for x in r]

	def _dedupe(self, items: list[tuple[Backend, Any]], key) -> dict[str, tuple[Backend, Any]]:
		out: dict[str, tuple[Backend, Any]] = {}
		for b, item in items:
			k = key(item)
			if k in out:
				if (b.name, k) not in self._shadowed:
					self._shadowed.add((b.name, k))
					log.warning("%s: %r is already provided by %s; ignored", b.name, k, out[k][0].name)
				continue
			out[k] = (b, item)
		return out

	async def tools(self) -> dict[str, tuple[Backend, types.Tool]]:
		return self._dedupe(await self._collect("list_tools", "tools"), lambda t: t.name)

	async def prompts(self) -> dict[str, tuple[Backend, types.Prompt]]:
		return self._dedupe(await self._collect("list_prompts", "prompts"), lambda p: p.name)

	async def resources(self) -> dict[str, tuple[Backend, types.Resource]]:
		return self._dedupe(await self._collect("list_resources", "resources"), lambda r: str(r.uri))

	def _build_server(self) -> Server:
		server: Server = Server("mcp-gateway", version=__version__)

		@server.list_tools()
		async def list_tools() -> list[types.Tool]:
			self._remember_session()
			return [t for _, t in (await self.tools()).values()]

		@server.call_tool(validate_input=False)
		async def call_tool(name: str, arguments: dict[str, Any]) -> types.CallToolResult:
			hit = (await self.tools()).get(name)
			if hit is None or hit[0].session is None:
				return types.CallToolResult(
					content=[types.TextContent(type="text", text=f"Unknown tool: {name}")], isError=True
				)
			return await hit[0].session.call_tool(name, arguments)

		@server.list_prompts()
		async def list_prompts() -> list[types.Prompt]:
			self._remember_session()
			return [p for _, p in (await self.prompts()).values()]

		@server.get_prompt()
		async def get_prompt(name: str, arguments: dict[str, str] | None) -> types.GetPromptResult:
			hit = (await self.prompts()).get(name)
			if hit is None or hit[0].session is None:
				raise McpError(types.ErrorData(code=types.INVALID_PARAMS, message=f"Unknown prompt: {name}"))
			return await hit[0].session.get_prompt(name, arguments)

		@server.list_resources()
		async def list_resources() -> list[types.Resource]:
			self._remember_session()
			return [r for _, r in (await self.resources()).values()]

		@server.list_resource_templates()
		async def list_resource_templates() -> list[types.ResourceTemplate]:
			items = await self._collect("list_resource_templates", "resourceTemplates")
			return [t for _, t in self._dedupe(items, lambda t: t.uriTemplate).values()]

		@server.read_resource()
		async def read_resource(uri) -> list:
			hit = (await self.resources()).get(str(uri))
			candidates = [hit[0]] if hit is not None else self._ready()
			last: Exception | None = None
			for b in candidates:
				if b.session is None:
					continue
				try:
					res = await b.session.read_resource(uri)
				except McpError as e:
					last = e
					continue
				return [_resource_contents(c) for c in res.contents]
			raise last or McpError(types.ErrorData(code=types.INVALID_PARAMS, message=f"Unknown resource: {uri}"))

		return server

	# ---- HTTP -------------------------------------------------------------

	def asgi(self, manager: StreamableHTTPSessionManager):
		expected = f"Bearer {self.token}".encode()

		async def app(scope, receive, send):
			if scope["type"] == "lifespan":
				await _lifespan(manager, receive, send)
				return
			if scope["type"] != "http":
				return
			path = scope.get("path", "")
			if path.rstrip("/") != paths.MCP_PATH:
				await _plain(send, 404, b"not found")
				return
			auth = dict(scope.get("headers") or []).get(b"authorization", b"")
			if not hmac.compare_digest(auth, expected):
				await _plain(send, 401, b"unauthorized", [(b"www-authenticate", b'Bearer realm="mcp-gateway"')])
				return
			await manager.handle_request(scope, receive, send)

		return app

	async def serve(self) -> None:
		manager = StreamableHTTPSessionManager(app=self.server, stateless=False)
		config = uvicorn.Config(
			self.asgi(manager),
			log_level="info",
			access_log=False,
			lifespan="on",
			timeout_graceful_shutdown=5,
		)
		userver = uvicorn.Server(config)
		userver.install_signal_handlers = lambda: None
		sock = _listen(paths.PORT)

		stop = asyncio.Event()
		loop = asyncio.get_running_loop()
		for sig in (signal.SIGTERM, signal.SIGINT):
			loop.add_signal_handler(sig, stop.set)

		tasks = [asyncio.create_task(b.run(self.on_backend_change), name=b.name) for b in self.backends]
		http = asyncio.create_task(userver.serve(sockets=[sock]))
		while not userver.started and not http.done():
			await asyncio.sleep(0.05)
		self.http_ready = userver.started
		self.write_status()
		log.info("listening on :%d%s (%d backend(s))", paths.PORT, paths.MCP_PATH, len(self.backends))
		await asyncio.wait([http, asyncio.create_task(stop.wait())], return_when=asyncio.FIRST_COMPLETED)
		log.info("shutting down")
		userver.should_exit = True
		for b in self.backends:
			b.stop()
		with contextlib.suppress(asyncio.TimeoutError):
			await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), 15)
		with contextlib.suppress(asyncio.TimeoutError):
			await asyncio.wait_for(http, 10)


def _resource_contents(c):
	from mcp.server.lowlevel.helper_types import ReadResourceContents

	if isinstance(c, types.TextResourceContents):
		return ReadResourceContents(content=c.text, mime_type=c.mimeType)
	import base64

	return ReadResourceContents(content=base64.b64decode(c.blob), mime_type=c.mimeType)


def _listen(port: int) -> socket.socket:
	"""Dual-stack listener when the kernel has IPv6, else IPv4."""
	try:
		sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
		sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
		addr: tuple = ("::", port)
	except OSError:
		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		addr = ("0.0.0.0", port)
	sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	sock.bind(addr)
	sock.listen(128)
	sock.set_inheritable(True)
	return sock


async def _plain(send, status: int, body: bytes, extra: list | None = None) -> None:
	headers = [(b"content-type", b"text/plain"), (b"content-length", str(len(body)).encode()), *(extra or [])]
	await send({"type": "http.response.start", "status": status, "headers": headers})
	await send({"type": "http.response.body", "body": body})


async def _lifespan(manager: StreamableHTTPSessionManager, receive, send) -> None:
	msg = await receive()
	assert msg["type"] == "lifespan.startup"
	async with manager.run():
		await send({"type": "lifespan.startup.complete"})
		msg = await receive()
	await send({"type": "lifespan.shutdown.complete"})
