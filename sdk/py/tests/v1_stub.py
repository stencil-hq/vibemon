"""In-process gRPC + HTTP stub of the vmon v1 daemon for thin-SDK behavior tests.

The v1 API is gRPC (five services from ``proto/vmon/v1/api.proto``); health,
Prometheus metrics, and the ports proxy remain HTTP. One front listener
multiplexes both protocols on a single port by sniffing the HTTP/2 preface, so
``V1StubServer.url`` works for the SDK's gRPC channel and httpx client alike.
"""

from __future__ import annotations

import base64
import contextlib
import hashlib
import json
import os
import queue
import socket
import subprocess
import sys
import tempfile
import threading
from concurrent import futures
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, NoReturn
from urllib.parse import parse_qs, unquote, urlsplit

import grpc

from vmon.v1 import api_pb2, api_pb2_grpc
from vmon.wsframe import encode_frame, read_frame_sync

_HTTP_STATUS_TO_GRPC = {
    400: grpc.StatusCode.INVALID_ARGUMENT,
    401: grpc.StatusCode.UNAUTHENTICATED,
    404: grpc.StatusCode.NOT_FOUND,
    405: grpc.StatusCode.INVALID_ARGUMENT,
    409: grpc.StatusCode.ABORTED,
    500: grpc.StatusCode.UNAVAILABLE,
}


@dataclass
class RecordedRequest:
    method: str
    path: str
    query: dict[str, list[str]]
    headers: dict[str, str]
    json: Any
    body: bytes


class StubError(Exception):
    def __init__(self, status: int, code: str, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message


def _start_doc(start: api_pb2.ExecStart) -> dict[str, Any]:
    """Mirror a typed ExecStart back into the legacy exec body document shape."""
    doc: dict[str, Any] = {"cmd": [str(part) for part in start.cmd], "tty": bool(start.tty)}
    if start.HasField("workdir"):
        doc["workdir"] = start.workdir
    if start.env:
        doc["env"] = dict(start.env)
    if start.HasField("timeout"):
        doc["timeout"] = start.timeout
    return doc


def _view_json(view: object) -> api_pb2.JsonView:
    return api_pb2.JsonView(json=json.dumps(view, separators=(",", ":")))


class _ConnectionMux:
    """One listening port routed to the HTTP or gRPC backend by protocol preface."""

    def __init__(self, stub: V1StubServer, http_port: int, grpc_port: int) -> None:
        self._stub = stub
        self._http_port = http_port
        self._grpc_port = grpc_port
        self._listener = socket.create_server(("127.0.0.1", 0))
        self.port: int = self._listener.getsockname()[1]
        self._lock = threading.Lock()
        self._conns: set[socket.socket] = set()
        self._closed = False
        self._thread = threading.Thread(target=self._accept_loop, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            conns = list(self._conns)
            self._conns.clear()
        with contextlib.suppress(OSError):
            self._listener.close()
        for conn in conns:
            with contextlib.suppress(OSError):
                conn.close()

    def _accept_loop(self) -> None:
        while True:
            try:
                conn, _ = self._listener.accept()
            except OSError:
                return
            threading.Thread(target=self._pipe, args=(conn,), daemon=True).start()

    def _pipe(self, client: socket.socket) -> None:
        try:
            client.settimeout(10)
            preface = b""
            while len(preface) < 4:
                chunk = client.recv(4 - len(preface))
                if not chunk:
                    client.close()
                    return
                preface += chunk
            is_grpc = preface == b"PRI "
            if is_grpc and self._stub.drop_requests:
                # HTTP handles drop_requests itself (it records the request first);
                # gRPC connections are refused outright to force a transport error.
                client.close()
                return
            target = self._grpc_port if is_grpc else self._http_port
            backend = socket.create_connection(("127.0.0.1", target))
            client.settimeout(None)
            backend.sendall(preface)
        except OSError:
            with contextlib.suppress(OSError):
                client.close()
            return
        with self._lock:
            if self._closed:
                client.close()
                backend.close()
                return
            self._conns.update((client, backend))
        upstream = threading.Thread(target=self._copy, args=(client, backend), daemon=True)
        upstream.start()
        self._copy(backend, client)
        upstream.join(timeout=5)
        with self._lock:
            self._conns.discard(client)
            self._conns.discard(backend)
        for conn in (client, backend):
            with contextlib.suppress(OSError):
                conn.close()

    @staticmethod
    def _copy(src: socket.socket, dst: socket.socket) -> None:
        try:
            while True:
                data = src.recv(65536)
                if not data:
                    break
                dst.sendall(data)
        except OSError:
            pass
        with contextlib.suppress(OSError):
            dst.shutdown(socket.SHUT_WR)


class V1StubServer:
    """Small v1 gRPC + HTTP server for thin-SDK behavior tests."""

    def __init__(
        self,
        *,
        node_id: str = "node-a",
        peers: list[V1StubServer] | None = None,
    ) -> None:
        self.requests: list[RecordedRequest] = []
        self.sandboxes: dict[str, dict[str, Any]] = {}
        self.volumes: set[str] = set()
        self.pools: dict[str, dict[str, Any]] = {}
        self.snapshots: set[str] = set()
        self.events: list[object] = [{"type": "ready", "sequence": 1}]
        self.health_response: object = {"ok": True}
        self.sandbox_metrics_response: object = {"vcpu_exits": 7}
        self.server_info_response: object = {"version": "test", "capabilities": {}}
        self.required_token: str | None = None
        self.drop_requests = False
        self.node_id = node_id
        self.mesh_peers = list(peers or [])
        self.mesh_enabled = True
        self._started = False
        self._stopped = False
        self._lock = threading.RLock()
        self._next_id = 1
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), self._handler_class())
        self._server.stub = self  # type: ignore[attr-defined]
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._grpc = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
        api_pb2_grpc.add_SandboxServiceServicer_to_server(_SandboxService(self), self._grpc)
        api_pb2_grpc.add_SnapshotServiceServicer_to_server(_SnapshotService(self), self._grpc)
        api_pb2_grpc.add_VolumeServiceServicer_to_server(_VolumeService(self), self._grpc)
        api_pb2_grpc.add_PoolServiceServicer_to_server(_PoolService(self), self._grpc)
        api_pb2_grpc.add_SystemServiceServicer_to_server(_SystemService(self), self._grpc)
        grpc_port = self._grpc.add_insecure_port("127.0.0.1:0")
        self._mux = _ConnectionMux(self, self._server.server_address[1], grpc_port)

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self._mux.port}"

    def start(self) -> V1StubServer:
        """Start serving requests and return this stub."""
        if not self._started:
            self._thread.start()
            self._grpc.start()
            self._mux.start()
            self._started = True
        return self

    def stop(self) -> None:
        """Stop serving requests idempotently."""
        if not self._started or self._stopped:
            return
        self._stopped = True
        self._mux.stop()
        self._grpc.stop(grace=0)
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)

    def __enter__(self) -> V1StubServer:
        return self.start()

    def __exit__(self, *_exc: object) -> None:
        self.stop()

    def recorded(self, method: str | None = None, path: str | None = None) -> list[RecordedRequest]:
        with self._lock:
            rows = list(self.requests)
        if method is not None:
            rows = [row for row in rows if row.method == method]
        if path is not None:
            rows = [row for row in rows if row.path == path]
        return rows

    def last(self, method: str, path: str) -> RecordedRequest:
        rows = self.recorded(method, path)
        assert rows, (
            f"no recorded {method} {path}; saw {[(r.method, r.path) for r in self.requests]}"
        )
        return rows[-1]

    def rpcs(self, method: str, **fields: Any) -> list[RecordedRequest]:
        """Return recorded RPCs by ``Service/Method`` name, filtered on request fields."""
        rows = self.recorded("RPC", f"/vmon.v1.{method}")
        if fields:
            rows = [
                row
                for row in rows
                if isinstance(row.json, dict)
                and all(row.json.get(key) == value for key, value in fields.items())
            ]
        return rows

    def last_rpc(self, method: str, **fields: Any) -> RecordedRequest:
        rows = self.rpcs(method, **fields)
        assert rows, (
            f"no recorded RPC {method} {fields}; saw {[(r.method, r.path) for r in self.requests]}"
        )
        return rows[-1]

    # -- HTTP surface (health, metrics, ports proxy) ---------------------------------

    def _handler_class(self) -> type[BaseHTTPRequestHandler]:

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, _format: str, *_args: object) -> None:
                return

            def do_GET(self) -> None:
                if self.headers.get("Upgrade", "").lower() == "websocket":
                    self._handle_websocket()
                else:
                    self._handle_http()

            def do_POST(self) -> None:
                self._handle_http()

            def do_PUT(self) -> None:
                self._handle_http()

            def do_DELETE(self) -> None:
                self._handle_http()

            def do_PATCH(self) -> None:
                self._handle_http()

            def do_OPTIONS(self) -> None:
                self._handle_http()

            def do_HEAD(self) -> None:
                self._handle_http()

            def _handle_websocket(self) -> None:
                stub: V1StubServer = self.server.stub  # type: ignore[attr-defined]
                split = urlsplit(self.path)
                path = split.path
                headers = {key.lower(): value for key, value in self.headers.items()}
                with stub._lock:
                    stub.requests.append(
                        RecordedRequest("GET", path, parse_qs(split.query), headers, None, b"")
                    )
                try:
                    stub._require_auth(headers)
                    if path.startswith("/v1/sandboxes/") and "/ports/" in path:
                        encoded_id = path.removeprefix("/v1/sandboxes/").split("/", 1)[0]
                        stub._require_connect_token(unquote(encoded_id), parse_qs(split.query))
                except StubError as exc:
                    raw = json.dumps(
                        {"code": exc.code, "message": exc.message}, separators=(",", ":")
                    ).encode("utf-8")
                    self.send_response(exc.status)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(raw)))
                    self.end_headers()
                    self.wfile.write(raw)
                    self.close_connection = True
                    return

                key = self.headers.get("Sec-WebSocket-Key")
                if not key:
                    self.send_error(400, "missing websocket key")
                    self.close_connection = True
                    return
                accept = base64.b64encode(
                    hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest()
                ).decode()
                response = (
                    "HTTP/1.1 101 Switching Protocols\r\n"
                    "Upgrade: websocket\r\n"
                    "Connection: Upgrade\r\n"
                    f"Sec-WebSocket-Accept: {accept}\r\n\r\n"
                )
                self.connection.sendall(response.encode("latin-1"))
                try:
                    stub._serve_ports_websocket(self.connection, path)
                finally:
                    self.close_connection = True

            def _handle_http(self) -> None:
                stub: V1StubServer = self.server.stub  # type: ignore[attr-defined]
                split = urlsplit(self.path)
                path = split.path
                length = int(self.headers.get("Content-Length") or "0")
                body = self.rfile.read(length) if length else b""
                json_body: Any = None
                if body and "json" in self.headers.get("Content-Type", ""):
                    json_body = json.loads(body.decode("utf-8"))
                headers = {key.lower(): value for key, value in self.headers.items()}
                request = RecordedRequest(
                    self.command,
                    path,
                    parse_qs(split.query),
                    headers,
                    json_body,
                    body,
                )
                with stub._lock:
                    stub.requests.append(request)
                if stub.drop_requests:
                    self.close_connection = True
                    self.connection.close()
                    return
                try:
                    status, payload, response_headers, raw = stub._dispatch(request)
                except StubError as exc:
                    status = exc.status
                    payload = {"code": exc.code, "message": exc.message}
                    response_headers = {"Content-Type": "application/json"}
                    raw = json.dumps(payload).encode("utf-8")
                self.send_response(status)
                for key, value in response_headers.items():
                    self.send_header(key, value)
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                if raw and self.command != "HEAD":
                    self.wfile.write(raw)

        return Handler

    def _json_response(
        self, status: int, payload: Any, headers: dict[str, str] | None = None
    ) -> tuple[int, Any, dict[str, str], bytes]:
        raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        return status, payload, {"Content-Type": "application/json", **(headers or {})}, raw

    def _bytes_response(
        self, status: int, body: bytes, content_type: str = "application/octet-stream"
    ) -> tuple[int, None, dict[str, str], bytes]:
        return status, None, {"Content-Type": content_type}, body

    def _require_auth(self, headers: dict[str, str]) -> None:
        if self.required_token is None:
            return
        if headers.get("authorization") != f"Bearer {self.required_token}":
            raise StubError(401, "unauthorized", "invalid bearer token")

    def _require_connect_token(self, sandbox_id: str, query: dict[str, list[str]]) -> None:
        expected = f"token-{sandbox_id}"
        supplied = [
            value
            for key in ("connect_token", "token", "access_token")
            for value in query.get(key, [])
        ]
        if expected not in supplied:
            raise StubError(401, "unauthorized", "invalid connect token")

    def _dispatch(self, request: RecordedRequest) -> tuple[int, Any, dict[str, str], bytes]:
        self._require_auth(request.headers)
        if request.path == "/healthz" and request.method == "GET":
            return self._json_response(200, self.health_response)
        if request.path == "/metrics" and request.method == "GET":
            return self._bytes_response(
                200,
                b"# TYPE vmon_test_total counter\nvmon_test_total 1\n",
                "text/plain; version=0.0.4",
            )

        prefix = "/v1/sandboxes/"
        if request.path.startswith(prefix):
            rest = request.path[len(prefix) :]
            encoded_id, _, suffix = rest.partition("/")
            sandbox_id = unquote(encoded_id)
            with self._lock:
                sandbox = self.sandboxes.get(sandbox_id)
            if sandbox is None:
                raise StubError(404, "not_found", f"unknown sandbox {sandbox_id}")
            if suffix.startswith("ports/"):
                self._require_connect_token(sandbox_id, request.query)
                _, _, proxy_rest = suffix.partition("/")
                port_text, _, guest_path = proxy_rest.partition("/")
                return self._json_response(
                    200,
                    {
                        "method": request.method,
                        "port": int(port_text),
                        "path": unquote(guest_path),
                        "query": request.query,
                        "body_b64": base64.b64encode(request.body).decode("ascii"),
                    },
                )

        raise StubError(404, "not_found", f"unknown route {request.path}")

    def _serve_ports_websocket(self, sock: Any, path: str) -> None:
        if "/ports/" in path and "/ws" in path:
            opcode, payload = read_frame_sync(sock)
            if opcode in {0x1, 0x2}:
                sock.sendall(encode_frame(opcode, payload))
        with contextlib.suppress(Exception):
            sock.sendall(encode_frame(0x8, b""))

    # -- shared engine state ----------------------------------------------------------

    def _kill_procs(self, sandbox: dict[str, Any]) -> None:
        """Kill live exec bridge processes, like vmond does when a VM dies."""
        with self._lock:
            procs = list(sandbox.get("procs") or ())
            sandbox["procs"] = []
        for proc in procs:
            with contextlib.suppress(Exception):
                proc.kill()

    def _create_sandbox(self, payload: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            sandbox_id = str(payload.get("name") or f"sb-{self._next_id}")
            self._next_id += 1
            tags = {str(k): str(v) for k, v in (payload.get("tags") or {}).items()}
            ha = str(payload.get("ha") or "off")
            view = {
                "id": sandbox_id,
                "name": sandbox_id,
                "status": "running",
                "desired_state": "running",
                "observed_state": "running",
                "state_generation": 1,
                "lifecycle_failure": None,
                "created_at": 1_783_296_000.0,
                "last_active": 1_783_296_000.0,
                "terminated_at": None,
                "error": None,
                "tags": tags,
                "returncode": None,
                "workdir": payload.get("workdir") or "/work",
                "node": self.node_id,
                "ha": ha,
                "restart_policy": "rerun" if "rerun" in ha else "none",
            }
            secret_env: dict[str, str] = {}
            for item in payload.get("secrets") or []:
                if isinstance(item, dict):
                    raw_values = item.get("values")
                    values = raw_values if isinstance(raw_values, dict) else item
                    secret_env.update({str(k): str(v) for k, v in values.items()})
            self.sandboxes[sandbox_id] = {
                "view": view,
                "env": {str(k): str(v) for k, v in (payload.get("env") or {}).items()},
                "secret_env": secret_env,
                "files": {},
                "logs": "created\n",
                "volumes": payload.get("volumes") or {},
                "network": {
                    "block_network": bool(payload.get("block_network", False)),
                    "cidr_allow": list(payload.get("egress_allow") or []),
                    "domain_allow": list(payload.get("egress_allow_domains") or []),
                },
                "tunnels": {},
                "procs": [],
            }
        return view

    def _sandbox_or_error(self, sandbox_id: str) -> dict[str, Any]:
        with self._lock:
            sandbox = self.sandboxes.get(sandbox_id)
        if sandbox is None:
            raise StubError(404, "not_found", f"unknown sandbox {sandbox_id}")
        return sandbox

    def _execute(
        self, sandbox_id: str, request: dict[str, Any], stdin: bytes
    ) -> tuple[bytes, bytes, int]:
        del stdin
        with self._lock:
            sandbox = self.sandboxes[sandbox_id]
            files = sandbox["files"]
            env = dict(sandbox["env"])
            env.update(sandbox["secret_env"])
        env.update({str(k): str(v) for k, v in (request.get("env") or {}).items()})
        cmd = [str(part) for part in request.get("cmd") or []]
        if cmd == ["env"]:
            return json.dumps(env, sort_keys=True).encode("utf-8"), b"", 0
        if cmd and cmd[0] == "cat" and len(cmd) == 2:
            return files.get(cmd[1], b""), b"", 0
        if cmd and cmd[0] == "printf" and len(cmd) >= 2:
            return cmd[1].encode("utf-8"), b"", 0
        return b"", b"", 0

    # -- gRPC glue --------------------------------------------------------------------

    def _grpc_record(
        self,
        context: grpc.ServicerContext,
        method: str,
        doc: Any,
        body: bytes = b"",
    ) -> RecordedRequest:
        headers = dict(context.invocation_metadata() or ())
        request = RecordedRequest("RPC", method, {}, headers, doc, body)
        with self._lock:
            self.requests.append(request)
        return request

    def _grpc_auth(self, context: grpc.ServicerContext) -> None:
        headers = dict(context.invocation_metadata() or ())
        try:
            self._require_auth(headers)
        except StubError as exc:
            self._grpc_abort(context, exc)

    def _grpc_abort(self, context: grpc.ServicerContext, exc: StubError) -> NoReturn:
        context.set_trailing_metadata((("vmon-code", exc.code),))
        context.abort(_HTTP_STATUS_TO_GRPC.get(exc.status, grpc.StatusCode.UNKNOWN), exc.message)
        raise AssertionError("unreachable")  # pragma: no cover


class _SandboxService(api_pb2_grpc.SandboxServiceServicer):
    def __init__(self, stub: V1StubServer) -> None:
        self._stub = stub

    def _enter(self, context: grpc.ServicerContext, method: str, doc: Any) -> None:
        self._stub._grpc_record(context, f"/vmon.v1.SandboxService/{method}", doc)
        self._stub._grpc_auth(context)

    def _sandbox(self, context: grpc.ServicerContext, sandbox_id: str) -> dict[str, Any]:
        try:
            return self._stub._sandbox_or_error(sandbox_id)
        except StubError as exc:
            self._stub._grpc_abort(context, exc)

    def Create(self, request: api_pb2.CreateSandboxRequest, context: Any) -> api_pb2.JsonView:
        payload = json.loads(request.spec_json or "{}")
        self._enter(context, "Create", payload)
        return _view_json(self._stub._create_sandbox(payload))

    def List(
        self, request: api_pb2.ListSandboxesRequest, context: Any
    ) -> api_pb2.ListSandboxesResponse:
        tag_filters = list(request.tags)
        self._enter(context, "List", {"tags": tag_filters})
        with self._stub._lock:
            views = [dict(item["view"]) for item in self._stub.sandboxes.values()]
        if tag_filters:
            wanted = dict(item.split("=", 1) for item in tag_filters if "=" in item)
            views = [
                view
                for view in views
                if all(
                    str((view.get("tags") or {}).get(key)) == value for key, value in wanted.items()
                )
            ]
        return api_pb2.ListSandboxesResponse(
            sandboxes_json=[json.dumps(view, separators=(",", ":")) for view in views]
        )

    def Get(self, request: api_pb2.SandboxRef, context: Any) -> api_pb2.JsonView:
        self._enter(context, "Get", {"id": request.id})
        sandbox = self._sandbox(context, request.id)
        return _view_json(dict(sandbox["view"]))

    def Stop(self, request: api_pb2.StopSandboxRequest, context: Any) -> api_pb2.JsonView:
        self._enter(context, "Stop", {"id": request.id})
        return self._set_status(context, request.id, "stopped", kill=True)

    def Terminate(self, request: api_pb2.SandboxRef, context: Any) -> api_pb2.JsonView:
        self._enter(context, "Terminate", {"id": request.id})
        return self._set_status(context, request.id, "terminated", kill=True)

    def Pause(self, request: api_pb2.SandboxRef, context: Any) -> api_pb2.JsonView:
        self._enter(context, "Pause", {"id": request.id})
        return self._set_status(context, request.id, "paused", kill=False)

    def Resume(self, request: api_pb2.SandboxRef, context: Any) -> api_pb2.JsonView:
        self._enter(context, "Resume", {"id": request.id})
        return self._set_status(context, request.id, "running", kill=False)

    def Suspend(self, request: api_pb2.SandboxRef, context: Any) -> api_pb2.JsonView:
        self._enter(context, "Suspend", {"id": request.id})
        return self._set_status(context, request.id, "suspended", kill=True)

    def History(self, request: api_pb2.SandboxRef, context: Any) -> api_pb2.RecoveryPointList:
        self._enter(context, "History", {"id": request.id})
        self._sandbox(context, request.id)
        return api_pb2.RecoveryPointList(
            points=[
                api_pb2.RecoveryPoint(
                    name=f"recovery-{request.id}",
                    kind="checkpoint",
                    created_at_unix_millis=1_700_000_000_000,
                    size_bytes=1_024,
                )
            ]
        )

    def Rollback(self, request: api_pb2.RollbackSandboxRequest, context: Any) -> api_pb2.JsonView:
        self._enter(
            context,
            "Rollback",
            {"id": request.id, "recovery_point": request.recovery_point},
        )
        return self._set_status(context, request.id, "running", kill=False)

    def _set_status(
        self, context: Any, sandbox_id: str, status: str, *, kill: bool
    ) -> api_pb2.JsonView:
        sandbox = self._sandbox(context, sandbox_id)
        with self._stub._lock:
            view = sandbox["view"]
            view["status"] = status
            view["desired_state"] = status
            view["observed_state"] = status
            view["state_generation"] = int(view.get("state_generation") or 0) + 1
            view["lifecycle_failure"] = None
        if kill:
            self._stub._kill_procs(sandbox)
        return _view_json(dict(view))

    def Remove(self, request: api_pb2.SandboxRef, context: Any) -> api_pb2.JsonView:
        self._enter(context, "Remove", {"id": request.id})
        sandbox = self._sandbox(context, request.id)
        with self._stub._lock:
            self._stub.sandboxes.pop(request.id, None)
        self._stub._kill_procs(sandbox)
        return _view_json({"ok": True})

    def Extend(self, request: api_pb2.ExtendSandboxRequest, context: Any) -> api_pb2.JsonView:
        self._enter(context, "Extend", {"id": request.id, "secs": int(request.secs)})
        self._sandbox(context, request.id)
        return _view_json({"deadline_unix": 1_800_000_000})

    def SetIdleTimeout(
        self, request: api_pb2.SetIdleTimeoutRequest, context: Any
    ) -> api_pb2.JsonView:
        self._enter(
            context,
            "SetIdleTimeout",
            {
                "id": request.id,
                "idle_timeout_secs": (
                    float(request.idle_timeout_secs)
                    if request.HasField("idle_timeout_secs")
                    else None
                ),
            },
        )
        sandbox = self._sandbox(context, request.id)
        with self._stub._lock:
            sandbox["view"]["idle_timeout_secs"] = float(request.idle_timeout_secs)
            view = dict(sandbox["view"])
        return _view_json(view)

    def Metrics(self, request: api_pb2.SandboxRef, context: Any) -> api_pb2.JsonView:
        self._enter(context, "Metrics", {"id": request.id})
        self._sandbox(context, request.id)
        return _view_json(self._stub.sandbox_metrics_response)

    def Logs(self, request: api_pb2.LogsRequest, context: Any) -> Any:
        self._enter(context, "Logs", {"id": request.id, "follow": bool(request.follow)})
        sandbox = self._sandbox(context, request.id)
        yield api_pb2.LogChunk(data=sandbox["logs"].encode("utf-8"))

    def ExecCapture(
        self, request: api_pb2.ExecCaptureRequest, context: Any
    ) -> api_pb2.ExecCaptureResponse:
        doc = _start_doc(request.exec)
        self._enter(context, "ExecCapture", {"id": request.id, **doc})
        self._sandbox(context, request.id)
        stdout, stderr, code = self._stub._execute(request.id, doc, b"")
        return api_pb2.ExecCaptureResponse(code=code, stdout=stdout, stderr=stderr)

    def Exec(self, request_iterator: Any, context: Any) -> Any:
        first = next(request_iterator, None)
        if first is None or first.WhichOneof("input") != "start":
            self._stub._grpc_auth(context)
            self._stub._grpc_abort(
                context, StubError(400, "invalid", "expected an exec start frame")
            )
        start = first.start
        if not start.sandbox_id:
            self._stub._grpc_auth(context)
            self._stub._grpc_abort(
                context, StubError(400, "invalid", "exec start omitted the sandbox id")
            )
        doc = _start_doc(start)
        sandbox_id = start.sandbox_id
        self._stub._grpc_auth(context)
        with self._stub._lock:
            sandbox = self._stub.sandboxes.get(sandbox_id)
            files = dict(sandbox["files"]) if sandbox is not None else {}
        if sandbox is None:
            self._stub._grpc_record(
                context, "/vmon.v1.SandboxService/Exec", {"id": sandbox_id, **doc}
            )
            self._stub._grpc_abort(
                context, StubError(404, "not_found", f"unknown sandbox {sandbox_id}")
            )
        # The client blocks on initial metadata before handing out the session;
        # emit it now since no output frame may arrive until stdin is closed.
        context.send_initial_metadata(())
        cmd = doc["cmd"]
        if len(cmd) >= 3 and cmd[:2] == ["python3", "-u"] and cmd[2] in files:
            self._stub._grpc_record(
                context, "/vmon.v1.SandboxService/Exec", {"id": sandbox_id, **doc}
            )
            yield from _bridge_runner(
                self._stub, sandbox, doc, files[cmd[2]], request_iterator, context
            )
            return
        stdin = bytearray()
        for message in request_iterator:
            kind = message.WhichOneof("input")
            if kind == "stdin":
                stdin.extend(message.stdin)
            elif kind == "eof":
                break
        self._stub._grpc_record(
            context, "/vmon.v1.SandboxService/Exec", {"id": sandbox_id, **doc}, bytes(stdin)
        )
        stdout, stderr, code = self._stub._execute(sandbox_id, doc, bytes(stdin))
        if stdout:
            yield api_pb2.ExecOutput(
                chunk=api_pb2.Output(stream=api_pb2.STREAM_STDOUT, data=stdout)
            )
        if stderr:
            yield api_pb2.ExecOutput(
                chunk=api_pb2.Output(stream=api_pb2.STREAM_STDERR, data=stderr)
            )
        yield api_pb2.ExecOutput(exit=api_pb2.Exit(code=code))

    def Shell(self, request_iterator: Any, context: Any) -> Any:
        first = next(request_iterator, None)
        if first is None or first.WhichOneof("input") != "shell_params_json":
            self._stub._grpc_auth(context)
            self._stub._grpc_abort(
                context, StubError(400, "invalid", "expected shell params frame")
            )
        params = json.loads(first.shell_params_json or "{}")
        self._stub._grpc_record(context, "/vmon.v1.SandboxService/Shell", params)
        self._stub._grpc_auth(context)
        yield api_pb2.ExecOutput(ready=api_pb2.Ready(sandbox_id="shell-stub"))
        cmd = [str(part) for part in params.get("cmd") or []]
        stdout = cmd[1].encode("utf-8") if cmd[:1] == ["printf"] and len(cmd) > 1 else b""
        if stdout:
            yield api_pb2.ExecOutput(
                chunk=api_pb2.Output(stream=api_pb2.STREAM_STDOUT, data=stdout)
            )
        yield api_pb2.ExecOutput(exit=api_pb2.Exit(code=0))

    def Attach(self, request: api_pb2.SandboxRef, context: Any) -> Any:
        self._enter(context, "Attach", {"id": request.id})
        sandbox = self._sandbox(context, request.id)
        yield api_pb2.ExecOutput(
            chunk=api_pb2.Output(
                stream=api_pb2.STREAM_CONSOLE, data=sandbox["logs"].encode("utf-8")
            )
        )

    def FileRead(self, request: api_pb2.FilePathRequest, context: Any) -> api_pb2.FileContent:
        self._enter(context, "FileRead", {"id": request.id, "path": request.path})
        sandbox = self._sandbox(context, request.id)
        body = sandbox["files"].get(request.path)
        if body is None:
            self._stub._grpc_abort(
                context, StubError(404, "not_found", f"unknown file {request.path}")
            )
        return api_pb2.FileContent(data=body)

    def FileWrite(self, request: api_pb2.FileWriteRequest, context: Any) -> api_pb2.Ok:
        self._stub._grpc_record(
            context,
            "/vmon.v1.SandboxService/FileWrite",
            {"id": request.id, "path": request.path},
            request.data,
        )
        self._stub._grpc_auth(context)
        sandbox = self._sandbox(context, request.id)
        sandbox["files"][request.path] = request.data
        return api_pb2.Ok()

    def FileDelete(self, request: api_pb2.FileDeleteRequest, context: Any) -> api_pb2.Ok:
        self._enter(
            context,
            "FileDelete",
            {"id": request.id, "path": request.path, "recursive": bool(request.recursive)},
        )
        sandbox = self._sandbox(context, request.id)
        sandbox["files"].pop(request.path, None)
        return api_pb2.Ok()

    def FileList(self, request: api_pb2.FilePathRequest, context: Any) -> api_pb2.JsonView:
        self._enter(context, "FileList", {"id": request.id, "path": request.path})
        sandbox = self._sandbox(context, request.id)
        root = request.path.rstrip("/")
        entries = []
        for path, body in sorted(sandbox["files"].items()):
            if not root or root == "." or path.startswith(root + "/") or path == root:
                entries.append({"path": path, "size": len(body), "type": "file"})
        return _view_json({"entries": entries})

    def FileStat(self, request: api_pb2.FilePathRequest, context: Any) -> api_pb2.JsonView:
        self._enter(context, "FileStat", {"id": request.id, "path": request.path})
        sandbox = self._sandbox(context, request.id)
        body = sandbox["files"].get(request.path)
        if body is None:
            self._stub._grpc_abort(
                context, StubError(404, "not_found", f"unknown file {request.path}")
            )
        return _view_json({"path": request.path, "size": len(body), "type": "file"})

    def NetworkGet(self, request: api_pb2.SandboxRef, context: Any) -> api_pb2.JsonView:
        self._enter(context, "NetworkGet", {"id": request.id})
        sandbox = self._sandbox(context, request.id)
        return _view_json(dict(sandbox["network"]))

    def NetworkSet(self, request: api_pb2.NetworkSetRequest, context: Any) -> api_pb2.JsonView:
        update: dict[str, Any] = {}
        if request.HasField("block_network"):
            update["block_network"] = request.block_network
        if request.HasField("cidr_allow"):
            update["cidr_allow"] = list(request.cidr_allow.values)
        if request.HasField("domain_allow"):
            update["domain_allow"] = list(request.domain_allow.values)
        self._enter(context, "NetworkSet", {"id": request.id, **update})
        sandbox = self._sandbox(context, request.id)
        sandbox["network"].update(update)
        return _view_json(dict(sandbox["network"]))

    def Tunnels(self, request: api_pb2.SandboxRef, context: Any) -> api_pb2.JsonView:
        self._enter(context, "Tunnels", {"id": request.id})
        sandbox = self._sandbox(context, request.id)
        return _view_json(
            {"connect_token": f"token-{request.id}", "tunnels": dict(sandbox["tunnels"])}
        )

    def Migrate(self, request: api_pb2.MigrateRequest, context: Any) -> api_pb2.JsonView:
        self._enter(context, "Migrate", {"id": request.id, "target": request.target})
        sandbox = self._sandbox(context, request.id)
        with self._stub._lock:
            sandbox["view"]["node"] = request.target
        return _view_json(dict(sandbox["view"]))

    def Snapshot(self, request: api_pb2.SnapshotRequest, context: Any) -> api_pb2.JsonView:
        name = request.name if request.HasField("name") else ""
        self._enter(context, "Snapshot", {"id": request.id, "name": name or None})
        self._sandbox(context, request.id)
        name = name or f"snap-{request.id}"
        self._stub.snapshots.add(name)
        return _view_json({"snapshot": name, "dir": f"/snapshots/{name}"})

    def SnapshotFs(self, request: api_pb2.SnapshotFsRequest, context: Any) -> api_pb2.JsonView:
        name = request.name if request.HasField("name") else ""
        self._enter(context, "SnapshotFs", {"id": request.id, "name": name or None})
        self._sandbox(context, request.id)
        return _view_json({"image": name or f"fs-{request.id}"})


class _SnapshotService(api_pb2_grpc.SnapshotServiceServicer):
    def __init__(self, stub: V1StubServer) -> None:
        self._stub = stub

    def _enter(self, context: Any, method: str, doc: Any) -> None:
        self._stub._grpc_record(context, f"/vmon.v1.SnapshotService/{method}", doc)
        self._stub._grpc_auth(context)

    def List(self, request: api_pb2.ListSnapshotsRequest, context: Any) -> api_pb2.SnapshotList:
        self._enter(context, "List", {})
        return api_pb2.SnapshotList(snapshots=sorted(self._stub.snapshots))

    def Restore(self, request: api_pb2.RestoreSnapshotRequest, context: Any) -> api_pb2.JsonView:
        payload = dict(json.loads(request.body_json or "{}"))
        self._enter(context, "Restore", {"name": request.name, **payload})
        payload.setdefault("name", f"{request.name}-restored-{self._stub._next_id}")
        return _view_json(self._stub._create_sandbox(payload))

    def Fork(self, request: api_pb2.ForkSnapshotRequest, context: Any) -> api_pb2.JsonView:
        body = dict(json.loads(request.body_json or "{}"))
        self._enter(context, "Fork", {"name": request.name, **body})
        count = int(body.get("count", 0))
        clones: list[dict[str, Any]] = []
        for _ in range(count):
            payload = dict(body)
            payload["name"] = f"{request.name}-clone-{self._stub._next_id}"
            view = self._stub._create_sandbox(payload)
            clones.append({**view, "reconstruct_ms": 1})
        return _view_json({"clones": clones})

    def Delete(self, request: api_pb2.SnapshotRef, context: Any) -> api_pb2.Ok:
        self._enter(context, "Delete", {"name": request.name})
        self._stub.snapshots.discard(request.name)
        return api_pb2.Ok()


class _VolumeService(api_pb2_grpc.VolumeServiceServicer):
    def __init__(self, stub: V1StubServer) -> None:
        self._stub = stub

    def _enter(self, context: Any, method: str, doc: Any) -> None:
        self._stub._grpc_record(context, f"/vmon.v1.VolumeService/{method}", doc)
        self._stub._grpc_auth(context)

    def List(self, request: api_pb2.ListVolumesRequest, context: Any) -> api_pb2.VolumeList:
        self._enter(context, "List", {})
        with self._stub._lock:
            return api_pb2.VolumeList(volumes=sorted(self._stub.volumes))

    def Create(self, request: api_pb2.VolumeRef, context: Any) -> api_pb2.Ok:
        self._enter(context, "Create", {"name": request.name})
        with self._stub._lock:
            self._stub.volumes.add(request.name)
        return api_pb2.Ok()

    def Delete(self, request: api_pb2.VolumeRef, context: Any) -> api_pb2.Ok:
        self._enter(context, "Delete", {"name": request.name})
        with self._stub._lock:
            self._stub.volumes.discard(request.name)
        return api_pb2.Ok()


class _PoolService(api_pb2_grpc.PoolServiceServicer):
    def __init__(self, stub: V1StubServer) -> None:
        self._stub = stub

    def _enter(self, context: Any, method: str, doc: Any) -> None:
        self._stub._grpc_record(context, f"/vmon.v1.PoolService/{method}", doc)
        self._stub._grpc_auth(context)

    def List(self, request: api_pb2.ListPoolsRequest, context: Any) -> api_pb2.JsonView:
        self._enter(context, "List", {})
        return _view_json(dict(self._stub.pools))

    def Set(self, request: api_pb2.PoolSetRequest, context: Any) -> api_pb2.JsonView:
        body = dict(json.loads(request.body_json or "{}"))
        self._enter(context, "Set", {"reference": request.reference, **body})
        size = int(body.get("size", 0))
        stats = {"size": size, "ready": size}
        self._stub.pools[request.reference] = stats
        return _view_json(stats)

    def Delete(self, request: api_pb2.PoolRef, context: Any) -> api_pb2.Ok:
        self._enter(context, "Delete", {"reference": request.reference})
        self._stub.pools.pop(request.reference, None)
        return api_pb2.Ok()


class _SystemService(api_pb2_grpc.SystemServiceServicer):
    def __init__(self, stub: V1StubServer) -> None:
        self._stub = stub

    def _enter(self, context: Any, method: str, doc: Any) -> None:
        self._stub._grpc_record(context, f"/vmon.v1.SystemService/{method}", doc)
        self._stub._grpc_auth(context)

    def Info(self, request: api_pb2.InfoRequest, context: Any) -> api_pb2.JsonView:
        self._enter(context, "Info", {})
        return _view_json(self._stub.server_info_response)

    def Events(self, request: api_pb2.EventsRequest, context: Any) -> Any:
        self._enter(context, "Events", {})
        for event in self._stub.events:
            yield _view_json(event)

    def MeshStatus(self, request: api_pb2.MeshStatusRequest, context: Any) -> api_pb2.JsonView:
        self._enter(context, "MeshStatus", {})
        stub = self._stub
        self_node = {
            "node_id": stub.node_id,
            "advertise": stub.url,
            "region": "test",
            "sandboxes": [dict(sandbox["view"]) for sandbox in stub.sandboxes.values()],
        }
        peers = [
            {
                "node_id": peer.node_id,
                "advertise": peer.url,
                "region": "test",
            }
            for peer in stub.mesh_peers
        ]
        return _view_json(
            {
                "enabled": stub.mesh_enabled,
                "self": self_node,
                "peers": peers,
                "replicas_held": 0,
            }
        )


def _bridge_runner(
    stub: V1StubServer,
    sandbox: dict[str, Any],
    request: dict[str, Any],
    source: bytes,
    request_iterator: Any,
    context: Any,
) -> Any:
    """Run a guest runner script as a real local subprocess, bridged full-duplex.

    ``ExecInput`` stdin/eof frames feed the process stdin; its stdout and
    stderr stream back as ``ExecOutput`` chunk frames, exit as an exit frame.
    Cancelling the RPC kills the process, matching vmond's SIGTERM-on-disconnect
    behavior.
    """
    env = dict(os.environ)
    env.update(sandbox["env"])
    env.update(sandbox["secret_env"])
    env.update({str(k): str(v) for k, v in (request.get("env") or {}).items()})
    script = tempfile.NamedTemporaryFile("wb", suffix=".py", delete=False)
    script.write(source)
    script.close()
    proc = subprocess.Popen(
        [sys.executable, "-u", script.name],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
    )
    with stub._lock:
        sandbox["procs"].append(proc)
    outputs: queue.SimpleQueue[api_pb2.ExecOutput | None] = queue.SimpleQueue()

    def pump(stream: Any, kind: int) -> None:
        fd = stream.fileno()
        while True:
            try:
                chunk = os.read(fd, 65536)
            except OSError:
                return
            if not chunk:
                return
            outputs.put(api_pb2.ExecOutput(chunk=api_pb2.Output(stream=kind, data=chunk)))

    pumps = [
        threading.Thread(target=pump, args=(proc.stdout, api_pb2.STREAM_STDOUT), daemon=True),
        threading.Thread(target=pump, args=(proc.stderr, api_pb2.STREAM_STDERR), daemon=True),
    ]
    for thread in pumps:
        thread.start()

    def waiter() -> None:
        code = proc.wait()
        for thread in pumps:
            thread.join(timeout=5)
        outputs.put(api_pb2.ExecOutput(exit=api_pb2.Exit(code=code)))
        outputs.put(None)

    wait_thread = threading.Thread(target=waiter, daemon=True)
    wait_thread.start()

    def feed_stdin() -> None:
        try:
            for message in request_iterator:
                kind = message.WhichOneof("input")
                if kind == "stdin" and proc.stdin is not None:
                    with contextlib.suppress(Exception):
                        proc.stdin.write(message.stdin)
                        proc.stdin.flush()
                if kind == "eof" and proc.stdin is not None:
                    with contextlib.suppress(Exception):
                        proc.stdin.close()
        except Exception:
            return

    stdin_thread = threading.Thread(target=feed_stdin, daemon=True)
    stdin_thread.start()
    context.add_callback(proc.kill)
    try:
        while True:
            item = outputs.get()
            if item is None:
                return
            yield item
    finally:
        proc.kill()
        wait_thread.join(timeout=5)
        with contextlib.suppress(OSError):
            os.unlink(script.name)


def configure_context(
    monkeypatch: Any,
    home: Path,
    server: V1StubServer,
    *,
    name: str = "prod",
    token: str = "test-token",
) -> None:
    from vmon.context import Context, ContextStore, contexts_path

    monkeypatch.setenv("VMON_HOME", str(home))
    monkeypatch.setenv("VMON_CONTEXT", name)
    monkeypatch.delenv("VMON_API_TOKEN", raising=False)
    store = ContextStore(contexts_path(home))
    store.load()
    store.put(Context(name, [server.url]))
    store.use(name)
    store.save_token(name, token)
