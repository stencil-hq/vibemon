"""Bound sandbox resources for the Python SDK."""

from __future__ import annotations

import asyncio
import builtins
import json
import math
import socket
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, TypeVar, overload

import grpc
import httpx

from ._endpoint import (
    GrpcStubs,
    WebSocketConnection,
    decode_view,
    path_segment,
    translate_rpc_error,
)
from .driver import response_endpoint
from .errors import APIError, ProtocolError
from .process import (
    ConsoleStream,
    LogStream,
    Process,
    PtyStream,
    open_process,
    open_pty_stream,
)
from .secret import Secret, merge_secrets
from .v1 import api_pb2
from .volume import S3Mount, Volume

T = TypeVar("T")

if TYPE_CHECKING:
    from .client import Client
    from .models import (
        FileInfo,
        RecoveryPoint,
        SandboxInfo,
        SandboxMetrics,
        SandboxNetworkPolicy,
        TunnelSet,
    )


@dataclass(frozen=True, slots=True)
class ExecResult:
    """Captured output and exit status from a non-streaming command."""

    returncode: int
    stdout: bytes
    stderr: bytes


def _coerce_cmd(cmd: tuple[str | Iterable[str], ...]) -> list[str]:
    if len(cmd) == 1 and not isinstance(cmd[0], str):
        return [str(part) for part in cmd[0]]
    return [str(part) for part in cmd]


def _drop_none(data: Mapping[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in data.items() if value is not None}


def _path_tail(value: str) -> str:
    return "/".join(path_segment(part) for part in value.lstrip("/").split("/"))


def _exec_start(body: Mapping[str, Any], *, sandbox_id: str | None = None) -> api_pb2.ExecStart:
    """Build a typed ExecStart from an exec body document."""
    start = api_pb2.ExecStart(
        cmd=[str(part) for part in body["cmd"]],
        tty=bool(body.get("tty", False)),
    )
    if sandbox_id is not None:
        start.sandbox_id = sandbox_id
    workdir = body.get("workdir")
    if workdir is not None:
        start.workdir = str(workdir)
    env = body.get("env")
    if env:
        start.env.update({str(key): str(value) for key, value in env.items()})
    timeout = body.get("timeout")
    if timeout is not None:
        start.timeout = float(timeout)
    return start


def _secret_wire(
    secrets: Iterable[Secret | Mapping[str, object]] | None,
) -> list[dict[str, Any]] | None:
    if secrets is None:
        return None
    wire: list[dict[str, Any]] = []
    for item in secrets:
        if isinstance(item, Secret):
            wire.append(item.to_wire())
        elif isinstance(item, Mapping):
            wire.append(Secret.from_dict(item).to_wire())
        else:
            raise TypeError(f"expected Secret or mapping, got {type(item).__name__}")
    return wire


def _volume_name(value: Any) -> str:
    if isinstance(value, Volume):
        return value.name
    if isinstance(value, str):
        return Volume(value).name
    raise TypeError("volume spec must be a Volume, name string, tuple, or mapping")


def _volume_wire(volumes: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if volumes is None:
        return None
    wire: dict[str, Any] = {}
    for mount, value in volumes.items():
        if isinstance(value, tuple):
            if len(value) != 2:
                raise TypeError("volume tuple must be (Volume, read_only)")
            name = _volume_name(value[0])
            read_only = bool(value[1])
        elif isinstance(value, Mapping):
            name_value = value.get("name")
            if not isinstance(name_value, str):
                raise TypeError("volume mapping requires a string 'name'")
            name = Volume(name_value).name
            read_only = bool(value.get("read_only", value.get("ro", False)))
        else:
            name = _volume_name(value)
            read_only = False
        wire[str(mount)] = {"name": name, "read_only": True} if read_only else name
    return wire


def _s3_mount_wire(
    s3_mounts: Mapping[str, S3Mount | str] | None,
) -> dict[str, dict[str, object]] | None:
    if s3_mounts is None:
        return None
    wire: dict[str, dict[str, object]] = {}
    for mount, value in s3_mounts.items():
        if isinstance(value, S3Mount):
            wire[str(mount)] = value.to_wire()
        elif isinstance(value, str):
            wire[str(mount)] = {"uri": value}
        else:
            raise TypeError("S3 mount spec must be an S3Mount or URI string")
    return wire


def _clone_create_extra(kwargs: Mapping[str, Any]) -> dict[str, Any]:
    allowed = {
        "image",
        "template",
        "dockerfile",
        "context",
        "name",
        "sandbox_name",
        "agent",
        "cpus",
        "memory",
        "disk_mb",
        "timeout",
        "timeout_secs",
        "idle_timeout_secs",
        "activity_threshold_bytes",
        "persistence",
        "nics",
        "workdir",
        "env",
        "secrets",
        "volumes",
        "s3_mounts",
        "tags",
        "fs_dir",
        "block_network",
        "ports",
        "egress_allow",
        "egress_allow_domains",
        "inbound_cidr_allowlist",
        "readiness_probe",
        "pool_size",
        "ha",
        "arch",
        "idempotency_key",
        "command",
    }
    unknown = sorted(set(kwargs) - allowed)
    if unknown:
        raise TypeError(f"unsupported sandbox option(s): {', '.join(unknown)}")
    extra = {key: value for key, value in kwargs.items() if value is not None}
    if "sandbox_name" in extra:
        if "name" in extra:
            raise TypeError("name and sandbox_name are mutually exclusive")
        extra["name"] = extra.pop("sandbox_name")
    return extra


class Files:
    """Filesystem operations bound to one sandbox."""

    def __init__(self, sandbox: Sandbox) -> None:
        self._sandbox = sandbox
        self._aio = _AsyncFiles(self)

    @property
    def aio(self) -> _AsyncFiles:
        """Return thread-backed async filesystem operations."""
        return self._aio

    def read_bytes(self, path: str) -> bytes:
        """Read an entire guest file as bytes."""
        sandbox = self._sandbox
        return sandbox._rpc(
            lambda stubs: stubs.sandbox.FileRead(api_pb2.FilePathRequest(id=sandbox.id, path=path))
        ).data

    def read_text(self, path: str, encoding: str = "utf-8") -> str:
        """Read and decode an entire guest text file."""
        return self.read_bytes(path).decode(encoding)

    def write_bytes(
        self,
        path: str,
        data: bytes | bytearray | memoryview,
        mode: int = 0o644,
    ) -> None:
        """Replace a guest file with the supplied bytes."""
        del mode
        sandbox = self._sandbox
        payload = bytes(data)
        sandbox._rpc(
            lambda stubs: stubs.sandbox.FileWrite(
                api_pb2.FileWriteRequest(id=sandbox.id, path=path, data=payload)
            )
        )

    def write_text(
        self,
        path: str,
        text: str,
        encoding: str = "utf-8",
        mode: int = 0o644,
    ) -> None:
        """Encode and replace a guest text file."""
        self.write_bytes(path, text.encode(encoding), mode=mode)

    def list(self, path: str = ".") -> list[FileInfo]:
        """List typed directory entries at a guest path."""
        sandbox = self._sandbox
        payload = sandbox._rpc(
            lambda stubs: stubs.sandbox.FileList(api_pb2.FilePathRequest(id=sandbox.id, path=path))
        ).json
        try:
            value = json.loads(payload)
        except ValueError as exc:
            raise ProtocolError("file list returned a malformed response") from exc
        entries = value.get("entries", value) if isinstance(value, Mapping) else value
        if not isinstance(entries, list) or not all(isinstance(item, Mapping) for item in entries):
            raise ProtocolError("file list returned a malformed response")
        from .models import FileInfo

        return [FileInfo.from_dict(item) for item in entries]

    def stat(self, path: str) -> FileInfo:
        """Return typed metadata for one guest path."""
        sandbox = self._sandbox
        value = sandbox._view_rpc(
            lambda stubs: stubs.sandbox.FileStat(api_pb2.FilePathRequest(id=sandbox.id, path=path)),
            error="file stat returned a malformed response",
        )
        from .models import FileInfo

        return FileInfo.from_dict(value)

    def delete(self, path: str, recursive: bool = False) -> None:
        """Delete a guest path, optionally recursively."""
        sandbox = self._sandbox
        sandbox._rpc(
            lambda stubs: stubs.sandbox.FileDelete(
                api_pb2.FileDeleteRequest(id=sandbox.id, path=path, recursive=bool(recursive))
            )
        )

    def mkdir(self, path: str, parents: bool = True) -> None:
        """Create a directory through the guest agent's exec service."""
        command = ["mkdir"]
        if parents:
            command.append("-p")
        command.append(path)
        result = self._sandbox.run(command)
        if result.returncode != 0:
            raise APIError(
                result.stderr.decode("utf-8", "replace") or "mkdir failed",
                code="engine",
            )

    def open(self, path: str) -> FileStream:
        """Open a closeable stream over a guest file's contents."""
        return FileStream(self.read_bytes(path))


class FileStream(Iterable[bytes]):
    """A closeable in-memory stream over one guest file's contents."""

    def __init__(self, data: bytes) -> None:
        self._data = data
        self._consumed = False

    def read(self, size: int = -1) -> bytes:
        """Read up to ``size`` bytes, or everything that remains."""
        if self._consumed:
            return b""
        if size < 0 or size >= len(self._data):
            data = self._data
            self._data = b""
            self._consumed = not data
            return data
        data = self._data[:size]
        self._data = self._data[size:]
        return data

    def iter_bytes(self) -> Iterable[bytes]:
        """Yield the remaining contents as one chunk."""
        data = self.read()
        if data:
            yield data

    def __iter__(self) -> Any:
        return self.iter_bytes()

    def close(self) -> None:
        """Release the buffered contents idempotently."""
        self._data = b""
        self._consumed = True

    def __enter__(self) -> FileStream:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


class Ports:
    """HTTP and WebSocket proxies bound to one sandbox's exposed ports."""

    def __init__(self, sandbox: Sandbox) -> None:
        self._sandbox = sandbox
        self._aio = _AsyncPorts(self)

    @property
    def aio(self) -> _AsyncPorts:
        """Return thread-backed async port proxy operations."""
        return self._aio

    def http(
        self,
        port: int,
        method: str,
        path: str = "",
        *,
        params: Any | None = None,
        headers: Mapping[str, str] | None = None,
        content: bytes | None = None,
        json: Any | None = None,
    ) -> httpx.Response:
        """Proxy one HTTP request to an exposed guest port."""
        port = _validate_port(port)
        method = method.upper()
        if method not in {"GET", "PUT", "POST", "DELETE", "OPTIONS", "HEAD", "PATCH"}:
            raise ValueError(f"unsupported proxy HTTP method {method!r}")
        tail = _path_tail(path)
        suffix = f"/{tail}" if tail else ""
        response = self._sandbox._request(
            method,
            f"{self._sandbox._base_path}/ports/{port}{suffix}",
            params=self._sandbox._proxy_params(params),
            headers={str(key): str(value) for key, value in (headers or {}).items()},
            content=content,
            json=json,
            raise_for_status=False,
        )
        if isinstance(response, tuple):
            raise ProtocolError("port proxy returned an unexpected response")
        return response

    def websocket(
        self,
        port: int,
        path: str = "",
        *,
        params: Any | None = None,
    ) -> WebSocketConnection:
        """Open a WebSocket proxied to an exposed guest port."""
        port = _validate_port(port)
        tail = _path_tail(path)
        suffix = f"/{tail}" if tail else ""
        result = self._sandbox._request(
            "WS",
            f"{self._sandbox._base_path}/ports/{port}/ws{suffix}",
            params=self._sandbox._proxy_params(params),
            websocket=True,
        )
        if not isinstance(result, tuple):
            raise ProtocolError("port proxy did not return a WebSocket")
        return result[0]


def _validate_port(port: int) -> int:
    value = int(port)
    if not 0 <= value <= 65535:
        raise ValueError("proxy port must be between 0 and 65535")
    return value


class Sandbox:
    """A sandbox resource bound to its client; mesh routing remains private."""

    def __init__(self, client: Client, info: Mapping[str, Any]) -> None:
        identifier = info.get("id") or info.get("name")
        if not isinstance(identifier, str) or not identifier:
            raise ValueError("sandbox id is required")
        self._client = client
        self._raw = dict(info)
        self._raw.setdefault("id", identifier)
        self._endpoint: str | None = None
        self._workdir = self._string_detail("workdir")
        raw_env = self._raw.get("env")
        self._env = (
            {str(key): str(value) for key, value in raw_env.items()}
            if isinstance(raw_env, Mapping)
            else {}
        )
        self._secret_env: dict[str, str] = {}
        self._terminated = False
        self._removed = False
        self._connect_token: str | None = None
        self.files = Files(self)
        self.ports = Ports(self)
        self._aio = _AsyncSandbox(self)

    @classmethod
    def _from_route(
        cls,
        client: Client,
        info: Mapping[str, Any],
        endpoint: str | None,
    ) -> Sandbox:
        sandbox = cls(client, info)
        sandbox._endpoint = endpoint
        return sandbox

    @property
    def id(self) -> str:
        """Return the stable sandbox identifier."""
        return str(self._raw["id"])

    @property
    def info(self) -> SandboxInfo:
        """Return a typed snapshot of the latest server view."""
        from .models import SandboxInfo

        return SandboxInfo.from_dict(self._raw)

    @property
    def node(self) -> str | None:
        """Return the mesh node that reported the latest sandbox view."""
        return self._string_detail("node")

    @property
    def aio(self) -> _AsyncSandbox:
        """Return a thread-backed async facade over every sandbox operation."""
        return self._aio

    @property
    def _base_path(self) -> str:
        return f"/v1/sandboxes/{path_segment(self.id)}"

    def __repr__(self) -> str:
        return f"Sandbox(id={self.id!r})"

    def __str__(self) -> str:
        return self.id

    def _bind_defaults(
        self,
        *,
        workdir: str | None,
        env: Mapping[str, str] | None,
        tags: Mapping[str, str] | None,
        secrets: Iterable[Secret | Mapping[str, object]] | None,
    ) -> None:
        self._workdir = workdir or self._workdir
        if env is not None:
            self._env = {str(key): str(value) for key, value in env.items()}
        if tags is not None:
            self._raw["tags"] = {str(key): str(value) for key, value in tags.items()}
        self._secret_env = merge_secrets(secrets)

    def _string_detail(self, key: str) -> str | None:
        value = self._raw.get(key)
        return value if isinstance(value, str) else None

    def _rpc(self, fn: Callable[[GrpcStubs], T], *, stream: bool = False) -> T:
        """Invoke one sandbox-scoped RPC, re-resolving the endpoint on not_found."""

        def send() -> tuple[T, str]:
            return self._client.driver.call(fn, endpoint=self._endpoint, stream=stream)

        try:
            result, endpoint = send()
        except APIError as original:
            if original.code != "not_found" or len(self._client.driver.endpoints()) <= 1:
                raise
            try:
                self._endpoint = self._client.driver.resolve_sandbox(self.id, self._endpoint)
            except APIError:
                raise original from None
            result, endpoint = send()
        self._endpoint = endpoint
        return result

    def _view_rpc(
        self, fn: Callable[[GrpcStubs], api_pb2.JsonView], *, error: str
    ) -> dict[str, Any]:
        return decode_view(self._rpc(fn).json, error)

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Any | None = None,
        json: Any | None = None,
        content: bytes | None = None,
        headers: dict[str, str] | None = None,
        websocket: bool = False,
        raise_for_status: bool = True,
    ) -> httpx.Response | tuple[WebSocketConnection, str]:
        """Issue one HTTP or WebSocket ports-proxy request pinned to this sandbox."""

        def send() -> httpx.Response | tuple[WebSocketConnection, str]:
            if websocket:
                return self._client.driver.websocket(
                    path,
                    query=params,
                    endpoint=self._endpoint,
                )
            return self._client.driver.request(
                method,
                path,
                params=params,
                json=json,
                content=content,
                headers=headers,
                endpoint=self._endpoint,
                raise_for_status=raise_for_status,
            )

        try:
            result = send()
        except APIError as original:
            if (
                original.code != "not_found"
                and original.status != 404
                or len(self._client.driver.endpoints()) <= 1
            ):
                raise
            try:
                self._endpoint = self._client.driver.resolve_sandbox(self.id, self._endpoint)
            except APIError:
                raise original from None
            result = send()

        if isinstance(result, tuple):
            self._endpoint = result[1]
        else:
            selected = response_endpoint(result)
            if selected is not None:
                self._endpoint = selected
        return result

    def refresh(self) -> SandboxInfo:
        """Refresh this object's server view and return its typed form."""
        value = self._view_rpc(
            lambda stubs: stubs.sandbox.Get(api_pb2.SandboxRef(id=self.id)),
            error="inspect returned a non-object response",
        )
        self._update(value, "inspect returned a non-object response")
        return self.info

    def run(
        self,
        *cmd: str | Iterable[str],
        workdir: str | None = None,
        env: Mapping[str, str] | None = None,
        timeout: float | None = None,
        tty: bool = False,
    ) -> ExecResult:
        """Run a command to completion and capture stdout, stderr, and exit status."""
        body = self._exec_body(cmd, workdir=workdir, env=env, timeout=timeout, tty=tty)
        request = api_pb2.ExecCaptureRequest(id=self.id, exec=_exec_start(body))
        response = self._rpc(lambda stubs: stubs.sandbox.ExecCapture(request))
        return ExecResult(
            returncode=response.code,
            stdout=response.stdout,
            stderr=response.stderr,
        )

    def exec(
        self,
        *cmd: str | Iterable[str],
        workdir: str | None = None,
        env: Mapping[str, str] | None = None,
        timeout: float | None = None,
        pty: bool = False,
        tty: bool = False,
    ) -> Process:
        """Open a streaming command session."""
        use_tty = bool(pty or tty)
        body = self._exec_body(
            cmd,
            workdir=workdir,
            env=env,
            timeout=timeout,
            tty=use_tty,
        )
        start = _exec_start(body, sandbox_id=self.id)

        def starter(
            make_inputs: Callable[[], Iterable[api_pb2.ExecInput]],
        ) -> tuple[Any, str]:
            return self._client.driver.call(
                lambda stubs: stubs.sandbox.Exec(make_inputs()),
                endpoint=self._endpoint,
                stream=True,
            )

        return open_process(
            starter,
            api_pb2.ExecInput(start=start),
            timeout=timeout,
            tty=use_tty,
            thread_name=f"vmon-exec-{self.id}",
        )

    def _exec_body(
        self,
        cmd: tuple[str | Iterable[str], ...],
        *,
        workdir: str | None,
        env: Mapping[str, str] | None,
        timeout: float | None,
        tty: bool,
    ) -> dict[str, Any]:
        argv = _coerce_cmd(cmd)
        if not argv:
            raise ValueError("exec command must not be empty")
        merged_env = {**self._env, **self._secret_env}
        if env:
            merged_env.update({str(key): str(value) for key, value in env.items()})
        return _drop_none(
            {
                "cmd": argv,
                "workdir": workdir or self._workdir,
                "env": merged_env or None,
                "timeout": timeout,
                "tty": tty,
            }
        )

    def attach(self) -> ConsoleStream:
        """Open a live read-only console stream."""
        responses = self._rpc(
            lambda stubs: stubs.sandbox.Attach(api_pb2.SandboxRef(id=self.id)),
            stream=True,
        )
        return ConsoleStream(responses, self._endpoint)

    @overload
    def logs(self, follow: Literal[False] = False) -> str: ...

    @overload
    def logs(self, follow: Literal[True]) -> LogStream: ...

    def logs(self, follow: bool = False) -> str | LogStream:
        """Return buffered logs or open a closeable live log stream."""
        chunks = self._rpc(
            lambda stubs: stubs.sandbox.Logs(api_pb2.LogsRequest(id=self.id, follow=bool(follow))),
            stream=True,
        )
        if follow:
            return LogStream(chunks, self._endpoint)
        try:
            data = b"".join(chunk.data for chunk in chunks)
        except grpc.RpcError as exc:
            raise translate_rpc_error(
                exc, endpoint=self._endpoint, fallback_message="logs failed"
            ) from exc
        return data.decode("utf-8", errors="replace")

    def metrics(self) -> SandboxMetrics:
        """Return this sandbox's typed runtime metrics."""
        from .models import SandboxMetrics

        value = self._view_rpc(
            lambda stubs: stubs.sandbox.Metrics(api_pb2.SandboxRef(id=self.id)),
            error="sandbox metrics returned a non-object response",
        )
        return SandboxMetrics.from_dict(value)

    def stop(self, wait: bool = True) -> None:
        """Stop the sandbox while retaining its server record."""
        del wait
        self._update_optional(
            self._view_rpc(
                lambda stubs: stubs.sandbox.Stop(api_pb2.StopSandboxRequest(id=self.id)),
                error="stop returned a non-object response",
            )
        )

    def terminate(self, wait: bool = True) -> None:
        """Terminate the sandbox idempotently."""
        del wait
        if self._terminated or self._removed:
            return
        self._update_optional(
            self._view_rpc(
                lambda stubs: stubs.sandbox.Terminate(api_pb2.SandboxRef(id=self.id)),
                error="terminate returned a non-object response",
            )
        )
        self._terminated = True

    def remove(self) -> None:
        """Delete the sandbox record idempotently."""
        if self._removed:
            return
        self._update_optional(
            self._view_rpc(
                lambda stubs: stubs.sandbox.Remove(api_pb2.SandboxRef(id=self.id)),
                error="remove returned a non-object response",
            )
        )
        self._terminated = True
        self._removed = True

    def pause(self) -> SandboxInfo:
        """Pause virtual CPUs and return the updated view."""
        self._update(
            self._view_rpc(
                lambda stubs: stubs.sandbox.Pause(api_pb2.SandboxRef(id=self.id)),
                error="pause returned invalid data",
            ),
            "pause returned invalid data",
        )
        return self.info

    def resume(self) -> SandboxInfo:
        """Resume a paused sandbox or restore a durably suspended sandbox."""
        self._update(
            self._view_rpc(
                lambda stubs: stubs.sandbox.Resume(api_pb2.SandboxRef(id=self.id)),
                error="resume returned invalid data",
            ),
            "resume returned invalid data",
        )
        return self.info

    def suspend(self) -> SandboxInfo:
        """Durably checkpoint and release the live VM while preserving its identity."""
        self._update(
            self._view_rpc(
                lambda stubs: stubs.sandbox.Suspend(api_pb2.SandboxRef(id=self.id)),
                error="suspend returned invalid data",
            ),
            "suspend returned invalid data",
        )
        return self.info

    def history(self) -> list[RecoveryPoint]:
        """Return retained recovery points from oldest to newest."""
        from .models import RecoveryPoint

        response = self._rpc(lambda stubs: stubs.sandbox.History(api_pb2.SandboxRef(id=self.id)))
        return [
            RecoveryPoint(
                name=point.name,
                kind=point.kind,
                created_at_unix_millis=point.created_at_unix_millis,
                size_bytes=point.size_bytes,
            )
            for point in response.points
        ]

    def rollback(self, recovery_point: str) -> SandboxInfo:
        """Restore this sandbox identity to one retained recovery point."""
        if not recovery_point:
            raise ValueError("recovery point is required")
        self._update(
            self._view_rpc(
                lambda stubs: stubs.sandbox.Rollback(
                    api_pb2.RollbackSandboxRequest(id=self.id, recovery_point=recovery_point)
                ),
                error="rollback returned invalid data",
            ),
            "rollback returned invalid data",
        )
        return self.info

    def extend(self, secs: int) -> SandboxInfo:
        """Extend the sandbox deadline by a non-negative number of seconds."""
        secs = int(secs)
        if secs < 0:
            raise ValueError("extension seconds must be >= 0")
        self._update(
            self._view_rpc(
                lambda stubs: stubs.sandbox.Extend(
                    api_pb2.ExtendSandboxRequest(id=self.id, secs=secs)
                ),
                error="extend returned a non-object response",
            ),
            "extend returned a non-object response",
        )
        return self.info

    def set_idle_timeout(self, secs: float) -> SandboxInfo:
        """Replace and rearm the network-idle suspension policy; zero disables it."""
        secs = float(secs)
        if not math.isfinite(secs) or secs < 0:
            raise ValueError("idle timeout seconds must be non-negative")
        self._update(
            self._view_rpc(
                lambda stubs: stubs.sandbox.SetIdleTimeout(
                    api_pb2.SetIdleTimeoutRequest(id=self.id, idle_timeout_secs=secs)
                ),
                error="set idle timeout returned a non-object response",
            ),
            "set idle timeout returned a non-object response",
        )
        return self.info

    def migrate(self, target: str) -> SandboxInfo:
        """Migrate the sandbox to a mesh node and re-pin its serving endpoint."""
        if not target:
            raise ValueError("target node id is required")
        self._update(
            self._view_rpc(
                lambda stubs: stubs.sandbox.Migrate(
                    api_pb2.MigrateRequest(id=self.id, target=target)
                ),
                error="migrate returned invalid data",
            ),
            "migrate returned invalid data",
        )
        self._endpoint = self._client.driver.resolve_sandbox(self.id, self._endpoint)
        return self.info

    def resize(
        self,
        *,
        cpus: int | None = None,
        memory_mib: int | None = None,
        disk_mb: int | None = None,
    ) -> SandboxInfo:
        """Resize vCPUs or memory, or grow the root disk, and return the updated view."""
        request = api_pb2.ResizeSandboxRequest(id=self.id)
        if cpus is not None:
            request.cpus = int(cpus)
        if memory_mib is not None:
            request.memory_mib = int(memory_mib)
        if disk_mb is not None:
            request.disk_mb = int(disk_mb)
        self._update(
            self._view_rpc(
                lambda stubs: stubs.sandbox.Resize(request),
                error="resize returned a malformed response",
            ),
            "resize returned a malformed response",
        )
        return self.info

    def pty_open(
        self,
        *,
        session_id: str | None = None,
        cols: int | None = None,
        rows: int | None = None,
        exec: str | None = None,
        env: Mapping[str, str] | None = None,
        workdir: str | None = None,
    ) -> PtyStream:
        """Create and attach to a server-persistent guest terminal."""
        start = api_pb2.PtyOpenStart(sandbox_id=self.id)
        if session_id is not None:
            start.session_id = session_id
        if cols is not None:
            start.cols = int(cols)
        if rows is not None:
            start.rows = int(rows)
        if exec is not None:
            start.exec = exec
        if env is not None:
            start.env.update({str(key): str(value) for key, value in env.items()})
        if workdir is not None:
            start.workdir = workdir
        return self._open_pty("PtyOpen", api_pb2.ExecInput(pty_open=start))

    def pty_attach(
        self,
        session_id: str,
        *,
        cols: int | None = None,
        rows: int | None = None,
    ) -> PtyStream:
        """Attach to a persistent terminal by stable session id."""
        start = api_pb2.PtyAttachStart(sandbox_id=self.id, session_id=session_id)
        if cols is not None:
            start.cols = int(cols)
        if rows is not None:
            start.rows = int(rows)
        return self._open_pty("PtyAttach", api_pb2.ExecInput(pty_attach=start))

    def _open_pty(self, rpc_name: str, first: api_pb2.ExecInput) -> PtyStream:
        def starter(
            make_inputs: Callable[[], Iterable[api_pb2.ExecInput]],
        ) -> tuple[Any, str]:
            return self._client.driver.call(
                lambda stubs: getattr(stubs.sandbox, rpc_name)(make_inputs()),
                endpoint=self._endpoint,
                stream=True,
            )

        return open_pty_stream(starter, first, thread_name=f"vmon-pty-{self.id}")

    def pty_list(self) -> list[api_pb2.PtySession]:
        """List persistent terminal sessions owned by this sandbox."""
        response = self._rpc(lambda stubs: stubs.sandbox.PtyList(api_pb2.SandboxRef(id=self.id)))
        return list(response.sessions)

    def pty_close(self, session_id: str) -> api_pb2.PtySessionCloseResponse:
        """Terminate a persistent terminal session."""
        return self._rpc(
            lambda stubs: stubs.sandbox.PtyClose(
                api_pb2.PtyCloseRequest(id=self.id, session_id=session_id)
            )
        )

    def pty_exec(
        self,
        session_id: str,
        command: str,
        timeout: float | None = None,
    ) -> ExecResult:
        """Run a sibling command in a persistent terminal's guest context."""
        request = api_pb2.PtyExecRequest(
            id=self.id,
            session_id=session_id,
            command=command,
        )
        if timeout is not None:
            request.timeout = float(timeout)
        response = self._rpc(lambda stubs: stubs.sandbox.PtyExec(request))
        return ExecResult(
            returncode=response.code,
            stdout=response.stdout,
            stderr=response.stderr,
        )

    def snapshot(self, name: str | None = None, *, stop: bool = False) -> str:
        """Create a full VM snapshot and return its server-assigned name."""
        request = api_pb2.SnapshotRequest(id=self.id, stop=bool(stop))
        if name is not None:
            request.name = str(name)
        value = self._view_rpc(
            lambda stubs: stubs.sandbox.Snapshot(request),
            error="snapshot returned a malformed response",
        )
        snapshot = value.get("snapshot")
        if not isinstance(snapshot, str) or not snapshot:
            raise ProtocolError("snapshot returned a malformed response")
        return snapshot

    def snapshot_filesystem(self, name: str | None = None) -> str:
        """Snapshot the guest filesystem and return its template image name."""
        request = api_pb2.SnapshotFsRequest(id=self.id)
        if name is not None:
            request.name = str(name)
        value = self._view_rpc(
            lambda stubs: stubs.sandbox.SnapshotFs(request),
            error="filesystem snapshot returned a malformed response",
        )
        image = value.get("image")
        if not isinstance(image, str) or not image:
            raise ProtocolError("filesystem snapshot returned a malformed response")
        return image

    def network(self) -> SandboxNetworkPolicy:
        """Return the current sandbox network policy."""
        from .models import SandboxNetworkPolicy

        value = self._view_rpc(
            lambda stubs: stubs.sandbox.NetworkGet(api_pb2.SandboxRef(id=self.id)),
            error="network policy returned a non-object response",
        )
        return SandboxNetworkPolicy.from_dict(value)

    def set_network(self, policy: SandboxNetworkPolicy | Mapping[str, Any]) -> SandboxNetworkPolicy:
        """Replace supplied fields in the sandbox network policy."""
        from .models import SandboxNetworkPolicy

        body = policy.to_dict() if isinstance(policy, SandboxNetworkPolicy) else policy
        request = api_pb2.NetworkSetRequest(id=self.id)
        if body.get("block_network") is not None:
            request.block_network = bool(body["block_network"])
        cidr_allow = body.get("cidr_allow")
        if cidr_allow is not None:
            if isinstance(cidr_allow, str) or not isinstance(cidr_allow, Iterable):
                raise TypeError("cidr_allow must be an iterable of strings")
            request.cidr_allow.SetInParent()
            request.cidr_allow.values.extend(str(item) for item in cidr_allow)
        domain_allow = body.get("domain_allow")
        if domain_allow is not None:
            if isinstance(domain_allow, str) or not isinstance(domain_allow, Iterable):
                raise TypeError("domain_allow must be an iterable of strings")
            request.domain_allow.SetInParent()
            request.domain_allow.values.extend(str(item) for item in domain_allow)
        value = self._view_rpc(
            lambda stubs: stubs.sandbox.NetworkSet(request),
            error="network update returned a non-object response",
        )
        return SandboxNetworkPolicy.from_dict(value)

    def tunnels(self) -> TunnelSet:
        """Return typed exposed-port targets and their proxy token."""
        from .models import TunnelSet

        value = self._view_rpc(
            lambda stubs: stubs.sandbox.Tunnels(api_pb2.SandboxRef(id=self.id)),
            error="tunnels returned a malformed response",
        )
        tunnels = TunnelSet.from_dict(value)
        if tunnels.connect_token:
            self._connect_token = tunnels.connect_token
        return tunnels

    def wait_ready(self, probe: Any = None, timeout: float = 300) -> None:
        """Poll a tunnel port or guest command until it succeeds or times out."""
        if probe is None:
            return
        deadline = time.monotonic() + timeout
        last_error: Exception | None = None
        while time.monotonic() < deadline:
            try:
                if isinstance(probe, int):
                    port = probe
                elif isinstance(probe, Mapping) and "port" in probe:
                    raw_probe_port = probe["port"]
                    if isinstance(raw_probe_port, bool) or not isinstance(
                        raw_probe_port, int | str
                    ):
                        raise ValueError("readiness probe port must be an integer")
                    port = int(raw_probe_port)
                else:
                    port = None
                if port is not None:
                    target = self.tunnels().get(port)
                    if target is not None:
                        with socket.create_connection(
                            (target.host, target.port), timeout=min(1.0, timeout)
                        ):
                            return
                else:
                    result = self.run(
                        "sh",
                        "-lc",
                        str(probe),
                        timeout=min(5.0, timeout),
                    )
                    if result.returncode == 0:
                        return
            except Exception as exc:
                last_error = exc
            time.sleep(0.1)
        message = f"sandbox readiness probe timed out after {timeout:.0f}s"
        raise TimeoutError(message) from last_error

    def _proxy_params(self, params: Any | None) -> builtins.list[tuple[str, str]]:
        if self._connect_token is None:
            self.tunnels()
        if self._connect_token is None:
            raise ProtocolError("server did not return a connect token")
        items = builtins.list(httpx.QueryParams(params).multi_items()) if params is not None else []
        filtered = [
            (key, value)
            for key, value in items
            if key not in {"connect_token", "token", "access_token"}
        ]
        filtered.append(("connect_token", self._connect_token))
        return filtered

    def _update(self, value: Any, error: str) -> None:
        if not isinstance(value, Mapping):
            raise ProtocolError(error)
        self._raw.update(value)

    def _update_optional(self, value: Any) -> None:
        if isinstance(value, Mapping):
            self._raw.update(value)

    def __enter__(self) -> Sandbox:
        return self

    def __exit__(self, *exc: object) -> None:
        self.terminate()


class _AsyncFiles:
    def __init__(self, files: Files) -> None:
        self._files = files

    async def read_bytes(self, *args: Any, **kwargs: Any) -> bytes:
        return await asyncio.to_thread(self._files.read_bytes, *args, **kwargs)

    async def read_text(self, *args: Any, **kwargs: Any) -> str:
        return await asyncio.to_thread(self._files.read_text, *args, **kwargs)

    async def write_bytes(self, *args: Any, **kwargs: Any) -> None:
        await asyncio.to_thread(self._files.write_bytes, *args, **kwargs)

    async def write_text(self, *args: Any, **kwargs: Any) -> None:
        await asyncio.to_thread(self._files.write_text, *args, **kwargs)

    async def list(self, *args: Any, **kwargs: Any) -> list[FileInfo]:
        return await asyncio.to_thread(self._files.list, *args, **kwargs)

    async def stat(self, *args: Any, **kwargs: Any) -> FileInfo:
        return await asyncio.to_thread(self._files.stat, *args, **kwargs)

    async def delete(self, *args: Any, **kwargs: Any) -> None:
        await asyncio.to_thread(self._files.delete, *args, **kwargs)

    async def mkdir(self, *args: Any, **kwargs: Any) -> None:
        await asyncio.to_thread(self._files.mkdir, *args, **kwargs)

    async def open(self, *args: Any, **kwargs: Any) -> FileStream:
        return await asyncio.to_thread(self._files.open, *args, **kwargs)


class _AsyncPorts:
    def __init__(self, ports: Ports) -> None:
        self._ports = ports

    async def http(self, *args: Any, **kwargs: Any) -> httpx.Response:
        return await asyncio.to_thread(self._ports.http, *args, **kwargs)

    async def websocket(self, *args: Any, **kwargs: Any) -> WebSocketConnection:
        return await asyncio.to_thread(self._ports.websocket, *args, **kwargs)


class _AsyncSandbox:
    """Thread-backed async facade for a bound sandbox."""

    def __init__(self, sandbox: Sandbox) -> None:
        self._sandbox = sandbox

    @property
    def files(self) -> _AsyncFiles:
        return self._sandbox.files.aio

    @property
    def ports(self) -> _AsyncPorts:
        return self._sandbox.ports.aio

    async def refresh(self) -> SandboxInfo:
        return await asyncio.to_thread(self._sandbox.refresh)

    async def run(self, *args: Any, **kwargs: Any) -> ExecResult:
        return await asyncio.to_thread(self._sandbox.run, *args, **kwargs)

    async def exec(self, *args: Any, **kwargs: Any) -> Process:
        return await asyncio.to_thread(self._sandbox.exec, *args, **kwargs)

    async def attach(self) -> ConsoleStream:
        return await asyncio.to_thread(self._sandbox.attach)

    async def logs(self, follow: bool = False) -> str | LogStream:
        if follow:
            return await asyncio.to_thread(self._sandbox.logs, True)
        return await asyncio.to_thread(self._sandbox.logs, False)

    async def metrics(self) -> SandboxMetrics:
        return await asyncio.to_thread(self._sandbox.metrics)

    async def stop(self, wait: bool = True) -> None:
        await asyncio.to_thread(self._sandbox.stop, wait)

    async def terminate(self, wait: bool = True) -> None:
        await asyncio.to_thread(self._sandbox.terminate, wait)

    async def remove(self) -> None:
        await asyncio.to_thread(self._sandbox.remove)

    async def pause(self) -> SandboxInfo:
        return await asyncio.to_thread(self._sandbox.pause)

    async def suspend(self) -> SandboxInfo:
        return await asyncio.to_thread(self._sandbox.suspend)

    async def history(self) -> list[RecoveryPoint]:
        return await asyncio.to_thread(self._sandbox.history)

    async def rollback(self, recovery_point: str) -> SandboxInfo:
        return await asyncio.to_thread(self._sandbox.rollback, recovery_point)

    async def resume(self) -> SandboxInfo:
        return await asyncio.to_thread(self._sandbox.resume)

    async def extend(self, secs: int) -> SandboxInfo:
        return await asyncio.to_thread(self._sandbox.extend, secs)

    async def set_idle_timeout(self, secs: float) -> SandboxInfo:
        """Replace and rearm the network-idle suspension policy."""
        return await asyncio.to_thread(self._sandbox.set_idle_timeout, secs)

    async def migrate(self, target: str) -> SandboxInfo:
        return await asyncio.to_thread(self._sandbox.migrate, target)

    async def resize(self, *args: Any, **kwargs: Any) -> SandboxInfo:
        return await asyncio.to_thread(self._sandbox.resize, *args, **kwargs)

    async def pty_open(self, *args: Any, **kwargs: Any) -> PtyStream:
        return await asyncio.to_thread(self._sandbox.pty_open, *args, **kwargs)

    async def pty_attach(self, *args: Any, **kwargs: Any) -> PtyStream:

        return await asyncio.to_thread(self._sandbox.pty_attach, *args, **kwargs)

    async def pty_list(self) -> list[api_pb2.PtySession]:
        return await asyncio.to_thread(self._sandbox.pty_list)

    async def pty_close(self, session_id: str) -> api_pb2.PtySessionCloseResponse:
        return await asyncio.to_thread(self._sandbox.pty_close, session_id)

    async def pty_exec(self, *args: Any, **kwargs: Any) -> ExecResult:
        return await asyncio.to_thread(self._sandbox.pty_exec, *args, **kwargs)

    async def snapshot(self, *args: Any, **kwargs: Any) -> str:
        return await asyncio.to_thread(self._sandbox.snapshot, *args, **kwargs)

    async def snapshot_filesystem(self, *args: Any, **kwargs: Any) -> str:
        return await asyncio.to_thread(self._sandbox.snapshot_filesystem, *args, **kwargs)

    async def network(self) -> SandboxNetworkPolicy:
        return await asyncio.to_thread(self._sandbox.network)

    async def set_network(
        self, policy: SandboxNetworkPolicy | Mapping[str, Any]
    ) -> SandboxNetworkPolicy:
        return await asyncio.to_thread(self._sandbox.set_network, policy)

    async def tunnels(self) -> TunnelSet:
        return await asyncio.to_thread(self._sandbox.tunnels)

    async def wait_ready(self, *args: Any, **kwargs: Any) -> None:
        await asyncio.to_thread(self._sandbox.wait_ready, *args, **kwargs)

    async def __aenter__(self) -> _AsyncSandbox:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.terminate()
