"""Per-endpoint gRPC channel, HTTP, and WebSocket mechanics for the Rust v1 API."""

from __future__ import annotations

import json
import socket
import ssl
import threading
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote, urlsplit, urlunsplit

import grpc
import httpx

from .errors import APIError, ProtocolError, TransportError
from .v1 import api_pb2_grpc
from .wsframe import (
    WebSocketHandshakeError,
    client_handshake,
    encode_frame,
    read_frame_sync_details,
)

DEFAULT_HTTP_TIMEOUT = 60.0


def _clean_base_url(url: str) -> str:
    value = (url or "").strip()
    if not value:
        raise ValueError("endpoint is empty")
    parsed = urlsplit(value)
    if not parsed.scheme:
        value = "http://" + value
        parsed = urlsplit(value)
    if parsed.scheme not in {"http", "https"}:
        raise ValueError(f"unsupported endpoint scheme {parsed.scheme!r}")
    if not parsed.netloc:
        raise ValueError(f"invalid endpoint {url!r}")
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path.rstrip("/"), "", ""))


def _safe_segment(value: str) -> str:
    encoded = quote(str(value), safe="")
    return encoded.replace(".", "%2E") if encoded in {".", ".."} else encoded


def dump_json(value: Any) -> str:
    """Serialize a JSON request document with the SDK's canonical encoding."""
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON number is not allowed: {value}")


def decode_view(payload: str, error: str) -> dict[str, Any]:
    """Parse a ``JsonView`` document, requiring a JSON object."""
    try:
        value = json.loads(payload, parse_constant=_reject_json_constant)
    except ValueError as exc:
        raise ProtocolError(error) from exc
    if not isinstance(value, dict):
        raise ProtocolError(error)
    return value


class WebSocketConnection:
    """Blocking RFC 6455 connection used by streaming SDK operations."""

    def __init__(self, sock: socket.socket) -> None:
        self._sock = sock
        self._send_lock = threading.Lock()
        self._closed = False

    @property
    def closed(self) -> bool:
        """Return whether this connection has been closed locally or remotely."""
        return self._closed

    def send_text(self, text: str) -> None:
        """Send one UTF-8 text message."""
        self._send(0x1, text.encode("utf-8"))

    def send_bytes(self, data: bytes | bytearray | memoryview) -> None:
        """Send one binary message."""
        self._send(0x2, bytes(data))

    def _send(self, opcode: int, data: bytes) -> None:
        with self._send_lock:
            if self._closed:
                raise TransportError("websocket is closed")
            try:
                self._sock.sendall(encode_frame(opcode, data))
            except OSError as exc:
                self._closed = True
                self._release_socket()
                raise TransportError(f"websocket write failed: {exc}") from exc

    def recv_message(self) -> str | bytes | None:
        """Receive one complete text or binary message, handling control frames."""
        message_opcode: int | None = None
        fragments: list[bytes] = []
        while True:
            try:
                final, opcode, payload = read_frame_sync_details(self._sock)
            except (ConnectionError, OSError) as exc:
                self._closed = True
                self._release_socket()
                raise TransportError(f"websocket read failed: {exc}") from exc
            if opcode == 0x8:
                self._closed = True
                with self._send_lock:
                    try:
                        self._sock.sendall(encode_frame(0x8, b""))
                    except OSError:
                        pass
                    self._release_socket()
                return None
            if opcode == 0x9:
                with self._send_lock:
                    try:
                        self._sock.sendall(encode_frame(0xA, payload))
                    except OSError as exc:
                        self._closed = True
                        self._release_socket()
                        raise TransportError(f"websocket pong failed: {exc}") from exc
                continue
            if opcode == 0xA:
                continue
            if opcode in {0x1, 0x2}:
                if message_opcode is not None:
                    raise ProtocolError("interleaved websocket message")
                message_opcode = opcode
                fragments.append(payload)
            elif opcode == 0x0:
                if message_opcode is None:
                    raise ProtocolError("unexpected websocket continuation")
                fragments.append(payload)
            else:
                raise ProtocolError(f"unsupported websocket opcode {opcode}")
            if not final:
                continue
            data = b"".join(fragments)
            if message_opcode == 0x2:
                return data
            try:
                return data.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise ProtocolError("invalid websocket text frame") from exc

    def _release_socket(self) -> None:
        try:
            self._sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self._sock.close()
        except OSError:
            pass

    def close(self) -> None:
        """Send a close frame and release the underlying socket."""
        with self._send_lock:
            if self._closed:
                self._release_socket()
                return
            self._closed = True
            try:
                self._sock.sendall(encode_frame(0x8, b""))
            except OSError:
                pass
            self._release_socket()

    def __enter__(self) -> WebSocketConnection:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


GRPC_MAX_MESSAGE = 64 * 1024 * 1024

_GRPC_STATUS_TO_CODE = {
    grpc.StatusCode.NOT_FOUND: "not_found",
    grpc.StatusCode.INVALID_ARGUMENT: "invalid",
    grpc.StatusCode.UNAUTHENTICATED: "unauthorized",
    grpc.StatusCode.PERMISSION_DENIED: "forbidden",
    grpc.StatusCode.FAILED_PRECONDITION: "not_running",
    grpc.StatusCode.ABORTED: "busy",
    grpc.StatusCode.UNIMPLEMENTED: "unsupported",
    grpc.StatusCode.UNAVAILABLE: "engine",
}


def translate_rpc_error(
    exc: grpc.RpcError,
    *,
    endpoint: str | None = None,
    fallback_message: str = "vmon API call failed",
    fallback_code: str = "engine",
) -> APIError | TransportError:
    """Translate a gRPC failure into the SDK's stable error categories.

    A ``vmon-code`` entry in the trailing metadata marks an error produced by
    vmond itself and wins over the status-code reverse map; a bare UNAVAILABLE,
    DEADLINE_EXCEEDED, or CANCELLED never reached a daemon and is a
    :class:`TransportError`, which drives mesh failover.
    """
    status = exc.code() if isinstance(exc, grpc.Call) else None
    message = (exc.details() or "") if isinstance(exc, grpc.Call) else str(exc)
    trailing = exc.trailing_metadata() if isinstance(exc, grpc.Call) else None
    trailing_dict = dict(trailing or ())
    code = trailing_dict.get("vmon-code")
    retryable_val = trailing_dict.get("vmon-retryable")
    action_val = trailing_dict.get("vmon-action")

    actual_code = (
        str(code)
        if code
        else (_GRPC_STATUS_TO_CODE.get(status) if status is not None else fallback_code)
    )

    if retryable_val is not None:
        retryable = str(retryable_val).lower() == "true"
    else:
        retryable = actual_code in {"busy", "ha_unavailable", "unavailable_secret"}

    action = str(action_val) if action_val else None

    if code:
        return APIError(
            message or fallback_message, code=str(code), retryable=retryable, action=action
        )
    if status == grpc.StatusCode.UNAVAILABLE:
        return TransportError(
            f"cannot reach vmon daemon at {endpoint or 'endpoint'}: {message}",
            endpoint=endpoint,
        )
    if status == grpc.StatusCode.DEADLINE_EXCEEDED:
        return TransportError("vmon API request timed out", endpoint=endpoint)
    if status == grpc.StatusCode.CANCELLED:
        return TransportError("vmon API call was cancelled", endpoint=endpoint)
    mapped = _GRPC_STATUS_TO_CODE.get(status) if status is not None else None
    return APIError(
        message or fallback_message,
        code=mapped or fallback_code,
        retryable=retryable,
        action=action,
    )


@dataclass(frozen=True, slots=True)
class _CallDetails(grpc.ClientCallDetails):
    method: str
    timeout: float | None
    metadata: tuple[tuple[str, str], ...] | None
    credentials: Any
    wait_for_ready: Any
    compression: Any


class _AuthInterceptor(
    grpc.UnaryUnaryClientInterceptor,
    grpc.UnaryStreamClientInterceptor,
    grpc.StreamUnaryClientInterceptor,
    grpc.StreamStreamClientInterceptor,
):
    """Adds bearer-token metadata and a default deadline for non-streaming calls."""

    def __init__(self, token: str | None, timeout: float) -> None:
        self._metadata: tuple[tuple[str, str], ...] = (
            (("authorization", f"Bearer {token}"),) if token else ()
        )
        self._timeout = timeout

    def _augment(self, details: grpc.ClientCallDetails, *, deadline: bool) -> _CallDetails:
        metadata = (*tuple(details.metadata or ()), *self._metadata)
        timeout = details.timeout
        if timeout is None and deadline:
            timeout = self._timeout
        return _CallDetails(
            method=details.method,
            timeout=timeout,
            metadata=metadata or None,
            credentials=details.credentials,
            wait_for_ready=getattr(details, "wait_for_ready", None),
            compression=getattr(details, "compression", None),
        )

    def intercept_unary_unary(self, continuation: Any, details: Any, request: Any) -> Any:
        return continuation(self._augment(details, deadline=True), request)

    def intercept_unary_stream(self, continuation: Any, details: Any, request: Any) -> Any:
        return continuation(self._augment(details, deadline=False), request)

    def intercept_stream_unary(self, continuation: Any, details: Any, request_iterator: Any) -> Any:
        return continuation(self._augment(details, deadline=True), request_iterator)

    def intercept_stream_stream(
        self, continuation: Any, details: Any, request_iterator: Any
    ) -> Any:
        return continuation(self._augment(details, deadline=False), request_iterator)


class GrpcStubs:
    """Per-endpoint service stubs sharing one authenticated channel."""

    def __init__(self, channel: grpc.Channel) -> None:
        self.sandbox = api_pb2_grpc.SandboxServiceStub(channel)
        self.snapshots = api_pb2_grpc.SnapshotServiceStub(channel)
        self.volumes = api_pb2_grpc.VolumeServiceStub(channel)
        self.pools = api_pb2_grpc.PoolServiceStub(channel)
        self.vpc = api_pb2_grpc.VpcServiceStub(channel)
        self.system = api_pb2_grpc.SystemServiceStub(channel)
        self.artifacts = api_pb2_grpc.ArtifactServiceStub(channel)
        self.functions = api_pb2_grpc.FunctionServiceStub(channel)
        self.calls = api_pb2_grpc.CallServiceStub(channel)
        self.actors = api_pb2_grpc.ActorServiceStub(channel)


class GrpcAioStubs:
    """Native asynchronous service stubs sharing one grpc.aio channel."""

    def __init__(self, channel: grpc.aio.Channel) -> None:
        self.artifacts = api_pb2_grpc.ArtifactServiceStub(channel)
        self.functions = api_pb2_grpc.FunctionServiceStub(channel)
        self.calls = api_pb2_grpc.CallServiceStub(channel)
        self.actors = api_pb2_grpc.ActorServiceStub(channel)


class EndpointTransport:
    """Synchronous transport bound to one HTTP or Unix-socket endpoint."""

    def __init__(
        self,
        *,
        base_url: str | None = None,
        token: str | None = None,
        uds: str | None = None,
        timeout: float = DEFAULT_HTTP_TIMEOUT,
    ) -> None:
        self.uds: str | None = uds
        self.base_url: str = _clean_base_url(base_url) if base_url is not None else "http://vmon"
        self.token: str | None = token
        self.timeout: float = float(timeout)
        headers = {"Accept": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        if self.uds is not None:
            transport = httpx.HTTPTransport(uds=self.uds)
            self._client = httpx.Client(
                base_url="http://vmon", headers=headers, timeout=self.timeout, transport=transport
            )
        else:
            self._client = httpx.Client(
                base_url=self.base_url, headers=headers, timeout=self.timeout
            )
        self._grpc_lock = threading.Lock()
        self._grpc_channel: grpc.Channel | None = None
        self._grpc_stubs: GrpcStubs | None = None
        self._aio_channel: grpc.aio.Channel | None = None
        self._aio_stubs: GrpcAioStubs | None = None

    def clone(self) -> EndpointTransport:
        """Return an independent transport with the same endpoint and credentials."""
        return type(self)(
            base_url=None if self.uds is not None else self.base_url,
            token=self.token,
            uds=self.uds,
            timeout=self.timeout,
        )

    def close(self) -> None:
        """Close pooled HTTP connections and the gRPC channel."""
        self._client.close()
        with self._grpc_lock:
            channel = self._grpc_channel
            self._grpc_channel = None
            self._grpc_stubs = None
        if channel is not None:
            channel.close()

    async def aclose(self) -> None:
        """Close synchronous resources and await grpc.aio channel shutdown."""
        channel = self._aio_channel
        self._aio_channel = None
        self._aio_stubs = None
        self.close()
        if channel is not None:
            await channel.close()

    def __enter__(self) -> EndpointTransport:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _grpc_target(self) -> str:
        if self.uds is not None:
            return f"unix:{self.uds}"
        parsed = urlsplit(self.base_url)
        host = parsed.hostname or "127.0.0.1"
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        return f"{host}:{port}"

    @property
    def stubs(self) -> GrpcStubs:
        """Return the lazily-created service stubs for this endpoint."""
        with self._grpc_lock:
            if self._grpc_stubs is None:
                options: tuple[tuple[str, Any], ...] = (
                    ("grpc.max_send_message_length", GRPC_MAX_MESSAGE),
                    ("grpc.max_receive_message_length", GRPC_MAX_MESSAGE),
                )
                if self.uds is not None:
                    # grpc-core uses the socket path as :authority for unix:
                    # targets; hyper's h2 rejects that as malformed (RST_STREAM
                    # PROTOCOL_ERROR), so pin a valid authority instead.
                    options = (*options, ("grpc.default_authority", "localhost"))
                target = self._grpc_target()
                if self.uds is None and urlsplit(self.base_url).scheme == "https":
                    channel = grpc.secure_channel(
                        target, grpc.ssl_channel_credentials(), options=options
                    )
                else:
                    channel = grpc.insecure_channel(target, options=options)
                self._grpc_channel = channel
                self._grpc_stubs = GrpcStubs(
                    grpc.intercept_channel(channel, _AuthInterceptor(self.token, self.timeout))
                )
            return self._grpc_stubs

    @property
    def aio_stubs(self) -> GrpcAioStubs:
        """Return lazily-created native grpc.aio function-service stubs."""
        if self._aio_stubs is None:
            options: tuple[tuple[str, Any], ...] = (
                ("grpc.max_send_message_length", GRPC_MAX_MESSAGE),
                ("grpc.max_receive_message_length", GRPC_MAX_MESSAGE),
            )
            if self.uds is not None:
                options = (*options, ("grpc.default_authority", "localhost"))
            target = self._grpc_target()
            if self.uds is None and urlsplit(self.base_url).scheme == "https":
                channel = grpc.aio.secure_channel(
                    target, grpc.ssl_channel_credentials(), options=options
                )
            else:
                channel = grpc.aio.insecure_channel(target, options=options)
            self._aio_channel = channel
            self._aio_stubs = GrpcAioStubs(channel)
        return self._aio_stubs

    @property
    def aio_metadata(self) -> tuple[tuple[str, str], ...]:
        """Return authentication metadata for native asynchronous RPCs."""
        return (("authorization", f"Bearer {self.token}"),) if self.token else ()

    def request(
        self,
        method: str,
        path: str,
        *,
        params: Any | None = None,
        json_body: Any | None = None,
        content: bytes | None = None,
        headers: dict[str, str] | None = None,
        raise_for_status: bool = True,
    ) -> httpx.Response:
        """Perform one buffered HTTP request."""
        request_headers, request_content = self._request_data(json_body, content, headers)
        try:
            response = self._client.request(
                method,
                path,
                params=params,
                content=request_content,
                headers=request_headers,
            )
        except httpx.ConnectError as exc:
            target = self.uds if self.uds is not None else self.base_url
            raise TransportError(
                f"cannot reach vmon daemon at {target}: {exc}", endpoint=target
            ) from exc
        except httpx.TimeoutException as exc:
            raise TransportError("vmon API request timed out", endpoint=self.base_url) from exc
        except httpx.HTTPError as exc:
            raise TransportError(
                f"vmon API transport failed: {exc}", endpoint=self.base_url
            ) from exc
        if raise_for_status and response.status_code >= 400:
            raise self._error_from_response(response)
        return response

    def _request_data(
        self,
        json_body: Any | None,
        content: bytes | None,
        headers: dict[str, str] | None,
    ) -> tuple[dict[str, str], bytes | None]:
        if json_body is not None and content is not None:
            raise ValueError("json_body and content are mutually exclusive")
        request_headers = dict(headers or {})
        if self.token:
            request_headers["Authorization"] = f"Bearer {self.token}"
        if json_body is not None:
            request_headers.setdefault("Content-Type", "application/json")
            content = json.dumps(
                json_body, ensure_ascii=False, separators=(",", ":"), sort_keys=True
            ).encode("utf-8")
        return request_headers, content

    def websocket(self, path: str, *, params: Any | None = None) -> WebSocketConnection:
        """Open an authenticated WebSocket connection."""
        headers = {"Authorization": f"Bearer {self.token}"} if self.token else {}
        ws_path = path
        if params:
            query = str(httpx.QueryParams(params))
            if query:
                ws_path = f"{ws_path}{'&' if '?' in ws_path else '?'}{query}"
        if self.uds is not None:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.settimeout(self.timeout)
            try:
                sock.connect(str(self.uds))
                client_handshake(sock, "vmon", 80, ws_path, headers)
            except WebSocketHandshakeError as exc:
                sock.close()
                raise self._error_from_handshake(exc) from exc
            except TimeoutError as exc:
                sock.close()
                raise TransportError("vmon websocket timed out", endpoint=self.uds) from exc
            except OSError as exc:
                sock.close()
                raise TransportError(
                    f"cannot reach vmon daemon at {self.uds}: {exc}", endpoint=self.uds
                ) from exc
            return WebSocketConnection(sock)

        parsed = urlsplit(self.base_url)
        host = parsed.hostname or "127.0.0.1"
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        base_path = parsed.path.rstrip("/")
        ws_path = f"{base_path}{ws_path}" if base_path else ws_path
        raw: socket.socket | None = None
        ws_sock: socket.socket | None = None
        try:
            raw = socket.create_connection((host, port), timeout=self.timeout)
            if parsed.scheme == "https":
                ws_sock = ssl.create_default_context().wrap_socket(raw, server_hostname=host)
            else:
                ws_sock = raw
            client_handshake(ws_sock, host, port, ws_path, headers)
        except WebSocketHandshakeError as exc:
            if ws_sock is not None:
                ws_sock.close()
            elif raw is not None:
                raw.close()
            raise self._error_from_handshake(exc) from exc
        except TimeoutError as exc:
            if ws_sock is not None:
                ws_sock.close()
            elif raw is not None:
                raw.close()
            raise TransportError("vmon websocket timed out", endpoint=self.base_url) from exc
        except OSError as exc:
            if ws_sock is not None:
                ws_sock.close()
            elif raw is not None:
                raw.close()
            raise TransportError(
                f"cannot reach vmon daemon at {self.base_url}: {exc}", endpoint=self.base_url
            ) from exc
        if ws_sock is None:
            raise ProtocolError("websocket connection was not established")
        return WebSocketConnection(ws_sock)

    @staticmethod
    def _error_from_handshake(error: WebSocketHandshakeError) -> APIError | ProtocolError:
        if error.status >= 400:
            response = httpx.Response(
                error.status,
                content=error.body,
                request=httpx.Request("GET", "http://vmon/websocket"),
            )
            return EndpointTransport._error_from_response(response)
        return ProtocolError(str(error))

    @staticmethod
    def _error_from_response(response: httpx.Response) -> APIError:
        code = (
            "unauthorized"
            if response.status_code == 401
            else "forbidden"
            if response.status_code == 403
            else str(response.status_code)
        )
        message = response.text.strip() or f"vmon API HTTP {response.status_code}"
        retryable = False
        action = None
        try:
            body = response.json()
        except ValueError:
            body = None
        if isinstance(body, dict):
            raw_code = body.get("code")
            raw_message = body.get("message")
            if isinstance(raw_code, str) and raw_code:
                code = raw_code
            detail = body.get("detail")
            if isinstance(raw_message, str) and raw_message:
                message = raw_message
            elif isinstance(detail, str) and detail:
                message = detail
            elif isinstance(detail, dict):
                detail_code = detail.get("code")
                detail_message = detail.get("message")
                if isinstance(detail_code, str) and detail_code:
                    code = detail_code
                if isinstance(detail_message, str) and detail_message:
                    message = detail_message

            raw_retryable = body.get("retryable")
            if isinstance(raw_retryable, bool):
                retryable = raw_retryable
            elif isinstance(raw_retryable, str):
                retryable = raw_retryable.lower() == "true"
            else:
                retryable = code in {"busy", "ha_unavailable", "unavailable_secret"}

            raw_action = body.get("action")
            if isinstance(raw_action, str):
                action = raw_action
        else:
            retryable = code in {"busy", "ha_unavailable", "unavailable_secret"}
        return APIError(
            message, code=code, status=response.status_code, retryable=retryable, action=action
        )


def path_segment(value: str) -> str:
    return _safe_segment(value)
