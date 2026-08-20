"""Connection-string parsing and mesh-aware endpoint failover."""

from __future__ import annotations

import json as jsonlib
import math
import os
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal, Protocol, TypeVar, runtime_checkable
from urllib.parse import parse_qsl, unquote, urlsplit, urlunsplit

import grpc
import httpx

from ._endpoint import (
    EndpointTransport,
    GrpcStubs,
    WebSocketConnection,
    translate_rpc_error,
)
from .context import ContextStore, state_dir
from .errors import APIError, ProtocolError, TransportError
from .v1 import api_pb2

T = TypeVar("T")

DEFAULT_PORT = 8000
DEFAULT_TIMEOUT = 60.0
ROSTER_TTL = 60.0
ENDPOINT_COOLDOWN = 5.0
_ENDPOINT_EXTENSION = "vmon.endpoint"


@dataclass(frozen=True, slots=True)
class DsnConfig:
    """Resolved endpoints and connection policy parsed from a vmon DSN."""

    endpoints: tuple[str, ...]
    token: str | None
    discover: bool
    timeout: float


@dataclass(frozen=True, slots=True)
class EndpointInfo:
    """A point-in-time view of one seed or mesh-discovered endpoint."""

    url: str
    healthy: bool
    source: Literal["seed", "discovered"]


@runtime_checkable
class Driver(Protocol):
    """Transport seam consumed by :class:`vmon.Client`."""

    def call(
        self,
        fn: Callable[[GrpcStubs], T],
        *,
        endpoint: str | None = None,
        stream: bool = False,
    ) -> tuple[T, str]:
        """Invoke one gRPC call with transport-only failover."""
        ...

    def request(
        self,
        method: str,
        path: str,
        *,
        params: Any | None = None,
        json: Any | None = None,
        content: bytes | None = None,
        headers: dict[str, str] | None = None,
        endpoint: str | None = None,
        raise_for_status: bool = True,
    ) -> httpx.Response:
        """Issue one HTTP request (ports proxy, health) with transport-only failover."""
        ...

    def websocket(
        self,
        path: str,
        *,
        query: Any | None = None,
        endpoint: str | None = None,
    ) -> tuple[WebSocketConnection, str]:
        """Open a WebSocket using the request failover and affinity policy."""
        ...

    def resolve_sandbox(self, sandbox_id: str, hint: str | None = None) -> str:
        """Find the endpoint currently hosting a sandbox."""
        ...

    def endpoints(self) -> list[EndpointInfo]:
        """Return a detached snapshot of the current roster."""
        ...

    def refresh(self, force: bool = False) -> None:
        """Refresh discovered mesh endpoints when discovery is due."""
        ...

    def close(self) -> None:
        """Release all endpoint transports."""
        ...


@dataclass(slots=True)
class _RosterEntry:
    url: str
    source: Literal["seed", "discovered"]
    healthy: bool = True
    cooldown_until: float = 0.0


def _query_options(query: str) -> tuple[str | None, bool, float]:
    values: dict[str, str] = {}
    for key, value in parse_qsl(query, keep_blank_values=True):
        if key not in {"token", "discover", "timeout"}:
            raise ValueError(f"invalid DSN parameter {key}")
        if key in values:
            raise ValueError(f"duplicate DSN parameter {key}")
        values[key] = value

    discover_value = values.get("discover", "on")
    if discover_value not in {"on", "off"}:
        raise ValueError("DSN discover must be 'on' or 'off'")

    timeout_value = values.get("timeout", str(DEFAULT_TIMEOUT))
    try:
        timeout = float(timeout_value)
    except ValueError as exc:
        raise ValueError("DSN timeout must be a positive number") from exc
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("DSN timeout must be a positive number")

    return values.get("token") or None, discover_value == "on", timeout


def _http_endpoint(value: str, *, default_port: int | None = None) -> str:
    parsed = urlsplit(value if "://" in value else f"http://{value}")
    scheme = parsed.scheme.lower()
    if scheme not in {"http", "https"}:
        raise ValueError(f"unsupported endpoint scheme {parsed.scheme!r}")
    if "@" in parsed.netloc:
        raise ValueError("userinfo is not allowed in a DSN")
    if not parsed.hostname:
        raise ValueError(f"invalid endpoint {value!r}")
    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError(f"invalid endpoint {value!r}") from exc

    host = parsed.hostname
    rendered_host = f"[{host}]" if ":" in host and not host.startswith("[") else host
    rendered_port = port if port is not None else default_port
    authority = rendered_host if rendered_port is None else f"{rendered_host}:{rendered_port}"
    path = parsed.path.rstrip("/")
    return urlunsplit((scheme, authority, path, "", ""))


def _split_hosts(authority: str) -> list[str]:
    hosts: list[str] = []
    start = 0
    bracket_depth = 0
    for index, character in enumerate(authority):
        if character == "[":
            bracket_depth += 1
        elif character == "]":
            bracket_depth -= 1
            if bracket_depth < 0:
                raise ValueError("invalid DSN host list")
        elif character == "," and bracket_depth == 0:
            hosts.append(authority[start:index])
            start = index + 1
    if bracket_depth != 0:
        raise ValueError("invalid DSN host list")
    hosts.append(authority[start:])
    if any(not host for host in hosts):
        raise ValueError("invalid DSN host list")
    return hosts


def _unix_endpoint(path: str) -> str:
    if not path.startswith("/"):
        raise ValueError("vmon+unix DSN requires an absolute socket path")
    return f"vmon+unix://{path}"


def _resolved_dsn(dsn: str | None) -> str:
    if dsn:
        return dsn
    environment_dsn = os.environ.get("VMON_DSN")
    if environment_dsn:
        return environment_dsn
    context_name = os.environ.get("VMON_CONTEXT")
    if context_name:
        return f"vmon+context://{context_name}"
    return _unix_endpoint(str(state_dir() / "vmond.sock"))


def parse_dsn(dsn: str | None = None) -> DsnConfig:
    """Parse a vmon connection string without making a network request."""
    value = _resolved_dsn(dsn).strip()
    if not value:
        raise ValueError("DSN is empty")
    parsed = urlsplit(value)
    scheme = parsed.scheme.lower()
    if parsed.fragment:
        raise ValueError("DSN fragments are not supported")
    token, discover, timeout = _query_options(parsed.query)

    if scheme in {"vmon", "vmons"}:
        if "@" in parsed.netloc:
            raise ValueError("userinfo is not allowed in a DSN")
        if not parsed.netloc:
            raise ValueError("vmon DSN requires at least one host")
        endpoint_scheme = "https" if scheme == "vmons" else "http"
        endpoints = tuple(
            _http_endpoint(
                urlunsplit((endpoint_scheme, host, parsed.path, "", "")),
                default_port=DEFAULT_PORT,
            )
            for host in _split_hosts(parsed.netloc)
        )
    elif scheme in {"http", "https"}:
        if "," in parsed.netloc:
            raise ValueError("http and https DSNs accept one endpoint")
        endpoints = (_http_endpoint(urlunsplit((scheme, parsed.netloc, parsed.path, "", ""))),)
    elif scheme == "vmon+unix":
        if parsed.netloc:
            raise ValueError("vmon+unix DSN must not contain a host")
        endpoints = (_unix_endpoint(unquote(parsed.path)),)
    elif scheme == "vmon+context":
        if "@" in parsed.netloc:
            raise ValueError("userinfo is not allowed in a DSN")
        if parsed.path not in {"", "/"} or not parsed.netloc:
            raise ValueError("vmon+context DSN requires a context name")
        name = unquote(parsed.netloc)
        store = ContextStore()
        store.load()
        context = store.get(name)
        if context is None:
            raise ValueError(f"context {name!r} was not found")
        if not context.endpoints:
            raise ValueError(f"context {name!r} has no endpoints")
        endpoints = tuple(_http_endpoint(endpoint) for endpoint in context.endpoints)
        token = token or os.environ.get("VMON_API_TOKEN") or store.load_token(name)
    else:
        raise ValueError(f"unsupported DSN scheme {parsed.scheme!r}")

    if scheme != "vmon+context":
        token = token or os.environ.get("VMON_API_TOKEN")
    return DsnConfig(endpoints=endpoints, token=token, discover=discover, timeout=timeout)


def roster_from_status(status: Mapping[str, Any]) -> list[str]:
    """Extract ordered, de-duplicated advertise URLs from mesh status data."""
    nodes: list[Mapping[str, Any]] = []
    self_node = status.get("self")
    if isinstance(self_node, Mapping):
        nodes.append(self_node)
    peers = status.get("peers")
    if isinstance(peers, list):
        nodes.extend(peer for peer in peers if isinstance(peer, Mapping))

    roster: list[str] = []
    seen: set[str] = set()
    for node in nodes:
        advertise = node.get("advertise")
        if not isinstance(advertise, str) or not advertise:
            continue
        try:
            normalized = _http_endpoint(advertise)
        except ValueError:
            continue
        if normalized not in seen:
            seen.add(normalized)
            roster.append(normalized)
    return roster


class MeshDriver:
    """Synchronous mesh driver with sticky failover and lazy discovery."""

    def __init__(
        self,
        dsn: str | DsnConfig | None = None,
        *,
        token: str | None = None,
        timeout: float | None = None,
        discover: bool | None = None,
    ) -> None:
        config = dsn if isinstance(dsn, DsnConfig) else parse_dsn(dsn)
        self.token: str | None = token if token is not None else config.token
        self.timeout: float = config.timeout if timeout is None else float(timeout)
        self.discover: bool = config.discover if discover is None else discover
        if not math.isfinite(self.timeout) or self.timeout <= 0:
            raise ValueError("timeout must be a positive number")

        self._lock = threading.RLock()
        self._seeds = tuple(dict.fromkeys(_normalize_endpoint(url) for url in config.endpoints))
        if not self._seeds:
            raise ValueError("DSN resolved to no endpoints")
        self._roster: list[_RosterEntry] = [
            _RosterEntry(url=url, source="seed") for url in self._seeds
        ]
        self._preferred = self._seeds[0]
        self._transports: dict[str, EndpointTransport] = {}
        self._last_refresh: float | None = None
        self._refreshing = False
        self._closed = False

    def call(
        self,
        fn: Callable[[GrpcStubs], T],
        *,
        endpoint: str | None = None,
        stream: bool = False,
    ) -> tuple[T, str]:
        """Invoke one gRPC call, retrying only failures that did not reach a daemon."""
        return self._call(fn, endpoint=endpoint, stream=stream, trigger_refresh=True)

    def aio_endpoint(
        self, endpoint: str | None = None
    ) -> tuple[Any, tuple[tuple[str, str], ...], float, str]:
        """Return native async stubs, auth metadata, the deadline, and selected endpoint."""
        selected = endpoint or self._preferred
        transport = self._transport(selected)
        return transport.aio_stubs, transport.aio_metadata, self.timeout, selected

    def _call(
        self,
        fn: Callable[[GrpcStubs], T],
        *,
        endpoint: str | None,
        stream: bool,
        trigger_refresh: bool,
    ) -> tuple[T, str]:
        candidates = self._candidates(endpoint)
        last_error: TransportError | None = None
        failed_over = False
        for candidate in candidates:
            transport = self._transport(candidate)
            try:
                result = fn(transport.stubs)
                if stream:
                    # Streaming calls return immediately; block until the server
                    # accepts the stream so connect failures fail over here.
                    rendezvous: Any = result
                    rendezvous.initial_metadata()
                    if rendezvous.done() and rendezvous.code() != grpc.StatusCode.OK:
                        raise rendezvous
            except grpc.RpcError as exc:
                error = translate_rpc_error(exc, endpoint=candidate)
                if isinstance(error, TransportError):
                    last_error = error
                    failed_over = True
                    self._mark_failed(candidate)
                    continue
                self._mark_success(candidate)
                if trigger_refresh:
                    self._refresh_after_request(failed_over)
                raise error from exc
            self._mark_success(candidate)
            if trigger_refresh:
                self._refresh_after_request(failed_over)
            return result, candidate

        if last_error is not None:
            raise last_error
        raise TransportError("no vmon endpoints are currently available")

    def request(
        self,
        method: str,
        path: str,
        *,
        params: Any | None = None,
        json: Any | None = None,
        content: bytes | None = None,
        headers: dict[str, str] | None = None,
        endpoint: str | None = None,
        raise_for_status: bool = True,
    ) -> httpx.Response:
        """Issue one HTTP request, retrying only failures that did not reach HTTP."""
        candidates = self._candidates(endpoint)
        last_error: TransportError | None = None
        failed_over = False
        for candidate in candidates:
            transport = self._transport(candidate)
            try:
                result = transport.request(
                    method,
                    path,
                    params=params,
                    json_body=json,
                    content=content,
                    headers=headers,
                    raise_for_status=raise_for_status,
                )
            except TransportError as exc:
                last_error = exc
                failed_over = True
                self._mark_failed(candidate)
                continue
            except APIError:
                self._mark_success(candidate)
                self._refresh_after_request(failed_over)
                raise

            result.extensions[_ENDPOINT_EXTENSION] = candidate
            self._mark_success(candidate)
            self._refresh_after_request(failed_over)
            return result

        if last_error is not None:
            raise last_error
        raise TransportError("no vmon endpoints are currently available")

    def websocket(
        self,
        path: str,
        *,
        query: Any | None = None,
        endpoint: str | None = None,
    ) -> tuple[WebSocketConnection, str]:
        """Open a WebSocket with the same affinity and failover policy as HTTP."""
        last_error: TransportError | None = None
        failed_over = False
        for candidate in self._candidates(endpoint):
            try:
                connection = self._transport(candidate).websocket(path, params=query)
            except TransportError as exc:
                last_error = exc
                failed_over = True
                self._mark_failed(candidate)
                continue
            except APIError:
                self._mark_success(candidate)
                self._refresh_after_request(failed_over)
                raise
            self._mark_success(candidate)
            self._refresh_after_request(failed_over)
            return connection, candidate
        if last_error is not None:
            raise last_error
        raise TransportError("no vmon endpoints are currently available")

    def resolve_sandbox(self, sandbox_id: str, hint: str | None = None) -> str:
        """Walk the roster until an endpoint reports that the sandbox exists."""
        original_not_found: APIError | None = None
        last_transport_error: TransportError | None = None
        for candidate in self._candidates(hint, include_cooling=True):
            try:
                self._transport(candidate).stubs.sandbox.Get(api_pb2.SandboxRef(id=sandbox_id))
            except grpc.RpcError as raw:
                error = translate_rpc_error(raw, endpoint=candidate)
                if isinstance(error, TransportError):
                    last_transport_error = error
                    self._mark_failed(candidate)
                    continue
                self._mark_success(candidate)
                if error.code == "not_found":
                    if original_not_found is None:
                        original_not_found = error
                    continue
                raise error from raw
            self._mark_success(candidate)
            return candidate

        if original_not_found is not None:
            raise original_not_found
        if last_transport_error is not None:
            raise last_transport_error
        raise APIError(f"sandbox {sandbox_id!r} was not found", code="not_found")

    def resolve_call(self, call_id: str, hint: str | None = None) -> str:
        """Walk the roster until an endpoint reports that the durable call exists."""
        original_not_found: APIError | None = None
        last_transport_error: TransportError | None = None
        for candidate in self._candidates(hint, include_cooling=True):
            try:
                self._transport(candidate).stubs.calls.Get(api_pb2.CallRef(call_id=call_id))
            except grpc.RpcError as raw:
                error = translate_rpc_error(raw, endpoint=candidate)
                if isinstance(error, TransportError):
                    last_transport_error = error
                    self._mark_failed(candidate)
                    continue
                self._mark_success(candidate)
                if error.code == "not_found":
                    original_not_found = original_not_found or error
                    continue
                raise error from raw
            self._mark_success(candidate)
            return candidate

        if original_not_found is not None:
            raise original_not_found
        if last_transport_error is not None:
            raise last_transport_error
        raise APIError(f"call {call_id!r} was not found", code="not_found")

    def endpoints(self) -> list[EndpointInfo]:
        """Return a detached roster snapshot in failover order."""
        now = time.monotonic()
        with self._lock:
            return [
                EndpointInfo(
                    url=entry.url,
                    healthy=entry.cooldown_until <= now,
                    source=entry.source,
                )
                for entry in self._roster
            ]

    def refresh(self, force: bool = False) -> None:
        """Refresh mesh-advertised endpoints, swallowing unsupported or unreachable status."""
        if not self.discover:
            return
        now = time.monotonic()
        with self._lock:
            if self._refreshing:
                return
            if (
                not force
                and self._last_refresh is not None
                and now - self._last_refresh < ROSTER_TTL
            ):
                return
            self._refreshing = True

        try:
            try:
                view, _ = self._call(
                    lambda stubs: stubs.system.MeshStatus(api_pb2.MeshStatusRequest()),
                    endpoint=None,
                    stream=False,
                    trigger_refresh=False,
                )
                value = jsonlib.loads(view.json)
                if not isinstance(value, Mapping):
                    raise ProtocolError("mesh status returned a malformed response")
                discovered = roster_from_status(value)
                self._merge_discovered(discovered)
            except APIError, ProtocolError, TransportError, ValueError:
                pass
        finally:
            with self._lock:
                self._last_refresh = time.monotonic()
                self._refreshing = False

    def close(self) -> None:
        """Close every lazily-created endpoint transport."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
            transports = list(self._transports.values())
            self._transports.clear()
        for transport in transports:
            transport.close()

    async def aclose(self) -> None:
        """Close every transport and await native async channel shutdown."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
            transports = list(self._transports.values())
            self._transports.clear()
        for transport in transports:
            await transport.aclose()

    def _refresh_after_request(self, failed_over: bool) -> None:
        with self._lock:
            last_refresh = self._last_refresh
        stale = last_refresh is None or time.monotonic() - last_refresh >= ROSTER_TTL
        if failed_over or stale:
            self.refresh(force=failed_over)

    def _candidates(self, hint: str | None, *, include_cooling: bool = False) -> list[str]:
        normalized_hint: str | None
        try:
            normalized_hint = _normalize_endpoint(hint) if hint else None
        except ValueError:
            normalized_hint = None
        now = time.monotonic()
        with self._lock:
            entries = list(self._roster)
            preferred = self._preferred

        ordered: list[_RosterEntry] = []
        for target in (normalized_hint, preferred):
            if target is None:
                continue
            entry = next((item for item in entries if item.url == target), None)
            if entry is not None and entry not in ordered:
                ordered.append(entry)
        ordered.extend(entry for entry in entries if entry not in ordered)

        if include_cooling or len(ordered) == 1:
            return [entry.url for entry in ordered]
        available = [entry.url for entry in ordered if entry.cooldown_until <= now]
        if available:
            return available
        return [min(ordered, key=lambda entry: entry.cooldown_until).url]

    def _transport(self, endpoint: str) -> EndpointTransport:
        with self._lock:
            if self._closed:
                raise TransportError("mesh driver is closed")
            transport = self._transports.get(endpoint)
            if transport is not None:
                return transport
            if endpoint.startswith("vmon+unix://"):
                path = unquote(urlsplit(endpoint).path)
                transport = EndpointTransport(uds=path, token=self.token, timeout=self.timeout)
            else:
                transport = EndpointTransport(
                    base_url=endpoint, token=self.token, timeout=self.timeout
                )
            self._transports[endpoint] = transport
            return transport

    def _mark_failed(self, endpoint: str) -> None:
        with self._lock:
            entry = next((item for item in self._roster if item.url == endpoint), None)
            if entry is not None:
                entry.healthy = False
                entry.cooldown_until = time.monotonic() + ENDPOINT_COOLDOWN

    def _mark_success(self, endpoint: str) -> None:
        with self._lock:
            entry = next((item for item in self._roster if item.url == endpoint), None)
            if entry is not None:
                entry.healthy = True
                entry.cooldown_until = 0.0
                self._preferred = endpoint

    def _merge_discovered(self, advertised: Sequence[str]) -> None:
        normalized = [
            endpoint
            for endpoint in dict.fromkeys(_normalize_endpoint(value) for value in advertised)
            if endpoint not in self._seeds
        ]
        keep = set(self._seeds) | set(normalized)
        with self._lock:
            previous = {entry.url: entry for entry in self._roster}
            self._roster = [previous.get(seed, _RosterEntry(seed, "seed")) for seed in self._seeds]
            self._roster.extend(
                previous.get(endpoint, _RosterEntry(endpoint, "discovered"))
                for endpoint in normalized
            )
            removed = [
                self._transports.pop(endpoint)
                for endpoint in list(self._transports)
                if endpoint not in keep
            ]
            if self._preferred not in keep:
                self._preferred = self._seeds[0]
        for transport in removed:
            transport.close()


def response_endpoint(response: httpx.Response) -> str | None:
    """Return the endpoint selected by a driver response when available."""
    endpoint = response.extensions.get(_ENDPOINT_EXTENSION)
    return endpoint if isinstance(endpoint, str) else None


def _normalize_endpoint(value: str) -> str:
    if value.startswith("vmon+unix://"):
        parsed = urlsplit(value)
        if parsed.netloc:
            raise ValueError("vmon+unix endpoint must not contain a host")
        return _unix_endpoint(unquote(parsed.path))
    return _http_endpoint(value)
