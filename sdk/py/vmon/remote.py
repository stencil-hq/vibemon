"""Durable server-native function calls.

Calls are persisted before execution and are therefore at-least-once: an
attempt may run again after a worker failure.  Call IDs are stable and may be
reconstructed in another process with :meth:`FunctionCall.from_id`.
"""

from __future__ import annotations

import asyncio
import functools
import hashlib
import inspect
import os
import threading
import time
import uuid
from collections.abc import (
    AsyncGenerator,
    AsyncIterable,
    AsyncIterator,
    Callable,
    Coroutine,
    Iterable,
    Iterator,
    Mapping,
)
from contextlib import suppress
from dataclasses import replace
from typing import Any, Protocol, Self, cast, overload

import grpc

from ._function_proto import (
    artifact_ref,
    digest,
    options_to_proto,
    package_to_proto,
    value_from_proto,
    value_to_proto,
)
from .errors import APIError, RemoteFunctionError, TransportError
from .image import Image
from .options import (
    CpuArchitecture,
    FunctionOptions,
    FunctionVolumeMount,
    HighAvailabilityPolicy,
    NetworkPolicy,
    RetryPolicy,
    SerializerPolicy,
)
from .package import PackageArtifact, SerializedCallable, package_callable
from .v1 import api_pb2 as pb
from .values import ValueCodec, ValueCompression, decode_value, encode_value

_UNSET = object()
_TERMINAL = {pb.CALL_STATUS_SUCCEEDED, pb.CALL_STATUS_FAILED, pb.CALL_STATUS_CANCELLED}


def is_remote() -> bool:
    """Return whether this process is executing inside a vmon worker."""
    return os.environ.get("VMON_REMOTE") == "1"


def _client(client: Any | None) -> Any:
    if client is not None:
        return client
    from .client import connect

    return connect()


def _call(
    client: Any,
    operation: Callable[[Any], Any],
    *,
    stream: bool = False,
    endpoint: str | None = None,
) -> Any:
    value = client.driver.call(operation, stream=stream, endpoint=endpoint)
    return value[0] if isinstance(value, tuple) and len(value) == 2 else value


def _call_owner(
    client: Any, operation: Callable[[Any], Any], *, endpoint: str | None = None
) -> tuple[Any, str | None]:
    value = client.driver.call(operation, endpoint=endpoint)
    return value if isinstance(value, tuple) and len(value) == 2 else (value, endpoint)


def _reconnectable_rpc(error: BaseException) -> bool:
    if isinstance(error, TransportError):
        return True
    if not isinstance(error, grpc.RpcError):
        return False
    return error.code() in {
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.RESOURCE_EXHAUSTED,
    }


def _batch_result_should_exist(record: pb.CallRecord, index: int) -> bool:
    return record.status == pb.CALL_STATUS_SUCCEEDED and index < record.input_count


def _watch_call(stubs: Any, *, request: pb.WatchCallRequest) -> Any:
    return stubs.calls.Watch(request)


def _get_call_result(stubs: Any, *, request: pb.GetCallResultRequest) -> Any:
    return stubs.calls.GetResult(request)


def _list_call_results(stubs: Any, *, request: pb.ListCallResultsRequest) -> Any:
    return stubs.calls.ListResults(request)


def _compose_function_options(
    options: FunctionOptions | None = None,
    *,
    image: Image | str | None = None,
    cpus: float | None = None,
    memory: int | None = None,
    disk: int | None = None,
    architecture: CpuArchitecture | str | None = None,
    ha: HighAvailabilityPolicy | str | None = None,
    volumes: tuple[FunctionVolumeMount, ...] | None = None,
    egress: Iterable[str] | None = None,
    network: NetworkPolicy | None = None,
    block_network: bool | None = None,
    egress_cidrs: Iterable[str] | None = None,
    egress_domains: Iterable[str] | None = None,
    inbound_cidrs: Iterable[str] | None = None,
    retries: RetryPolicy | int | None = None,
    startup_timeout: float | None = None,
    execution_timeout: float | None = None,
    queue_timeout: float | None = None,
    idle_timeout: float | None = None,
    result_ttl: float | None = None,
    min_workers: int | None = None,
    max_workers: int | None = None,
    buffer_workers: int | None = None,
    scale_down_after: float | None = None,
    max_outstanding_inputs: int | None = None,
    serializer: SerializerPolicy | ValueCodec | str | None = None,
) -> FunctionOptions:
    supplied = (
        image,
        cpus,
        memory,
        disk,
        architecture,
        ha,
        volumes,
        network,
        egress,
        block_network,
        egress_cidrs,
        egress_domains,
        inbound_cidrs,
        retries,
        startup_timeout,
        execution_timeout,
        queue_timeout,
        idle_timeout,
        result_ttl,
        min_workers,
        max_workers,
        buffer_workers,
        scale_down_after,
        max_outstanding_inputs,
        serializer,
    )
    network_parts = (egress, block_network, egress_cidrs, egress_domains, inbound_cidrs)
    if network is not None and any(value is not None for value in network_parts):
        raise TypeError("network cannot be combined with network shorthand settings")
    if egress is not None and any(value is not None for value in (egress_cidrs, egress_domains)):
        raise TypeError("egress cannot be combined with egress_cidrs/egress_domains")
    if options is not None and any(value is not None for value in supplied):
        raise TypeError("options cannot be combined with explicit function settings")
    if options is not None:
        return options
    base = FunctionOptions()
    resource = replace(
        base.resources,
        cpu_millis=base.resources.cpu_millis if cpus is None else int(cpus * 1000),
        memory_bytes=base.resources.memory_bytes if memory is None else memory,
        ephemeral_disk_bytes=base.resources.ephemeral_disk_bytes if disk is None else disk,
        architecture=base.resources.architecture
        if architecture is None
        else CpuArchitecture(architecture),
        high_availability=base.resources.high_availability
        if ha is None
        else HighAvailabilityPolicy(ha),
        network=(
            network
            if network is not None
            else NetworkPolicy(
                block_network=bool(block_network),
                egress_cidrs=(
                    tuple(egress_cidrs)
                    if egress_cidrs is not None
                    else tuple(value for value in egress or () if "/" in value)
                ),
                egress_domains=(
                    tuple(egress_domains)
                    if egress_domains is not None
                    else tuple(value for value in egress or () if "/" not in value)
                ),
                inbound_cidrs=tuple(inbound_cidrs or ()),
            )
        ),
        volume_mounts=base.resources.volume_mounts if volumes is None else volumes,
    )
    timeout = replace(
        base.timeouts,
        startup=base.timeouts.startup if startup_timeout is None else startup_timeout,
        execution=base.timeouts.execution if execution_timeout is None else execution_timeout,
        queue=base.timeouts.queue if queue_timeout is None else queue_timeout,
        result_ttl=base.timeouts.result_ttl if result_ttl is None else result_ttl,
    )
    worker_idle = scale_down_after if scale_down_after is not None else idle_timeout
    workers = replace(
        base.workers,
        idle_timeout=base.workers.idle_timeout if worker_idle is None else worker_idle,
        min_workers=base.workers.min_workers if min_workers is None else min_workers,
        max_workers=base.workers.max_workers if max_workers is None else max_workers,
        buffer_workers=base.workers.buffer_workers if buffer_workers is None else buffer_workers,
        max_outstanding_inputs=base.workers.max_outstanding_inputs
        if max_outstanding_inputs is None
        else max_outstanding_inputs,
    )
    retry = (
        base.retry
        if retries is None
        else RetryPolicy(max_attempts=retries + 1)
        if isinstance(retries, int)
        else retries
    )
    serialization = (
        base.serializer
        if serializer is None
        else serializer
        if isinstance(serializer, SerializerPolicy)
        else SerializerPolicy(
            input_serializer=ValueCodec(serializer),
            result_serializer=ValueCodec(serializer),
            allow_trusted_python=ValueCodec(serializer) is ValueCodec.CLOUDPICKLE,
        )
    )
    resolved_image = (
        base.image
        if image is None
        else image
        if isinstance(image, Image)
        else Image.from_registry(image)
    )
    return replace(
        base,
        image=resolved_image,
        resources=resource,
        retry=retry,
        timeouts=timeout,
        workers=workers,
        serializer=serialization,
    )


async def _run_blocking[R](function: Callable[..., R], *args: Any, **kwargs: Any) -> R:
    """Compatibility bridge for injected synchronous drivers."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, functools.partial(function, *args, **kwargs))


def _aio_endpoint(
    client: Any, endpoint: str | None = None
) -> tuple[Any, Any, float, str | None] | None:
    provider = getattr(client.driver, "aio_endpoint", None)
    if provider is None:
        return None
    try:
        selected = provider(endpoint)
    except TypeError:
        selected = provider()
    if len(selected) == 4:
        return selected
    stubs, metadata, deadline = selected
    return stubs, metadata, deadline, endpoint


def _next_or_done(iterator: Iterator[Any]) -> tuple[bool, Any]:
    try:
        return True, next(iterator)
    except StopIteration:
        return False, None


async def _iterate_async(
    source: Iterable[Any] | AsyncIterable[Any],
) -> AsyncGenerator[Any]:
    if isinstance(source, AsyncIterable):
        iterator = source.__aiter__()
        try:
            while True:
                try:
                    yield await anext(iterator)
                except StopAsyncIteration:
                    return
        finally:
            close = getattr(iterator, "aclose", None)
            if close is not None:
                await close()
    else:
        for item in source:
            yield item


async def _zip_async(
    sources: tuple[Iterable[Any] | AsyncIterable[Any], ...],
) -> AsyncIterator[tuple[Any, ...]]:
    iterators = [_iterate_async(source) for source in sources]
    try:
        while True:
            row = []
            for iterator in iterators:
                try:
                    row.append(await anext(iterator))
                except StopAsyncIteration:
                    return
            yield tuple(row)
    finally:
        for iterator in iterators:
            await iterator.aclose()


def _raise(error: pb.CallError) -> None:
    traceback_text = "\n".join(
        f'  File "{frame.file}", line {frame.line}, in {frame.function}' for frame in error.frames
    )
    exc = RemoteFunctionError(
        error.message or "remote function failed",
        code=error.code,
        remote_type=error.type,
        traceback_text=traceback_text,
    )
    if traceback_text:
        exc.add_note(f"Remote traceback:\n{traceback_text}")
    raise exc


def _invocation(args: Iterable[Any], kwargs: Mapping[str, Any]) -> dict[str, Any]:
    """Encode the versioned Python-only callable invocation ABI."""
    return {
        "__vmon_python_call__": 1,
        "args": list(args),
        "kwargs": dict(kwargs),
    }


def _result_value(
    client: Any,
    result: pb.CallResult,
    *,
    trusted: bool,
    endpoint: str | None = None,
) -> Any:
    if result.WhichOneof("outcome") == "error":
        _raise(result.error)
    envelope = result.value
    data = None
    if envelope.WhichOneof("storage") == "artifact":
        chunks = _call(
            client,
            lambda s: s.artifacts.Get(pb.GetArtifactRequest(artifact=envelope.artifact)),
            stream=True,
            endpoint=endpoint,
        )
        data = b"".join(chunk.data for chunk in chunks)
    return decode_value(value_from_proto(envelope, data), trusted=trusted)


def _encode_input(
    value: object, codec: ValueCodec, *, trusted: bool, compression: Any = None
) -> tuple[pb.ValueEnvelope, bytes | None]:
    if codec is ValueCodec.CLOUDPICKLE and not trusted:
        raise PermissionError("cloudpickle calls require allow_trusted_python=True")
    envelope = encode_value(value, codec=codec, compression=compression)
    data = envelope.artifact.data if envelope.artifact else None
    return value_to_proto(envelope), data


def _upload(
    client: Any,
    data: bytes,
    sha256: str,
    media_type: str,
    *,
    endpoint: str | None = None,
) -> pb.ArtifactRef:
    ref = artifact_ref(sha256, len(data))
    try:
        _call(client, lambda stubs: stubs.artifacts.Stat(ref), endpoint=endpoint)
        return ref
    except APIError as exc:
        if exc.code != "not_found" and exc.status != 404:
            raise

    def frames() -> Iterator[pb.PutArtifactRequest]:
        yield pb.PutArtifactRequest(
            header=pb.PutArtifactHeader(
                expected_digest=digest(sha256),
                expected_size_bytes=len(data),
                media_type=media_type,
            )
        )
        for offset in range(0, len(data), 256 * 1024):
            yield pb.PutArtifactRequest(data=data[offset : offset + 256 * 1024])

    record = _call(
        client,
        lambda stubs: stubs.artifacts.Put(frames()),
        endpoint=endpoint,
    )
    return record.ref


def _arguments(
    client: Any,
    args: Iterable[Any],
    kwargs: Mapping[str, Any],
    *,
    codec: ValueCodec,
    trusted: bool,
    compression: Any,
    endpoint: str | None = None,
) -> pb.InvocationArguments:
    message = pb.InvocationArguments()
    for value in args:
        envelope, artifact = _encode_input(value, codec, trusted=trusted, compression=compression)
        if artifact is not None:
            ref = _upload(
                client,
                artifact,
                envelope.artifact.digest.value.hex(),
                "application/vnd.vmon.value",
                endpoint=endpoint,
            )
            envelope.artifact.CopyFrom(ref)
        message.positional.append(envelope)
    for name, value in kwargs.items():
        envelope, artifact = _encode_input(value, codec, trusted=trusted, compression=compression)
        if artifact is not None:
            ref = _upload(
                client,
                artifact,
                envelope.artifact.digest.value.hex(),
                "application/vnd.vmon.value",
                endpoint=endpoint,
            )
            envelope.artifact.CopyFrom(ref)
        message.named[name].CopyFrom(envelope)
    return message


async def _async_result_value(
    stubs: Any,
    metadata: Any,
    result: pb.CallResult,
    *,
    trusted: bool,
) -> Any:
    if result.WhichOneof("outcome") == "error":
        _raise(result.error)
    envelope = result.value
    data = None
    if envelope.WhichOneof("storage") == "artifact":
        chunks: list[bytes] = []
        async for chunk in stubs.artifacts.Get(
            pb.GetArtifactRequest(artifact=envelope.artifact),
            metadata=metadata,
        ):
            chunks.append(chunk.data)
        data = b"".join(chunks)
    return decode_value(value_from_proto(envelope, data), trusted=trusted)


async def _async_upload(
    stubs: Any,
    metadata: Any,
    deadline: float,
    data: bytes,
    sha256: str,
    media_type: str,
) -> pb.ArtifactRef:
    ref = artifact_ref(sha256)
    try:
        await stubs.artifacts.Stat(ref, metadata=metadata, timeout=deadline)
        return ref
    except grpc.aio.AioRpcError as exc:
        if exc.code() is not grpc.StatusCode.NOT_FOUND:
            raise

    async def frames() -> AsyncIterator[pb.PutArtifactRequest]:
        yield pb.PutArtifactRequest(
            header=pb.PutArtifactHeader(
                expected_digest=digest(sha256),
                expected_size_bytes=len(data),
                media_type=media_type,
            )
        )
        for offset in range(0, len(data), 256 * 1024):
            yield pb.PutArtifactRequest(data=data[offset : offset + 256 * 1024])

    record = await stubs.artifacts.Put(frames(), metadata=metadata, timeout=deadline)
    return record.ref


async def _async_arguments(
    stubs: Any,
    metadata: Any,
    deadline: float,
    args: Iterable[Any],
    kwargs: Mapping[str, Any],
    *,
    codec: ValueCodec,
    trusted: bool,
    compression: Any,
) -> pb.InvocationArguments:
    message = pb.InvocationArguments()
    for name, value in [
        *((None, value) for value in args),
        *((key, value) for key, value in kwargs.items()),
    ]:
        envelope, artifact = _encode_input(value, codec, trusted=trusted, compression=compression)
        if artifact is not None:
            ref = await _async_upload(
                stubs,
                metadata,
                deadline,
                artifact,
                envelope.artifact.digest.value.hex(),
                "application/vnd.vmon.value",
            )
            envelope.artifact.CopyFrom(ref)
        if name is None:
            message.positional.append(envelope)
        else:
            message.named[name].CopyFrom(envelope)
    return message


class FunctionCall[R]:
    """A durable unary or generator call handle."""

    def __init__(
        self,
        call_id: str,
        client: Any,
        *,
        trusted: bool = False,
        generator: bool = False,
        endpoint: str | None = None,
    ) -> None:
        self.call_id = call_id
        self._client = client
        self._trusted = trusted
        self._generator = generator
        self._endpoint = endpoint

    @property
    def id(self) -> str:
        """Return the stable durable call identifier."""
        return self.call_id

    @classmethod
    def from_id(
        cls, call_id: str, *, client: Any | None = None, trusted: bool = False
    ) -> FunctionCall[Any]:
        if not call_id:
            raise ValueError("call_id must be non-empty")
        bound_client = _client(client)
        if not hasattr(bound_client, "driver"):
            return cls(call_id, bound_client, trusted=trusted)
        resolver = getattr(bound_client.driver, "resolve_call", None)
        if callable(resolver):
            endpoint = resolver(call_id)
            _call(
                bound_client,
                lambda stubs: stubs.calls.Get(pb.CallRef(call_id=call_id)),
                endpoint=endpoint,
            )
        else:
            _, endpoint = _call_owner(
                bound_client,
                lambda stubs: stubs.calls.Get(pb.CallRef(call_id=call_id)),
            )
        return cls(call_id, bound_client, trusted=trusted, endpoint=endpoint)

    @property
    def aio(self) -> _AsyncCall[R]:
        """Return the native asynchronous facade for this durable call."""
        return _AsyncCall(self)

    def _invoke(self, operation: Callable[[Any], Any], *, stream: bool = False) -> Any:
        try:
            return _call(
                self._client,
                operation,
                stream=stream,
                endpoint=self._endpoint,
            )
        except APIError as error:
            if error.code != "not_found" and error.status != 404:
                raise
            resolver = getattr(self._client.driver, "resolve_call", None)
            if not callable(resolver):
                raise
            try:
                self._endpoint = resolver(self.call_id, self._endpoint)
            except TypeError:
                self._endpoint = resolver(self.call_id)
            return _call(
                self._client,
                operation,
                stream=stream,
                endpoint=self._endpoint,
            )

    def status(self) -> int:
        return self.get_record().status

    def done(self) -> bool:
        """Return whether the durable call has reached a terminal state."""
        return self.status() in _TERMINAL

    def get_record(self) -> pb.CallRecord:
        return self._invoke(lambda stubs: stubs.calls.Get(pb.CallRef(call_id=self.call_id)))

    def stats(self) -> pb.CallStats:
        return self.get_record().stats

    def graph(self) -> pb.CallGraph:
        return self.get_record().graph

    def cancel(self, reason: str = "cancelled by client") -> pb.CallRecord:
        return self._invoke(
            lambda stubs: stubs.calls.Cancel(
                pb.CancelCallRequest(
                    call=pb.CallRef(call_id=self.call_id),
                    reason=reason,
                    request_id=str(uuid.uuid4()),
                )
            )
        )

    def events(self, *, after_sequence: int = 0, follow: bool = True) -> Iterator[pb.CallEvent]:
        cursor = after_sequence
        failures = 0
        while True:
            try:
                request = pb.WatchCallRequest(
                    cursor=pb.EventCursor(
                        call=pb.CallRef(call_id=self.call_id),
                        after_sequence=cursor,
                    ),
                    follow=follow,
                )
                stream = self._invoke(
                    functools.partial(_watch_call, request=request),
                    stream=True,
                )
                failures = 0
                for event in stream:
                    if event.sequence <= cursor:
                        continue
                    cursor = event.sequence
                    yield event
                if not follow or self.status() in _TERMINAL:
                    return
            except (TransportError, grpc.RpcError) as error:
                if not _reconnectable_rpc(error):
                    raise
                failures += 1
                if failures > 5:
                    raise
                resolver = getattr(self._client.driver, "resolve_call", None)
                if callable(resolver):
                    try:
                        self._endpoint = resolver(self.call_id, self._endpoint)
                    except TypeError:
                        self._endpoint = resolver(self.call_id)
                time.sleep(min(0.05 * (2 ** (failures - 1)), 1.0))

    def logs(self, *, after_sequence: int = 0, follow: bool = True) -> Iterator[bytes]:
        for event in self.events(after_sequence=after_sequence, follow=follow):
            if event.WhichOneof("payload") == "log":
                yield event.log.data

    def iter_yields(self, *, after_sequence: int = 0) -> Iterator[R]:
        for event in self.events(after_sequence=after_sequence):
            if event.WhichOneof("payload") == "yield_result":
                yield cast(
                    R,
                    _result_value(
                        self._client,
                        event.yield_result,
                        trusted=self._trusted,
                        endpoint=self._endpoint,
                    ),
                )

    def get(self, timeout: float | None = None) -> R:
        """Wait locally up to ``timeout`` without changing server execution."""
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            record = self.get_record()
            if record.status in _TERMINAL:
                if record.HasField("error"):
                    _raise(record.error)
                if record.status != pb.CALL_STATUS_SUCCEEDED:
                    raise RemoteFunctionError(
                        f"remote call ended with status {record.status}",
                        code="cancelled",
                    )
                request = pb.GetCallResultRequest(call=record.ref, index=0)
                item = self._invoke(
                    functools.partial(_get_call_result, request=request),
                )
                return cast(
                    R,
                    _result_value(
                        self._client,
                        item,
                        trusted=self._trusted,
                        endpoint=self._endpoint,
                    ),
                )
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError(f"call {self.call_id} did not complete within {timeout} seconds")
            remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
            time.sleep(min(0.05, remaining) if remaining is not None else 0.05)

    @staticmethod
    def gather(*calls: FunctionCall[Any], return_exceptions: bool = False) -> list[Any]:
        values = []
        for call in calls:
            try:
                values.append(call.get())
            except Exception as exc:
                if not return_exceptions:
                    raise
                values.append(exc)
        return values


class BatchCall[R]:
    """Detached durable batch handle whose results are fetched lazily."""

    def __init__(
        self,
        call: FunctionCall[Any],
        input_count: int | None = None,
        *,
        feeder_stop: threading.Event | None = None,
        feeder_errors: list[BaseException] | None = None,
    ) -> None:
        self._call = call
        self.input_count = input_count
        self._feeder_stop = feeder_stop
        self._feeder_errors = feeder_errors if feeder_errors is not None else []

    @classmethod
    def from_id(
        cls, call_id: str, *, client: Any | None = None, trusted: bool = False
    ) -> BatchCall[Any]:
        """Reconstruct a detached durable batch handle."""
        return cls(FunctionCall.from_id(call_id, client=client, trusted=trusted))

    @property
    def id(self) -> str:
        return self._call.id

    def status(self) -> int:
        return self._call.status()

    def done(self) -> bool:
        return self._call.done()

    def cancel(self, reason: str = "cancelled by client") -> pb.CallRecord:
        if self._feeder_stop is not None:
            self._feeder_stop.set()
        return self._call.cancel(reason)

    @property
    def aio(self) -> _AsyncBatchCall[R]:
        """Return the native asynchronous facade for this durable batch."""
        return _AsyncBatchCall(self._call)

    def results(
        self,
        *,
        order_outputs: bool = True,
        return_exceptions: bool = False,
    ) -> Iterator[R]:
        try:
            if order_outputs:
                next_index = 0
                terminal_retry: int | None = None
                while True:
                    request = pb.GetCallResultRequest(
                        call=pb.CallRef(call_id=self.id),
                        index=next_index,
                    )
                    try:
                        item = _call(
                            self._call._client,
                            functools.partial(_get_call_result, request=request),
                            endpoint=self._call._endpoint,
                        )
                    except (APIError, grpc.RpcError) as error:
                        not_found = (
                            isinstance(error, APIError)
                            and (error.code == "not_found" or error.status == 404)
                        ) or (
                            isinstance(error, grpc.RpcError)
                            and error.code() == grpc.StatusCode.NOT_FOUND
                        )
                        if not not_found:
                            raise
                        # A missing result is the normal pending state.  Probe the
                        # pinned owner before asking the driver to re-resolve it.
                        try:
                            record = _call(
                                self._call._client,
                                lambda stubs: stubs.calls.Get(pb.CallRef(call_id=self.id)),
                                endpoint=self._call._endpoint,
                            )
                        except (APIError, grpc.RpcError) as record_error:
                            owner_missing = (
                                isinstance(record_error, APIError)
                                and (record_error.code == "not_found" or record_error.status == 404)
                            ) or (
                                isinstance(record_error, grpc.RpcError)
                                and record_error.code() == grpc.StatusCode.NOT_FOUND
                            )
                            if not owner_missing:
                                raise
                            record = self._call.get_record()
                        if self._feeder_errors:
                            raise self._feeder_errors[0] from None
                        if record.status not in _TERMINAL:
                            time.sleep(0.02)
                            continue
                        if _batch_result_should_exist(record, next_index):
                            if terminal_retry == next_index:
                                raise RuntimeError(
                                    f"batch {self.id} is missing committed result {next_index}"
                                ) from None
                            terminal_retry = next_index
                            continue
                        failure: BaseException | None = None
                        if record.HasField("error"):
                            failure = RemoteFunctionError(
                                record.error.message or "remote batch failed",
                                code=record.error.code,
                                remote_type=record.error.type,
                            )
                        elif record.status == pb.CALL_STATUS_CANCELLED:
                            failure = RemoteFunctionError(
                                "remote batch was cancelled", code="cancelled"
                            )
                        elif record.status != pb.CALL_STATUS_SUCCEEDED:
                            failure = RemoteFunctionError(
                                f"remote batch ended with status {record.status}",
                                code="remote_error",
                            )
                        if failure is not None:
                            if return_exceptions:
                                yield cast(R, failure)
                                return
                            raise failure from None
                        return
                    try:
                        value = _result_value(
                            self._call._client,
                            item,
                            trusted=self._call._trusted,
                            endpoint=self._call._endpoint,
                        )
                    except Exception as exc:
                        if not return_exceptions:
                            raise
                        value = exc
                    yield value
                    next_index += 1

            after_sequence = 0
            while True:
                request = pb.ListCallResultsRequest(
                    cursor=pb.ResultCursor(
                        call=pb.CallRef(call_id=self.id),
                        after_sequence=after_sequence,
                    ),
                    page_size=128,
                )
                page = self._call._invoke(
                    functools.partial(_list_call_results, request=request),
                )
                for item in page.results:
                    after_sequence = max(after_sequence, item.sequence)
                    try:
                        value = _result_value(
                            self._call._client,
                            item,
                            trusted=self._call._trusted,
                            endpoint=self._call._endpoint,
                        )
                    except Exception as exc:
                        if not return_exceptions:
                            raise
                        value = exc
                    yield value
                if self._feeder_errors:
                    raise self._feeder_errors[0]
                if page.end and self.done():
                    record = self._call.get_record()
                    terminal_failure: BaseException | None = None
                    if record.HasField("error"):
                        terminal_failure = RemoteFunctionError(
                            record.error.message or "remote batch failed",
                            code=record.error.code,
                            remote_type=record.error.type,
                        )
                    elif record.status == pb.CALL_STATUS_CANCELLED:
                        terminal_failure = RemoteFunctionError(
                            "remote batch was cancelled", code="cancelled"
                        )
                    elif record.status != pb.CALL_STATUS_SUCCEEDED:
                        terminal_failure = RemoteFunctionError(
                            f"remote batch ended with status {record.status}",
                            code="remote_error",
                        )
                    if terminal_failure is not None:
                        if return_exceptions:
                            yield cast(R, terminal_failure)
                            return
                        raise terminal_failure
                    return
                time.sleep(0.02)
        except GeneratorExit:
            self.cancel("batch result consumer closed")
            raise

    def get(self, **kwargs: Any) -> Iterator[R]:
        return self.results(**kwargs)


class _AsyncCall[R]:
    def __init__(self, call: FunctionCall[R]) -> None:
        self._call = call

    async def cancel(self, reason: str = "cancelled by client") -> pb.CallRecord:
        endpoint = _aio_endpoint(self._call._client, self._call._endpoint)
        if endpoint is None:
            return await _run_blocking(self._call.cancel, reason)
        stubs, metadata, deadline, selected = endpoint
        self._call._endpoint = selected
        return await stubs.calls.Cancel(
            pb.CancelCallRequest(
                call=pb.CallRef(call_id=self._call.id),
                reason=reason,
                request_id=str(uuid.uuid4()),
            ),
            metadata=metadata,
            timeout=deadline,
        )

    async def _events(
        self,
        *,
        after_sequence: int = 0,
        follow: bool = True,
    ) -> AsyncIterator[tuple[pb.CallEvent, Any, Any, float]]:
        cursor = after_sequence
        failures = 0
        while True:
            endpoint = _aio_endpoint(self._call._client, self._call._endpoint)
            if endpoint is None:
                raise TransportError("native async endpoint became unavailable")
            stubs, metadata, deadline, selected = endpoint
            self._call._endpoint = selected
            try:
                stream = stubs.calls.Watch(
                    pb.WatchCallRequest(
                        cursor=pb.EventCursor(
                            call=pb.CallRef(call_id=self._call.id),
                            after_sequence=cursor,
                        ),
                        follow=follow,
                    ),
                    metadata=metadata,
                )
                async for event in stream:
                    failures = 0
                    if event.sequence <= cursor:
                        continue
                    cursor = event.sequence
                    yield event, stubs, metadata, deadline
                if not follow:
                    return
                record = await stubs.calls.Get(
                    pb.CallRef(call_id=self._call.id),
                    metadata=metadata,
                    timeout=deadline,
                )
                if record.status in _TERMINAL:
                    return
            except grpc.RpcError as error:
                if not _reconnectable_rpc(error):
                    raise
                failures += 1
                if failures > 5:
                    raise
                resolver = getattr(self._call._client.driver, "resolve_call", None)
                if callable(resolver):
                    try:
                        self._call._endpoint = await _run_blocking(
                            resolver,
                            self._call.id,
                            self._call._endpoint,
                        )
                    except TypeError:
                        self._call._endpoint = await _run_blocking(
                            resolver,
                            self._call.id,
                        )
                await asyncio.sleep(min(0.05 * (2 ** (failures - 1)), 1.0))

    async def get(self, timeout: float | None = None) -> R:
        endpoint = _aio_endpoint(self._call._client, self._call._endpoint)
        if endpoint is None:
            task = asyncio.create_task(_run_blocking(self._call.get, timeout))
            try:
                return await task
            except asyncio.CancelledError:
                await asyncio.shield(self.cancel("async task cancelled"))
                raise

        async def wait() -> R:
            cursor = 0
            failures = 0
            while True:
                current = _aio_endpoint(self._call._client, self._call._endpoint)
                if current is None:
                    return await _run_blocking(self._call.get)
                stubs, metadata, deadline, selected = current
                self._call._endpoint = selected
                try:
                    stream = stubs.calls.Watch(
                        pb.WatchCallRequest(
                            cursor=pb.EventCursor(
                                call=pb.CallRef(call_id=self._call.id),
                                after_sequence=cursor,
                            ),
                            follow=True,
                        ),
                        metadata=metadata,
                    )
                    async for event in stream:
                        failures = 0
                        cursor = max(cursor, event.sequence)
                        kind = event.WhichOneof("payload")
                        if kind == "result":
                            return cast(
                                R,
                                await _async_result_value(
                                    stubs,
                                    metadata,
                                    event.result,
                                    trusted=self._call._trusted,
                                ),
                            )
                        if kind == "error":
                            _raise(event.error)
                    record = await stubs.calls.Get(
                        pb.CallRef(call_id=self._call.id),
                        metadata=metadata,
                        timeout=deadline,
                    )
                    if record.status in _TERMINAL:
                        if record.HasField("error"):
                            _raise(record.error)
                        if record.status == pb.CALL_STATUS_CANCELLED:
                            raise RemoteFunctionError(
                                "remote call was cancelled",
                                code="cancelled",
                            )
                        if record.status != pb.CALL_STATUS_SUCCEEDED:
                            raise RemoteFunctionError(
                                f"remote call ended with status {record.status}",
                            )
                        result = await stubs.calls.GetResult(
                            pb.GetCallResultRequest(call=record.ref, index=0),
                            metadata=metadata,
                            timeout=deadline,
                        )
                        return cast(
                            R,
                            await _async_result_value(
                                stubs,
                                metadata,
                                result,
                                trusted=self._call._trusted,
                            ),
                        )
                except grpc.RpcError as error:
                    if not _reconnectable_rpc(error):
                        raise
                    failures += 1
                    if failures > 5:
                        raise
                    resolver = getattr(self._call._client.driver, "resolve_call", None)
                    if callable(resolver):
                        try:
                            self._call._endpoint = await _run_blocking(
                                resolver,
                                self._call.id,
                                self._call._endpoint,
                            )
                        except TypeError:
                            self._call._endpoint = await _run_blocking(
                                resolver,
                                self._call.id,
                            )
                    await asyncio.sleep(min(0.05 * (2 ** (failures - 1)), 1.0))

        try:
            if timeout is None:
                return await wait()
            async with asyncio.timeout(timeout):
                return await wait()
        except TimeoutError:
            raise TimeoutError(
                f"call {self._call.id} did not complete within {timeout} seconds"
            ) from None
        except asyncio.CancelledError:
            await asyncio.shield(self.cancel("async task cancelled"))
            raise


class _AsyncBatchCall[R]:
    """Native asynchronous access to a detached durable batch."""

    def __init__(
        self,
        call: FunctionCall[Any],
        *,
        feeder: asyncio.Task[None] | None = None,
    ) -> None:
        self._call = call
        self._feeder = feeder

    @property
    def id(self) -> str:
        return self._call.id

    async def cancel(self, reason: str = "cancelled by client") -> pb.CallRecord:
        if self._feeder is not None and not self._feeder.done():
            self._feeder.cancel()
            with suppress(asyncio.CancelledError):
                await self._feeder
        return await _AsyncCall(self._call).cancel(reason)

    async def status(self) -> int:
        endpoint = _aio_endpoint(self._call._client, self._call._endpoint)
        if endpoint is None:
            return await _run_blocking(self._call.status)
        stubs, metadata, deadline, selected = endpoint
        self._call._endpoint = selected
        record = await stubs.calls.Get(
            pb.CallRef(call_id=self.id),
            metadata=metadata,
            timeout=deadline,
        )
        return record.status

    async def done(self) -> bool:
        return await self.status() in _TERMINAL

    async def results(
        self,
        *,
        order_outputs: bool = True,
        return_exceptions: bool = False,
    ) -> AsyncIterator[R]:
        try:
            endpoint = _aio_endpoint(self._call._client, self._call._endpoint)
            if endpoint is None:
                iterator = BatchCall[R](self._call).results(
                    order_outputs=order_outputs,
                    return_exceptions=return_exceptions,
                )
                while True:
                    present, value = await _run_blocking(_next_or_done, iterator)
                    if not present:
                        return
                    yield value

            if order_outputs:
                next_index = 0
                terminal_retry: int | None = None
                failures = 0
                while True:
                    current = _aio_endpoint(self._call._client, self._call._endpoint)
                    if current is None:
                        iterator = BatchCall[R](self._call).results(
                            order_outputs=True,
                            return_exceptions=return_exceptions,
                        )
                        while True:
                            present, value = await _run_blocking(_next_or_done, iterator)
                            if not present:
                                return
                            yield value
                    stubs, metadata, deadline, selected = current
                    self._call._endpoint = selected
                    try:
                        item = await stubs.calls.GetResult(
                            pb.GetCallResultRequest(
                                call=pb.CallRef(call_id=self.id),
                                index=next_index,
                            ),
                            metadata=metadata,
                            timeout=deadline,
                        )
                        failures = 0
                    except grpc.RpcError as error:
                        if error.code() == grpc.StatusCode.NOT_FOUND:
                            try:
                                record = await stubs.calls.Get(
                                    pb.CallRef(call_id=self.id),
                                    metadata=metadata,
                                    timeout=deadline,
                                )
                            except grpc.RpcError as record_error:
                                owner_missing = record_error.code() == grpc.StatusCode.NOT_FOUND
                                if not owner_missing and not _reconnectable_rpc(record_error):
                                    raise
                                failures += 1
                                if failures > 5:
                                    raise
                                resolver = getattr(
                                    self._call._client.driver,
                                    "resolve_call",
                                    None,
                                )
                                if not callable(resolver):
                                    if owner_missing:
                                        raise
                                else:
                                    try:
                                        self._call._endpoint = await _run_blocking(
                                            resolver,
                                            self._call.id,
                                            self._call._endpoint,
                                        )
                                    except TypeError:
                                        self._call._endpoint = await _run_blocking(
                                            resolver,
                                            self._call.id,
                                        )
                                if not owner_missing:
                                    await asyncio.sleep(min(0.05 * (2 ** (failures - 1)), 1.0))
                                continue
                            failures = 0
                            if record.status not in _TERMINAL:
                                await asyncio.sleep(0.02)
                                continue
                            if _batch_result_should_exist(record, next_index):
                                if terminal_retry == next_index:
                                    raise RuntimeError(
                                        f"batch {self.id} is missing committed result {next_index}"
                                    ) from None
                                terminal_retry = next_index
                                continue
                            if self._feeder is not None:
                                await self._feeder
                            failure: BaseException | None = None
                            if record.HasField("error"):
                                failure = RemoteFunctionError(
                                    record.error.message or "remote batch failed",
                                    code=record.error.code,
                                    remote_type=record.error.type,
                                )
                            elif record.status == pb.CALL_STATUS_CANCELLED:
                                failure = RemoteFunctionError(
                                    "remote batch was cancelled",
                                    code="cancelled",
                                )
                            elif record.status != pb.CALL_STATUS_SUCCEEDED:
                                failure = RemoteFunctionError(
                                    f"remote batch ended with status {record.status}",
                                )
                            if failure is not None:
                                if return_exceptions:
                                    yield cast(R, failure)
                                    return
                                raise failure from None
                            return
                        if not _reconnectable_rpc(error):
                            raise
                        failures += 1
                        if failures > 5:
                            raise
                        resolver = getattr(self._call._client.driver, "resolve_call", None)
                        if callable(resolver):
                            try:
                                self._call._endpoint = await _run_blocking(
                                    resolver,
                                    self._call.id,
                                    self._call._endpoint,
                                )
                            except TypeError:
                                self._call._endpoint = await _run_blocking(
                                    resolver,
                                    self._call.id,
                                )
                        await asyncio.sleep(min(0.05 * (2 ** (failures - 1)), 1.0))
                        continue
                    try:
                        value = await _async_result_value(
                            stubs,
                            metadata,
                            item,
                            trusted=self._call._trusted,
                        )
                    except Exception as exc:
                        if not return_exceptions:
                            raise
                        value = exc
                    yield value
                    next_index += 1

            async for event, stubs, metadata, deadline in _AsyncCall(self._call)._events():
                kind = event.WhichOneof("payload")
                if kind == "result":
                    try:
                        value = await _async_result_value(
                            stubs,
                            metadata,
                            event.result,
                            trusted=self._call._trusted,
                        )
                    except Exception as exc:
                        if not return_exceptions:
                            raise
                        value = exc
                    yield value
                elif kind == "error":
                    if return_exceptions:
                        yield cast(
                            R,
                            RemoteFunctionError(
                                event.error.message,
                                code=event.error.code,
                                remote_type=event.error.type,
                            ),
                        )
                        continue
                    _raise(event.error)
                elif kind == "status" and event.status.status in _TERMINAL:
                    if self._feeder is not None:
                        await self._feeder
                    if event.status.status == pb.CALL_STATUS_SUCCEEDED:
                        return
                    record = await stubs.calls.Get(
                        pb.CallRef(call_id=self.id),
                        metadata=metadata,
                        timeout=deadline,
                    )
                    if record.HasField("error"):
                        terminal_error = RemoteFunctionError(
                            record.error.message or "remote batch failed",
                            code=record.error.code,
                            remote_type=record.error.type,
                        )
                    elif record.status == pb.CALL_STATUS_CANCELLED:
                        terminal_error = RemoteFunctionError(
                            "remote batch was cancelled",
                            code="cancelled",
                        )
                    else:
                        terminal_error = RemoteFunctionError(
                            f"remote batch ended with status {record.status}",
                        )
                    if return_exceptions:
                        yield cast(R, terminal_error)
                        return
                    raise terminal_error
        except asyncio.CancelledError, GeneratorExit:
            await asyncio.shield(self.cancel("async batch result consumer closed"))
            raise


class _AsyncRemoteFunction[**P, R]:
    def __init__(self, function: RemoteFunction[P, R]) -> None:
        self._function = function

    async def spawn(self, *args: P.args, **kwargs: P.kwargs) -> _AsyncCall[R]:
        function = self._function
        if function._signature:
            function._signature.bind(*args, **kwargs)
        client = _client(function._client)
        function._client = client
        endpoint = _aio_endpoint(client, function._endpoint)
        if endpoint is None:
            return _AsyncCall(await _run_blocking(function.spawn, *args, **kwargs))
        stubs, metadata, deadline, selected = endpoint
        function._endpoint = selected
        revision = await function._resolve_async(stubs, metadata, deadline)
        trusted = function.options.serializer.allow_trusted_python
        arguments = await _async_arguments(
            stubs,
            metadata,
            deadline,
            args,
            kwargs,
            codec=function.options.serializer.input_serializer,
            trusted=trusted,
            compression=function.options.serializer.compression,
        )
        record = await stubs.calls.Create(
            pb.CreateCallRequest(
                type=pb.CALL_TYPE_GENERATOR if function._is_generator else pb.CALL_TYPE_UNARY,
                target=pb.CallTarget(function=revision.ref),
                inputs=[pb.CallInput(index=0, input_id=str(uuid.uuid4()), arguments=arguments)],
                inputs_closed=True,
                request_id=str(uuid.uuid4()),
            ),
            metadata=metadata,
            timeout=deadline,
        )
        return _AsyncCall(
            FunctionCall(
                record.ref.call_id,
                client,
                trusted=trusted,
                generator=function._is_generator,
                endpoint=selected,
            )
        )

    async def remote(self, *args: P.args, **kwargs: P.kwargs) -> R:
        if self._function._is_generator:
            raise ValueError("generator functions must use remote_gen()")
        call = await self.spawn(*args, **kwargs)
        return await call.get()

    async def remote_gen(self, *args: P.args, **kwargs: P.kwargs) -> AsyncIterator[R]:
        if not self._function._is_generator:
            raise ValueError("non-generator functions must use remote()")
        async_call = await self.spawn(*args, **kwargs)
        call = async_call._call
        endpoint = _aio_endpoint(call._client, call._endpoint)
        if endpoint is None:
            iterator = call.iter_yields()
            while True:
                present, value = await _run_blocking(_next_or_done, iterator)
                if not present:
                    return
                yield value
            return
        try:
            async for event, stubs, metadata, _ in async_call._events():
                kind = event.WhichOneof("payload")
                if kind == "yield_result":
                    yield await _async_result_value(
                        stubs,
                        metadata,
                        event.yield_result,
                        trusted=call._trusted,
                    )
                elif kind == "error":
                    _raise(event.error)
                elif kind == "status" and event.status.status in _TERMINAL:
                    return
        except asyncio.CancelledError:
            await asyncio.shield(async_call.cancel("async generator cancelled"))
            raise

    async def spawn_map(
        self,
        iterable: Iterable[Any] | AsyncIterable[Any],
        *,
        kwargs: Mapping[str, Any] | None = None,
        starmap: bool = False,
        max_in_flight: int | None = None,
    ) -> _AsyncBatchCall[R]:
        if max_in_flight is not None and (
            isinstance(max_in_flight, bool)
            or not isinstance(max_in_flight, int)
            or max_in_flight < 1
        ):
            raise ValueError("max_in_flight must be a positive integer")
        function = self._function
        client = _client(function._client)
        function._client = client
        endpoint = _aio_endpoint(client, function._endpoint)
        if endpoint is None:
            if isinstance(iterable, AsyncIterable):
                raise RuntimeError("AsyncIterable map inputs require a native async driver")
            batch = await _run_blocking(
                function.spawn_map,
                iterable,
                kwargs=kwargs,
                starmap=starmap,
                max_in_flight=max_in_flight,
            )
            return batch.aio

        stubs, metadata, deadline, selected = endpoint
        function._endpoint = selected
        revision = await function._resolve_async(stubs, metadata, deadline)
        record = await stubs.calls.Create(
            pb.CreateCallRequest(
                type=pb.CALL_TYPE_BATCH,
                target=pb.CallTarget(function=revision.ref),
                inputs_closed=False,
                request_id=str(uuid.uuid4()),
            ),
            metadata=metadata,
            timeout=deadline,
        )
        call = FunctionCall[Any](
            record.ref.call_id,
            client,
            trusted=function.options.serializer.allow_trusted_python,
            endpoint=selected,
        )
        slots = asyncio.Semaphore(0)
        opener_acknowledged = asyncio.Event()
        count = 0
        call_kwargs = dict(kwargs or {})

        async def inputs() -> AsyncIterator[pb.StreamCallInputsRequest]:
            nonlocal count
            yield pb.StreamCallInputsRequest(call=record.ref)
            await opener_acknowledged.wait()
            source = _iterate_async(iterable)
            try:
                while True:
                    await slots.acquire()
                    try:
                        item = await anext(source)
                    except StopAsyncIteration:
                        return
                    args = tuple(item) if starmap else (item,)
                    if function._signature:
                        function._signature.bind(*args, **call_kwargs)
                    arguments = await _async_arguments(
                        stubs,
                        metadata,
                        deadline,
                        args,
                        call_kwargs,
                        codec=function.options.serializer.input_serializer,
                        trusted=function.options.serializer.allow_trusted_python,
                        compression=function.options.serializer.compression,
                    )
                    index = count
                    count += 1
                    yield pb.StreamCallInputsRequest(
                        input=pb.CallInput(
                            index=index,
                            input_id=str(uuid.uuid4()),
                            arguments=arguments,
                        )
                    )
            finally:
                await source.aclose()

        async def feed() -> None:
            try:
                first = True
                responses = stubs.calls.StreamInputs(
                    inputs(),
                    metadata=metadata,
                    timeout=deadline,
                )
                async for response in responses:
                    if first:
                        advertised = max(1, response.max_inputs_outstanding)
                        window = (
                            advertised if max_in_flight is None else min(advertised, max_in_flight)
                        )
                        for _ in range(window):
                            slots.release()
                        opener_acknowledged.set()
                        first = False
                    elif response.HasField("last_input"):
                        slots.release()
                if first:
                    raise RuntimeError("input stream ended before opener acknowledgement")
                await stubs.calls.CloseInputs(
                    pb.CloseCallInputsRequest(
                        call=record.ref,
                        expected_input_count=count,
                    ),
                    metadata=metadata,
                    timeout=deadline,
                )
            except asyncio.CancelledError:
                raise
            except BaseException:
                await asyncio.shield(_AsyncCall(call).cancel("async batch input feeder failed"))
                raise

        feeder = asyncio.create_task(feed(), name=f"vmon-batch-feeder-{call.id}")
        return _AsyncBatchCall(call, feeder=feeder)

    async def map(
        self,
        *iterables: Iterable[Any] | AsyncIterable[Any],
        **options: Any,
    ) -> AsyncIterator[R]:
        if not iterables:
            raise ValueError("map requires at least one iterable")
        call_kwargs = options.pop("kwargs", None)
        if call_kwargs is not None and not isinstance(call_kwargs, Mapping):
            raise TypeError("kwargs must be a mapping")
        ordered = bool(options.pop("order_outputs", True))
        return_exceptions = bool(options.pop("return_exceptions", False))
        max_in_flight = options.pop("max_in_flight", None)
        if options:
            unknown = ", ".join(sorted(options))
            raise TypeError(f"unsupported async map options: {unknown}")
        iterable: Iterable[Any] | AsyncIterable[Any]
        if len(iterables) == 1:
            iterable = iterables[0]
        else:
            iterable = _zip_async(iterables)
        batch = await self.spawn_map(
            iterable,
            kwargs=call_kwargs,
            starmap=len(iterables) > 1,
            max_in_flight=max_in_flight,
        )
        async for value in batch.results(
            order_outputs=ordered,
            return_exceptions=return_exceptions,
        ):
            yield value

    async def starmap(
        self,
        iterable: Iterable[Iterable[Any]] | AsyncIterable[Iterable[Any]],
        **options: Any,
    ) -> AsyncIterator[R]:
        call_kwargs = options.pop("kwargs", None)
        if call_kwargs is not None and not isinstance(call_kwargs, Mapping):
            raise TypeError("kwargs must be a mapping")
        ordered = bool(options.pop("order_outputs", True))
        return_exceptions = bool(options.pop("return_exceptions", False))
        max_in_flight = options.pop("max_in_flight", None)
        if options:
            unknown = ", ".join(sorted(options))
            raise TypeError(f"unsupported async starmap options: {unknown}")
        batch = await self.spawn_map(
            iterable,
            kwargs=call_kwargs,
            starmap=True,
            max_in_flight=max_in_flight,
        )
        async for value in batch.results(
            order_outputs=ordered,
            return_exceptions=return_exceptions,
        ):
            yield value

    async def for_each(
        self,
        *iterables: Iterable[Any] | AsyncIterable[Any],
        **kwargs: Any,
    ) -> None:
        async for _ in self.map(*iterables, **kwargs):
            pass


class RemoteFunction[**P, R]:
    """Immutable typed reference to a zero-deploy or registered function."""

    __vmon_class_lifecycle__: str
    __vmon_lifecycle_metadata__: Any

    def __init__(
        self,
        function: Callable[P, Any] | None = None,
        *,
        client: Any | None = None,
        namespace: str = "default",
        name: str | None = None,
        revision: str | None = None,
        options: FunctionOptions | None = None,
        generator: bool = False,
        package_mode: str = "package",
        package_root: str | None = None,
        include: Iterable[str] = (),
        exclude: Iterable[str] = (),
        local_packages: Iterable[str] = (),
    ) -> None:
        if function is None and not name:
            raise ValueError("name is required for deployed function lookup")
        self._function, self._client = function, client
        self.namespace, self.name, self.revision = (
            namespace,
            name or (function.__name__ if function is not None else ""),
            revision,
        )
        attached_options = getattr(function, "__vmon_options__", None) if function else None
        self.options = options or attached_options or FunctionOptions()
        if not isinstance(self.options, FunctionOptions):
            raise TypeError("__vmon_options__ must be FunctionOptions")
        if package_mode == "cloudpickle":
            if options is not None and not self.options.serializer.allow_trusted_python:
                raise ValueError(
                    "cloudpickle package mode conflicts with allow_trusted_python=False"
                )
            self.options = replace(
                self.options,
                serializer=replace(self.options.serializer, allow_trusted_python=True),
            )
        if not isinstance(self.options, FunctionOptions):
            raise TypeError("__vmon_options__ must be FunctionOptions")
        self.package_mode, self.package_root = package_mode, package_root
        self.include, self.exclude, self.local_packages = (
            tuple(include),
            tuple(exclude),
            tuple(local_packages),
        )
        self._registered: pb.FunctionRevision | None = None
        self._endpoint: str | None = None
        self._lock = threading.Lock()
        self._signature = inspect.signature(function) if function else None
        self._is_generator = bool(
            generator
            or function
            and (inspect.isgeneratorfunction(function) or inspect.isasyncgenfunction(function))
        )
        if function:
            functools.update_wrapper(self, function)

    @classmethod
    def from_name(
        cls,
        name: str,
        *,
        namespace: str = "default",
        revision: str | None = None,
        client: Any | None = None,
        options: FunctionOptions | None = None,
        generator: bool = False,
    ) -> RemoteFunction[..., Any]:
        return cls(
            None,
            name=name,
            namespace=namespace,
            revision=revision,
            client=client,
            options=options,
            generator=generator,
        )

    def with_options(self, options: FunctionOptions | None = None, **changes: Any) -> Self:
        updated = options or replace(self.options, **changes)
        return type(self)(
            self._function,
            client=self._client,
            namespace=self.namespace,
            name=self.name,
            revision=self.revision,
            options=updated,
            generator=self._is_generator,
            package_mode=self.package_mode,
            package_root=self.package_root,
            include=self.include,
            exclude=self.exclude,
            local_packages=self.local_packages,
        )

    def _adopt_revision_options(self, revision: pb.FunctionRevision) -> None:
        if self._function is not None:
            return
        spec = revision.spec.serializer
        codecs = {
            pb.VALUE_SERIALIZER_JSON: ValueCodec.JSON,
            pb.VALUE_SERIALIZER_CBOR: ValueCodec.CBOR,
            pb.VALUE_SERIALIZER_CLOUDPICKLE: ValueCodec.CLOUDPICKLE,
        }
        compressions = {
            pb.VALUE_COMPRESSION_NONE: ValueCompression.NONE,
            pb.VALUE_COMPRESSION_GZIP: ValueCompression.GZIP,
            pb.VALUE_COMPRESSION_ZSTD: ValueCompression.ZSTD,
        }
        self.options = replace(
            self.options,
            serializer=SerializerPolicy(
                input_serializer=codecs[spec.input_serializer],
                result_serializer=codecs[spec.result_serializer],
                compression=compressions[spec.compression],
                allow_trusted_python=spec.allow_trusted_python,
            ),
        )

    @property
    def aio(self) -> _AsyncRemoteFunction[P, R]:
        return _AsyncRemoteFunction(self)

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Any:
        if self._function is None:
            raise TypeError("deployed function references cannot run locally")
        return self._function(*args, **kwargs)

    def local(self, *args: P.args, **kwargs: P.kwargs) -> Any:
        return self(*args, **kwargs)

    def _make_spec(
        self,
        package: PackageArtifact | SerializedCallable,
        source: pb.ArtifactRef,
        image_refs: dict[str, pb.ArtifactRef],
    ) -> pb.FunctionSpec:
        fields = options_to_proto(self.options, image_refs)
        lifecycle_name = getattr(self, "__vmon_class_lifecycle__", None)
        metadata = getattr(self, "__vmon_lifecycle_metadata__", None)
        lifecycle = (
            pb.FUNCTION_LIFECYCLE_ACTOR
            if lifecycle_name == "actor"
            else pb.FUNCTION_LIFECYCLE_INSTANCE
            if lifecycle_name == "service"
            else pb.FUNCTION_LIFECYCLE_STATELESS
        )
        hooks = pb.LifecycleHooks()
        if metadata is not None and lifecycle_name is None:
            assert self._function is not None
            module = self._function.__module__
            owner = self._function.__qualname__

            def hook(method_name: str) -> pb.LifecycleHookRef:
                return pb.LifecycleHookRef(module=module, qualname=f"{owner}.{method_name}")

            initialize = (*metadata.snapshot_enter, *metadata.enter)
            if initialize:
                hooks.initialize.CopyFrom(hook(initialize[0]))
            if metadata.exit:
                hooks.shutdown.CopyFrom(hook(metadata.exit[0]))
            if metadata.snapshot_enter:
                hooks.snapshot.CopyFrom(hook(metadata.snapshot_enter[0]))
            if metadata.restore:
                hooks.restore.CopyFrom(hook(metadata.restore[0]))
        if metadata is not None:
            hooks.snapshot_after_initialize = metadata.snapshot_after_initialize
            hooks.snapshot_on_worker_retire = metadata.snapshot_on_worker_retire
        python_abi = (
            package.manifest.python_abi
            if isinstance(package, PackageArtifact)
            else package.python_abi
        )
        reproducibility = pb.ReproducibilitySpec(
            build_inputs_digest=digest(self.options.canonical_digest(package)),
            builder_id="vmon.python.function",
            builder_version="1",
            source_date_epoch=315_532_800,
            environment={
                "PACKAGE_MODE": package.codec,
                "PYTHON_ABI": python_abi,
            },
        )
        return pb.FunctionSpec(
            function=pb.FunctionRef(namespace=self.namespace, name=self.name),
            package=package_to_proto(
                package,
                source,
                trusted_serialization=self.options.serializer.allow_trusted_python,
            ),
            lifecycle=lifecycle,
            lifecycle_hooks=hooks,
            reproducibility=reproducibility,
            **fields,
        )

    async def _resolve_async(
        self, stubs: Any, metadata: Any, deadline: float
    ) -> pb.FunctionRevision:
        if self._registered is not None:
            return self._registered
        if self._function is None:
            selector = (
                pb.FunctionSelector(
                    pinned=pb.RevisionRef(
                        function=pb.FunctionRef(namespace=self.namespace, name=self.name),
                        revision_id=self.revision,
                    )
                )
                if self.revision
                else pb.FunctionSelector(
                    current=pb.FunctionRef(namespace=self.namespace, name=self.name)
                )
            )
            self._registered = await stubs.functions.Get(
                pb.GetFunctionRequest(function=selector),
                metadata=metadata,
                timeout=deadline,
            )
            self._adopt_revision_options(self._registered)
            return self._registered
        package = package_callable(
            self._function,
            mode=cast(Any, self.package_mode),
            root=self.package_root,
            include=self.include,
            exclude=self.exclude,
            local_packages=self.local_packages,
        )
        source = await _async_upload(
            stubs,
            metadata,
            deadline,
            package.data,
            package.sha256,
            "application/vnd.vmon.python-package",
        )
        image_refs: dict[str, pb.ArtifactRef] = {}
        for artifact in self.options.image.artifacts():
            image_refs[artifact.key] = await _async_upload(
                stubs,
                metadata,
                deadline,
                artifact.data,
                artifact.sha256,
                artifact.media_type,
            )
        spec = self._make_spec(package, source, image_refs)
        request_id = hashlib.sha256(spec.SerializeToString(deterministic=True)).hexdigest()
        self._registered = await stubs.functions.Register(
            pb.RegisterFunctionRequest(spec=spec, request_id=request_id),
            metadata=metadata,
            timeout=deadline,
        )
        return self._registered

    def _resolve(self) -> pb.FunctionRevision:
        if self._registered is not None:
            return self._registered
        client = _client(self._client)
        self._client = client
        if self._function is None:
            selector = (
                pb.FunctionSelector(
                    pinned=pb.RevisionRef(
                        function=pb.FunctionRef(namespace=self.namespace, name=self.name),
                        revision_id=self.revision,
                    )
                )
                if self.revision
                else pb.FunctionSelector(
                    current=pb.FunctionRef(namespace=self.namespace, name=self.name)
                )
            )
            self._registered, self._endpoint = _call_owner(
                client,
                lambda stubs: stubs.functions.Get(pb.GetFunctionRequest(function=selector)),
            )
            self._adopt_revision_options(self._registered)
            return self._registered
        with self._lock:
            if self._registered is not None:
                return self._registered
            package = package_callable(
                self._function,
                mode=cast(Any, self.package_mode),
                root=self.package_root,
                include=self.include,
                exclude=self.exclude,
                local_packages=self.local_packages,
            )
            source = _upload(
                client, package.data, package.sha256, "application/vnd.vmon.python-package"
            )
            image_refs: dict[str, pb.ArtifactRef] = {}
            for artifact in self.options.image.artifacts():
                image_refs[artifact.key] = _upload(
                    client,
                    artifact.data,
                    artifact.sha256,
                    artifact.media_type,
                    endpoint=self._endpoint,
                )
            spec = self._make_spec(package, source, image_refs)
            request_id = hashlib.sha256(spec.SerializeToString(deterministic=True)).hexdigest()
            self._registered, self._endpoint = _call_owner(
                client,
                lambda stubs: stubs.functions.Register(
                    pb.RegisterFunctionRequest(spec=spec, request_id=request_id)
                ),
            )
            return self._registered

    def activate(self) -> pb.FunctionRecord:
        """Atomically make this immutable revision current."""
        revision = self._resolve()
        client = _client(self._client)
        return _call(
            client,
            lambda stubs: stubs.functions.Activate(
                pb.ActivateFunctionRequest(revision=revision.ref)
            ),
            endpoint=self._endpoint,
        )

    def spawn(self, *args: P.args, **kwargs: P.kwargs) -> FunctionCall[R]:
        if self._signature:
            self._signature.bind(*args, **kwargs)
        revision = self._resolve()
        client = _client(self._client)
        codec = self.options.serializer.input_serializer
        trusted = self.options.serializer.allow_trusted_python
        arguments = _arguments(
            client,
            args,
            kwargs,
            codec=codec,
            trusted=trusted,
            compression=self.options.serializer.compression,
            endpoint=self._endpoint,
        )
        call_type = pb.CALL_TYPE_GENERATOR if self._is_generator else pb.CALL_TYPE_UNARY
        request = pb.CreateCallRequest(
            type=call_type,
            target=pb.CallTarget(function=revision.ref),
            inputs=[pb.CallInput(index=0, input_id=str(uuid.uuid4()), arguments=arguments)],
            inputs_closed=True,
            request_id=str(uuid.uuid4()),
        )
        record, endpoint = _call_owner(
            client, lambda stubs: stubs.calls.Create(request), endpoint=self._endpoint
        )
        return FunctionCall(
            record.ref.call_id,
            client,
            trusted=trusted,
            generator=self._is_generator,
            endpoint=endpoint,
        )

    def _spawn_actor(
        self, actor_id: str, method: str, args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> FunctionCall[Any]:
        if not actor_id or not method:
            raise ValueError("actor_id and method must be non-empty")
        revision = self._resolve()
        client = _client(self._client)
        trusted = self.options.serializer.allow_trusted_python
        arguments = _arguments(
            client,
            args,
            kwargs,
            codec=self.options.serializer.input_serializer,
            trusted=trusted,
            compression=self.options.serializer.compression,
            endpoint=self._endpoint,
        )
        request = pb.CreateCallRequest(
            type=pb.CALL_TYPE_ACTOR,
            target=pb.CallTarget(
                function=revision.ref,
                actor=pb.ActorTarget(actor=pb.ActorRef(actor_id=actor_id), method=method),
            ),
            inputs=[pb.CallInput(index=0, input_id=str(uuid.uuid4()), arguments=arguments)],
            inputs_closed=True,
            request_id=str(uuid.uuid4()),
        )
        record, endpoint = _call_owner(
            client, lambda stubs: stubs.calls.Create(request), endpoint=self._endpoint
        )
        return FunctionCall(record.ref.call_id, client, trusted=trusted, endpoint=endpoint)

    def _spawn_service(
        self,
        service_key: str,
        constructor_args: tuple[Any, ...],
        constructor_kwargs: dict[str, Any],
        method: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> FunctionCall[Any]:
        if not service_key or not method:
            raise ValueError("service_key and method must be non-empty")
        revision = self._resolve()
        client = _client(self._client)
        trusted = self.options.serializer.allow_trusted_python
        constructor = _arguments(
            client,
            constructor_args,
            constructor_kwargs,
            codec=self.options.serializer.input_serializer,
            trusted=trusted,
            compression=self.options.serializer.compression,
            endpoint=self._endpoint,
        )
        arguments = _arguments(
            client,
            args,
            kwargs,
            codec=self.options.serializer.input_serializer,
            trusted=trusted,
            compression=self.options.serializer.compression,
            endpoint=self._endpoint,
        )
        request = pb.CreateCallRequest(
            type=pb.CALL_TYPE_UNARY,
            target=pb.CallTarget(
                function=revision.ref,
                service=pb.ServiceTarget(
                    service_key=service_key,
                    method=method,
                    constructor=constructor,
                ),
            ),
            inputs=[pb.CallInput(index=0, input_id=str(uuid.uuid4()), arguments=arguments)],
            inputs_closed=True,
            request_id=str(uuid.uuid4()),
        )
        record, endpoint = _call_owner(
            client, lambda stubs: stubs.calls.Create(request), endpoint=self._endpoint
        )
        return FunctionCall(record.ref.call_id, client, trusted=trusted, endpoint=endpoint)

    def remote(self, *args: P.args, **kwargs: P.kwargs) -> R:
        if self._is_generator:
            raise ValueError("generator functions must use remote_gen()")
        return self.spawn(*args, **kwargs).get()

    def remote_gen(self, *args: P.args, **kwargs: P.kwargs) -> Iterator[R]:
        if not self._is_generator:
            raise ValueError("non-generator functions must use remote()")
        return self.spawn(*args, **kwargs).iter_yields()

    def spawn_map(
        self,
        iterable: Iterable[Any],
        *,
        kwargs: Mapping[str, Any] | None = None,
        starmap: bool = False,
        max_in_flight: int | None = None,
    ) -> BatchCall[R]:
        if max_in_flight is not None and (
            isinstance(max_in_flight, bool)
            or not isinstance(max_in_flight, int)
            or max_in_flight < 1
        ):
            raise ValueError("max_in_flight must be a positive integer")
        revision = self._resolve()
        client = _client(self._client)
        trusted = self.options.serializer.allow_trusted_python
        request = pb.CreateCallRequest(
            type=pb.CALL_TYPE_BATCH,
            target=pb.CallTarget(function=revision.ref),
            inputs_closed=False,
            request_id=str(uuid.uuid4()),
        )
        record, endpoint = _call_owner(
            client, lambda stubs: stubs.calls.Create(request), endpoint=self._endpoint
        )
        stop = threading.Event()
        errors: list[BaseException] = []
        batch: BatchCall[R] = BatchCall(
            FunctionCall(record.ref.call_id, client, trusted=trusted, endpoint=endpoint),
            feeder_stop=stop,
            feeder_errors=errors,
        )

        def feed() -> None:
            count = 0
            condition = threading.Condition()
            acknowledged = 0
            window = 0
            call_kwargs = dict(kwargs or {})
            source = iter(iterable)
            try:

                def frames() -> Iterator[pb.StreamCallInputsRequest]:
                    nonlocal count
                    yield pb.StreamCallInputsRequest(call=record.ref)
                    while True:
                        with condition:
                            while not stop.is_set() and count - acknowledged >= window:
                                condition.wait(0.05)
                            if stop.is_set():
                                return
                        try:
                            item = next(source)
                        except StopIteration:
                            return
                        args = tuple(item) if starmap else (item,)
                        if self._signature:
                            self._signature.bind(*args, **call_kwargs)
                        arguments = _arguments(
                            client,
                            args,
                            call_kwargs,
                            codec=self.options.serializer.input_serializer,
                            trusted=trusted,
                            compression=self.options.serializer.compression,
                            endpoint=endpoint,
                        )
                        index = count
                        count += 1
                        yield pb.StreamCallInputsRequest(
                            input=pb.CallInput(
                                index=index,
                                input_id=str(uuid.uuid4()),
                                arguments=arguments,
                            )
                        )

                responses = _call(
                    client,
                    lambda stubs: stubs.calls.StreamInputs(frames()),
                    stream=True,
                    endpoint=endpoint,
                )
                first = True
                for response in responses:
                    with condition:
                        if first:
                            advertised = max(1, response.max_inputs_outstanding)
                            window = (
                                advertised
                                if max_in_flight is None
                                else min(advertised, max_in_flight)
                            )
                            first = False
                        else:
                            acknowledged = max(
                                acknowledged,
                                response.committed_input_count,
                            )
                        condition.notify_all()
                    if stop.is_set():
                        break
                if first and not stop.is_set():
                    raise RuntimeError("input stream ended before opener acknowledgement")
                if not stop.is_set():
                    _call(
                        client,
                        lambda stubs: stubs.calls.CloseInputs(
                            pb.CloseCallInputsRequest(call=record.ref, expected_input_count=count)
                        ),
                        endpoint=endpoint,
                    )
                    batch.input_count = count
            except BaseException as exc:
                errors.append(exc)
                stop.set()
                try:
                    _call(
                        client,
                        lambda stubs: stubs.calls.Cancel(
                            pb.CancelCallRequest(
                                call=record.ref,
                                reason="batch input feeder failed",
                                request_id=str(uuid.uuid4()),
                            )
                        ),
                        endpoint=endpoint,
                    )
                except BaseException:
                    pass

        threading.Thread(
            target=feed,
            name=f"vmon-batch-feeder-{record.ref.call_id}",
            daemon=True,
        ).start()
        return batch

    def map(self, *iterables: Iterable[Any], **options: Any) -> Iterator[R]:
        if not iterables:
            raise ValueError("map requires at least one iterable")
        call_kwargs = options.pop("kwargs", None)
        max_in_flight = options.pop("max_in_flight", None)
        order_outputs = bool(options.pop("order_outputs", True))
        return_exceptions = bool(options.pop("return_exceptions", False))
        if options:
            unknown = ", ".join(sorted(options))
            raise TypeError(f"unsupported map options: {unknown}")
        items = iterables[0] if len(iterables) == 1 else zip(*iterables, strict=False)
        return self.spawn_map(
            items,
            kwargs=call_kwargs,
            starmap=len(iterables) > 1,
            max_in_flight=max_in_flight,
        ).results(
            order_outputs=order_outputs,
            return_exceptions=return_exceptions,
        )

    def starmap(self, iterable: Iterable[Iterable[Any]], **options: Any) -> Iterator[R]:
        call_kwargs = options.pop("kwargs", None)
        max_in_flight = options.pop("max_in_flight", None)
        order_outputs = bool(options.pop("order_outputs", True))
        return_exceptions = bool(options.pop("return_exceptions", False))
        if options:
            unknown = ", ".join(sorted(options))
            raise TypeError(f"unsupported starmap options: {unknown}")
        return self.spawn_map(
            iterable,
            kwargs=call_kwargs,
            starmap=True,
            max_in_flight=max_in_flight,
        ).results(
            order_outputs=order_outputs,
            return_exceptions=return_exceptions,
        )

    def for_each(self, *iterables: Iterable[Any], **kwargs: Any) -> None:
        for _ in self.map(*iterables, **kwargs):
            pass


class SyncRemoteFunction[**P, R](RemoteFunction[P, R]):
    """Typed wrapper whose local implementation is a synchronous function."""

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        return super().__call__(*args, **kwargs)

    def local(self, *args: P.args, **kwargs: P.kwargs) -> R:
        return self(*args, **kwargs)


class AsyncRemoteFunction[**P, R](RemoteFunction[P, R]):
    """Typed wrapper whose local implementation is a coroutine function."""

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Coroutine[Any, Any, R]:
        if self._function is None:
            raise TypeError("deployed function references cannot run locally")
        return self._function(*args, **kwargs)

    def local(self, *args: P.args, **kwargs: P.kwargs) -> Coroutine[Any, Any, R]:
        return self(*args, **kwargs)


class GeneratorRemoteFunction[**P, R](RemoteFunction[P, R]):
    """Typed wrapper whose local implementation is a synchronous generator."""

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Iterator[R]:
        if self._function is None:
            raise TypeError("deployed function references cannot run locally")
        return self._function(*args, **kwargs)

    def local(self, *args: P.args, **kwargs: P.kwargs) -> Iterator[R]:
        return self(*args, **kwargs)


class AsyncGeneratorRemoteFunction[**P, R](RemoteFunction[P, R]):
    """Typed wrapper whose local implementation is an asynchronous generator."""

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> AsyncIterator[R]:
        if self._function is None:
            raise TypeError("deployed function references cannot run locally")
        return self._function(*args, **kwargs)

    def local(self, *args: P.args, **kwargs: P.kwargs) -> AsyncIterator[R]:
        return self(*args, **kwargs)


class _FunctionDecorator(Protocol):
    @overload
    def __call__[**Q, T](  # type: ignore[overload-overlap]
        self, function: Callable[Q, Iterator[T]], /
    ) -> GeneratorRemoteFunction[Q, T]: ...

    @overload
    def __call__[**Q, T](  # type: ignore[overload-overlap]
        self, function: Callable[Q, AsyncIterator[T]], /
    ) -> AsyncGeneratorRemoteFunction[Q, T]: ...

    @overload
    def __call__[**Q, T](  # type: ignore[overload-overlap]
        self, function: Callable[Q, Coroutine[Any, Any, T]], /
    ) -> AsyncRemoteFunction[Q, T]: ...

    @overload
    def __call__[**Q, T](self, function: Callable[Q, T], /) -> SyncRemoteFunction[Q, T]: ...


@overload
def function[**P, Y](  # type: ignore[overload-overlap]
    function: Callable[P, Iterator[Y]], /
) -> GeneratorRemoteFunction[P, Y]: ...


@overload
def function[**P, Y](  # type: ignore[overload-overlap]
    function: Callable[P, AsyncIterator[Y]], /
) -> AsyncGeneratorRemoteFunction[P, Y]: ...


@overload
def function[**P, Y](  # type: ignore[overload-overlap]
    function: Callable[P, Coroutine[Any, Any, Y]], /
) -> AsyncRemoteFunction[P, Y]: ...


@overload
def function[**P, R](function: Callable[P, R], /) -> SyncRemoteFunction[P, R]: ...
@overload
def function(
    function: None = None,
    /,
    *,
    client: Any | None = None,
    namespace: str = "default",
    name: str | None = None,
    options: FunctionOptions | None = None,
    image: Image | str | None = None,
    cpus: float | None = None,
    memory: int | None = None,
    disk: int | None = None,
    architecture: CpuArchitecture | str | None = None,
    ha: HighAvailabilityPolicy | str | None = None,
    volumes: tuple[FunctionVolumeMount, ...] | None = None,
    network: NetworkPolicy | None = None,
    egress: Iterable[str] | None = None,
    block_network: bool | None = None,
    egress_cidrs: Iterable[str] | None = None,
    egress_domains: Iterable[str] | None = None,
    inbound_cidrs: Iterable[str] | None = None,
    retries: RetryPolicy | int | None = None,
    startup_timeout: float | None = None,
    execution_timeout: float | None = None,
    queue_timeout: float | None = None,
    idle_timeout: float | None = None,
    result_ttl: float | None = None,
    min_workers: int | None = None,
    max_workers: int | None = None,
    buffer_workers: int | None = None,
    scale_down_after: float | None = None,
    max_outstanding_inputs: int | None = None,
    serializer: SerializerPolicy | ValueCodec | str | None = None,
    package_mode: str = "package",
    package_root: str | None = None,
    include: Iterable[str] = (),
    exclude: Iterable[str] = (),
    local_packages: Iterable[str] = (),
) -> _FunctionDecorator: ...
def function(
    function: Callable[..., Any] | None = None,
    /,
    *,
    client: Any | None = None,
    namespace: str = "default",
    name: str | None = None,
    options: FunctionOptions | None = None,
    image: Image | str | None = None,
    cpus: float | None = None,
    memory: int | None = None,
    disk: int | None = None,
    architecture: CpuArchitecture | str | None = None,
    ha: HighAvailabilityPolicy | str | None = None,
    volumes: tuple[FunctionVolumeMount, ...] | None = None,
    network: NetworkPolicy | None = None,
    egress: Iterable[str] | None = None,
    block_network: bool | None = None,
    egress_cidrs: Iterable[str] | None = None,
    egress_domains: Iterable[str] | None = None,
    inbound_cidrs: Iterable[str] | None = None,
    retries: RetryPolicy | int | None = None,
    startup_timeout: float | None = None,
    execution_timeout: float | None = None,
    queue_timeout: float | None = None,
    idle_timeout: float | None = None,
    result_ttl: float | None = None,
    min_workers: int | None = None,
    max_workers: int | None = None,
    buffer_workers: int | None = None,
    scale_down_after: float | None = None,
    max_outstanding_inputs: int | None = None,
    serializer: SerializerPolicy | ValueCodec | str | None = None,
    package_mode: str = "package",
    package_root: str | None = None,
    include: Iterable[str] = (),
    exclude: Iterable[str] = (),
    local_packages: Iterable[str] = (),
) -> Any:
    """Decorate a Python callable as a typed, durable remote function."""
    has_explicit_options = options is not None or any(
        value is not None
        for value in (
            image,
            cpus,
            memory,
            disk,
            architecture,
            ha,
            volumes,
            network,
            egress,
            block_network,
            egress_cidrs,
            egress_domains,
            inbound_cidrs,
            retries,
            startup_timeout,
            execution_timeout,
            queue_timeout,
            idle_timeout,
            result_ttl,
            min_workers,
            max_workers,
            buffer_workers,
            scale_down_after,
            max_outstanding_inputs,
            serializer,
        )
    )
    configured = _compose_function_options(
        options,
        image=image,
        cpus=cpus,
        memory=memory,
        disk=disk,
        architecture=architecture,
        ha=ha,
        volumes=volumes,
        network=network,
        egress=egress,
        block_network=block_network,
        egress_cidrs=egress_cidrs,
        egress_domains=egress_domains,
        inbound_cidrs=inbound_cidrs,
        retries=retries,
        startup_timeout=startup_timeout,
        execution_timeout=execution_timeout,
        queue_timeout=queue_timeout,
        idle_timeout=idle_timeout,
        result_ttl=result_ttl,
        min_workers=min_workers,
        max_workers=max_workers,
        buffer_workers=buffer_workers,
        scale_down_after=scale_down_after,
        max_outstanding_inputs=max_outstanding_inputs,
        serializer=serializer,
    )

    def decorate(inner: Callable[..., Any]) -> RemoteFunction[..., Any]:
        wrapper: type[RemoteFunction[..., Any]]
        if inspect.isasyncgenfunction(inner):
            wrapper = AsyncGeneratorRemoteFunction
        elif inspect.isgeneratorfunction(inner):
            wrapper = GeneratorRemoteFunction
        elif inspect.iscoroutinefunction(inner):
            wrapper = AsyncRemoteFunction
        else:
            wrapper = SyncRemoteFunction
        return wrapper(
            inner,
            client=client,
            namespace=namespace,
            name=name,
            options=configured if has_explicit_options else None,
            package_mode=package_mode,
            package_root=package_root,
            include=include,
            exclude=exclude,
            local_packages=local_packages,
        )

    return decorate(function) if function is not None else decorate
