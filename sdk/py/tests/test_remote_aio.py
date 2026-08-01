from __future__ import annotations

import asyncio
from types import SimpleNamespace

import grpc
import pytest

import vmon.remote as remote_module
from vmon._function_proto import value_to_proto
from vmon.errors import RemoteFunctionError
from vmon.remote import BatchCall, FunctionCall, RemoteFunction, _AsyncCall
from vmon.v1 import api_pb2
from vmon.values import ValueCodec, ValueCompression, encode_value


class _NeverEndingWatch:
    def __aiter__(self):
        return self

    async def __anext__(self):
        await asyncio.sleep(60)
        raise StopAsyncIteration


class _Calls:
    def __init__(self) -> None:
        self.cancelled = asyncio.Event()

    def Watch(self, request, **kwargs):
        return _NeverEndingWatch()

    async def Cancel(self, request, **kwargs):
        self.cancelled.set()
        return api_pb2.CallRecord(ref=request.call, status=api_pb2.CALL_STATUS_CANCELLED)


class _Driver:
    def __init__(self, calls: _Calls) -> None:
        self.stubs = SimpleNamespace(calls=calls)

    def aio_endpoint(self):
        return self.stubs, (), 1.0


def test_native_aio_task_cancellation_is_durably_forwarded() -> None:
    async def scenario() -> None:
        calls = _Calls()
        client = SimpleNamespace(driver=_Driver(calls))
        handle = _AsyncCall(FunctionCall("call-aio", client))
        task = asyncio.create_task(handle.get())
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert calls.cancelled.is_set()

    asyncio.run(scenario())


class _BatchCalls:
    def __init__(self) -> None:
        self.first_received = asyncio.Event()
        self.release_first = asyncio.Event()
        self.closed = asyncio.Event()
        self.cancelled = asyncio.Event()
        self.prefetch: asyncio.Task[api_pb2.StreamCallInputsRequest] | None = None
        self.request_iterator = None

    async def Create(self, request, **kwargs):
        return api_pb2.CallRecord(
            ref=api_pb2.CallRef(call_id="batch-aio"),
            type=request.type,
            status=api_pb2.CALL_STATUS_PENDING,
        )

    def StreamInputs(self, requests, **kwargs):
        async def responses():
            iterator = requests.__aiter__()
            self.request_iterator = iterator
            opener = await anext(iterator)
            yield api_pb2.StreamCallInputsResponse(
                call=opener.call,
                committed_input_count=0,
                max_inputs_outstanding=8,
            )
            first = await anext(iterator)
            self.first_received.set()
            self.prefetch = asyncio.create_task(anext(iterator))
            await self.release_first.wait()
            yield api_pb2.StreamCallInputsResponse(
                call=opener.call,
                committed_input_count=1,
                last_input=api_pb2.InputRef(
                    input_id=first.input.input_id,
                    input_index=first.input.index,
                ),
                max_inputs_outstanding=8,
            )
            second = await self.prefetch
            exhausted = asyncio.create_task(anext(iterator))
            yield api_pb2.StreamCallInputsResponse(
                call=opener.call,
                committed_input_count=2,
                last_input=api_pb2.InputRef(
                    input_id=second.input.input_id,
                    input_index=second.input.index,
                ),
            )
            with pytest.raises(StopAsyncIteration):
                await exhausted

        return responses()

    async def CloseInputs(self, request, **kwargs):
        self.closed.set()
        return api_pb2.CallRecord(
            ref=request.call,
            type=api_pb2.CALL_TYPE_BATCH,
            status=api_pb2.CALL_STATUS_QUEUED,
        )

    async def Cancel(self, request, **kwargs):
        self.cancelled.set()
        if self.prefetch is not None and not self.prefetch.done():
            self.prefetch.cancel()
            try:
                await self.prefetch
            except asyncio.CancelledError:
                pass
        if self.request_iterator is not None:
            await self.request_iterator.aclose()
        return api_pb2.CallRecord(
            ref=request.call,
            type=api_pb2.CALL_TYPE_BATCH,
            status=api_pb2.CALL_STATUS_CANCELLED,
        )


def _async_batch_function(calls: _BatchCalls) -> RemoteFunction:
    client = SimpleNamespace(driver=_Driver(calls))
    function = RemoteFunction(lambda value: value, client=client)
    function._registered = api_pb2.FunctionRevision(
        ref=api_pb2.RevisionRef(
            function=api_pb2.FunctionRef(namespace="default", name="batch"),
            revision_id="r1",
        )
    )
    return function


def test_native_aio_spawn_map_honors_input_window_before_pulling_source() -> None:
    async def scenario() -> None:
        calls = _BatchCalls()
        pulls: list[int] = []

        async def source():
            for value in (1, 2):
                pulls.append(value)
                yield value

        batch = await _async_batch_function(calls).aio.spawn_map(source(), max_in_flight=1)
        await calls.first_received.wait()
        await asyncio.sleep(0)
        assert pulls == [1]
        assert calls.prefetch is not None
        assert not calls.prefetch.done()
        calls.release_first.set()
        await asyncio.wait_for(calls.closed.wait(), 1)
        assert pulls == [1, 2]
        assert batch.id == "batch-aio"

    asyncio.run(scenario())


def test_native_aio_batch_cancel_stops_feeder_and_closes_source() -> None:
    async def scenario() -> None:
        calls = _BatchCalls()
        source_closed = asyncio.Event()

        async def source():
            try:
                yield 1
                yield 2
            finally:
                source_closed.set()

        batch = await _async_batch_function(calls).aio.spawn_map(source(), max_in_flight=1)
        await calls.first_received.wait()
        await batch.cancel()
        assert calls.cancelled.is_set()
        await asyncio.wait_for(source_closed.wait(), 1)

    asyncio.run(scenario())


def test_aio_remote_modes_fail_before_submitting_wrong_call_kind() -> None:
    async def scenario() -> None:
        def generator():
            yield 1

        async def async_generator():
            yield 1

        with pytest.raises(ValueError, match="remote_gen"):
            await RemoteFunction(generator).aio.remote()
        with pytest.raises(ValueError, match="remote_gen"):
            await RemoteFunction(async_generator).aio.remote()
        stream = RemoteFunction(lambda: 1).aio.remote_gen()
        with pytest.raises(ValueError, match="remote"):
            await anext(stream)

    asyncio.run(scenario())


class _Unavailable(grpc.RpcError):
    def code(self):
        return grpc.StatusCode.UNAVAILABLE


class _ReconnectCalls:
    def __init__(self) -> None:
        self.requests = []

    def Watch(self, request, **kwargs):
        self.requests.append(request)

        async def events():
            if len(self.requests) == 1:
                yield api_pb2.CallEvent(
                    call=request.cursor.call,
                    sequence=1,
                    status=api_pb2.StatusEvent(status=api_pb2.CALL_STATUS_RUNNING),
                )
                raise _Unavailable()
            envelope = encode_value(
                42,
                codec=ValueCodec.JSON,
                compression=ValueCompression.NONE,
            )
            yield api_pb2.CallEvent(
                call=request.cursor.call,
                sequence=2,
                result=api_pb2.CallResult(
                    call=request.cursor.call,
                    value=value_to_proto(envelope),
                ),
            )

        return events()


class _ReconnectDriver(_Driver):
    def __init__(self, calls: _ReconnectCalls) -> None:
        super().__init__(calls)
        self.resolved = []

    def resolve_call(self, call_id, endpoint=None):
        self.resolved.append((call_id, endpoint))
        return "owner-2"


def test_native_aio_get_resumes_watch_after_midstream_unavailable() -> None:
    async def scenario() -> None:
        calls = _ReconnectCalls()
        driver = _ReconnectDriver(calls)
        client = SimpleNamespace(driver=driver)
        result = await _AsyncCall(FunctionCall("call-reconnect", client)).get()
        assert result == 42
        assert [request.cursor.after_sequence for request in calls.requests] == [0, 1]
        assert driver.resolved == [("call-reconnect", None)]

    asyncio.run(scenario())


class _NotFound(grpc.RpcError):
    def code(self):
        return grpc.StatusCode.NOT_FOUND


def test_native_aio_ordered_batch_fetches_only_next_index_and_cancels_partial_consumer(
    monkeypatch,
) -> None:
    async def scenario() -> None:
        requested: list[int] = []
        decoded: list[int] = []

        class Calls:
            def __init__(self) -> None:
                self.first_requested = asyncio.Event()
                self.release_first = asyncio.Event()
                self.cancelled = asyncio.Event()
                self.completed_suffix = range(1, 5_001)

            async def GetResult(self, request, **kwargs):
                requested.append(request.index)
                if request.index == 0 and not self.release_first.is_set():
                    self.first_requested.set()
                    raise _NotFound()
                if request.index == 0 or request.index in self.completed_suffix:
                    envelope = encode_value(
                        request.index,
                        codec=ValueCodec.JSON,
                        compression=ValueCompression.NONE,
                    )
                    return api_pb2.CallResult(
                        call=request.call,
                        input_id=f"input-{request.index}",
                        input_index=request.index,
                        value=value_to_proto(envelope),
                    )
                raise _NotFound()

            async def Get(self, request, **kwargs):
                return api_pb2.CallRecord(
                    ref=request,
                    status=api_pb2.CALL_STATUS_RUNNING,
                )

            async def Cancel(self, request, **kwargs):
                self.cancelled.set()
                return api_pb2.CallRecord(
                    ref=request.call,
                    status=api_pb2.CALL_STATUS_CANCELLED,
                )

        calls = Calls()
        client = SimpleNamespace(driver=_Driver(calls))
        detached = BatchCall(FunctionCall("bounded-aio", client))
        assert detached.input_count is None
        original = remote_module._async_result_value

        async def track_decode(*args, **kwargs):
            result = args[2]
            decoded.append(result.input_index)
            return await original(*args, **kwargs)

        monkeypatch.setattr(remote_module, "_async_result_value", track_decode)
        iterator = detached.aio.results()
        consumer = asyncio.create_task(anext(iterator))
        await calls.first_requested.wait()
        await asyncio.sleep(0.05)
        assert set(requested) == {0}
        assert decoded == []

        calls.release_first.set()
        assert await asyncio.wait_for(consumer, 1) == 0
        assert decoded == [0]
        await iterator.aclose()
        assert calls.cancelled.is_set()

    asyncio.run(scenario())


def test_native_aio_ordered_batch_preserves_item_errors_before_terminal_cancel() -> None:
    async def scenario() -> None:
        class Calls:
            async def GetResult(self, request, **kwargs):
                if request.index == 0:
                    return api_pb2.CallResult(
                        call=request.call,
                        input_id="input-0",
                        input_index=0,
                        error=api_pb2.CallError(
                            code="invalid",
                            message="bad item",
                            type="ValueError",
                        ),
                    )
                if request.index == 1:
                    envelope = encode_value(
                        9,
                        codec=ValueCodec.JSON,
                        compression=ValueCompression.NONE,
                    )
                    return api_pb2.CallResult(
                        call=request.call,
                        input_id="input-1",
                        input_index=1,
                        value=value_to_proto(envelope),
                    )
                raise _NotFound()

            async def Get(self, request, **kwargs):
                return api_pb2.CallRecord(
                    ref=request,
                    status=api_pb2.CALL_STATUS_CANCELLED,
                )

        calls = Calls()
        client = SimpleNamespace(driver=_Driver(calls))
        iterator = BatchCall(FunctionCall("errors-aio", client)).aio.results(return_exceptions=True)
        values = [value async for value in iterator]
        assert len(values) == 3
        assert isinstance(values[0], RemoteFunctionError)
        assert values[0].code == "invalid"
        assert values[1] == 9
        assert isinstance(values[2], RemoteFunctionError)
        assert values[2].code == "cancelled"

    asyncio.run(scenario())


def test_native_aio_ordered_batch_retries_terminal_result_race() -> None:
    async def scenario() -> None:
        class Calls:
            def __init__(self) -> None:
                self.last_result_attempts = 0

            async def GetResult(self, request, **kwargs):
                if request.index == 0:
                    envelope = encode_value(
                        2,
                        codec=ValueCodec.JSON,
                        compression=ValueCompression.NONE,
                    )
                    return api_pb2.CallResult(
                        call=request.call,
                        input_id="input-0",
                        input_index=0,
                        value=value_to_proto(envelope),
                    )
                if request.index == 1:
                    self.last_result_attempts += 1
                    if self.last_result_attempts == 1:
                        raise _NotFound()
                    return api_pb2.CallResult(
                        call=request.call,
                        input_id="input-1",
                        input_index=1,
                        error=api_pb2.CallError(
                            code="invalid",
                            message="bad item",
                            type="TypeError",
                        ),
                    )
                raise _NotFound()

            async def Get(self, request, **kwargs):
                return api_pb2.CallRecord(
                    ref=request,
                    status=api_pb2.CALL_STATUS_SUCCEEDED,
                    input_count=2,
                )

        calls = Calls()
        client = SimpleNamespace(driver=_Driver(calls))
        iterator = BatchCall(FunctionCall("terminal-race-aio", client)).aio.results(
            return_exceptions=True
        )
        values = [value async for value in iterator]
        assert values[0] == 2
        assert isinstance(values[1], RemoteFunctionError)
        assert values[1].remote_type == "TypeError"
        assert calls.last_result_attempts == 2

    asyncio.run(scenario())


def test_native_aio_ordered_batch_reconnects_get_result_transport() -> None:
    async def scenario() -> None:
        requests: list[tuple[str, int]] = []

        class Calls:
            def __init__(self, owner: str) -> None:
                self.owner = owner

            async def GetResult(self, request, **kwargs):
                requests.append((self.owner, request.index))
                if self.owner == "owner-1":
                    raise _Unavailable()
                envelope = encode_value(
                    5,
                    codec=ValueCodec.JSON,
                    compression=ValueCompression.NONE,
                )
                return api_pb2.CallResult(
                    call=request.call,
                    input_id="input-0",
                    input_index=0,
                    value=value_to_proto(envelope),
                )

            async def Cancel(self, request, **kwargs):
                return api_pb2.CallRecord(
                    ref=request.call,
                    status=api_pb2.CALL_STATUS_CANCELLED,
                )

        class Driver:
            def __init__(self) -> None:
                self.calls = {
                    "owner-1": Calls("owner-1"),
                    "owner-2": Calls("owner-2"),
                }
                self.resolved: list[tuple[str, str | None]] = []

            def aio_endpoint(self, endpoint=None):
                selected = endpoint or "owner-1"
                return (
                    SimpleNamespace(calls=self.calls[selected]),
                    (),
                    1.0,
                    selected,
                )

            def resolve_call(self, call_id, endpoint=None):
                self.resolved.append((call_id, endpoint))
                return "owner-2"

        driver = Driver()
        iterator = BatchCall(
            FunctionCall("reconnect-batch-aio", SimpleNamespace(driver=driver))
        ).aio.results()
        assert await anext(iterator) == 5
        assert requests == [("owner-1", 0), ("owner-2", 0)]
        assert driver.resolved == [("reconnect-batch-aio", "owner-1")]
        await iterator.aclose()

    asyncio.run(scenario())


def test_native_aio_unordered_batch_keeps_completion_sequence() -> None:
    async def scenario() -> None:
        class Calls:
            def Watch(self, request, **kwargs):
                async def events():
                    for sequence, input_index in ((1, 2), (2, 0)):
                        envelope = encode_value(
                            input_index,
                            codec=ValueCodec.JSON,
                            compression=ValueCompression.NONE,
                        )
                        yield api_pb2.CallEvent(
                            call=request.cursor.call,
                            sequence=sequence,
                            result=api_pb2.CallResult(
                                call=request.cursor.call,
                                sequence=sequence,
                                input_id=f"input-{input_index}",
                                input_index=input_index,
                                value=value_to_proto(envelope),
                            ),
                        )
                    yield api_pb2.CallEvent(
                        call=request.cursor.call,
                        sequence=3,
                        status=api_pb2.StatusEvent(status=api_pb2.CALL_STATUS_SUCCEEDED),
                    )

                return events()

            async def GetResult(self, request, **kwargs):
                raise AssertionError("unordered iteration must not poll by input index")

        calls = Calls()
        client = SimpleNamespace(driver=_Driver(calls))
        values = [
            value
            async for value in BatchCall(FunctionCall("unordered-aio", client)).aio.results(
                order_outputs=False
            )
        ]
        assert values == [2, 0]

    asyncio.run(scenario())
