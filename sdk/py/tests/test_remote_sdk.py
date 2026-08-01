from __future__ import annotations

import inspect
import threading
from types import SimpleNamespace

import pytest

import vmon.remote as remote_module
from vmon._function_proto import artifact_ref, package_to_proto, value_from_proto, value_to_proto
from vmon.errors import APIError, RemoteFunctionError
from vmon.package import package_callable
from vmon.remote import BatchCall, FunctionCall, RemoteFunction, _invocation, function
from vmon.v1 import api_pb2
from vmon.values import ValueCodec, decode_value, encode_value


def test_decorator_preserves_local_signature_and_result() -> None:
    @function
    def add(left: int, right: int = 1) -> int:
        return left + right

    assert inspect.signature(add) == inspect.signature(add._function)
    assert add(2, right=3) == 5
    assert add.local(4) == 5


def test_from_name_and_with_options_are_immutable() -> None:
    deployed = RemoteFunction.from_name(
        "resize", namespace="images", revision="r1", client=object()
    )
    changed = deployed.with_options(deployed.options)
    assert changed is not deployed
    assert (changed.namespace, changed.name, changed.revision) == ("images", "resize", "r1")


def test_function_call_id_validation() -> None:
    with pytest.raises(ValueError, match="call_id"):
        FunctionCall.from_id("", client=object())
    call = FunctionCall.from_id("call-1", client=object())
    assert call.id == "call-1"


@pytest.mark.parametrize("codec", [ValueCodec.JSON, ValueCodec.CBOR])
def test_portable_value_protobuf_round_trip(codec: ValueCodec) -> None:
    envelope = encode_value({"ok": [1, 2, 3]}, codec=codec, compress=False)
    restored = value_from_proto(value_to_proto(envelope))
    assert decode_value(restored) == {"ok": [1, 2, 3]}


def test_cloudpickle_remains_explicitly_trusted() -> None:
    envelope = encode_value({1, 2}, codec=ValueCodec.CLOUDPICKLE, compress=False)
    restored = value_from_proto(value_to_proto(envelope))
    with pytest.raises(PermissionError, match="trusted"):
        decode_value(restored)
    assert decode_value(restored, trusted=True) == {1, 2}


def test_trusted_serialization_declares_cloudpickle_for_normal_packages() -> None:
    import cloudpickle

    package = package_callable(test_trusted_serialization_declares_cloudpickle_for_normal_packages)
    spec = package_to_proto(
        package,
        artifact_ref(package.sha256),
        trusted_serialization=True,
    )
    assert spec.python.cloudpickle_version == cloudpickle.__version__

    serialized = package_callable(
        test_trusted_serialization_declares_cloudpickle_for_normal_packages,
        mode="cloudpickle",
    )
    serialized_spec = package_to_proto(
        serialized,
        artifact_ref(serialized.sha256),
        trusted_serialization=True,
    )
    assert serialized_spec.mode == api_pb2.PACKAGE_MODE_TRUSTED_SERIALIZED
    assert not serialized_spec.included_paths
    assert serialized_spec.python.cloudpickle_version == cloudpickle.__version__


def test_python_invocation_abi_is_versioned_and_collision_safe() -> None:
    ordinary = {"args": [1], "kwargs": {"named": 2}}
    assert _invocation((ordinary,), {}) == {
        "__vmon_python_call__": 1,
        "args": [ordinary],
        "kwargs": {},
    }
    assert _invocation((), {}) == {
        "__vmon_python_call__": 1,
        "args": [],
        "kwargs": {},
    }
    assert _invocation((1, 2), {"named": 3}) == {
        "__vmon_python_call__": 1,
        "args": [1, 2],
        "kwargs": {"named": 3},
    }


def test_reconstructed_call_pins_owner_when_preferred_endpoint_changes() -> None:
    class Calls:
        def __init__(self, owner: str) -> None:
            self.owner = owner
            self.cancelled = False

        def Get(self, request):
            if self.owner != "a":
                raise AssertionError("wrong endpoint")
            return api_pb2.CallRecord(ref=request, status=api_pb2.CALL_STATUS_RUNNING)

        def Cancel(self, request):
            self.cancelled = True
            return api_pb2.CallRecord(ref=request.call, status=api_pb2.CALL_STATUS_CANCELLED)

    class Driver:
        def __init__(self) -> None:
            self.preferred = "a"
            self.calls = {"a": Calls("a"), "b": Calls("b")}

        def call(self, operation, *, endpoint=None, stream=False):
            selected = endpoint or self.preferred
            return operation(SimpleNamespace(calls=self.calls[selected])), selected

    driver = Driver()
    call = FunctionCall.from_id("durable", client=SimpleNamespace(driver=driver))
    driver.preferred = "b"
    call.cancel()
    assert driver.calls["a"].cancelled
    assert not driver.calls["b"].cancelled


def test_batch_terminal_cancellation_is_not_silently_discarded() -> None:
    class Calls:
        def ListResults(self, request):
            return api_pb2.ListCallResultsResponse(next_cursor=request.cursor, end=True)

        def GetResult(self, request):
            raise APIError("pending", code="not_found", status=404)

        def Get(self, request):
            return api_pb2.CallRecord(ref=request, status=api_pb2.CALL_STATUS_CANCELLED)

    class Driver:
        def call(self, operation, *, endpoint=None, stream=False):
            return operation(SimpleNamespace(calls=Calls())), "owner"

    batch = BatchCall.from_id("cancelled", client=SimpleNamespace(driver=Driver()))
    with pytest.raises(Exception, match="cancel"):
        list(batch.results())
    values = list(batch.results(return_exceptions=True))
    assert len(values) == 1
    assert isinstance(values[0], Exception)


def test_sync_batch_producer_failure_durably_cancels_call() -> None:
    cancelled = threading.Event()

    class Calls:
        def Create(self, request):
            return api_pb2.CallRecord(ref=api_pb2.CallRef(call_id="batch"))

        def StreamInputs(self, requests):
            def responses():
                for index, _ in enumerate(requests):
                    yield api_pb2.StreamCallInputsResponse(
                        call=api_pb2.CallRef(call_id="batch"),
                        committed_input_count=index,
                    )

            return responses()

        def Cancel(self, request):
            cancelled.set()
            return api_pb2.CallRecord(ref=request.call, status=api_pb2.CALL_STATUS_CANCELLED)

    class Driver:
        def call(self, operation, *, endpoint=None, stream=False):
            return operation(SimpleNamespace(calls=Calls())), "owner"

    function = RemoteFunction.from_name(
        "work", revision="r1", client=SimpleNamespace(driver=Driver())
    )
    function._registered = api_pb2.FunctionRevision(
        ref=api_pb2.RevisionRef(
            function=api_pb2.FunctionRef(namespace="default", name="work"),
            revision_id="r1",
        )
    )

    def failing():
        yield 1
        raise RuntimeError("producer failed")

    function.spawn_map(failing())
    assert cancelled.wait(1)


def test_ordered_batch_fetches_only_next_index_and_cancels_partial_consumer(
    monkeypatch,
) -> None:
    release_first = threading.Event()
    first_requested = threading.Event()
    cancelled = threading.Event()
    requested: list[int] = []
    decoded: list[int] = []

    class Calls:
        terminal = False
        completed_suffix = range(1, 5_001)

        def GetResult(self, request):
            requested.append(request.index)
            if request.index == 0 and not release_first.is_set():
                first_requested.set()
                raise APIError("pending", code="not_found", status=404)
            if request.index == 0:
                envelope = encode_value(0, codec=ValueCodec.JSON, compress=False)
                return api_pb2.CallResult(
                    call=request.call,
                    input_id="input-0",
                    input_index=0,
                    value=value_to_proto(envelope),
                )
            if request.index in self.completed_suffix:
                envelope = encode_value(request.index, codec=ValueCodec.JSON, compress=False)
                return api_pb2.CallResult(
                    call=request.call,
                    input_id=f"input-{request.index}",
                    input_index=request.index,
                    value=value_to_proto(envelope),
                )
            raise APIError("exhausted", code="not_found", status=404)

        def Get(self, request):
            return api_pb2.CallRecord(
                ref=request,
                status=(
                    api_pb2.CALL_STATUS_SUCCEEDED if self.terminal else api_pb2.CALL_STATUS_RUNNING
                ),
            )

        def ListResults(self, request):
            raise AssertionError("ordered iteration must not list completed suffixes")

        def Cancel(self, request):
            cancelled.set()
            return api_pb2.CallRecord(
                ref=request.call,
                status=api_pb2.CALL_STATUS_CANCELLED,
            )

    calls = Calls()

    class Driver:
        resolve_count = 0

        def call(self, operation, *, endpoint=None, stream=False):
            return operation(SimpleNamespace(calls=calls)), "owner"

        def resolve_call(self, call_id, endpoint=None):
            self.resolve_count += 1
            return "owner"

    driver = Driver()
    batch = BatchCall.from_id("bounded", client=SimpleNamespace(driver=driver))
    assert batch.input_count is None
    initial_resolve_count = driver.resolve_count
    original = remote_module._result_value

    def track_decode(*args, **kwargs):
        result = args[1]
        decoded.append(result.input_index)
        return original(*args, **kwargs)

    monkeypatch.setattr(remote_module, "_result_value", track_decode)
    iterator = batch.results()
    yielded: list[object] = []
    consumer = threading.Thread(target=lambda: yielded.append(next(iterator)))
    consumer.start()
    assert first_requested.wait(1)
    assert set(requested) == {0}
    assert decoded == []
    assert driver.resolve_count == initial_resolve_count

    release_first.set()
    consumer.join(1)
    assert not consumer.is_alive()
    assert yielded == [0]
    assert decoded == [0]
    iterator.close()
    assert cancelled.wait(1)


def test_ordered_batch_return_exceptions_is_per_item_and_then_exhausts() -> None:
    class Calls:
        def GetResult(self, request):
            if request.index == 0:
                return api_pb2.CallResult(
                    call=request.call,
                    input_id="input-0",
                    input_index=0,
                    error=api_pb2.CallError(
                        code="invalid",
                        message="bad item",
                        type="ValueError",
                        frames=[
                            api_pb2.ErrorFrame(
                                file="worker.py",
                                line=17,
                                function="remote_double",
                            )
                        ],
                    ),
                )
            if request.index == 1:
                envelope = encode_value(7, codec=ValueCodec.JSON, compress=False)
                return api_pb2.CallResult(
                    call=request.call,
                    input_id="input-1",
                    input_index=1,
                    value=value_to_proto(envelope),
                )
            raise APIError("exhausted", code="not_found", status=404)

        def Get(self, request):
            return api_pb2.CallRecord(
                ref=request,
                status=api_pb2.CALL_STATUS_SUCCEEDED,
            )

    calls = Calls()

    class Driver:
        def call(self, operation, *, endpoint=None, stream=False):
            return operation(SimpleNamespace(calls=calls)), "owner"

    batch = BatchCall.from_id("item-errors", client=SimpleNamespace(driver=Driver()))
    values = list(batch.results(return_exceptions=True))
    assert len(values) == 2
    assert isinstance(values[0], RemoteFunctionError)
    assert values[0].code == "invalid"
    assert "worker.py" in values[0].traceback
    assert "remote_double" in values[0].traceback
    assert values[0].__notes__ == [f"Remote traceback:\n{values[0].traceback}"]
    assert values[1] == 7


def test_ordered_batch_retries_result_that_commits_with_terminal_status() -> None:
    class Calls:
        def __init__(self) -> None:
            self.last_result_attempts = 0

        def GetResult(self, request):
            if request.index == 0:
                envelope = encode_value(2, codec=ValueCodec.JSON, compress=False)
                return api_pb2.CallResult(
                    call=request.call,
                    input_id="input-0",
                    input_index=0,
                    value=value_to_proto(envelope),
                )
            if request.index == 1:
                self.last_result_attempts += 1
                if self.last_result_attempts == 1:
                    raise APIError("result commit raced fetch", code="not_found", status=404)
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
            raise APIError("exhausted", code="not_found", status=404)

        def Get(self, request):
            return api_pb2.CallRecord(
                ref=request,
                status=api_pb2.CALL_STATUS_SUCCEEDED,
                input_count=2,
            )

    calls = Calls()

    class Driver:
        def call(self, operation, *, endpoint=None, stream=False):
            return operation(SimpleNamespace(calls=calls)), "owner"

    batch = BatchCall.from_id("terminal-race", client=SimpleNamespace(driver=Driver()))
    values = list(batch.results(return_exceptions=True))
    assert values[0] == 2
    assert isinstance(values[1], RemoteFunctionError)
    assert values[1].remote_type == "TypeError"
    assert calls.last_result_attempts == 2


def test_unordered_batch_keeps_durable_completion_sequence() -> None:
    class Calls:
        def GetResult(self, request):
            raise AssertionError("unordered iteration must not poll by input index")

        def ListResults(self, request):
            results = []
            for sequence, input_index in ((1, 2), (2, 0)):
                envelope = encode_value(input_index, codec=ValueCodec.JSON, compress=False)
                results.append(
                    api_pb2.CallResult(
                        call=request.cursor.call,
                        sequence=sequence,
                        input_id=f"input-{input_index}",
                        input_index=input_index,
                        value=value_to_proto(envelope),
                    )
                )
            return api_pb2.ListCallResultsResponse(
                results=results,
                next_cursor=api_pb2.ResultCursor(
                    call=request.cursor.call,
                    after_sequence=2,
                ),
                end=True,
            )

        def Get(self, request):
            return api_pb2.CallRecord(
                ref=request,
                status=api_pb2.CALL_STATUS_SUCCEEDED,
            )

    calls = Calls()

    class Driver:
        def call(self, operation, *, endpoint=None, stream=False):
            return operation(SimpleNamespace(calls=calls)), "owner"

    batch = BatchCall.from_id("unordered", client=SimpleNamespace(driver=Driver()))
    assert list(batch.results(order_outputs=False)) == [2, 0]


def test_ordered_batch_surfaces_terminal_failure_after_committed_prefix() -> None:
    class Calls:
        def GetResult(self, request):
            if request.index == 0:
                envelope = encode_value(3, codec=ValueCodec.JSON, compress=False)
                return api_pb2.CallResult(
                    call=request.call,
                    input_id="input-0",
                    input_index=0,
                    value=value_to_proto(envelope),
                )
            raise APIError("no later result", code="not_found", status=404)

        def Get(self, request):
            return api_pb2.CallRecord(
                ref=request,
                status=api_pb2.CALL_STATUS_FAILED,
                error=api_pb2.CallError(
                    code="worker_lost",
                    message="batch failed",
                    type="WorkerLost",
                ),
            )

    calls = Calls()

    class Driver:
        def call(self, operation, *, endpoint=None, stream=False):
            return operation(SimpleNamespace(calls=calls)), "owner"

    batch = BatchCall.from_id("failed-prefix", client=SimpleNamespace(driver=Driver()))
    iterator = batch.results()
    assert next(iterator) == 3
    with pytest.raises(RemoteFunctionError) as raised:
        next(iterator)
    assert raised.value.code == "worker_lost"

    values = list(batch.results(return_exceptions=True))
    assert values[0] == 3
    assert isinstance(values[1], RemoteFunctionError)
    assert values[1].code == "worker_lost"
