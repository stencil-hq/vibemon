"""Definition decorators and execution context for durable functions."""

from __future__ import annotations

import contextlib
import contextvars
from collections.abc import Callable, Iterator
from dataclasses import dataclass, replace
from typing import Any, TypeVar, overload

from .options import BatchingPolicy, ConcurrencyPolicy, FunctionOptions, OptionsError

F = TypeVar("F", bound=Callable[..., Any])


@dataclass(frozen=True, slots=True)
class CurrentCall:
    """Metadata for the durable call currently executing in a worker."""

    call_id: str
    function_id: str = ""
    definition_id: str = ""
    request_id: str = ""
    input_id: str = ""
    input_index: int = 0
    attempt: int = 1
    parent_request_id: str | None = None
    parent_call_id: str | None = None
    execution_mode: str = ""
    actor_id: str | None = None
    deadline_unix_ms: int | None = None


_current_call: contextvars.ContextVar[CurrentCall | None] = contextvars.ContextVar(
    "vmon_current_call", default=None
)


def current_call() -> CurrentCall:
    """Return metadata for the current worker call.

    Raises ``RuntimeError`` outside a vmon worker call rather than returning a
    misleading process-global placeholder.
    """

    value = _current_call.get()
    if value is None:
        raise RuntimeError("current_call() is only available while executing a vmon call")
    return value


@contextlib.contextmanager
def _use_current_call(value: CurrentCall) -> Iterator[None]:
    """Install call metadata while the worker dispatches user code."""

    token = _current_call.set(value)
    try:
        yield
    finally:
        _current_call.reset(token)


def _definition_options(target: Any) -> FunctionOptions:
    value = getattr(target, "__vmon_options__", None)
    if value is None:
        value = getattr(target, "options", None)
    if value is None:
        return FunctionOptions()
    if not isinstance(value, FunctionOptions):
        raise TypeError("decorated object's options must be FunctionOptions")
    return value


def _set_definition_options[F: Callable[..., Any]](target: F, options: FunctionOptions) -> F:
    with_options = getattr(target, "with_options", None)
    if callable(with_options):
        try:
            return with_options(options=options)
        except TypeError:
            return with_options(options)
    target.__vmon_options__ = options  # type: ignore[attr-defined]
    return target


@overload
def concurrent(max_inputs: int, /) -> Callable[[F], F]: ...


@overload
def concurrent(*, max_concurrent_calls: int) -> Callable[[F], F]: ...


def concurrent(
    max_inputs: int | None = None, *, max_concurrent_calls: int | None = None
) -> Callable[[F], F]:
    """Set the maximum number of calls admitted concurrently by one worker."""

    if max_inputs is not None and max_concurrent_calls is not None:
        raise OptionsError("set either max_inputs or max_concurrent_calls, not both")
    limit = max_inputs if max_inputs is not None else max_concurrent_calls
    if limit is None:
        raise OptionsError("concurrent requires a concurrency limit")
    policy = ConcurrencyPolicy(max_concurrent_calls=limit)

    def decorate(target: F) -> F:
        options = _definition_options(target)
        if options.concurrency != ConcurrencyPolicy():
            raise OptionsError("concurrency policy is already configured")
        return _set_definition_options(target, replace(options, concurrency=policy))

    return decorate


def batched(
    *, max_batch_size: int, max_wait: float | None = None, wait_ms: int | None = None
) -> Callable[[F], F]:
    """Enable worker-side batching with a bounded batch size and wait."""

    if max_wait is not None and wait_ms is not None:
        raise OptionsError("set either max_wait or wait_ms, not both")
    wait = max_wait if max_wait is not None else (wait_ms / 1000 if wait_ms is not None else None)
    if wait is None:
        raise OptionsError("batched requires max_wait or wait_ms")
    policy = BatchingPolicy(enabled=True, max_batch_size=max_batch_size, max_wait=wait)

    def decorate(target: F) -> F:
        options = _definition_options(target)
        if options.batching != BatchingPolicy():
            raise OptionsError("batching policy is already configured")
        return _set_definition_options(target, replace(options, batching=policy))

    return decorate
