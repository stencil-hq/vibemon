"""Structured lifetime management for groups of durable calls."""

from __future__ import annotations

import asyncio
from collections.abc import Iterable, Iterator, Sequence
from typing import Any


class CallGroup[R]:
    """A durable collection of calls with structured cancellation.

    The group never owns call results.  It stores only durable handles and
    requests cancellation for all members if gathering fails or its scope is
    exited before :meth:`gather` completes.
    """

    def __init__(self, calls: Iterable[Any] = (), *, cancel_on_exit: bool = True) -> None:
        self._calls = list(calls)
        self.cancel_on_exit = bool(cancel_on_exit)
        self._gathered = False

    @property
    def calls(self) -> tuple[Any, ...]:
        """Return the durable handles in submission order."""

        return tuple(self._calls)

    @property
    def ids(self) -> tuple[str, ...]:
        """Return stable call identifiers suitable for persistence."""

        return tuple(_call_id(call) for call in self._calls)

    def add(self, call: Any) -> Any:
        """Add and return a durable call handle."""

        if self._gathered:
            raise RuntimeError("cannot add calls after gather")
        _call_id(call)
        self._calls.append(call)
        return call

    def cancel(self, reason: str = "call group cancelled") -> None:
        """Durably request cancellation of every member."""

        first: BaseException | None = None
        for call in self._calls:
            try:
                call.cancel(reason=reason)
            except TypeError:
                try:
                    call.cancel()
                except BaseException as exc:  # preserve first while cancelling siblings
                    first = first or exc
            except BaseException as exc:
                first = first or exc
        if first is not None:
            raise first

    def gather(self) -> list[R]:
        """Wait in order; cancel unfinished siblings if any call fails."""

        values: list[R] = []
        try:
            for call in self._calls:
                values.append(_result(call))
        except BaseException:
            try:
                self.cancel("sibling call failed")
            except BaseException:
                pass
            raise
        self._gathered = True
        return values

    @classmethod
    def from_ids(
        cls, call_ids: Sequence[str], *, client: Any = None, call_type: type | None = None
    ) -> CallGroup[Any]:
        """Reconnect a group from durable IDs in another process."""

        if call_type is None:
            from .remote import FunctionCall

            call_type = FunctionCall
        from_id = getattr(call_type, "from_id", None)
        if not callable(from_id):
            raise TypeError("call_type must expose from_id")
        calls = []
        for call_id in call_ids:
            try:
                calls.append(from_id(call_id, client=client))
            except TypeError:
                calls.append(from_id(call_id, client))
        return cls(calls)

    def __iter__(self) -> Iterator[Any]:
        return iter(self._calls)

    def __enter__(self) -> CallGroup[R]:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        if self.cancel_on_exit and (exc_type is not None or not self._gathered):
            try:
                self.cancel("call group scope exited")
            except BaseException:
                if exc_type is None:
                    raise

    async def __aenter__(self) -> CallGroup[R]:
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        if self.cancel_on_exit and (exc_type is not None or not self._gathered):
            try:
                await asyncio.to_thread(self.cancel, "call group scope exited")
            except BaseException:
                if exc_type is None:
                    raise


def _call_id(call: Any) -> str:
    value = getattr(call, "id", None) or getattr(call, "call_id", None)
    if not isinstance(value, str) or not value:
        raise TypeError("call handle must expose a non-empty durable id")
    return value


def _result(call: Any) -> Any:
    getter = getattr(call, "get", None)
    if callable(getter):
        return getter()
    getter = getattr(call, "result", None)
    if callable(getter):
        return getter()
    raise TypeError("call handle must expose get() or result()")
