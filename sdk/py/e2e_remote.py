"""Importable callables exercised by the real-VM durable-function smoke tests."""

from __future__ import annotations

import datetime
import time
from collections.abc import Callable, Iterator
from typing import Any


def remote_double(value: int) -> int:
    print(f"doubling {value}")
    if value < 0:
        raise ValueError("value must be non-negative")
    return value * 2


def remote_shift(
    stamp: datetime.datetime,
    pair: tuple[int, int],
) -> tuple[datetime.datetime, tuple[int, int]]:
    return stamp + datetime.timedelta(days=1), (pair[1], pair[0])


def remote_ticks() -> Iterator[int]:
    for index in range(3):
        yield index
        time.sleep(0.2)


def remote_add(a: int, b: int) -> int:
    return a + b


def _tag[F: Callable[..., Any]](fn: F, **attributes: object) -> F:
    for name, value in attributes.items():
        setattr(fn, name, value)
    return fn


def _method[F: Callable[..., Any]](fn: F) -> F:
    return _tag(fn, __vmon_method__=True)


def _enter[F: Callable[..., Any]](fn: F) -> F:
    return _tag(fn, __vmon_enter__=True, __vmon_snapshot_enter__=False)


def _exit[F: Callable[..., Any]](fn: F) -> F:
    return _tag(fn, __vmon_exit__=True)


class RemoteCounter:
    """Stateful actor used by the real-VM SDK smoke test."""

    def __init__(self, start: int = 0) -> None:
        self.value: int = start
        self.entered: bool = False

    @_enter
    def _mark(self) -> None:
        self.entered = True

    @_method
    def bump(self) -> int:
        assert self.entered, "enter hook must run before methods"
        self.value += 1
        return self.value

    @_exit
    def _farewell(self) -> None:
        print("counter exiting")
