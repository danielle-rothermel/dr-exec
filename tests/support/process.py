"""Shared state synchronization and hang protection for process tests."""

from __future__ import annotations

import os
import signal
import threading
from concurrent.futures import Future
from dataclasses import dataclass
from pathlib import Path
from time import monotonic
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Sequence


WATCHDOG_SECONDS = 30.0


@pytest.fixture(autouse=True)
def process_watchdog() -> Iterator[object]:
    """Fail a hung process case without treating time as success evidence."""
    timer = threading.Timer(
        WATCHDOG_SECONDS,
        lambda: os.kill(os.getpid(), signal.SIGALRM),
    )
    previous = signal.signal(
        signal.SIGALRM,
        lambda *_: pytest.fail("watchdog fired: the case did not finish"),
    )
    timer.start()
    yield timer
    timer.cancel()
    signal.signal(signal.SIGALRM, previous)


@dataclass(frozen=True, slots=True)
class Gate:
    """One FIFO that synchronizes parent and child on an observed event."""

    path: Path

    @classmethod
    def create(cls, directory: Path, name: str, /) -> Gate:
        path = directory / name
        os.mkfifo(path)
        return cls(path=path)

    def receive(self) -> str:
        """Block until the peer writes, then return exactly what it sent."""
        with self.path.open() as reader:
            return reader.read()

    def release(self, message: str = "go", /) -> None:
        """Unblock a peer waiting on this gate."""
        with self.path.open("w") as writer:
            writer.write(message)


@dataclass(frozen=True, slots=True)
class ThreadedCall[T]:
    """One caller thread whose return or failure is retained for its owner."""

    thread: threading.Thread
    future: Future[T]


def start_threaded_calls[T](
    calls: Sequence[Callable[[], T]],
    /,
) -> tuple[ThreadedCall[T], ...]:
    """Start calls together without losing a return value or exception."""
    started: list[ThreadedCall[T]] = []
    for index, call in enumerate(calls):
        future: Future[T] = Future()

        def invoke(
            call: Callable[[], T] = call,
            future: Future[T] = future,
        ) -> None:
            try:
                future.set_result(call())
            except BaseException as error:  # noqa: BLE001
                future.set_exception(error)

        thread = threading.Thread(
            target=invoke,
            name=f"test-caller-{index}",
            daemon=True,
        )
        thread.start()
        started.append(ThreadedCall(thread=thread, future=future))
    return tuple(started)


def finish_threaded_calls[T](
    calls: Sequence[ThreadedCall[T]],
    /,
) -> tuple[T, ...]:
    """Join every caller, then return all results or surface all failures."""
    deadline = monotonic() + WATCHDOG_SECONDS
    for call in calls:
        call.thread.join(timeout=max(0.0, deadline - monotonic()))
    alive = [call.thread.name for call in calls if call.thread.is_alive()]
    if alive:
        pytest.fail(f"caller thread watchdog fired: {alive}")

    failures = [
        error
        for call in calls
        if (error := call.future.exception()) is not None
    ]
    if failures:
        raise BaseExceptionGroup("threaded test calls failed", failures)
    return tuple(call.future.result() for call in calls)


__all__ = [
    "Gate",
    "ThreadedCall",
    "finish_threaded_calls",
    "process_watchdog",
    "start_threaded_calls",
]
