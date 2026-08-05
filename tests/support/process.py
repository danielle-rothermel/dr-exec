"""Shared state synchronization and hang protection for process tests."""

from __future__ import annotations

import os
import signal
import threading
from concurrent.futures import Future
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from time import monotonic
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Sequence


WATCHDOG_SECONDS = 30.0
_PID_EXIT_WATCHDOG_SECONDS = 5.0
_PID_POLL_SECONDS = 0.01


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


def exact_pid_exists(pid: int, /) -> bool:
    """Probe only the exact process identifier supplied by a test case."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def _kill_and_await_exact_pid(pid: int, /) -> None:
    """Kill one registered PID and require its kernel identity to disappear."""
    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        return

    try:
        waited, _ = os.waitpid(pid, 0)
    except ChildProcessError:
        waited = 0
    if waited == pid:
        return

    deadline = monotonic() + _PID_EXIT_WATCHDOG_SECONDS
    poll_gate = threading.Event()
    while exact_pid_exists(pid):
        if monotonic() >= deadline:
            pytest.fail(f"exact PID {pid} survived test cleanup")
        # The state probe above is the evidence; this wait only avoids a
        # busy loop while the system reaps an orphaned descendant.
        poll_gate.wait(_PID_POLL_SECONDS)


@contextmanager
def cleanup_exact_pids() -> Iterator[list[int]]:
    """Unconditionally kill only the exact PIDs registered by one case."""
    registered: list[int] = []
    try:
        yield registered
    finally:
        for pid in registered:
            _kill_and_await_exact_pid(pid)


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
    "cleanup_exact_pids",
    "exact_pid_exists",
    "finish_threaded_calls",
    "process_watchdog",
    "start_threaded_calls",
]
