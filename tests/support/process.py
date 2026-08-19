from __future__ import annotations

import os
import signal
import sys
import threading
from concurrent.futures import Future
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from time import monotonic
from typing import TYPE_CHECKING

import pytest

requires_posix = pytest.mark.skipif(
    sys.platform not in {"darwin", "linux"},
    reason="real POSIX process semantics",
)

assert_fd_count_unchanged = pytest.mark.skipif(
    sys.platform != "darwin",
    reason="/dev/fd listing is unreliable on Linux CI",
)


def open_fd_count() -> int:
    return len(os.listdir("/dev/fd"))


if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Sequence


WATCHDOG_SECONDS = 30.0
_PID_EXIT_WATCHDOG_SECONDS = 5.0
_PID_POLL_SECONDS = 0.01


@pytest.fixture
def process_watchdog() -> Iterator[object]:
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
    path: Path

    @classmethod
    def create(cls, directory: Path, name: str, /) -> Gate:
        path = directory / name
        os.mkfifo(path)
        return cls(path=path)

    def receive(self) -> str:
        with self.path.open() as reader:
            return reader.read()

    def release(self, message: str = "go", /) -> None:
        with self.path.open("w") as writer:
            writer.write(message)


def exact_pid_exists(pid: int, /) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def _kill_and_await_exact_pid(pid: int, /) -> None:
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
    registered: list[int] = []
    try:
        yield registered
    finally:
        for pid in registered:
            _kill_and_await_exact_pid(pid)


@dataclass(frozen=True, slots=True)
class ThreadedCall[T]:
    thread: threading.Thread
    future: Future[T]


def start_threaded_calls[T](
    calls: Sequence[Callable[[], T]],
    /,
) -> tuple[ThreadedCall[T], ...]:
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
    "assert_fd_count_unchanged",
    "cleanup_exact_pids",
    "exact_pid_exists",
    "finish_threaded_calls",
    "open_fd_count",
    "process_watchdog",
    "requires_posix",
    "start_threaded_calls",
]
