"""Shared state synchronization and hang protection for process tests."""

from __future__ import annotations

import os
import signal
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Iterator


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


__all__ = ["Gate", "process_watchdog"]
