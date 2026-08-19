from __future__ import annotations

import signal
from collections.abc import Iterator
from contextlib import contextmanager
from threading import Event
from typing import cast


class CancelToken:
    """One-shot cooperative cancellation for one execution call."""

    __slots__ = ("_event",)

    def __init__(self) -> None:
        self._event = Event()

    def cancel(self) -> None:
        self._event.set()

    @property
    def cancelled(self) -> bool:
        return self._event.is_set()

    def _wait(self, timeout: float | None = None) -> bool:
        return self._event.wait(timeout)


@contextmanager
def forward_parent_signals(token: CancelToken, /) -> Iterator[None]:
    """Map SIGTERM and SIGINT received by this process to ``token.cancel()``.

    Install this on the thread or process that receives parent shutdown
    signals, typically the main thread of a worker wrapping ``run_blocking()``.
    """

    previous: dict[int, signal.Handlers] = {}

    def forward(_signum: int, _frame: object) -> None:
        token.cancel()

    for signum in (signal.SIGTERM, signal.SIGINT):
        previous[signum] = cast(
            signal.Handlers,
            signal.signal(signum, forward),
        )
    try:
        yield
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)


__all__ = ["CancelToken", "forward_parent_signals"]
