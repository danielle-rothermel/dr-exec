from __future__ import annotations

from threading import Event


class CancelToken:
    """One-shot cooperative cancellation signal for one execution call."""

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


__all__ = ["CancelToken"]
