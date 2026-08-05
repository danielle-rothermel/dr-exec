from __future__ import annotations

from collections import deque
from collections.abc import Callable, Iterable
from threading import Lock

from dr_exec.cancel import CancelToken
from dr_exec.declare import ExecutionJob
from dr_exec.record import CompletedExecution


class FakeExecutor:
    _responses: deque[CompletedExecution]
    _responder: (
        Callable[[ExecutionJob, CancelToken | None], CompletedExecution] | None
    )
    _calls: list[ExecutionJob]
    _lock: Lock

    def __init__(
        self,
        responses: Iterable[CompletedExecution] = (),
        *,
        responder: (
            Callable[[ExecutionJob, CancelToken | None], CompletedExecution]
            | None
        ) = None,
    ) -> None:
        response_items = tuple(responses)
        if response_items and responder is not None:
            raise ValueError("responses and responder are mutually exclusive")
        self._responses = deque(response_items)
        self._responder = responder
        self._calls = []
        self._lock = Lock()

    def run(
        self,
        job: ExecutionJob,
        /,
        *,
        cancellation: CancelToken | None = None,
    ) -> CompletedExecution:
        raise NotImplementedError("FakeExecutor.run is not implemented")

    @property
    def calls(self) -> tuple[ExecutionJob, ...]:
        raise NotImplementedError("FakeExecutor.calls is not implemented")


__all__ = ["FakeExecutor"]
