from __future__ import annotations

from collections import deque
from collections.abc import Iterable
from threading import Lock

from dr_exec.declare import ExecutionJob
from dr_exec.record import CompletedExecution


class FakeExecutor:
    _responses: deque[CompletedExecution]
    _calls: list[ExecutionJob]
    _lock: Lock

    def __init__(
        self,
        responses: Iterable[CompletedExecution] = (),
    ) -> None:
        self._responses = deque(responses)
        self._calls = []
        self._lock = Lock()

    def run(
        self,
        job: ExecutionJob,
        /,
    ) -> CompletedExecution:
        raise NotImplementedError("FakeExecutor.run is not implemented")

    @property
    def calls(self) -> tuple[ExecutionJob, ...]:
        raise NotImplementedError("FakeExecutor.calls is not implemented")


__all__ = ["FakeExecutor"]
