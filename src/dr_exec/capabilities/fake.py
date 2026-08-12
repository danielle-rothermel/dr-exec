from __future__ import annotations

from collections import deque
from collections.abc import Callable, Iterable
from threading import Lock

from dr_exec.core.cancel import CancelToken
from dr_exec.core.errors import ExecutorFailure
from dr_exec.core.kinds import ExecutorFailureCode
from dr_exec.declarations.models import ExecutionJob
from dr_exec.declarations.validation import validate_declaration
from dr_exec.recording.models import CompletedExecution, FakeRecordReceipt
from dr_exec.scheduling.offload import offload_run_blocking


class FakeExecutor:
    """Scripted executor that skips production platform checks and makes no containment claim."""

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

    async def run(
        self,
        job: ExecutionJob,
        /,
        *,
        cancellation: CancelToken | None = None,
    ) -> CompletedExecution:
        return await offload_run_blocking(self, job, cancellation=cancellation)

    def run_blocking(
        self,
        job: ExecutionJob,
        /,
        *,
        cancellation: CancelToken | None = None,
    ) -> CompletedExecution:
        validate_declaration(job)
        responder = self._responder
        if responder is not None:
            # Consumer responders run outside the lock so calls can overlap.
            self._record_call(job)
            return _fake_receipted(responder(job, cancellation))
        return _fake_receipted(self._record_call_taking_response(job))

    def _record_call(self, job: ExecutionJob, /) -> None:
        with self._lock:
            self._calls.append(job)

    def _record_call_taking_response(
        self, job: ExecutionJob, /
    ) -> CompletedExecution:
        with self._lock:
            self._calls.append(job)
            return self._next_response()

    def _next_response(self) -> CompletedExecution:
        if not self._responses:
            raise ExecutorFailure(
                "the fake executor has no scripted response left",
                code=ExecutorFailureCode.FAKE_NO_RESPONSE,
            )
        return self._responses.popleft()

    @property
    def calls(self) -> tuple[ExecutionJob, ...]:
        with self._lock:
            return tuple(self._calls)


def _fake_receipted(completed: CompletedExecution, /) -> CompletedExecution:
    """Reject receipts for durable state a fake never recorded."""
    if not isinstance(completed.record_receipt, FakeRecordReceipt):
        raise ExecutorFailure(
            "fake completions must carry a fake record receipt, not "
            f"{completed.record_receipt.kind}",
            code=ExecutorFailureCode.FAKE_RECEIPT_MISMATCH,
        )
    return completed


__all__ = ["FakeExecutor"]
