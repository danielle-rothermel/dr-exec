"""The library-owned contract-enforcing fake executor.

`FakeExecutor` exists so consumer logic can be tested against the same
`Executor` boundary production uses without pretending that a scripted
completion proves anything about spawning, containment, or durable
recording. It executes no payload, creates no scratch space, writes no
record, and touches no host process.

What it does enforce is everything a fake *can* honestly enforce:

- the same declaration rules `ProcessExecutor` applies, from the one
  shared declaration path, so a job a fake accepts is a job production
  would also accept -- the fake conforms to production, never the reverse;
- exactly one response source, chosen at construction: an in-order queue
  or a declaration-dependent responder, never both;
- the fake receipt on every completion it hands back, so a fake call can
  never be mistaken for a recorded one and a production no-record option
  never comes into existence;
- thread safety, because the `Executor` protocol promises concurrent calls
  and a fake that serializes differently would hide consumer races.

Host support is the one production rule the fake does not apply. Refusing
an unsupported platform states where a containment claim holds, and the
fake makes no containment claim -- applying it would make consumer logic
tests unrunnable off macOS for no gain in fidelity.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Callable, Iterable
from threading import Lock

from dr_exec.core.cancel import CancelToken
from dr_exec.core.errors import ExecutorFailure
from dr_exec.declarations.models import ExecutionJob
from dr_exec.declarations.validation import validate_declaration
from dr_exec.recording.models import CompletedExecution, FakeRecordReceipt


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
        """Validate one job, record it, and return its scripted completion.

        Validation happens before the call is recorded, so a rejected
        declaration leaves no trace of an execution that never conceptually
        started -- exactly as production leaves nothing durable behind a
        pre-spawn refusal.

        A responder is invoked outside the lock and receives the call's own
        cancellation token, so a consumer can script cancellation-dependent
        behavior and concurrent responder calls do not serialize against
        each other. Only the queue pop and the call append are synchronized,
        which is the whole of the fake's mutable state.
        """
        validate_declaration(job)
        responder = self._responder
        if responder is not None:
            # The responder runs outside the lock: it is consumer code of
            # arbitrary duration, and holding the lock across it would make
            # this fake serialize calls the Executor protocol promises can
            # overlap.
            self._record_call(job)
            return _fake_receipted(responder(job, cancellation))
        return _fake_receipted(self._record_call_taking_response(job))

    def _record_call(self, job: ExecutionJob, /) -> None:
        with self._lock:
            self._calls.append(job)

    def _record_call_taking_response(
        self, job: ExecutionJob, /
    ) -> CompletedExecution:
        """Record one call and take its response as one atomic step.

        Doing both under one lock is what makes ordering deterministic
        under contention: the Nth recorded call is the call that took the
        Nth scripted response, however many threads are calling.
        """
        with self._lock:
            self._calls.append(job)
            return self._next_response()

    def _next_response(self) -> CompletedExecution:
        """Pop the next scripted completion under the caller's lock.

        Ordering is the queue's own: concurrent calls take distinct
        responses in queue order, and exhaustion is an explicit failure
        rather than a manufactured completion the consumer never scripted.
        """
        if not self._responses:
            raise ExecutorFailure(
                "the fake executor has no scripted response left"
            )
        return self._responses.popleft()

    @property
    def calls(self) -> tuple[ExecutionJob, ...]:
        """Every job this fake accepted, in call order.

        The tuple is a snapshot taken under the lock, so reading it while
        other threads are calling `run` yields a consistent prefix rather
        than a list mutating underfoot. `ExecutionJob` is itself immutable,
        so nothing a caller does to the snapshot can reach recorded state.
        """
        with self._lock:
            return tuple(self._calls)


def _fake_receipted(completed: CompletedExecution, /) -> CompletedExecution:
    """Refuse any completion carrying a production record receipt.

    A fake call recorded nothing, so a `CompleteRecordReceipt` or
    `DegradedRecordReceipt` here would be a claim about durable state that
    does not exist. Rejecting it keeps `RecordReceiptKind.NOT_APPLICABLE`
    meaning "no record was ever attempted" instead of decaying into a
    production no-record option.
    """
    if not isinstance(completed.record_receipt, FakeRecordReceipt):
        raise ExecutorFailure(
            "fake completions must carry a fake record receipt, not "
            f"{completed.record_receipt.kind}"
        )
    return completed


__all__ = ["FakeExecutor"]
