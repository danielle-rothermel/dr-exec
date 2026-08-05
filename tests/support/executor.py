"""Job and completion builders shared by the executor test modules.

Scripting a fake response means constructing a `CompletedExecution` by
hand, and consumers will do exactly this. Keeping the builders here lets
the fake and conformance suites stay about executor behavior rather than
about reassembling a valid result in every case, and lets both suites
build the *same* jobs so a parity claim compares like with like.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from threading import Lock, Thread
from uuid import uuid4

from dr_serialize import build_identity_document

from dr_exec import (
    AttemptId,
    Budgets,
    CompletedExecution,
    CompleteRecordReceipt,
    ContainmentProfile,
    EnvGrant,
    ExecutionAttribution,
    ExecutionId,
    ExecutionJob,
    ExecutionMeasurements,
    ExecutionResult,
    ExecutionTarget,
    ExitedOutcome,
    FailureOwner,
    FakeRecordReceipt,
    JobId,
    PayloadOutputs,
    RetainedPayloadStream,
    TrustedCommandTarget,
    UntrustedCommandTarget,
    UntrustedPythonTarget,
)

# A driver that emits one output echoing its request, used wherever a
# Python target needs to be well-formed rather than interesting.
ECHO_DRIVER = """
def dr_exec_main(request, emit):
    emit({
        "schema": "dr_exec.test_output",
        "schema_version": 1,
        "payload": {"echo": request["payload"]["echo"]},
    })
"""


@dataclass(frozen=True, slots=True)
class ThreadCallResults[T]:
    """Terminal values, failures, and unfinished calls from one thread batch."""

    values: tuple[T, ...]
    errors: tuple[Exception, ...]
    unfinished: int


def run_thread_calls[T](
    calls: Iterable[Callable[[], T]],
    /,
    *,
    timeout: float,
) -> ThreadCallResults[T]:
    """Run calls together and retain every terminal worker outcome."""
    values: list[T] = []
    errors: list[Exception] = []
    guard = Lock()

    def capture(call: Callable[[], T]) -> None:
        try:
            value = call()
        except Exception as error:  # noqa: BLE001 - propagate worker failures
            with guard:
                errors.append(error)
        else:
            with guard:
                values.append(value)

    threads = [Thread(target=capture, args=(call,)) for call in calls]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout)
    return ThreadCallResults(
        values=tuple(values),
        errors=tuple(errors),
        unfinished=sum(thread.is_alive() for thread in threads),
    )


# --- Targets and jobs ----------------------------------------------------


def trusted_target(argv: tuple[str, ...], /) -> ExecutionTarget:
    return TrustedCommandTarget(argv=argv)


def untrusted_command_target(argv: tuple[str, ...], /) -> ExecutionTarget:
    return UntrustedCommandTarget(
        argv=argv,
        containment_profile=ContainmentProfile.PROCESS_BOUNDARY_ONLY,
    )


def python_target(echo: str = "ran", /) -> ExecutionTarget:
    return UntrustedPythonTarget(
        driver_source=ECHO_DRIVER,
        request=build_identity_document(
            schema="dr_exec.test_request",
            schema_version=1,
            payload={"echo": echo},
        ),
        containment_profile=ContainmentProfile.PROCESS_BOUNDARY_ONLY,
    )


def job_for(
    target: ExecutionTarget,
    /,
    *,
    env: EnvGrant | None = None,
    budgets: Budgets | None = None,
) -> ExecutionJob:
    return ExecutionJob(
        job_id=JobId(uuid4()),
        target=target,
        env=EnvGrant.none() if env is None else env,
        budgets=Budgets.unbudgeted() if budgets is None else budgets,
    )


# --- Completions ---------------------------------------------------------


def empty_payload_outputs() -> PayloadOutputs:
    empty = RetainedPayloadStream(
        head=b"", tail=b"", produced_bytes=0, dropped_bytes=0
    )
    return PayloadOutputs(stdout=empty, stderr=empty)


def execution_result(execution_id: ExecutionId, /) -> ExecutionResult:
    """One minimal clean-exit result for the given execution."""
    moment = datetime.now(UTC)
    return ExecutionResult(
        execution_id=execution_id,
        outcome=ExitedOutcome(exit_code=0),
        attribution=ExecutionAttribution(owner=FailureOwner.NONE),
        protocol_outputs=(),
        payload_outputs=empty_payload_outputs(),
        measurements=ExecutionMeasurements(
            started_at=moment,
            finished_at=moment,
            duration_ns=0,
            teardown_duration_ns=0,
            input_bytes=0,
            protocol_bytes_received=0,
        ),
    )


def completion_for(job_id: JobId, /) -> CompletedExecution:
    """A fake-receipted completion bound to one job's identity."""
    execution_id = ExecutionId(job_id=job_id, attempt_id=AttemptId(uuid4()))
    return CompletedExecution(
        result=execution_result(execution_id),
        record_receipt=FakeRecordReceipt(execution_id=execution_id),
    )


def fake_completion() -> CompletedExecution:
    """A fake-receipted completion whose identity does not matter."""
    return completion_for(JobId(uuid4()))


def real_receipted_completion() -> CompletedExecution:
    """A completion carrying a production receipt.

    Only the fake's receipt enforcement uses this: it is precisely the
    value a fake must refuse, because a fake call recorded nothing.
    """
    execution_id = ExecutionId(
        job_id=JobId(uuid4()), attempt_id=AttemptId(uuid4())
    )
    return CompletedExecution(
        result=execution_result(execution_id),
        record_receipt=CompleteRecordReceipt(
            execution_id=execution_id,
            record_dir=Path("/dr-exec-test/records/run-0"),
        ),
    )
