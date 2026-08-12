from __future__ import annotations

import time
from collections.abc import Callable
from datetime import UTC, datetime

from dr_serialize import IdentityDocument

from dr_exec.core.kinds import BudgetAxis, FailureOwner, ProtocolFailureCode
from dr_exec.core.names import ExecutionId
from dr_exec.recording.models import (
    BudgetExceededOutcome,
    CancelledOutcome,
    CompletedExecution,
    ExecutionAttribution,
    ExecutionMeasurements,
    ExecutionOutcome,
    ExecutionResult,
    ExitedOutcome,
    PayloadOutputs,
    ProtocolFailedOutcome,
    RecordReceipt,
    RetainedPayloadStream,
    SignaledOutcome,
    SpawnAbsentOutcome,
    SpawnFailedOutcome,
)


def attribute_outcome(outcome: ExecutionOutcome, /) -> ExecutionAttribution:
    match outcome:
        case ExitedOutcome():
            return ExecutionAttribution(
                owner=FailureOwner.NONE
                if outcome.exit_code == 0
                else FailureOwner.PAYLOAD,
                detail=None
                if outcome.exit_code == 0
                else "the payload exited nonzero",
            )
        case SignaledOutcome():
            return ExecutionAttribution(
                owner=FailureOwner.PAYLOAD,
                detail="the payload died on a signal",
            )
        case SpawnAbsentOutcome():
            return ExecutionAttribution(
                owner=FailureOwner.EXECUTOR,
                detail="the declared executable was not found",
            )
        case SpawnFailedOutcome():
            return ExecutionAttribution(
                owner=FailureOwner.MACHINE,
                detail="the child could not be started",
            )
        case BudgetExceededOutcome():
            if outcome.axis is BudgetAxis.WALL_TIME:
                return ExecutionAttribution(
                    owner=FailureOwner.EXECUTOR,
                    detail="the wall-time budget was exceeded",
                )
            return ExecutionAttribution(
                owner=FailureOwner.PAYLOAD,
                detail=f"the payload exceeded its {outcome.axis} budget",
            )
        case ProtocolFailedOutcome():
            return ExecutionAttribution(
                owner=FailureOwner.EXECUTOR
                if outcome.failure_code is ProtocolFailureCode.OVERSIZED_FRAME
                else FailureOwner.PAYLOAD,
                detail=outcome.failure_detail,
            )
        case CancelledOutcome():
            return ExecutionAttribution(
                owner=FailureOwner.NONE,
                detail="the call was cancelled",
            )


def executor_protocol_failure_attribution(
    outcome: ProtocolFailedOutcome,
    /,
) -> ExecutionAttribution:
    return ExecutionAttribution(
        owner=FailureOwner.EXECUTOR,
        detail=outcome.failure_detail,
    )


def completed_execution(
    *,
    execution_id: ExecutionId,
    record_receipt: RecordReceipt,
    outcome: ExecutionOutcome,
    protocol_outputs: tuple[IdentityDocument, ...],
    started_at: datetime,
    started_ns: int,
    input_bytes: int,
    attribution_detail: str | None,
    attribution_override: Callable[
        [ProtocolFailedOutcome], ExecutionAttribution
    ]
    | None,
) -> CompletedExecution:
    """Build one record-less completion from an attempt's facts.

    Record-less executors produce no payload streams and no teardown of their
    own, so the measurements those axes describe are reported as zero rather
    than left unmeasured.
    """

    if attribution_override is not None and isinstance(
        outcome, ProtocolFailedOutcome
    ):
        attribution = attribution_override(outcome)
    else:
        attribution = attribute_outcome(outcome)
    if attribution_detail is not None:
        attribution = attribution.model_copy(
            update={"detail": attribution_detail}
        )
    return CompletedExecution(
        result=ExecutionResult(
            execution_id=execution_id,
            outcome=outcome,
            attribution=attribution,
            protocol_outputs=protocol_outputs,
            payload_outputs=empty_payload_outputs(),
            measurements=ExecutionMeasurements(
                started_at=started_at,
                finished_at=datetime.now(UTC),
                duration_ns=time.monotonic_ns() - started_ns,
                teardown_duration_ns=0,
                input_bytes=input_bytes,
                protocol_bytes_received=0,
            ),
        ),
        record_receipt=record_receipt,
    )


def empty_payload_outputs() -> PayloadOutputs:
    empty = RetainedPayloadStream(
        head=b"",
        tail=b"",
        produced_bytes=0,
        dropped_bytes=0,
    )
    return PayloadOutputs(stdout=empty, stderr=empty)


__all__ = [
    "attribute_outcome",
    "completed_execution",
    "empty_payload_outputs",
    "executor_protocol_failure_attribution",
]
