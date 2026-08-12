from __future__ import annotations

from dr_exec.core.kinds import BudgetAxis, FailureOwner, ProtocolFailureCode
from dr_exec.recording.models import (
    BudgetExceededOutcome,
    CancelledOutcome,
    ExecutionAttribution,
    ExecutionOutcome,
    ExitedOutcome,
    PayloadOutputs,
    ProtocolFailedOutcome,
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


def empty_payload_outputs() -> PayloadOutputs:
    empty = RetainedPayloadStream(
        head=b"",
        tail=b"",
        produced_bytes=0,
        dropped_bytes=0,
    )
    return PayloadOutputs(stdout=empty, stderr=empty)


def finite_duration_ns(budget: object, /) -> int | None:
    from dr_exec.declarations.models import FiniteDurationLimit

    if isinstance(budget, FiniteDurationLimit):
        return budget.max_ns
    return None


__all__ = [
    "attribute_outcome",
    "empty_payload_outputs",
    "executor_protocol_failure_attribution",
    "finite_duration_ns",
]
