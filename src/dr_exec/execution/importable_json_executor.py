from __future__ import annotations

import time
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from threading import Timer
from uuid import uuid4

from dr_serialize import build_identity_document

from dr_exec.core.cancel import CancelToken
from dr_exec.core.errors import ExecutorFailure
from dr_exec.core.kinds import BudgetAxis, ProtocolFailureCode
from dr_exec.core.names import AttemptId, ExecutionId
from dr_exec.declarations.models import (
    ExecutionJob,
    InProcessImportableJsonTarget,
)
from dr_exec.declarations.transport import request_transport_bytes
from dr_exec.declarations.validation import validate_declaration
from dr_exec.execution.executor import _run_batch
from dr_exec.execution.outcomes import (
    attribute_outcome,
    empty_payload_outputs,
    executor_protocol_failure_attribution,
    finite_duration_ns,
)
from dr_exec.importable_json import (
    ImportableJsonExecutorDispatchError,
    ImportableJsonPayloadDispatchError,
    ImportableJsonPayloadResultError,
    _invoke_importable_entry_point,
)
from dr_exec.recording.models import (
    BudgetExceededOutcome,
    CancelledOutcome,
    CompletedExecution,
    ExecutionAttribution,
    ExecutionMeasurements,
    ExecutionOutcome,
    ExecutionResult,
    ExitedOutcome,
    InProcessRecordReceipt,
    ProtocolFailedOutcome,
)
from dr_exec.scheduling.pool import (
    ExecutionPool,
    ExecutionPoolConfig,
    _resolve_capacity,
)

_ENVELOPE_SCHEMA = "dr_exec.importable_json"
_ENVELOPE_SCHEMA_VERSION = 1


@dataclass(slots=True)
class _StopState:
    caller_cancelled: bool = False
    deadline_expired: bool = False
    local_token: CancelToken = field(default_factory=CancelToken)


@dataclass(frozen=True, slots=True)
class ImportableJsonExecutor:
    """Run trusted importable-JSON entry points in-process."""

    def run(
        self,
        job: ExecutionJob,
        /,
        *,
        cancellation: CancelToken | None = None,
    ) -> CompletedExecution:
        validate_declaration(job)
        target = job.target
        if not isinstance(target, InProcessImportableJsonTarget):
            raise ExecutorFailure(
                "the importable JSON executor accepts only in-process "
                "importable JSON targets"
            )
        execution_id = ExecutionId(
            job_id=job.job_id,
            attempt_id=AttemptId(uuid4()),
        )
        started_at = datetime.now(UTC)
        started_ns = time.monotonic_ns()
        input_bytes = len(request_transport_bytes(target.request))
        stop = _StopState()
        if cancellation is not None and cancellation.cancelled:
            stop.caller_cancelled = True
        deadline_timer: Timer | None = None
        deadline_ns = finite_duration_ns(job.budgets.wall_time)
        if deadline_ns is not None:
            delay_seconds = max(
                0.0,
                (started_ns + deadline_ns - time.monotonic_ns()) / 1e9,
            )

            def on_deadline() -> None:
                if cancellation is not None and cancellation.cancelled:
                    stop.caller_cancelled = True
                if not stop.caller_cancelled:
                    stop.deadline_expired = True
                stop.local_token.cancel()

            deadline_timer = Timer(delay_seconds, on_deadline)
            deadline_timer.daemon = True
            deadline_timer.start()
        try:
            return self._run_body(
                target=target,
                execution_id=execution_id,
                started_at=started_at,
                started_ns=started_ns,
                input_bytes=input_bytes,
                cancellation=cancellation,
                stop=stop,
            )
        except BaseException as error:  # noqa: BLE001 - pool must not break
            if isinstance(error, SystemExit):
                code = error.code
                exit_code = code if isinstance(code, int) else 1
                return _completed(
                    execution_id=execution_id,
                    outcome=ExitedOutcome(exit_code=exit_code),
                    protocol_outputs=(),
                    started_at=started_at,
                    started_ns=started_ns,
                    input_bytes=input_bytes,
                )
            return _completed(
                execution_id=execution_id,
                outcome=ExitedOutcome(exit_code=1),
                protocol_outputs=(),
                started_at=started_at,
                started_ns=started_ns,
                input_bytes=input_bytes,
                attribution_detail="the importable JSON entry point terminated",
            )
        finally:
            if deadline_timer is not None:
                deadline_timer.cancel()

    def _run_body(
        self,
        *,
        target: InProcessImportableJsonTarget,
        execution_id: ExecutionId,
        started_at: datetime,
        started_ns: int,
        input_bytes: int,
        cancellation: CancelToken | None,
        stop: _StopState,
    ) -> CompletedExecution:
        if _should_stop(cancellation, stop):
            return _completed(
                execution_id=execution_id,
                outcome=_outcome_for_stop(stop),
                protocol_outputs=(),
                started_at=started_at,
                started_ns=started_ns,
                input_bytes=input_bytes,
            )
        try:
            result = _invoke_importable_entry_point(
                target.entry_point,
                target.request,
            )
        except ImportableJsonExecutorDispatchError as error:
            return _completed(
                execution_id=execution_id,
                outcome=ProtocolFailedOutcome(
                    failure_code=ProtocolFailureCode.MALFORMED_FRAME,
                    failure_detail=str(error),
                    accepted_output_count=0,
                ),
                protocol_outputs=(),
                started_at=started_at,
                started_ns=started_ns,
                input_bytes=input_bytes,
                attribution_override=executor_protocol_failure_attribution,
            )
        except ImportableJsonPayloadResultError as error:
            return _completed(
                execution_id=execution_id,
                outcome=ProtocolFailedOutcome(
                    failure_code=ProtocolFailureCode.MALFORMED_FRAME,
                    failure_detail=str(error),
                    accepted_output_count=0,
                ),
                protocol_outputs=(),
                started_at=started_at,
                started_ns=started_ns,
                input_bytes=input_bytes,
            )
        except ImportableJsonPayloadDispatchError as error:
            if _should_stop(cancellation, stop):
                return _completed(
                    execution_id=execution_id,
                    outcome=_outcome_for_stop(stop),
                    protocol_outputs=(),
                    started_at=started_at,
                    started_ns=started_ns,
                    input_bytes=input_bytes,
                )
            return _completed(
                execution_id=execution_id,
                outcome=ExitedOutcome(exit_code=1),
                protocol_outputs=(),
                started_at=started_at,
                started_ns=started_ns,
                input_bytes=input_bytes,
                attribution_detail=str(error),
            )
        if _should_stop(cancellation, stop):
            return _completed(
                execution_id=execution_id,
                outcome=_outcome_for_stop(stop),
                protocol_outputs=(),
                started_at=started_at,
                started_ns=started_ns,
                input_bytes=input_bytes,
            )
        envelope = build_identity_document(
            schema=_ENVELOPE_SCHEMA,
            schema_version=_ENVELOPE_SCHEMA_VERSION,
            payload=result,
        )
        return _completed(
            execution_id=execution_id,
            outcome=ExitedOutcome(exit_code=0),
            protocol_outputs=(envelope,),
            started_at=started_at,
            started_ns=started_ns,
            input_bytes=input_bytes,
        )

    def run_many(
        self,
        jobs: Iterable[ExecutionJob],
        /,
        *,
        config: ExecutionPoolConfig | None = None,
    ) -> Iterator[CompletedExecution]:
        return _run_batch(
            self,
            jobs,
            capacity=_resolve_capacity(
                (config or ExecutionPoolConfig()).capacity
            ).max_active_jobs,
        )

    def open_pool(
        self,
        *,
        config: ExecutionPoolConfig | None = None,
    ) -> ExecutionPool:
        return ExecutionPool(
            executor=self,
            config=config or ExecutionPoolConfig(),
        )


def _should_stop(
    cancellation: CancelToken | None, stop: _StopState, /
) -> bool:
    if cancellation is not None and cancellation.cancelled:
        stop.caller_cancelled = True
    return (
        stop.caller_cancelled
        or stop.deadline_expired
        or stop.local_token.cancelled
    )


def _outcome_for_stop(stop: _StopState, /) -> ExecutionOutcome:
    if stop.caller_cancelled:
        return CancelledOutcome()
    if stop.deadline_expired:
        return BudgetExceededOutcome(axis=BudgetAxis.WALL_TIME)
    return CancelledOutcome()


def _completed(
    *,
    execution_id: ExecutionId,
    outcome: ExecutionOutcome,
    protocol_outputs: tuple[object, ...],
    started_at: datetime,
    started_ns: int,
    input_bytes: int,
    attribution_detail: str | None = None,
    attribution_override: Callable[
        [ProtocolFailedOutcome], ExecutionAttribution
    ]
    | None = None,
) -> CompletedExecution:
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
    finished_at = datetime.now(UTC)
    result = ExecutionResult(
        execution_id=execution_id,
        outcome=outcome,
        attribution=attribution,
        protocol_outputs=protocol_outputs,  # ty: ignore[invalid-argument-type]
        payload_outputs=empty_payload_outputs(),
        measurements=ExecutionMeasurements(
            started_at=started_at,
            finished_at=finished_at,
            duration_ns=time.monotonic_ns() - started_ns,
            teardown_duration_ns=0,
            input_bytes=input_bytes,
            protocol_bytes_received=0,
        ),
    )
    completed = CompletedExecution(
        result=result,
        record_receipt=InProcessRecordReceipt(execution_id=execution_id),
    )
    return _in_process_receipted(completed)


def _in_process_receipted(
    completed: CompletedExecution, /
) -> CompletedExecution:
    if not isinstance(completed.record_receipt, InProcessRecordReceipt):
        raise ExecutorFailure(
            "in-process completions must carry an in-process record receipt, "
            f"not {completed.record_receipt.kind}"
        )
    return completed


__all__ = ["ImportableJsonExecutor"]
