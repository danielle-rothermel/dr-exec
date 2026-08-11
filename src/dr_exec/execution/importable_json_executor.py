from __future__ import annotations

import time
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
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
        token = cancellation if cancellation is not None else CancelToken()
        deadline_timer: Timer | None = None
        deadline_ns = finite_duration_ns(job.budgets.wall_time)
        if deadline_ns is not None:
            delay_seconds = max(
                0.0,
                (started_ns + deadline_ns - time.monotonic_ns()) / 1e9,
            )
            deadline_timer = Timer(delay_seconds, token.cancel)
            deadline_timer.daemon = True
            deadline_timer.start()
        try:
            if token.cancelled:
                return _completed(
                    execution_id=execution_id,
                    outcome=_cancel_or_budget(token, deadline_ns, started_ns),
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
                if token.cancelled:
                    return _completed(
                        execution_id=execution_id,
                        outcome=_cancel_or_budget(
                            token, deadline_ns, started_ns
                        ),
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
            if token.cancelled:
                return _completed(
                    execution_id=execution_id,
                    outcome=_cancel_or_budget(token, deadline_ns, started_ns),
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
        finally:
            if deadline_timer is not None:
                deadline_timer.cancel()

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


def _cancel_or_budget(
    token: CancelToken,
    deadline_ns: int | None,
    started_ns: int,
    /,
) -> ExecutionOutcome:
    if (
        token.cancelled
        and deadline_ns is not None
        and started_ns + deadline_ns <= time.monotonic_ns()
    ):
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
