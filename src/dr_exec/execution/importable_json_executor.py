from __future__ import annotations

import asyncio
import time
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from threading import Timer

from dr_serialize import IdentityDocument, build_identity_document

from dr_exec.core.cancel import CancelToken
from dr_exec.core.errors import ExecutorFailure
from dr_exec.core.kinds import (
    BudgetAxis,
    ExecutorFailureCode,
)
from dr_exec.core.names import ExecutionId
from dr_exec.declarations.models import (
    ExecutionJob,
    InProcessImportableJsonTarget,
)
from dr_exec.declarations.transport import request_transport_bytes
from dr_exec.declarations.validation import validate_declaration
from dr_exec.execution.outcomes import (
    completed_execution,
    executor_protocol_failure_attribution,
    malformed_frame_outcome,
)
from dr_exec.importable_json import (
    ENVELOPE_SCHEMA,
    ENVELOPE_SCHEMA_VERSION,
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
    ExecutionOutcome,
    ExitedOutcome,
    InProcessRecordReceipt,
    ProtocolFailedOutcome,
)
from dr_exec.recording.references import attempt_id_for_job
from dr_exec.scheduling.offload import offload_blocking_daemon
from dr_exec.scheduling.pool import (
    AutoPoolCapacity,
    ExecutionPool,
    ExecutionPoolConfig,
    batch_capacity,
)
from dr_exec.scheduling.scheduler import run_batch


@dataclass(slots=True)
class _StopState:
    """Whether the armed wall-time deadline has fired for this job."""

    deadline_expired: bool = False

    def outcome(
        self, cancellation: CancelToken | None, /
    ) -> ExecutionOutcome | None:
        """Report why this job must stop, or ``None`` while it may run.

        A caller's cancel outranks an expired deadline: a job the caller
        already gave up on is cancelled, not over budget.
        """

        if cancellation is not None and cancellation.cancelled:
            return CancelledOutcome()
        if self.deadline_expired:
            return BudgetExceededOutcome(axis=BudgetAxis.WALL_TIME)
        return None


@dataclass(frozen=True, slots=True)
class _Execution:
    """One job's identity, timing, and completion construction."""

    execution_id: ExecutionId
    started_at: datetime
    started_ns: int
    input_bytes: int

    def completed(
        self,
        *,
        outcome: ExecutionOutcome,
        protocol_outputs: tuple[IdentityDocument, ...] = (),
        attribution_detail: str | None = None,
        attribution_override: Callable[
            [ProtocolFailedOutcome], ExecutionAttribution
        ]
        | None = None,
    ) -> CompletedExecution:
        return completed_execution(
            execution_id=self.execution_id,
            record_receipt=InProcessRecordReceipt(
                execution_id=self.execution_id
            ),
            outcome=outcome,
            protocol_outputs=protocol_outputs,
            started_at=self.started_at,
            started_ns=self.started_ns,
            input_bytes=self.input_bytes,
            attribution_detail=attribution_detail,
            attribution_override=attribution_override,
        )


@dataclass(frozen=True, slots=True)
class ImportableJsonExecutor:
    """Run trusted importable-JSON entry points in-process."""

    async def run(
        self,
        job: ExecutionJob,
        /,
        *,
        cancellation: CancelToken | None = None,
    ) -> CompletedExecution:
        token = cancellation if cancellation is not None else CancelToken()
        # The attempt facts are built once, before the offload, so an interrupt
        # reports the elapsed run rather than the moment the interrupt arrived.
        target = _in_process_target(job)
        execution = _attempt(job, target)
        try:
            return await offload_blocking_daemon(
                self._run_attempt, execution, target, job, cancellation=token
            )
        except (KeyboardInterrupt, asyncio.CancelledError):
            token.cancel()
            return execution.completed(
                outcome=ExitedOutcome(exit_code=1),
                attribution_detail="the importable JSON entry point terminated",
            )

    def run_blocking(
        self,
        job: ExecutionJob,
        /,
        *,
        cancellation: CancelToken | None = None,
    ) -> CompletedExecution:
        target = _in_process_target(job)
        return self._run_attempt(
            _attempt(job, target), target, job, cancellation=cancellation
        )

    def _run_attempt(
        self,
        execution: _Execution,
        target: InProcessImportableJsonTarget,
        job: ExecutionJob,
        /,
        *,
        cancellation: CancelToken | None,
    ) -> CompletedExecution:
        stop = _StopState()
        deadline_timer: Timer | None = None
        deadline_ns = job.budgets.wall_time.limit
        if deadline_ns is not None:
            deadline_at_ns = execution.started_ns + deadline_ns
            delay_seconds = max(
                0.0, (deadline_at_ns - time.monotonic_ns()) / 1e9
            )

            def on_deadline() -> None:
                stop.deadline_expired = True

            deadline_timer = Timer(delay_seconds, on_deadline)
            deadline_timer.daemon = True
            deadline_timer.start()
        try:
            return self._run_body(execution, target, cancellation, stop)
        except BaseException as error:  # noqa: BLE001 - pool must not break
            if isinstance(error, SystemExit):
                code = error.code
                exit_code = code if isinstance(code, int) else 1
                return execution.completed(
                    outcome=ExitedOutcome(exit_code=exit_code)
                )
            return execution.completed(
                outcome=ExitedOutcome(exit_code=1),
                attribution_detail="the importable JSON entry point terminated",
            )
        finally:
            if deadline_timer is not None:
                deadline_timer.cancel()

    def _run_body(
        self,
        execution: _Execution,
        target: InProcessImportableJsonTarget,
        cancellation: CancelToken | None,
        stop: _StopState,
        /,
    ) -> CompletedExecution:
        stopped = _stopped(execution, cancellation, stop)
        if stopped is not None:
            return stopped
        try:
            result = _invoke_importable_entry_point(
                target.entry_point,
                target.request,
            )
        except ImportableJsonExecutorDispatchError as error:
            return execution.completed(
                outcome=malformed_frame_outcome(str(error)),
                attribution_override=executor_protocol_failure_attribution,
            )
        except ImportableJsonPayloadResultError as error:
            return execution.completed(
                outcome=malformed_frame_outcome(str(error))
            )
        except ImportableJsonPayloadDispatchError as error:
            stopped = _stopped(execution, cancellation, stop)
            if stopped is not None:
                return stopped
            return execution.completed(
                outcome=ExitedOutcome(exit_code=1),
                attribution_detail=str(error),
            )
        stopped = _stopped(execution, cancellation, stop)
        if stopped is not None:
            return stopped
        return execution.completed(
            outcome=ExitedOutcome(exit_code=0),
            protocol_outputs=(
                build_identity_document(
                    schema=ENVELOPE_SCHEMA,
                    schema_version=ENVELOPE_SCHEMA_VERSION,
                    payload=result,
                ),
            ),
        )

    def run_many(
        self,
        jobs: Iterable[ExecutionJob],
        /,
        *,
        config: ExecutionPoolConfig | None = None,
    ) -> Iterator[CompletedExecution]:
        return run_batch(
            self,
            jobs,
            capacity=batch_capacity(config, default=AutoPoolCapacity()),
        )

    def open_pool(
        self,
        *,
        config: ExecutionPoolConfig | None = None,
    ) -> ExecutionPool:
        return ExecutionPool(
            executor=self,
            config=config or ExecutionPoolConfig(capacity=AutoPoolCapacity()),
        )


def _in_process_target(job: ExecutionJob, /) -> InProcessImportableJsonTarget:
    validate_declaration(job)
    target = job.target
    if not isinstance(target, InProcessImportableJsonTarget):
        raise ExecutorFailure(
            "the importable JSON executor accepts only in-process "
            "importable JSON targets",
            code=ExecutorFailureCode.IMPORTABLE_JSON_TARGET_MISMATCH,
        )
    return target


def _attempt(
    job: ExecutionJob,
    target: InProcessImportableJsonTarget,
    /,
) -> _Execution:
    return _Execution(
        execution_id=ExecutionId(
            job_id=job.job_id,
            attempt_id=attempt_id_for_job(job.job_id),
        ),
        started_at=datetime.now(UTC),
        started_ns=time.monotonic_ns(),
        input_bytes=len(request_transport_bytes(target.request)),
    )


def _stopped(
    execution: _Execution,
    cancellation: CancelToken | None,
    stop: _StopState,
    /,
) -> CompletedExecution | None:
    outcome = stop.outcome(cancellation)
    if outcome is None:
        return None
    return execution.completed(outcome=outcome)


__all__ = ["ImportableJsonExecutor"]
