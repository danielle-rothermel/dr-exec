"""Long-lived worker processes for trusted importable JSON jobs."""

from __future__ import annotations

import json
import os
import queue
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from types import TracebackType
from typing import IO, Final

from dr_serialize import (
    IdentityDocument,
    Jsonable,
    build_identity_document,
    validate_identity_document,
)

from dr_exec.core.cancel import CancelToken
from dr_exec.core.errors import ExecutorFailure
from dr_exec.core.kinds import (
    BudgetAxis,
    ExecutorFailureCode,
)
from dr_exec.core.names import ExecutionId
from dr_exec.declarations.models import (
    ExecutionJob,
    FiniteDurationLimit,
    ImportableEntryPoint,
    InProcessImportableJsonTarget,
)
from dr_exec.declarations.transport import request_transport_bytes
from dr_exec.declarations.validation import validate_declaration
from dr_exec.execution.outcomes import (
    completed_execution,
    executor_protocol_failure_attribution,
    malformed_frame_outcome,
)
from dr_exec.execution.spawn import ESCALATION_SIGNAL, signal_process_group
from dr_exec.execution.worker_pool_worker import (
    DETAIL_KEY,
    READY_FRAME,
    RESULT_KEY,
    STATUS_KEY,
    WORKER_FRAME_TERMINATOR,
    WorkerFrameStatus,
)
from dr_exec.importable_json import (
    ENVELOPE_SCHEMA,
    ENVELOPE_SCHEMA_VERSION,
    is_importable_json_envelope,
)
from dr_exec.recording.models import (
    BudgetExceededOutcome,
    CancelledOutcome,
    CompletedExecution,
    ExecutionAttribution,
    ExecutionOutcome,
    ExitedOutcome,
    ProtocolFailedOutcome,
    SignaledOutcome,
    SpawnFailedOutcome,
    WorkerPoolRecordReceipt,
)
from dr_exec.recording.references import attempt_id_for_job
from dr_exec.scheduling.offload import offload_blocking, offload_run_blocking
from dr_exec.scheduling.pool import (
    AutoPoolCapacity,
    ExecutionPool,
    ExecutionPoolConfig,
    FixedPoolCapacity,
    batch_capacity,
    resolve_pool_capacity,
)
from dr_exec.scheduling.scheduler import run_batch

_WORKER_MODULE: Final = "dr_exec.execution.worker_pool_worker"

# How often a job that declared a stop condition re-checks it while waiting
# for its worker. A job with no cancel token and no finite wall-time budget
# never polls: it blocks until its worker answers or dies.
_STOP_POLL_SECONDS: Final = 0.01


class _WorkerDied(Exception):
    """A worker process ended before delivering a result frame."""


class _WorkerStartupFailed(Exception):
    """A spawned worker never became ready to serve requests."""


class _WorkerSetClosed(Exception):
    """The pool closed while this job was spawning its worker."""


class _StopRequested(Exception):
    """A caller cancel or a declared finite wall-time budget ended the job."""

    def __init__(self, outcome: ExecutionOutcome, /) -> None:
        super().__init__(outcome.kind)
        self.outcome = outcome


@dataclass(frozen=True, slots=True)
class _SpawnFailure(Exception):
    """The operating system refused to start a worker process."""

    errno: int
    error_message: str


@dataclass(slots=True, eq=False)
class _Worker:
    """One long-lived worker process and its two dedicated pipes.

    Compared by identity: two workers are the same worker only when they are
    the same process, which is what the live-worker registry tracks.
    """

    process: subprocess.Popen[bytes]
    requests: IO[bytes]
    frames: queue.SimpleQueue[bytes | None]
    reader: threading.Thread
    ready: bool = False

    def send(self, frame: bytes, /) -> None:
        try:
            self.requests.write(frame)
            self.requests.flush()
        except OSError as error:
            raise _WorkerDied("the worker closed its request pipe") from error

    def receive(self, /, *, stop: _StopWatch) -> bytes:
        """Return the next result frame, or report the worker's death.

        The reader thread drains the result pipe while the caller writes the
        request, so a large request and a large result never block each other.
        """

        frame = _get_watched(self.frames, stop=stop)
        if frame is None:
            raise _WorkerDied("the worker ended without a result frame")
        return frame

    def terminate(self) -> None:
        pid = self.process.pid
        if self.process.poll() is None:
            signal_process_group(pid, ESCALATION_SIGNAL)
            if self.process.poll() is None:
                self.process.kill()
        self.process.wait()
        signal_process_group(pid, ESCALATION_SIGNAL)
        self.reader.join()
        _close_quietly(self.requests)

    def wait_for_ready(self, /, *, stop: _StopWatch) -> None:
        """Block until the worker has imported its entry point, or died.

        A startup import can run arbitrarily long, so a caller's declared stop
        condition is observed here exactly as it is while a job runs: an
        entry-point module that blocks on import never outlives the cancel
        token or finite wall-time budget the caller declared.
        """

        if self.ready:
            return
        try:
            frame = self.receive(stop=stop)
        except _WorkerDied as died:
            raise _WorkerStartupFailed(
                "the worker failed its entry-point import at startup"
            ) from died
        if frame != READY_FRAME:
            raise _WorkerStartupFailed(
                "the worker failed its entry-point import at startup"
            )
        self.ready = True

    @property
    def death(self) -> ExecutionOutcome:
        """Describe how the worker process ended.

        The result pipe reaches end of file as soon as the worker stops
        holding it open, which happens before the process finishes exiting.
        Waiting for the process is what makes the reported death the worker's
        own: killing a worker that is already on its way out would report the
        kill instead of the exit status the entry point asked for.
        """

        code = self.process.wait()
        if code < 0:
            return SignaledOutcome(signal_number=-code)
        return ExitedOutcome(exit_code=code)


class _WorkerLease:
    """One worker slot held for exactly one job.

    Releasing returns the worker to its pool. Discarding kills it and returns
    an empty slot instead, so the pool keeps its width and respawns on the
    next job that needs the slot.
    """

    __slots__ = ("_discarded", "_pool", "worker")

    def __init__(self, pool: _WorkerSet, worker: _Worker, /) -> None:
        self._pool = pool
        self._discarded = False
        self.worker = worker

    def __enter__(self) -> _WorkerLease:  # noqa: PYI034
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self._discarded:
            self._pool.retire_and_free(self.worker)
            return
        self._pool.release(self.worker)

    def discard(self) -> None:
        """Mark this worker for the kill this lease's exit carries out."""

        self._discarded = True


class _WorkerSet:
    """Fixed-width set of long-lived workers bound to one entry point.

    Every spawned worker is registered as live until it is terminated, so
    closing reaches workers whose slot is currently held by a running job as
    well as workers idling in the slot queue.
    """

    __slots__ = (
        "_closed",
        "_entry_point",
        "_live",
        "_lock",
        "_slots",
        "_width",
    )

    def __init__(
        self, *, entry_point: ImportableEntryPoint, width: int
    ) -> None:
        self._entry_point = entry_point
        self._width = width
        self._slots: queue.SimpleQueue[_Worker | None] = queue.SimpleQueue()
        self._closed = threading.Event()
        self._lock = threading.Lock()
        self._live: set[_Worker] = set()
        for _ in range(width):
            self._slots.put(None)

    @property
    def width(self) -> int:
        return self._width

    def lease(self, /, *, stop: _StopWatch) -> _WorkerLease:
        """Take one slot, spawning its worker when the slot is empty.

        Waiting for a slot is inside the caller's declared stop condition:
        every slot may be held by an unbudgeted job that never returns one, so
        a job that declared a deadline or a cancel token must be able to give
        up here rather than wait behind work it never bounded.
        """

        if self._closed.is_set():
            raise _WorkerSetClosed

        slot = self._take_slot(stop=stop)
        if slot is not None:
            if self._closed.is_set():
                self.retire_and_free(slot)
                raise _WorkerSetClosed
            return _WorkerLease(self, slot)
        try:
            worker = _spawn_worker(self._entry_point)
        except _SpawnFailure:
            self._slots.put(None)
            raise
        # Registering under the same lock close() snapshots under is what
        # keeps a worker spawned concurrently with close() from outliving it:
        # either close() sees it here, or it is retired on the spot.
        with self._lock:
            if self._closed.is_set():
                worker.terminate()
                self._slots.put(None)
                raise _WorkerSetClosed
            self._live.add(worker)
        return _WorkerLease(self, worker)

    def _take_slot(self, /, *, stop: _StopWatch) -> _Worker | None:
        """Wait for a free slot, honoring a declared stop condition."""

        return _get_watched(self._slots, stop=stop)

    def release(self, worker: _Worker, /) -> None:
        if self._closed.is_set():
            self.retire_and_free(worker)
            return
        self._slots.put(worker)

    def retire_and_free(self, worker: _Worker, /) -> None:
        """Kill one worker and free an empty slot for a later respawn."""

        self._retire(worker)
        self._slots.put(None)

    def close(self) -> None:
        """Terminate every live worker without waiting for any job to end.

        Closing never blocks on a slot: a slot held by a running job may never
        come back, because an unbudgeted job runs as long as it likes. Killing
        the worker directly ends that job loudly through the pipe-EOF path the
        executor already attributes, rather than hanging the caller.
        """

        if self._closed.is_set():
            return
        self._closed.set()
        with self._lock:
            live = tuple(self._live)
            self._live.clear()
        for worker in live:
            worker.terminate()
        self._drain_idle_workers()

    def _drain_idle_workers(self) -> None:
        """Retire idle workers still waiting in the slot queue.

        A worker returned to the queue before ``close()`` remains dequeueable
        even after it is terminated; replace those entries with empty slots.
        """

        pending: list[_Worker | None] = []
        while True:
            try:
                pending.append(self._slots.get(block=False))
            except queue.Empty:
                break
        for slot in pending:
            if slot is not None:
                slot.terminate()
            self._slots.put(None)

    def _retire(self, worker: _Worker, /) -> None:
        with self._lock:
            present = worker in self._live
            self._live.discard(worker)
        if present:
            worker.terminate()


@dataclass(slots=True)
class WorkerPoolImportableJsonExecutor:
    """Run trusted importable-JSON entry points on long-lived workers.

    Workers are fresh spawned interpreters that import the declared entry
    point once and then serve jobs over pipes. The executor provides
    parallelism, not isolation: it makes no containment claim, accepts no
    environment grant, and creates no durable run record.
    """

    entry_point: ImportableEntryPoint
    worker_count: int | None = None
    _workers: _WorkerSet = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        width = (
            resolve_pool_capacity(AutoPoolCapacity()).max_active_jobs
            if self.worker_count is None
            else self.worker_count
        )
        if width < 1:
            raise ValueError("worker_count must be positive")
        self._workers = _WorkerSet(entry_point=self.entry_point, width=width)

    @property
    def width(self) -> int:
        """Return the number of worker processes this executor runs."""

        return self._workers.width

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
        target = job.target
        if not isinstance(target, InProcessImportableJsonTarget):
            raise ExecutorFailure(
                "the worker pool executor accepts only in-process importable "
                "JSON targets",
                code=ExecutorFailureCode.WORKER_POOL_TARGET_MISMATCH,
            )
        if target.entry_point != self.entry_point:
            raise ExecutorFailure(
                "a worker pool serves only the entry point it was opened with",
                code=ExecutorFailureCode.WORKER_POOL_ENTRY_POINT_MISMATCH,
            )
        execution = _Execution(
            execution_id=ExecutionId(
                job_id=job.job_id,
                attempt_id=attempt_id_for_job(job.job_id),
            ),
            started_at=datetime.now(UTC),
            started_ns=time.monotonic_ns(),
            transport=request_transport_bytes(target.request),
        )
        if cancellation is not None and cancellation.cancelled:
            return execution.completed(outcome=CancelledOutcome())
        return self._dispatch(
            execution,
            deadline_ns=job.budgets.wall_time.limit,
            cancellation=cancellation,
        )

    def _dispatch(
        self,
        execution: _Execution,
        /,
        *,
        deadline_ns: int | None,
        cancellation: CancelToken | None,
    ) -> CompletedExecution:
        stop = _StopWatch(
            deadline_ns=None
            if deadline_ns is None
            else execution.started_ns + deadline_ns,
            cancellation=cancellation,
        )
        try:
            lease = self._workers.lease(stop=stop)
        except _SpawnFailure as failure:
            return execution.completed(
                outcome=SpawnFailedOutcome(
                    errno=failure.errno,
                    error_message=failure.error_message,
                )
            )
        except _StopRequested as requested:
            return execution.completed(outcome=requested.outcome)
        except _WorkerSetClosed:
            return execution.protocol_failed(
                "the worker pool closed while this job was starting a worker"
            )
        with lease:
            try:
                lease.worker.wait_for_ready(stop=stop)
            except _StopRequested as requested:
                lease.discard()
                return execution.completed(outcome=requested.outcome)
            except _WorkerStartupFailed as failure:
                lease.discard()
                return execution.protocol_failed(str(failure))
            return _exchange(lease, execution, stop=stop)

    def run_many(
        self,
        jobs: Iterable[ExecutionJob],
        /,
        *,
        config: ExecutionPoolConfig | None = None,
        wall_time: FiniteDurationLimit | None = None,
    ) -> Iterator[CompletedExecution]:
        """Stream a finite batch in completion order across the workers."""

        return run_batch(
            self,
            jobs,
            capacity=batch_capacity(
                config,
                default=FixedPoolCapacity(max_active_jobs=self.width),
            ),
            wall_time=wall_time,
        )

    def open_pool(
        self,
        *,
        config: ExecutionPoolConfig | None = None,
    ) -> ExecutionPool:
        """Open a scheduling pool saturating the workers by default."""

        return ExecutionPool(
            executor=self,
            config=config
            or ExecutionPoolConfig(
                capacity=FixedPoolCapacity(max_active_jobs=self.width)
            ),
        )

    def close_blocking(self) -> None:
        """Stop every worker process this executor owns."""

        self._workers.close()

    async def close(self) -> None:
        """Stop every worker process without blocking the event loop."""

        await offload_blocking(self.close_blocking)

    def __enter__(self) -> WorkerPoolImportableJsonExecutor:  # noqa: PYI034
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close_blocking()

    async def __aenter__(self) -> WorkerPoolImportableJsonExecutor:  # noqa: PYI034
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.close()


@dataclass(frozen=True, slots=True)
class _StopWatch:
    """Decide whether a running job must stop, and why.

    A job with neither a caller token nor a declared finite wall-time budget
    is unwatched: it runs to completion with no deadline of any kind.
    """

    deadline_ns: int | None
    cancellation: CancelToken | None

    @property
    def unwatched(self) -> bool:
        return self.deadline_ns is None and self.cancellation is None

    def outcome(self) -> ExecutionOutcome | None:
        if self.cancellation is not None and self.cancellation.cancelled:
            return CancelledOutcome()
        if (
            self.deadline_ns is not None
            and time.monotonic_ns() >= self.deadline_ns
        ):
            return BudgetExceededOutcome(axis=BudgetAxis.WALL_TIME)
        return None

    def poll_seconds(self) -> float:
        """Return how long to wait before re-checking the stop condition."""

        if self.deadline_ns is None:
            return _STOP_POLL_SECONDS
        remaining = (self.deadline_ns - time.monotonic_ns()) / 1e9
        return max(0.0, min(_STOP_POLL_SECONDS, remaining))


def _get_watched[T](source: queue.SimpleQueue[T], /, *, stop: _StopWatch) -> T:
    """Take the next item, giving up when a declared stop condition fires."""

    if stop.unwatched:
        return source.get()
    while True:
        try:
            return source.get(timeout=stop.poll_seconds())
        except queue.Empty:
            outcome = stop.outcome()
            if outcome is not None:
                raise _StopRequested(outcome) from None


def _exchange(
    lease: _WorkerLease,
    execution: _Execution,
    /,
    *,
    stop: _StopWatch,
) -> CompletedExecution:
    """Send one request, take one result, and attribute anything else."""

    try:
        lease.worker.send(execution.transport + WORKER_FRAME_TERMINATOR)
        frame = lease.worker.receive(stop=stop)
    except _StopRequested as requested:
        lease.discard()
        return execution.completed(outcome=requested.outcome)
    except _WorkerDied:
        death = lease.worker.death
        lease.discard()
        return execution.completed(
            outcome=death,
            attribution_detail="the worker died while running the payload",
        )
    return execution.interpret(frame)


@dataclass(frozen=True, slots=True)
class _Execution:
    """One job's identity, timing, and completion construction."""

    execution_id: ExecutionId
    started_at: datetime
    started_ns: int
    transport: bytes

    def interpret(self, frame: bytes, /) -> CompletedExecution:
        try:
            payload = _worker_frame_payload(frame)
        except ValueError as error:
            return self.protocol_failed(str(error))
        detail = payload.get(DETAIL_KEY)
        match _frame_status(payload):
            case WorkerFrameStatus.OK:
                return self.completed(
                    outcome=ExitedOutcome(exit_code=0),
                    protocol_outputs=(
                        build_identity_document(
                            schema=ENVELOPE_SCHEMA,
                            schema_version=ENVELOPE_SCHEMA_VERSION,
                            payload=payload.get(RESULT_KEY),
                        ),
                    ),
                )
            case WorkerFrameStatus.PAYLOAD_RAISED:
                return self.completed(
                    outcome=ExitedOutcome(exit_code=1),
                    attribution_detail=str(detail),
                )
            case WorkerFrameStatus.PAYLOAD_RESULT_INVALID:
                return self.completed(
                    outcome=malformed_frame_outcome(str(detail))
                )
            case WorkerFrameStatus.EXECUTOR_REJECTED:
                return self.protocol_failed(str(detail))
            case _:
                return self.protocol_failed(
                    "the worker returned an unknown result status"
                )

    def protocol_failed(self, detail: str, /) -> CompletedExecution:
        return self.completed(
            outcome=malformed_frame_outcome(detail),
            attribution_override=executor_protocol_failure_attribution,
        )

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
            record_receipt=WorkerPoolRecordReceipt(
                execution_id=self.execution_id
            ),
            outcome=outcome,
            protocol_outputs=protocol_outputs,
            started_at=self.started_at,
            started_ns=self.started_ns,
            input_bytes=len(self.transport),
            attribution_detail=attribution_detail,
            attribution_override=attribution_override,
        )


def _frame_status(payload: dict[str, Jsonable], /) -> WorkerFrameStatus | None:
    """Read the frame's status, or ``None`` for one this parent cannot name."""

    try:
        return WorkerFrameStatus(payload.get(STATUS_KEY))
    except ValueError:
        return None


def _worker_frame_payload(frame: bytes, /) -> dict[str, Jsonable]:
    try:
        document = validate_identity_document(
            json.loads(frame.decode("utf-8"))
        )
    except Exception as error:
        raise ValueError("the worker returned a malformed frame") from error
    if not is_importable_json_envelope(document):
        raise ValueError("the worker result does not use the envelope")
    payload = document.payload
    if not isinstance(payload, dict):
        raise ValueError(  # noqa: TRY004 - one frame rejection, one type
            "the worker result payload is not an object"
        )
    return payload


def _spawn_worker(entry_point: ImportableEntryPoint, /) -> _Worker:
    """Start one fresh interpreter serving one entry point over two pipes."""

    request_read, request_write = os.pipe()
    result_read, result_write = os.pipe()
    try:
        process = subprocess.Popen(
            (
                sys.executable,
                "-c",
                _worker_bootstrap(),
                entry_point.module_name,
                entry_point.attribute_name,
                str(request_read),
                str(result_write),
                # Read here, not in the child: a child that reads it after
                # losing its parent would see the reaper and never orphan.
                str(os.getpid()),
            ),
            pass_fds=(request_read, result_write),
            close_fds=True,
            process_group=0,
        )
    except OSError as error:
        for descriptor in (
            request_read,
            request_write,
            result_read,
            result_write,
        ):
            os.close(descriptor)
        raise _SpawnFailure(
            errno=error.errno or 0,
            error_message=str(error),
        ) from error
    os.close(request_read)
    os.close(result_write)
    frames: queue.SimpleQueue[bytes | None] = queue.SimpleQueue()
    # Buffered so a frame is read in blocks rather than a byte at a time; the
    # reader still consumes exactly one frame at a time and never bounds it.
    results = os.fdopen(result_read, "rb")
    reader = threading.Thread(
        target=_drain_frames,
        args=(results, frames),
        name="dr-exec-worker-pool-reader",
        daemon=True,
    )
    reader.start()
    return _Worker(
        process=process,
        requests=os.fdopen(request_write, "wb", buffering=0),
        frames=frames,
        reader=reader,
    )


def _drain_frames(
    stream: IO[bytes], frames: queue.SimpleQueue[bytes | None], /
) -> None:
    """Publish result frames as they arrive, then publish worker death."""

    try:
        chunks: list[bytes] = []
        while True:
            chunk = stream.readline()
            if not chunk:
                break
            chunks.append(chunk)
            if chunk.endswith(WORKER_FRAME_TERMINATOR):
                frames.put(b"".join(chunks))
                chunks = []
    except OSError:
        pass
    finally:
        _close_quietly(stream)
        frames.put(None)


def _close_quietly(stream: IO[bytes], /) -> None:
    try:
        stream.close()
    except OSError:
        pass


def _worker_bootstrap() -> str:
    """Give the worker the caller's import path, then run its serve loop.

    A worker must import the caller's entry-point module, so it starts from
    the caller's own ``sys.path`` rather than an isolated interpreter path.
    """

    return (
        "import sys; "
        f"sys.path[:] = {json.dumps(sys.path)}; "
        f"__import__({_WORKER_MODULE!r}, fromlist=['main']).main()"
    )


__all__ = ["WorkerPoolImportableJsonExecutor"]
