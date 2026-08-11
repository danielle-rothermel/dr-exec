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
from uuid import uuid4

from dr_serialize import (
    IdentityDocument,
    Jsonable,
    build_identity_document,
    validate_identity_document,
)

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
from dr_exec.execution.worker_pool_worker import (
    DETAIL_KEY,
    ENVELOPE_SCHEMA,
    ENVELOPE_SCHEMA_VERSION,
    FRAME_TERMINATOR,
    READY_FRAME,
    RESULT_KEY,
    STATUS_EXECUTOR_REJECTED,
    STATUS_KEY,
    STATUS_OK,
    STATUS_PAYLOAD_RAISED,
    STATUS_PAYLOAD_RESULT_INVALID,
)
from dr_exec.importable_json_entry_point import ImportableEntryPoint
from dr_exec.recording.models import (
    BudgetExceededOutcome,
    CancelledOutcome,
    CompletedExecution,
    ExecutionAttribution,
    ExecutionMeasurements,
    ExecutionOutcome,
    ExecutionResult,
    ExitedOutcome,
    ProtocolFailedOutcome,
    SignaledOutcome,
    SpawnFailedOutcome,
    WorkerPoolRecordReceipt,
)
from dr_exec.scheduling.pool import (
    AutoPoolCapacity,
    ExecutionPool,
    ExecutionPoolConfig,
    FixedPoolCapacity,
    resolve_pool_capacity,
)

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

        frame = (
            self.frames.get()
            if stop.unwatched
            else self._receive_watched(stop)
        )
        if frame is None:
            raise _WorkerDied("the worker ended without a result frame")
        return frame

    def _receive_watched(self, stop: _StopWatch, /) -> bytes | None:
        """Wait for a frame while a caller's stop condition can intervene."""

        while True:
            try:
                return self.frames.get(timeout=stop.poll_seconds())
            except queue.Empty:
                outcome = stop.outcome()
                if outcome is not None:
                    raise _StopRequested(outcome) from None

    def terminate(self) -> None:
        if self.process.poll() is None:
            self.process.kill()
        self.process.wait()
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
            self._pool.release_empty()
            return
        self._pool.release(self.worker)

    def discard(self) -> None:
        """Kill this worker so the pool respawns into its freed slot."""

        self._discarded = True
        self._pool.discard(self.worker)


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

        slot = self._take_slot(stop=stop)
        if slot is not None:
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

        if stop.unwatched:
            return self._slots.get()
        while True:
            try:
                return self._slots.get(timeout=stop.poll_seconds())
            except queue.Empty:
                outcome = stop.outcome()
                if outcome is not None:
                    raise _StopRequested(outcome) from None

    def release(self, worker: _Worker, /) -> None:
        if self._closed.is_set():
            self._retire(worker)
            self._slots.put(None)
            return
        self._slots.put(worker)

    def release_empty(self) -> None:
        self._slots.put(None)

    def discard(self, worker: _Worker, /) -> None:
        """Kill one worker and forget it, freeing its slot for a respawn."""

        self._retire(worker)

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
                "the worker pool executor accepts only in-process importable "
                "JSON targets"
            )
        if target.entry_point != self.entry_point:
            raise ExecutorFailure(
                "a worker pool serves only the entry point it was opened with"
            )
        execution = _Execution(
            execution_id=ExecutionId(
                job_id=job.job_id,
                attempt_id=AttemptId(uuid4()),
            ),
            started_at=datetime.now(UTC),
            started_ns=time.monotonic_ns(),
            transport=request_transport_bytes(target.request),
        )
        if cancellation is not None and cancellation.cancelled:
            return execution.completed(outcome=CancelledOutcome())
        return self._dispatch(
            execution,
            deadline_ns=finite_duration_ns(job.budgets.wall_time),
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
    ) -> Iterator[CompletedExecution]:
        """Stream a finite batch in completion order across the workers."""

        return _run_batch(self, jobs, capacity=self._pool_capacity(config))

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

    def close(self) -> None:
        """Stop every worker process this executor owns."""

        self._workers.close()

    def __enter__(self) -> WorkerPoolImportableJsonExecutor:  # noqa: PYI034
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    def _pool_capacity(self, config: ExecutionPoolConfig | None, /) -> int:
        if config is None:
            return self.width
        return resolve_pool_capacity(config.capacity).max_active_jobs


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


def _exchange(
    lease: _WorkerLease,
    execution: _Execution,
    /,
    *,
    stop: _StopWatch,
) -> CompletedExecution:
    """Send one request, take one result, and attribute anything else."""

    try:
        lease.worker.send(execution.transport + FRAME_TERMINATOR)
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
        status = payload.get(STATUS_KEY)
        detail = payload.get(DETAIL_KEY)
        match status:
            case _ if status == STATUS_OK:
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
            case _ if status == STATUS_PAYLOAD_RAISED:
                return self.completed(
                    outcome=ExitedOutcome(exit_code=1),
                    attribution_detail=str(detail),
                )
            case _ if status == STATUS_PAYLOAD_RESULT_INVALID:
                return self.completed(
                    outcome=ProtocolFailedOutcome(
                        failure_code=ProtocolFailureCode.MALFORMED_FRAME,
                        failure_detail=str(detail),
                        accepted_output_count=0,
                    )
                )
            case _ if status == STATUS_EXECUTOR_REJECTED:
                return self.protocol_failed(str(detail))
            case _:
                return self.protocol_failed(
                    "the worker returned an unknown result status"
                )

    def protocol_failed(self, detail: str, /) -> CompletedExecution:
        return self.completed(
            outcome=ProtocolFailedOutcome(
                failure_code=ProtocolFailureCode.MALFORMED_FRAME,
                failure_detail=detail,
                accepted_output_count=0,
            ),
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
        result = ExecutionResult(
            execution_id=self.execution_id,
            outcome=outcome,
            attribution=attribution,
            protocol_outputs=protocol_outputs,
            payload_outputs=empty_payload_outputs(),
            measurements=ExecutionMeasurements(
                started_at=self.started_at,
                finished_at=datetime.now(UTC),
                duration_ns=time.monotonic_ns() - self.started_ns,
                teardown_duration_ns=0,
                input_bytes=len(self.transport),
                protocol_bytes_received=0,
            ),
        )
        return CompletedExecution(
            result=result,
            record_receipt=WorkerPoolRecordReceipt(
                execution_id=self.execution_id
            ),
        )


def _worker_frame_payload(frame: bytes, /) -> dict[str, Jsonable]:
    try:
        document = validate_identity_document(
            json.loads(frame.decode("utf-8"))
        )
    except Exception as error:
        raise ValueError("the worker returned a malformed frame") from error
    if (
        document.schema != ENVELOPE_SCHEMA
        or document.schema_version != ENVELOPE_SCHEMA_VERSION
    ):
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
            if chunk.endswith(FRAME_TERMINATOR):
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
