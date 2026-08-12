from __future__ import annotations

import errno as errno_module
import os
import selectors
import shutil
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from threading import Event, Thread
from typing import Final

from dr_serialize import IdentityDocument, Sha256Digest

from dr_exec.capabilities.protocols import RunStore, Runtime
from dr_exec.core.cancel import CancelToken
from dr_exec.core.errors import DeclarationError, ExecutorFailure
from dr_exec.core.kinds import (
    BudgetAxis,
    OutputOverflowPolicy,
    RecordState,
)
from dr_exec.core.names import ExecutionId
from dr_exec.declarations.models import (
    ExecutionJob,
    ExecutorSelfBudgets,
    FiniteDurationLimit,
    FiniteOutput,
    InProcessImportableJsonTarget,
    TrustedCommandTarget,
    TrustedPythonTarget,
    UntrustedCommandTarget,
    UntrustedPythonTarget,
)
from dr_exec.declarations.validation import (
    granted_environment,
    validate_command_resolvability,
    validate_input_budget,
)
from dr_exec.execution.outcomes import attribute_outcome
from dr_exec.execution.retention import PayloadRetention, StreamRetention
from dr_exec.execution.spawn import (
    ESCALATION_SIGNAL,
    PAYLOAD_PROTOCOL_DESCRIPTOR,
    PAYLOAD_STDERR_DESCRIPTOR,
    PAYLOAD_STDIN_DESCRIPTOR,
    PAYLOAD_STDOUT_DESCRIPTOR,
    SETUP_STAGE_EXEC,
    SETUP_STAGE_SESSION,
    TERMINATION_SIGNAL,
    SetupFailure,
    launch_bootstrap,
    parse_setup_status,
    signal_process_group,
)
from dr_exec.recording.identity import (
    _build_env_grant_record,
    _build_executor_config_identity,
    _build_executor_identity,
    _canonical_declaration_digest,
)
from dr_exec.recording.models import (
    BudgetExceededOutcome,
    CancelledOutcome,
    CompletedExecution,
    DegradedRecordReceipt,
    ExecutionMeasurements,
    ExecutionOutcome,
    ExecutionResult,
    ExecutionTargetRecord,
    ExitedOutcome,
    PayloadOutputs,
    PreparedRecord,
    ProcessRecord,
    ProtocolFailedOutcome,
    RealRecordReceipt,
    RecordingFailure,
    RetainedPayloadStream,
    RunDeclaration,
    RunRecordHeader,
    SignaledOutcome,
    SpawnAbsentOutcome,
    SpawnFailedOutcome,
    TrustedCommandTargetRecord,
    TrustedPythonTargetRecord,
    UntrustedCommandTargetRecord,
    UntrustedPythonTargetRecord,
)
from dr_exec.recording.provenance import _executor_source_snapshot
from dr_exec.recording.references import attempt_id_for_job
from dr_exec.recording.store import FinalizableRun, PreparedRun
from dr_exec.runtime.protocol import ProtocolStreamResult, read_protocol_stream

SUPPORTED_PLATFORM: Final = "darwin"

SCRATCH_DIRECTORY_PREFIX: Final = "dr-exec-run-"

_DRAIN_CHUNK_BYTES: Final = 65536
_COOPERATIVE_WAKE_SECONDS: Final = 0.05


@dataclass(frozen=True, slots=True)
class _ResolvedTarget:
    executable: str
    argv: tuple[str, ...]
    stdin_bytes: bytes
    record: ExecutionTargetRecord
    request_id_sha256: Sha256Digest | None
    wants_protocol: bool


def _now() -> datetime:
    return datetime.now(UTC)


def _finite_ns(budget: object, /) -> int | None:
    return budget.max_ns if isinstance(budget, FiniteDurationLimit) else None


def _validate_platform() -> None:
    if sys.platform != SUPPORTED_PLATFORM:
        raise DeclarationError(
            f"dr-exec v1 executes only on {SUPPORTED_PLATFORM}"
        )


def _resolve_executable(
    argv: tuple[str, ...],
    environment: dict[str, str],
    /,
) -> str:
    name = argv[0]
    if Path(name).is_absolute():
        return name
    validate_command_resolvability(argv, environment)
    resolved = shutil.which(name, path=environment["PATH"])
    return resolved if resolved is not None else name


def _target_of(job: ExecutionJob, runtime: Runtime, /) -> _ResolvedTarget:
    digest = _canonical_declaration_digest(job.target)
    match job.target:
        case TrustedCommandTarget():
            return _ResolvedTarget(
                executable=job.target.argv[0],
                argv=job.target.argv,
                stdin_bytes=job.target.stdin,
                record=TrustedCommandTargetRecord(
                    canonical_declaration_sha256=digest
                ),
                request_id_sha256=None,
                wants_protocol=False,
            )
        case UntrustedCommandTarget():
            return _ResolvedTarget(
                executable=job.target.argv[0],
                argv=job.target.argv,
                stdin_bytes=job.target.stdin,
                record=UntrustedCommandTargetRecord(
                    canonical_declaration_sha256=digest,
                    containment_profile=job.target.containment_profile,
                ),
                request_id_sha256=None,
                wants_protocol=False,
            )
        case TrustedPythonTarget() | UntrustedPythonTarget():
            prepared = runtime.prepare(job.target)
            record = (
                TrustedPythonTargetRecord(
                    canonical_declaration_sha256=digest,
                    request_id_sha256=prepared.request_id_sha256,
                    runtime=prepared.runtime_record,
                )
                if isinstance(job.target, TrustedPythonTarget)
                else UntrustedPythonTargetRecord(
                    canonical_declaration_sha256=digest,
                    request_id_sha256=prepared.request_id_sha256,
                    containment_profile=job.target.containment_profile,
                    runtime=prepared.runtime_record,
                )
            )
            return _ResolvedTarget(
                executable=prepared.argv[0],
                argv=prepared.argv,
                stdin_bytes=prepared.request_bytes,
                record=record,
                request_id_sha256=prepared.request_id_sha256,
                wants_protocol=True,
            )
        case InProcessImportableJsonTarget():
            raise ExecutorFailure(
                "the process executor cannot run in-process importable JSON "
                "targets"
            )


@contextmanager
def _scratch_workspace() -> Iterator[Path]:
    """Use a fresh scratch directory whose cleanup is best-effort and unreported in v1."""

    directory = Path(tempfile.mkdtemp(prefix=SCRATCH_DIRECTORY_PREFIX))
    try:
        yield directory
    finally:
        shutil.rmtree(directory, ignore_errors=True)


@dataclass(slots=True)
class _DrainState:
    retention: PayloadRetention
    overflow: Event = field(default_factory=Event)
    protocol_result: ProtocolStreamResult | None = None


def _feed_stdin(
    descriptor: int, payload: bytes, release_descriptor: int, /
) -> None:
    selector = selectors.DefaultSelector()
    try:
        os.set_blocking(descriptor, False)
        selector.register(descriptor, selectors.EVENT_WRITE)
        selector.register(release_descriptor, selectors.EVENT_READ)
        _feed(selector, descriptor, payload, release_descriptor)
    except OSError:
        pass
    finally:
        selector.close()
        with suppress(OSError):
            os.close(descriptor)


def _feed(
    selector: selectors.BaseSelector,
    descriptor: int,
    payload: bytes,
    release_descriptor: int,
    /,
) -> None:
    offset = 0
    while offset < len(payload):
        ready = {int(key.fd) for key, _ in selector.select()}
        if release_descriptor in ready:
            return
        if descriptor in ready:
            with suppress(BlockingIOError):
                offset += os.write(descriptor, payload[offset:])


@dataclass(slots=True)
class _OutputPump:
    """Drain all child outputs concurrently and remain release-interruptible."""

    state: _DrainState
    stdout_descriptor: int
    stderr_descriptor: int
    protocol_descriptor: int | None
    protocol_forward: int | None
    release_descriptor: int

    def run(self) -> None:
        selector = selectors.DefaultSelector()
        live = {
            self.stdout_descriptor: self.state.retention.stdout,
            self.stderr_descriptor: self.state.retention.stderr,
        }
        try:
            for descriptor in (
                self.stdout_descriptor,
                self.stderr_descriptor,
                self.protocol_descriptor,
                self.release_descriptor,
            ):
                if descriptor is not None:
                    os.set_blocking(descriptor, False)
                    selector.register(descriptor, selectors.EVENT_READ)
            self._pump(selector, live)
        finally:
            selector.close()
            if self.protocol_forward is not None:
                with suppress(OSError):
                    os.close(self.protocol_forward)

    def _pump(
        self,
        selector: selectors.BaseSelector,
        live: dict[int, StreamRetention],
        /,
    ) -> None:
        remaining = set(live) | (
            set()
            if self.protocol_descriptor is None
            else {self.protocol_descriptor}
        )
        while remaining:
            for key, _ in selector.select():
                descriptor = int(key.fd)
                if descriptor == self.release_descriptor:
                    return
                chunk = _read_available(descriptor)
                if chunk is None:
                    selector.unregister(descriptor)
                    remaining.discard(descriptor)
                    continue
                retained = live.get(descriptor)
                if retained is not None:
                    retained.offer(chunk)
                    if self.state.retention.overflowed:
                        self.state.overflow.set()
                elif self.protocol_forward is not None:
                    with suppress(OSError):
                        os.write(self.protocol_forward, chunk)


def _read_available(descriptor: int, /) -> bytes | None:
    try:
        chunk = os.read(descriptor, _DRAIN_CHUNK_BYTES)
    except BlockingIOError:
        return b""
    except OSError:
        return None
    return chunk if chunk else None


def _read_protocol(
    descriptor: int,
    state: _DrainState,
    request_id_sha256: Sha256Digest,
    self_budgets: ExecutorSelfBudgets,
    /,
) -> None:
    with os.fdopen(descriptor, "rb") as reader:
        state.protocol_result = read_protocol_stream(
            reader,
            request_id_sha256=request_id_sha256,
            self_budgets=self_budgets,
        )


def _read_setup_status(descriptor: int, startup_ns: int | None, /) -> bytes:
    deadline = None if startup_ns is None else time.monotonic_ns() + startup_ns
    collected = bytearray()
    selector = selectors.DefaultSelector()
    try:
        selector.register(descriptor, selectors.EVENT_READ)
        while True:
            if deadline is not None:
                remaining = (deadline - time.monotonic_ns()) / 1e9
                if remaining <= 0 or not selector.select(remaining):
                    raise ExecutorFailure(
                        "the execution bootstrap did not reach the payload "
                        "within the startup budget"
                    )
            try:
                chunk = os.read(descriptor, _DRAIN_CHUNK_BYTES)
            except OSError:
                return bytes(collected)
            if not chunk:
                return bytes(collected)
            collected.extend(chunk)
    finally:
        selector.close()


def _close_descriptors(descriptors: Iterable[int], /) -> None:
    for descriptor in descriptors:
        with suppress(OSError):
            os.close(descriptor)


@dataclass(slots=True)
class _WorkerFailure:
    error: BaseException | None = None


@dataclass(slots=True)
class _TransportWorker:
    name: str
    thread: Thread
    failure: _WorkerFailure

    @property
    def failed(self) -> bool:
        return self.failure.error is not None

    def raise_if_failed(self) -> None:
        error = self.failure.error
        if error is None:
            return
        raise ExecutorFailure(
            f"the {self.name} transport worker failed"
        ) from error


def _started_thread(
    target: Callable[[], None], name: str, /
) -> _TransportWorker:
    failure = _WorkerFailure()

    def observe_failure() -> None:
        try:
            target()
        except BaseException as error:  # noqa: BLE001 - thread boundary
            failure.error = error

    thread = Thread(target=observe_failure, name=name, daemon=True)
    thread.start()
    return _TransportWorker(
        name=name.removeprefix("dr-exec-"),
        thread=thread,
        failure=failure,
    )


@dataclass(frozen=True, slots=True)
class _StopReason:
    axis: BudgetAxis | None
    cancelled: bool


@dataclass(slots=True)
class _AttemptObservation:
    prepared: PreparedRun
    setup_failure: SetupFailure | None = None
    leads_group: bool = True
    state: _DrainState | None = None
    stop: _StopReason | None = None
    running: FinalizableRun | None = None
    recording_failures: tuple[RecordingFailure, ...] = ()

    def reached_payload(self) -> _DrainState | None:
        return None if self.setup_failure is not None else self.state

    def latest_run(self) -> FinalizableRun:
        return self.prepared if self.running is None else self.running


def _await_child(
    process: subprocess.Popen[bytes],
    state: _DrainState,
    /,
    *,
    deadline_ns: int | None,
    fail_on_overflow: bool,
    cancellation: CancelToken | None,
    transport_failed: Callable[[], bool],
) -> _StopReason | None:
    while True:
        if process.poll() is not None:
            return None
        if transport_failed():
            # Tear down before surfacing the worker failure; partial I/O is
            # not a trustworthy execution outcome.
            return None
        if fail_on_overflow and state.overflow.is_set():
            return _StopReason(axis=BudgetAxis.PAYLOAD_OUTPUT, cancelled=False)
        if cancellation is not None and cancellation.cancelled:
            return _StopReason(axis=None, cancelled=True)
        # Every process attempt runs payload transport workers that may fail
        # asynchronously while the child continues; cancellation and overflow
        # need the same bounded wakeups when no wall-time budget applies.
        needs_cooperative_wake = True
        timeout: float | None = None
        if deadline_ns is not None:
            remaining_ns = deadline_ns - time.monotonic_ns()
            if remaining_ns <= 0:
                return _StopReason(axis=BudgetAxis.WALL_TIME, cancelled=False)
            timeout = remaining_ns / 1e9
            if needs_cooperative_wake:
                timeout = min(timeout, _COOPERATIVE_WAKE_SECONDS)
        elif needs_cooperative_wake:
            # Machinery stop conditions still need wakeups when no wall-time
            # budget bounds the wait; blocking until child exit would hide them.
            timeout = _COOPERATIVE_WAKE_SECONDS
        with suppress(subprocess.TimeoutExpired):
            process.wait(timeout=timeout)


def _tear_down(
    process: subprocess.Popen[bytes],
    self_budgets: ExecutorSelfBudgets,
    /,
    *,
    leads_group: bool = True,
) -> int:
    """Reap the child and signal its original process group.

    Group teardown reaches ordinary descendants but not descendants that
    escape by creating another session with ``setsid``.
    """

    started_ns = time.monotonic_ns()
    if not leads_group:
        # No child-owned group exists on this setup-failure path.
        with suppress(OSError):
            process.send_signal(TERMINATION_SIGNAL)
        if not _reaped_within(
            process, _finite_ns(self_budgets.termination_time)
        ):
            with suppress(OSError):
                process.send_signal(ESCALATION_SIGNAL)
        process.wait()
        return time.monotonic_ns() - started_ns
    signal_process_group(process.pid, TERMINATION_SIGNAL)
    with suppress(OSError):
        process.send_signal(TERMINATION_SIGNAL)
    if not _reaped_within(process, _finite_ns(self_budgets.termination_time)):
        signal_process_group(process.pid, ESCALATION_SIGNAL)
        with suppress(OSError):
            process.send_signal(ESCALATION_SIGNAL)
    # Reap before probing so the leader's zombie cannot keep the group alive.
    process.wait()
    if _group_survives(process.pid):
        signal_process_group(process.pid, ESCALATION_SIGNAL)
    return time.monotonic_ns() - started_ns


def _group_survives(pid: int, /) -> bool:
    return signal_process_group(pid, 0)


def _reaped_within(
    process: subprocess.Popen[bytes],
    termination_ns: int | None,
    /,
) -> bool:
    if termination_ns is None:
        process.wait()
        return True
    try:
        process.wait(timeout=termination_ns / 1_000_000_000)
    except subprocess.TimeoutExpired:
        return False
    return True


@dataclass(slots=True)
class _Transports:
    stdin_write: int | None
    stdout_read: int
    stderr_read: int
    protocol_read: int | None
    status_read: int
    release_read: int
    release_write: int
    protocol_forward_read: int | None
    protocol_forward_write: int | None
    threads: tuple[_TransportWorker, ...] = ()

    def take_stdin(self) -> int:
        descriptor = self.stdin_write
        if descriptor is None:  # pragma: no cover - one call per attempt
            raise ExecutorFailure("the stdin transport was already taken")
        self.stdin_write = None
        return descriptor

    def take_protocol_reader(self) -> int:
        descriptor = self.protocol_forward_read
        if descriptor is None:  # pragma: no cover - one call per attempt
            raise ExecutorFailure("the protocol transport was already taken")
        self.protocol_forward_read = None
        return descriptor

    def take_protocol_forward_write(self) -> int | None:
        descriptor = self.protocol_forward_write
        self.protocol_forward_write = None
        return descriptor

    def adopt(self, thread: _TransportWorker, /) -> None:
        self.threads = (*self.threads, thread)

    def failed(self) -> bool:
        return any(worker.failed for worker in self.threads)

    def release(self) -> None:
        with suppress(OSError):
            os.write(self.release_write, b"\0")

    def join(self, self_budgets: ExecutorSelfBudgets, /) -> None:
        join_ns = _finite_ns(self_budgets.join_time)
        deadline = None if join_ns is None else time.monotonic_ns() + join_ns
        for worker in self.threads:
            remaining = (
                None
                if deadline is None
                else max(0.0, (deadline - time.monotonic_ns()) / 1e9)
            )
            worker.thread.join(remaining)
        if any(worker.thread.is_alive() for worker in self.threads):
            raise ExecutorFailure(
                "payload transports did not reach EOF within the join budget"
            )
        for worker in self.threads:
            worker.raise_if_failed()

    def close(self) -> None:
        self.release()
        for worker in self.threads:
            worker.thread.join()
        _close_descriptors(
            descriptor
            for descriptor in (
                self.stdin_write,
                self.stdout_read,
                self.stderr_read,
                self.protocol_read,
                self.status_read,
                self.release_read,
                self.release_write,
                self.protocol_forward_read,
                self.protocol_forward_write,
            )
            if descriptor is not None
        )


def _spawn_outcome(
    failure: SetupFailure,
    executable: str,
    /,
) -> ExecutionOutcome:
    if (
        failure.stage == SETUP_STAGE_EXEC
        and failure.errno == errno_module.ENOENT
    ):
        return SpawnAbsentOutcome(executable=executable)
    return SpawnFailedOutcome(
        errno=failure.errno if failure.errno is not None else 0,
        error_message=failure.stage,
    )


def _exit_outcome(returncode: int, /) -> ExecutionOutcome:
    if returncode < 0:
        return SignaledOutcome(signal_number=-returncode)
    return ExitedOutcome(exit_code=returncode)


def _empty_payload_outputs() -> PayloadOutputs:
    empty = RetainedPayloadStream(
        head=b"",
        tail=b"",
        produced_bytes=0,
        dropped_bytes=0,
    )
    return PayloadOutputs(stdout=empty, stderr=empty)


def _degraded_from(
    run: FinalizableRun,
    store: RunStore,
    result: ExecutionResult,
    /,
    *,
    prior_failures: tuple[RecordingFailure, ...] = (),
) -> RealRecordReceipt:
    try:
        receipt = store.finalize(run, result)
    except ExecutorFailure:
        return _degraded_receipt(run, "finalize", prior_failures)
    if not prior_failures:
        return receipt
    return DegradedRecordReceipt(
        execution_id=receipt.execution_id,
        reference=receipt.reference,
        latest_state=receipt.latest_state,
        failures=(
            *prior_failures,
            *(
                receipt.failures
                if isinstance(receipt, DegradedRecordReceipt)
                else ()
            ),
        ),
    )


def _degraded_receipt(
    run: FinalizableRun,
    operation: str,
    prior_failures: tuple[RecordingFailure, ...] = (),
    /,
) -> RealRecordReceipt:
    return DegradedRecordReceipt(
        execution_id=run.execution_id,
        reference=run.reference,
        latest_state=RecordState.PREPARED
        if isinstance(run, PreparedRun)
        else RecordState.RUNNING,
        failures=(
            *prior_failures,
            RecordingFailure(
                operation=operation,
                errno=None,
                detail=ExecutorFailure.__name__,
            ),
        ),
    )


@dataclass(frozen=True, slots=True)
class _EngineCall:
    runtime: Runtime
    run_store: RunStore
    self_budgets: ExecutorSelfBudgets

    def run(
        self,
        job: ExecutionJob,
        /,
        *,
        cancellation: CancelToken | None,
    ) -> CompletedExecution:
        if isinstance(job.target, InProcessImportableJsonTarget):
            raise ExecutorFailure(
                "the process executor cannot run in-process importable JSON "
                "targets"
            )
        _validate_platform()
        target = _target_of(job, self.runtime)
        validate_input_budget(job, target.stdin_bytes)
        environment = granted_environment(job.env)
        executable = _resolve_executable(target.argv, environment)

        execution_id = ExecutionId(
            job_id=job.job_id,
            attempt_id=attempt_id_for_job(job.job_id),
        )
        prepared = self.run_store.prepare(
            self._prepared_record(job, target, execution_id)
        )
        if cancellation is not None and cancellation.cancelled:
            return self._finalize_pre_spawn(
                prepared,
                CancelledOutcome(),
            )
        with _scratch_workspace() as scratch:
            return self._run_spawned(
                job,
                target,
                prepared,
                executable=executable,
                environment=environment,
                scratch=scratch,
                cancellation=cancellation,
            )

    def _prepared_record(
        self,
        job: ExecutionJob,
        target: _ResolvedTarget,
        execution_id: ExecutionId,
        /,
    ) -> PreparedRecord:
        return PreparedRecord(
            header=RunRecordHeader(
                executor_identity=_build_executor_identity(
                    _executor_source_snapshot()
                ),
                executor_config_identity=_build_executor_config_identity(
                    self.self_budgets
                ),
                prepared_at=_now(),
            ),
            declaration=RunDeclaration(
                execution_id=execution_id,
                target=target.record,
                env=_build_env_grant_record(job.env),
                budgets=job.budgets,
            ),
        )

    def _finalize_pre_spawn(
        self,
        prepared: PreparedRun,
        outcome: ExecutionOutcome,
        /,
    ) -> CompletedExecution:
        moment = _now()
        result = ExecutionResult(
            execution_id=prepared.execution_id,
            outcome=outcome,
            attribution=attribute_outcome(outcome),
            protocol_outputs=(),
            payload_outputs=_empty_payload_outputs(),
            measurements=ExecutionMeasurements(
                started_at=moment,
                finished_at=moment,
                duration_ns=0,
                teardown_duration_ns=0,
                input_bytes=0,
                protocol_bytes_received=0,
            ),
        )
        return CompletedExecution(
            result=result,
            record_receipt=_degraded_from(prepared, self.run_store, result),
        )

    def _run_spawned(
        self,
        job: ExecutionJob,
        target: _ResolvedTarget,
        prepared: PreparedRun,
        /,
        *,
        executable: str,
        environment: dict[str, str],
        scratch: Path,
        cancellation: CancelToken | None,
    ) -> CompletedExecution:
        stdin_read, stdin_write = os.pipe()
        stdout_read, stdout_write = os.pipe()
        stderr_read, stderr_write = os.pipe()
        protocol_read, protocol_write = (
            os.pipe() if target.wants_protocol else (None, None)
        )
        forward_read, forward_write = (
            os.pipe() if target.wants_protocol else (None, None)
        )
        status_read, status_write = os.pipe()
        release_read, release_write = os.pipe()
        descriptor_map: list[tuple[int, int]] = [
            (stdin_read, PAYLOAD_STDIN_DESCRIPTOR),
            (stdout_write, PAYLOAD_STDOUT_DESCRIPTOR),
            (stderr_write, PAYLOAD_STDERR_DESCRIPTOR),
        ]
        if protocol_write is not None:
            descriptor_map.append(
                (protocol_write, PAYLOAD_PROTOCOL_DESCRIPTOR)
            )
        child_ends = [stdin_read, stdout_write, stderr_write, status_write]
        if protocol_write is not None:
            child_ends.append(protocol_write)
        transports = _Transports(
            stdin_write=stdin_write,
            stdout_read=stdout_read,
            stderr_read=stderr_read,
            protocol_read=protocol_read,
            status_read=status_read,
            release_read=release_read,
            release_write=release_write,
            protocol_forward_read=forward_read,
            protocol_forward_write=forward_write,
        )
        started_at = _now()
        started_ns = time.monotonic_ns()
        try:
            process = launch_bootstrap(
                executable=executable,
                argv=target.argv,
                environment=environment,
                scratch_directory=scratch.as_posix(),
                descriptor_map=tuple(descriptor_map),
                status_write=status_write,
            )
        except OSError as error:
            _close_descriptors(child_ends)
            transports.close()
            raise ExecutorFailure(
                "could not start the execution bootstrap"
            ) from error
        # Parent copies of child ends would suppress EOF.
        _close_descriptors(child_ends)
        try:
            return self._carry_attempt(
                job,
                target,
                prepared,
                process=process,
                transports=transports,
                started_at=started_at,
                started_ns=started_ns,
                cancellation=cancellation,
            )
        finally:
            transports.close()

    def _carry_attempt(
        self,
        job: ExecutionJob,
        target: _ResolvedTarget,
        prepared: PreparedRun,
        /,
        *,
        process: subprocess.Popen[bytes],
        transports: _Transports,
        started_at: datetime,
        started_ns: int,
        cancellation: CancelToken | None,
    ) -> CompletedExecution:
        observation = _AttemptObservation(prepared=prepared)
        try:
            self._observe_attempt(
                job,
                target,
                observation,
                process=process,
                transports=transports,
                started_at=started_at,
                started_ns=started_ns,
                cancellation=cancellation,
            )
        finally:
            teardown_ns = _tear_down(
                process,
                self.self_budgets,
                leads_group=observation.leads_group,
            )
        transports.join(self.self_budgets)
        setup_failure = observation.setup_failure
        if setup_failure is not None:
            return self._complete(
                prepared,
                outcome=_spawn_outcome(setup_failure, target.executable),
                protocol_outputs=(),
                payload_outputs=_empty_payload_outputs(),
                started_at=started_at,
                started_ns=started_ns,
                teardown_duration_ns=teardown_ns,
                input_bytes=0,
                protocol_bytes_received=0,
            )
        state = observation.reached_payload()
        if state is None:  # pragma: no cover - a raise already left the call
            raise ExecutorFailure("the attempt produced no drain state")
        protocol = state.protocol_result
        return self._complete(
            observation.latest_run(),
            outcome=self._outcome_of(
                process, state, observation.stop, protocol
            ),
            protocol_outputs=() if protocol is None else protocol.outputs,
            payload_outputs=state.retention.snapshot(),
            started_at=started_at,
            started_ns=started_ns,
            teardown_duration_ns=teardown_ns,
            input_bytes=len(target.stdin_bytes),
            protocol_bytes_received=(
                0 if protocol is None else protocol.bytes_received
            ),
            recording_failures=observation.recording_failures,
        )

    def _observe_attempt(
        self,
        job: ExecutionJob,
        target: _ResolvedTarget,
        observation: _AttemptObservation,
        /,
        *,
        process: subprocess.Popen[bytes],
        transports: _Transports,
        started_at: datetime,
        started_ns: int,
        cancellation: CancelToken | None,
    ) -> None:
        setup_failure = parse_setup_status(
            _read_setup_status(
                transports.status_read,
                _finite_ns(self.self_budgets.startup_time),
            )
        )
        if setup_failure is not None:
            observation.setup_failure = setup_failure
            # A failed ``setsid`` leaves no child-owned group to signal.
            observation.leads_group = (
                setup_failure.stage != SETUP_STAGE_SESSION
            )
            return
        state = _DrainState(
            retention=PayloadRetention.for_budget(job.budgets.payload_output)
        )
        observation.state = state
        # Start drains before durable publication can stall a live payload.
        self._start_transports(target, transports, state)
        self._mark_running(observation, process, started_at)
        observation.stop = _await_child(
            process,
            state,
            deadline_ns=self._deadline_ns(job, started_ns),
            fail_on_overflow=_fails_on_overflow(job),
            cancellation=cancellation,
            transport_failed=transports.failed,
        )

    def _mark_running(
        self,
        observation: _AttemptObservation,
        process: subprocess.Popen[bytes],
        started_at: datetime,
        /,
    ) -> None:
        try:
            observation.running = self.run_store.mark_running(
                observation.prepared,
                ProcessRecord(pid=process.pid, started_at=started_at),
            )
        except ExecutorFailure as error:
            observation.recording_failures = (
                *observation.recording_failures,
                RecordingFailure(
                    operation="mark_running",
                    errno=None,
                    detail=type(error).__name__,
                ),
            )

    def _start_transports(
        self,
        target: _ResolvedTarget,
        transports: _Transports,
        state: _DrainState,
        /,
    ) -> None:
        payload = target.stdin_bytes
        self._adopt_started(
            transports,
            lambda descriptor: _feed_stdin(
                descriptor, payload, transports.release_read
            ),
            "dr-exec-stdin",
            take=transports.take_stdin,
        )
        self._adopt_started(
            transports,
            lambda descriptor: _OutputPump(
                state=state,
                stdout_descriptor=transports.stdout_read,
                stderr_descriptor=transports.stderr_read,
                protocol_descriptor=transports.protocol_read,
                protocol_forward=descriptor,
                release_descriptor=transports.release_read,
            ).run(),
            "dr-exec-output",
            take=transports.take_protocol_forward_write,
        )
        digest = target.request_id_sha256
        if transports.protocol_forward_read is not None and digest is not None:
            budgets = self.self_budgets
            self._adopt_started(
                transports,
                lambda descriptor: _read_protocol(
                    descriptor, state, digest, budgets
                ),
                "dr-exec-protocol",
                take=transports.take_protocol_reader,
            )

    @staticmethod
    def _adopt_started[DescriptorT: (int, int | None)](
        transports: _Transports,
        body: Callable[[DescriptorT], None],
        name: str,
        /,
        *,
        take: Callable[[], DescriptorT],
    ) -> None:
        descriptor = take()
        try:
            transports.adopt(_started_thread(lambda: body(descriptor), name))
        except RuntimeError:
            if descriptor is not None:
                _close_descriptors((descriptor,))
            raise

    def _deadline_ns(
        self, job: ExecutionJob, started_ns: int, /
    ) -> int | None:
        wall_time_ns = _finite_ns(job.budgets.wall_time)
        return None if wall_time_ns is None else started_ns + wall_time_ns

    def _outcome_of(
        self,
        process: subprocess.Popen[bytes],
        state: _DrainState,
        stop: _StopReason | None,
        protocol: ProtocolStreamResult | None,
        /,
    ) -> ExecutionOutcome:
        if state.retention.overflowed:
            return BudgetExceededOutcome(axis=BudgetAxis.PAYLOAD_OUTPUT)
        if stop is not None and stop.axis is BudgetAxis.WALL_TIME:
            return BudgetExceededOutcome(axis=BudgetAxis.WALL_TIME)
        if stop is not None and stop.cancelled:
            return CancelledOutcome()
        if protocol is not None and protocol.failure is not None:
            return ProtocolFailedOutcome(
                failure_code=protocol.failure.code,
                failure_detail=protocol.failure.detail,
                accepted_output_count=len(protocol.outputs),
            )
        return _exit_outcome(process.returncode)

    def _complete(
        self,
        run: FinalizableRun,
        /,
        *,
        outcome: ExecutionOutcome,
        protocol_outputs: tuple[IdentityDocument, ...],
        payload_outputs: PayloadOutputs,
        started_at: datetime,
        started_ns: int,
        teardown_duration_ns: int,
        input_bytes: int,
        protocol_bytes_received: int,
        recording_failures: tuple[RecordingFailure, ...] = (),
    ) -> CompletedExecution:
        result = ExecutionResult(
            execution_id=run.execution_id,
            outcome=outcome,
            attribution=attribute_outcome(outcome),
            protocol_outputs=protocol_outputs,
            payload_outputs=payload_outputs,
            measurements=ExecutionMeasurements(
                started_at=started_at,
                finished_at=_now(),
                duration_ns=time.monotonic_ns() - started_ns,
                teardown_duration_ns=teardown_duration_ns,
                input_bytes=input_bytes,
                protocol_bytes_received=protocol_bytes_received,
            ),
        )
        return CompletedExecution(
            result=result,
            record_receipt=_degraded_from(
                run,
                self.run_store,
                result,
                prior_failures=recording_failures,
            ),
        )


def _fails_on_overflow(job: ExecutionJob, /) -> bool:
    budget = job.budgets.payload_output
    return (
        isinstance(budget, FiniteOutput)
        and budget.overflow_policy is OutputOverflowPolicy.FAIL
    )


def run_execution(
    job: ExecutionJob,
    /,
    *,
    runtime: Runtime,
    run_store: RunStore,
    self_budgets: ExecutorSelfBudgets,
    cancellation: CancelToken | None = None,
) -> CompletedExecution:
    return _EngineCall(
        runtime=runtime,
        run_store=run_store,
        self_budgets=self_budgets,
    ).run(job, cancellation=cancellation)


__all__ = ["SUPPORTED_PLATFORM"]
