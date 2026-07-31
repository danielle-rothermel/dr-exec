"""The call-scoped spawn/lifecycle/IPC core. Internal: consumers use `run`.

Every public entry point routes through :func:`execute`, so the shared
invariants have exactly one implementation: a fresh session per spawn,
concurrent feed and drain, group-targeted teardown on every exit path, and
exactly one attribution decided after teardown from recorded flags.

No module-level mutable state: the engine is safe under concurrent calls
from one process.
"""

from __future__ import annotations

import errno
import json
import logging
import os
import shutil
import signal
import subprocess
import tempfile
import threading
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum, unique
from pathlib import Path
from typing import IO, Any, Final

from dr_exec.declare import (
    INVOCATION_AGGREGATE_BOUND_BYTES,
    IPC_JOIN_SELF_BUDGET_SECONDS,
    TERMINATION_SELF_BUDGET_SECONDS,
    UNBUDGETED,
    Budgets,
    ContainmentProfile,
    EnvironmentGrant,
    ExitPolicy,
    GrantKind,
    OutputBudget,
    OverflowPolicy,
    PythonRuntime,
    Records,
    RecordsKind,
    StreamBounds,
    contents_digest_of,
)
from dr_exec.errors import DeclarationError, ExecutorFailure
from dr_exec.record import (
    EXECUTOR_IDENTITY,
    Attribution,
    BudgetAxis,
    Measurements,
    Outcome,
    OutputsLocation,
    RecordStatus,
    RunRecord,
    RunResult,
    TruncationMark,
    TrustCategory,
    format_record_timestamp,
    new_run_id,
    record_filename,
    serialize_budgets,
    serialize_grant,
)

_READ_CHUNK_BYTES: Final[int] = 64 * 1024
_POLL_INTERVAL_SECONDS: Final[float] = 0.01
_SCRATCH_PREFIX: Final[str] = "dr-exec-"

_logger: Final[logging.Logger] = logging.getLogger("dr_exec.engine")
_record_logger: Final[logging.Logger] = logging.getLogger("dr_exec.record")
_scratch_logger: Final[logging.Logger] = logging.getLogger("dr_exec.scratch")


@dataclass(frozen=True, slots=True)
class Invocation:
    """What the engine is asked to run, already trust-categorized.

    ``source`` is present only for Python-source invocations; the record
    identifies both halves of a trusted-driver-over-untrusted-stdin run, so
    input is always digested regardless.
    """

    argv: tuple[str, ...]
    trust_category: TrustCategory
    input_text: str = ""
    source: str | None = None
    runtime: PythonRuntime | None = None
    profile: ContainmentProfile | None = None


@dataclass(frozen=True, slots=True)
class Declaration:
    """The full call-scoped declaration the engine executes.

    ``stream_bounds`` is the one place per-stream capture bounds exist: a
    run whose stdout carries a protocol channel and whose stderr carries
    payload declares them so a flood on one cannot void the other. Plain
    runs leave it ``None`` and keep the single shared output bound.
    """

    invocation: Invocation
    budgets: Budgets
    records: Records
    environment: EnvironmentGrant
    exit_policy: ExitPolicy
    stream_bounds: StreamBounds | None = None


@dataclass(slots=True)
class _Capture:
    """Byte accounting shared by both drain threads.

    ``produced`` keeps counting past the bound so a consumer can size a
    bound from an overflowing run; ``retained`` is what capture kept.

    ``limit_bytes`` is the shared bound both streams draw from.
    ``stdout_limit_bytes`` and ``stderr_limit_bytes`` are the per-stream
    bounds a protocol run declares; where one is set it replaces the shared
    bound for that stream, so a flood on one stream cannot consume the
    other's budget.

    Overflow is flagged per stream for the same reason: a stream that never
    crossed its own bound keeps draining to EOF under every policy, so a
    flood on the payload stream can never abandon result bytes already
    produced on the protocol stream.
    """

    limit_bytes: int | None
    stdout_limit_bytes: int | None = None
    stderr_limit_bytes: int | None = None
    lock: threading.Lock = field(default_factory=threading.Lock)
    stdout_overflow: threading.Event = field(default_factory=threading.Event)
    stderr_overflow: threading.Event = field(default_factory=threading.Event)
    retained_bytes: int = 0
    stdout: bytearray = field(default_factory=bytearray)
    stderr: bytearray = field(default_factory=bytearray)
    stdout_produced: int = 0
    stderr_produced: int = 0
    stdout_dropped: int = 0
    stderr_dropped: int = 0

    def overflow_for(self, *, is_stdout: bool) -> threading.Event:
        return self.stdout_overflow if is_stdout else self.stderr_overflow

    @property
    def any_overflow(self) -> bool:
        """Whether either stream crossed the bound in force for it."""
        return self.stdout_overflow.is_set() or self.stderr_overflow.is_set()


@unique
class _IpcSide(StrEnum):
    """Which of the run's three pipes a fault happened on."""

    STDIN = "stdin"
    STDOUT = "stdout"
    STDERR = "stderr"


@dataclass(frozen=True, slots=True)
class _IpcFault:
    """One fault on the executor's own plumbing, kept as attribution evidence.

    A drain fault is evidence for a ``CHANNEL`` claim: the pipe carrying the
    payload's bytes broke, which is neither payload misbehavior nor an
    executor bug. A feed fault is unknown-cause, so it is ``EXECUTOR`` — the
    payload is never blamed by elimination.
    """

    side: _IpcSide
    error: BaseException

    @property
    def attribution(self) -> Attribution:
        return (
            Attribution.EXECUTOR if self.side is _IpcSide.STDIN else Attribution.CHANNEL
        )


class _IpcThread(threading.Thread):
    """A feed or drain thread that knows which pipe it serves.

    The side travels with the thread so a join that expires names the pipe
    it could not finish, which is what makes the fault attributable.
    """

    def __init__(self, *, side: _IpcSide, **thread_arguments: Any) -> None:
        super().__init__(**thread_arguments)
        self.side: Final[_IpcSide] = side


@dataclass(slots=True)
class _Enforcement:
    """Flags teardown records; attribution is decided from them afterwards."""

    deadline_expired: bool = False
    output_overflowed: bool = False
    ipc_fault: _IpcFault | None = None


@dataclass(frozen=True, slots=True)
class ValidatedDeclaration:
    """A declaration that passed every pre-spawn check, plus what they built.

    Validation is a pure function of the declaration, so it is the one thing
    a non-spawning executor can share with this one verbatim: whatever
    :func:`validate_declaration` accepts here is exactly what any executor
    honoring this contract accepts.
    """

    argv: tuple[str, ...]
    input_payload: bytes
    child_environment: dict[str, str]


def validate_declaration(declaration: Declaration) -> ValidatedDeclaration:
    """Every pre-spawn caller check, in order, before any child exists.

    Raises :class:`DeclarationError` and nothing else; no side effects.
    """
    argv = _validate_argv(declaration.invocation.argv)
    input_payload = _validated_input(
        declaration.invocation.input_text, declaration.budgets
    )
    child_environment = _materialize_environment(declaration.environment)
    _validate_invocation_size(argv, child_environment)
    _validate_program_resolvable(argv[0], child_environment)
    return ValidatedDeclaration(
        argv=argv,
        input_payload=input_payload,
        child_environment=child_environment,
    )


def execute(declaration: Declaration) -> RunResult:
    """Run one declaration to completion and return its attributed outcome.

    Raises :class:`DeclarationError` before any spawn and
    :class:`ExecutorFailure` only when the executor's own machinery broke.
    """
    validated = validate_declaration(declaration)
    argv = validated.argv
    input_payload = validated.input_payload
    child_environment = validated.child_environment

    run_id = new_run_id()
    started_at = datetime.now(UTC)
    # Resolved, so the recorded path is the one the payload's own getcwd
    # reports on platforms where the temp root is a symlink.
    scratch = Path(tempfile.mkdtemp(prefix=_SCRATCH_PREFIX)).resolve()
    _scratch_logger.debug("scratch workspace created at %s", scratch)

    record_state = _RecordState(
        records=declaration.records,
        record=_spawn_record(
            declaration=declaration,
            argv=argv,
            run_id=run_id,
            started_at=started_at,
            scratch=scratch,
            input_payload=input_payload,
        ),
        started_at=started_at,
    )
    record_state.write_spawn()

    try:
        result = _spawn_and_supervise(
            declaration=declaration,
            argv=argv,
            input_payload=input_payload,
            child_environment=child_environment,
            scratch=scratch,
        )
    except ExecutorFailure as executor_failure:
        # Kept regardless of outcome: an executor failure is an outcome in
        # this contract's vocabulary, and the durable twin is exactly the
        # artifact that has to survive when no in-memory result does. Left
        # at `spawned`, the record could not be told apart from a parent
        # that died mid-run.
        record_state.mark_executor_failure(executor_failure)
        raise
    finally:
        _remove_scratch(scratch)

    record_state.finalize(result)
    return result


def _spawn_and_supervise(
    *,
    declaration: Declaration,
    argv: tuple[str, ...],
    input_payload: bytes,
    child_environment: dict[str, str],
    scratch: Path,
) -> RunResult:
    _logger.info("spawning %s in %s", argv[0], scratch)
    _logger.debug("argv %r", argv)

    spawn_started = time.monotonic()
    try:
        process = subprocess.Popen(
            argv,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
            env=child_environment,
            cwd=str(scratch),
            close_fds=True,
        )
    except OSError as spawn_error:
        return _spawn_failure_result(
            spawn_error=spawn_error,
            elapsed=time.monotonic() - spawn_started,
            input_bytes=len(input_payload),
        )

    bounds = declaration.stream_bounds
    capture = _Capture(
        limit_bytes=_output_limit(declaration.budgets),
        stdout_limit_bytes=None if bounds is None else bounds.stdout_bytes,
        stderr_limit_bytes=None if bounds is None else bounds.stderr_bytes,
    )
    enforcement = _Enforcement()
    ipc_errors: list[_IpcFault] = []
    threads = _start_ipc_threads(
        process=process,
        input_payload=input_payload,
        capture=capture,
        ipc_errors=ipc_errors,
    )

    _supervise(
        process=process,
        budgets=declaration.budgets,
        capture=capture,
        enforcement=enforcement,
        spawn_started=spawn_started,
    )

    teardown_started = time.monotonic()
    try:
        _terminate_group(process)
    finally:
        _join_ipc_threads(threads, ipc_errors=ipc_errors)
    reaped_at = time.monotonic()
    teardown_seconds = reaped_at - teardown_started
    duration_seconds = reaped_at - spawn_started
    _logger.info(
        "reaped pid %d with returncode %s after %.3fs",
        process.pid,
        process.returncode,
        duration_seconds,
    )

    enforcement.ipc_fault = _first_ipc_fault(ipc_errors)

    with capture.lock:
        stdout_bytes = bytes(capture.stdout)
        stderr_bytes = bytes(capture.stderr)
        truncation = TruncationMark(
            stdout_bytes_dropped=capture.stdout_dropped,
            stderr_bytes_dropped=capture.stderr_dropped,
        )
        measurements = Measurements(
            duration_seconds=duration_seconds,
            teardown_seconds=teardown_seconds,
            stdout_bytes_produced=capture.stdout_produced,
            stderr_bytes_produced=capture.stderr_produced,
            input_bytes=len(input_payload),
        )

    outcome = _decide_attribution(
        returncode=process.returncode,
        enforcement=enforcement,
        exit_policy=declaration.exit_policy,
    )
    return RunResult(
        returncode=process.returncode,
        stdout=stdout_bytes.decode("utf-8", errors="replace"),
        stderr=stderr_bytes.decode("utf-8", errors="replace"),
        truncation=truncation,
        measurements=measurements,
        outcome=outcome,
    )


def _supervise(
    *,
    process: subprocess.Popen[bytes],
    budgets: Budgets,
    capture: _Capture,
    enforcement: _Enforcement,
    spawn_started: float,
) -> None:
    """Poll to exit, deadline, or enforcing overflow, whichever lands first.

    Enforcement reads *any* stream's crossing: killing on a crossing is the
    caller's declared ``FAIL`` policy, whichever stream's bound was the one
    crossed. Where the crossing does not reach — which stream keeps draining
    — is the drain's business, not the kill's.
    """
    deadline = (
        None if budgets.wall_clock is UNBUDGETED else spawn_started + budgets.wall_clock
    )
    enforcing_overflow = _overflow_policy(budgets) is OverflowPolicy.FAIL

    _logger.debug("waiting on pid %d", process.pid)
    while process.poll() is None:
        if enforcing_overflow and capture.any_overflow:
            enforcement.output_overflowed = True
            _logger.info("output budget exceeded; killing pid %d", process.pid)
            return
        if deadline is not None and time.monotonic() >= deadline:
            enforcement.deadline_expired = True
            _logger.info("wall-clock budget exceeded; killing pid %d", process.pid)
            return
        time.sleep(_POLL_INTERVAL_SECONDS)

    # A violation recorded during the final poll window wins over the exit
    # that raced it: the payload still crossed a declared bound.
    if enforcing_overflow and capture.any_overflow:
        enforcement.output_overflowed = True
    if deadline is not None and time.monotonic() >= deadline:
        enforcement.deadline_expired = True


def _decide_attribution(
    *,
    returncode: int | None,
    enforcement: _Enforcement,
    exit_policy: ExitPolicy,
) -> Outcome:
    """Exactly one attribution, in the pinned order.

    Absence is decided at spawn, so the order here resumes at the output
    budget: an overflow that expired the deadline while draining is an
    output outcome, never a timeout. A fault on the executor's own plumbing
    sits below both — the bounds the caller declared were still the thing
    that ended the run — and above exit-status interpretation, because a
    payload is never blamed by elimination for bytes the channel lost.
    """
    if enforcement.output_overflowed:
        return Outcome(attribution=Attribution.BUDGET, violated_axis=BudgetAxis.OUTPUT)
    if enforcement.deadline_expired:
        return Outcome(
            attribution=Attribution.BUDGET, violated_axis=BudgetAxis.WALL_CLOCK
        )
    if enforcement.ipc_fault is not None:
        return Outcome(attribution=enforcement.ipc_fault.attribution)
    return Outcome(
        attribution=Attribution.PAYLOAD,
        exit_verdict=exit_policy.verdict_for(returncode),
    )


def _spawn_failure_result(
    *, spawn_error: OSError, elapsed: float, input_bytes: int
) -> RunResult:
    """ENOENT is absence; every other spawn errno is machine, errno kept."""
    if spawn_error.errno == errno.ENOENT:
        _logger.info("program absent: %s", spawn_error)
        outcome = Outcome(attribution=Attribution.ABSENCE, spawn_errno=errno.ENOENT)
    else:
        _logger.warning("spawn failed: %s", spawn_error)
        outcome = Outcome(
            attribution=Attribution.MACHINE, spawn_errno=spawn_error.errno
        )
    return RunResult(
        returncode=None,
        stdout="",
        stderr="",
        truncation=TruncationMark(),
        measurements=Measurements(
            duration_seconds=elapsed,
            teardown_seconds=0.0,
            stdout_bytes_produced=0,
            stderr_bytes_produced=0,
            input_bytes=input_bytes,
        ),
        outcome=outcome,
    )


def _start_ipc_threads(
    *,
    process: subprocess.Popen[bytes],
    input_payload: bytes,
    capture: _Capture,
    ipc_errors: list[_IpcFault],
) -> tuple[_IpcThread, ...]:
    """Feed and drain concurrently: a caller can never deadlock a run."""
    threads = (
        _IpcThread(
            side=_IpcSide.STDIN,
            target=_feed_input,
            args=(process.stdin, input_payload, ipc_errors),
            daemon=True,
        ),
        _IpcThread(
            side=_IpcSide.STDOUT,
            target=_drain,
            args=(process.stdout, True, capture, ipc_errors),
            daemon=True,
        ),
        _IpcThread(
            side=_IpcSide.STDERR,
            target=_drain,
            args=(process.stderr, False, capture, ipc_errors),
            daemon=True,
        ),
    )
    for thread in threads:
        thread.start()
    return threads


def _join_ipc_threads(
    threads: tuple[_IpcThread, ...], *, ipc_errors: list[_IpcFault]
) -> None:
    """Join within one shared self-budget; a thread that outlives it is data.

    An escaped descendant holding an inherited write end keeps a drain
    blocked in ``read1`` forever. That is payload behavior, not broken
    machinery: the child was reaped and its captured bytes exist, so the run
    has a result. The stranded thread is abandoned — it is a daemon and its
    capture is already accounted under the lock — and the unread tail is
    recorded as a channel fault so the outcome says the capture is
    incomplete rather than claiming a clean payload exit.

    One shared deadline, not one per thread: the pinned bound is the
    deadline plus the termination and join self-budgets, singular.
    """
    join_deadline = time.monotonic() + IPC_JOIN_SELF_BUDGET_SECONDS
    for thread in threads:
        thread.join(timeout=max(join_deadline - time.monotonic(), 0.0))
    for thread in threads:
        if not thread.is_alive():
            continue
        ipc_errors.append(
            _IpcFault(
                side=thread.side,
                error=ExecutorFailure(
                    f"the {thread.side.value} pipe never reached EOF within the "
                    "join self-budget"
                ),
            )
        )


def _first_ipc_fault(ipc_errors: list[_IpcFault]) -> _IpcFault | None:
    """The fault that decides attribution, narrated so it is never silent."""
    if not ipc_errors:
        return None
    for fault in ipc_errors:
        _logger.warning(
            "fault on the run's %s pipe: %r — capture is incomplete",
            fault.side.value,
            fault.error,
        )
    return ipc_errors[0]


def _feed_input(
    stream: IO[bytes] | None,
    payload: bytes,
    ipc_errors: list[_IpcFault],
) -> None:
    """Write exactly the declared input, then close: the child sees EOF.

    A payload closing its own stdin is ordinary and benign; anything else is
    recorded so the outcome can say the child never saw its declared input.
    """
    if stream is None:
        return
    try:
        if payload:
            stream.write(payload)
        stream.close()
    except BrokenPipeError:
        return
    except BaseException as write_error:
        # Broad by design: a thread that dies silently is a capture the
        # outcome would claim as clean, so every way this can fail is
        # recorded and attributed rather than lost with the thread.
        ipc_errors.append(_IpcFault(side=_IpcSide.STDIN, error=write_error))


def _drain(
    stream: IO[bytes] | None,
    is_stdout: bool,
    capture: _Capture,
    ipc_errors: list[_IpcFault],
) -> None:
    """Read to EOF, retaining up to the bound in force and counting all bytes.

    The loop keeps reading past the bound under every policy and discards the
    excess: the pipe is never closed early, so an executor-side capture
    decision cannot change how the payload dies, and bytes a payload already
    produced on this stream are never abandoned because some *other* stream
    crossed its bound. Under ``FAIL``, :func:`_supervise` reads the crossing
    and kills; ending the drain is the kill's consequence, never its cause.
    """
    if stream is None:
        return
    # read1 delivers whatever a single pipe read returns; read() would block
    # for a full chunk, so a payload that writes past the bound and then
    # sleeps would not be accounted until it exited.
    try:
        read_available = getattr(stream, "read1", stream.read)
        while chunk := read_available(_READ_CHUNK_BYTES):
            with capture.lock:
                _account(capture, chunk, is_stdout=is_stdout)
    except BaseException as read_error:
        # Broad by design, and the read primitive is looked up inside the
        # try: a drain thread that dies silently leaves a truncated capture
        # that every integrity field would report as complete.
        ipc_errors.append(
            _IpcFault(
                side=_IpcSide.STDOUT if is_stdout else _IpcSide.STDERR,
                error=read_error,
            )
        )


def _account(capture: _Capture, chunk: bytes, *, is_stdout: bool) -> None:
    """Byte-denominated accounting under the bounds in force. Holds the lock.

    A stream with its own declared bound is measured against that bound
    alone; a stream without one draws from the shared bound, as every plain
    run's two streams do.
    """
    destination = capture.stdout if is_stdout else capture.stderr
    stream_limit = (
        capture.stdout_limit_bytes if is_stdout else capture.stderr_limit_bytes
    )
    if stream_limit is not None:
        remaining = max(stream_limit - len(destination), 0)
    elif capture.limit_bytes is None:
        remaining = len(chunk)
    else:
        remaining = max(capture.limit_bytes - capture.retained_bytes, 0)
    kept = min(len(chunk), remaining)
    dropped = len(chunk) - kept

    destination.extend(chunk[:kept])
    if stream_limit is None:
        capture.retained_bytes += kept
    if is_stdout:
        capture.stdout_produced += len(chunk)
        capture.stdout_dropped += dropped
    else:
        capture.stderr_produced += len(chunk)
        capture.stderr_dropped += dropped
    if dropped:
        capture.overflow_for(is_stdout=is_stdout).set()


def _terminate_group(process: subprocess.Popen[bytes]) -> None:
    """Group-targeted SIGKILL and reap; no survivors before the call returns."""
    _logger.debug("killing process group %d", process.pid)
    signaling_error = _signal_group(process.pid)
    if signaling_error is not None and process.poll() is None:
        try:
            process.kill()
        except ProcessLookupError:
            pass
        except OSError as kill_error:
            signaling_error = kill_error

    try:
        process.wait(timeout=TERMINATION_SELF_BUDGET_SECONDS)
    except subprocess.TimeoutExpired as expired:
        raise ExecutorFailure(
            "process group outlived the termination self-budget"
        ) from expired
    if signaling_error is None:
        return

    # Completion may win between the liveness poll and Popen.kill(). Retry
    # the group after reaping the leader so descendants cannot survive that
    # race; a normally exited leader makes the first error stale.
    remaining_error = _signal_group(process.pid)
    if remaining_error is None or process.returncode != -signal.SIGKILL:
        return
    raise ExecutorFailure(
        "process group could not be signaled: "
        f"errno={remaining_error.errno} "
        f"({remaining_error.strerror or str(remaining_error)})"
    ) from remaining_error


def _signal_group(process_group_id: int) -> OSError | None:
    try:
        os.killpg(process_group_id, signal.SIGKILL)
    except ProcessLookupError:
        return None
    except OSError as signal_error:
        return signal_error
    return None


def _remove_scratch(scratch: Path) -> None:
    """Remove the workspace on every exit path; failure never raises."""
    try:
        shutil.rmtree(scratch)
    except OSError as cleanup_error:
        _scratch_logger.warning(
            "scratch workspace %s could not be removed: %s", scratch, cleanup_error
        )
    else:
        _scratch_logger.debug("scratch workspace %s removed", scratch)


@dataclass(slots=True)
class _RecordState:
    """The record's lifecycle: written at spawn, finalized at exit.

    A write failure is narrated and marked in the record's own status; it
    never fails the run.
    """

    records: Records
    record: RunRecord
    started_at: datetime
    path: Path | None = None
    write_failed: bool = False

    @property
    def enabled(self) -> bool:
        return self.records.kind is RecordsKind.DIRECTORY

    def write_spawn(self) -> None:
        if not self.enabled:
            return
        directory = self.records.path
        assert directory is not None
        self.path = directory / record_filename(
            started_at=self.started_at, run_id=self.record.run_id
        )
        self._write(self.record)
        if not self.write_failed:
            _record_logger.info("run record at %s", self.path)

    def finalize(self, result: RunResult) -> None:
        if not self.enabled or self.write_failed:
            return
        self.record = self.record.finalized_with(
            result=result, finished_at=datetime.now(UTC)
        )
        self._write(self.record)
        self._mark_write_failure()

    def mark_executor_failure(self, failure: ExecutorFailure) -> None:
        """Write the terminal status for a run the executor could not finish.

        No :class:`RunResult` exists — that is what an executor failure means
        — so there is nothing to finalize *with*; the status alone is what
        distinguishes "the executor gave up here" from "this run is still in
        flight" for anyone reading the directory afterwards.
        """
        if not self.enabled or self.write_failed:
            return
        _record_logger.warning(
            "run record %s marks an executor failure: %s", self.path, failure
        )
        self.record = self.record.model_copy(
            update={
                "finished_at": format_record_timestamp(datetime.now(UTC)),
                "attribution": Attribution.EXECUTOR,
                "record_status": RecordStatus.EXECUTOR_FAILED,
            }
        )
        self._write(self.record)
        self._mark_write_failure()

    def _mark_write_failure(self) -> None:
        """Best-effort second write so a failed write is itself recorded.

        The first write is what failed, so this one may fail too; when it
        does, the narration is all that is left. Attempting it is what keeps
        ``write_failed`` a status a reader can actually find on disk rather
        than a value only the dead in-memory record ever held.
        """
        if not self.write_failed:
            return
        assert self.path is not None
        try:
            self.path.write_text(_render_record(self.record), encoding="utf-8")
        except OSError:
            return

    def _write(self, record: RunRecord) -> None:
        assert self.path is not None
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.path.write_text(
                _render_record(record),
                encoding="utf-8",
            )
        except OSError as write_error:
            self.write_failed = True
            self.record = record.model_copy(
                update={"record_status": RecordStatus.WRITE_FAILED}
            )
            _record_logger.warning(
                "run record %s could not be written: %s", self.path, write_error
            )


def _render_record(record: RunRecord) -> str:
    return json.dumps(record.to_wire(), sort_keys=True, indent=2)


def _spawn_record(
    *,
    declaration: Declaration,
    argv: tuple[str, ...],
    run_id: str,
    started_at: datetime,
    scratch: Path,
    input_payload: bytes,
) -> RunRecord:
    invocation = declaration.invocation
    budgets = serialize_budgets(declaration.budgets)
    grant = serialize_grant(declaration.environment)
    runtime = invocation.runtime
    return RunRecord(
        executor_identity=EXECUTOR_IDENTITY,
        trust_category=invocation.trust_category,
        run_id=run_id,
        # Argv *or* source digest: for a source-carrying invocation the
        # source is argv's last element, so recording both would persist the
        # payload body verbatim beside the digest that exists to represent
        # it — and untrusted source routinely embeds credentials. The
        # digest plus the runtime is what identifies a source run.
        argv=None if invocation.source is not None else argv,
        source_digest=(
            None if invocation.source is None else contents_digest_of(invocation.source)
        ),
        input_digest=contents_digest_of(invocation.input_text),
        grant_kind=grant["grant_kind"],
        grant_names=grant["grant_names"],
        grant_exclusions=grant["grant_exclusions"],
        grant_contents_digest=grant["grant_contents_digest"],
        profile_name=None if invocation.profile is None else invocation.profile.name,
        budget_wall_clock_seconds=budgets.wall_clock_seconds,
        budget_output_bytes=budgets.output_bytes,
        budget_output_overflow_policy=budgets.output_overflow_policy,
        budget_input_bytes=budgets.input_bytes,
        unbudgeted_axes=budgets.unbudgeted_axes,
        runtime_name=None if runtime is None else runtime.name,
        runtime_interpreter=None if runtime is None else runtime.interpreter,
        started_at=format_record_timestamp(started_at),
        input_bytes=len(input_payload),
        outputs_location=OutputsLocation.CAPTURED,
        scratch_path=str(scratch),
        record_status=RecordStatus.SPAWNED,
    )


def _output_limit(budgets: Budgets) -> int | None:
    if budgets.output is UNBUDGETED:
        return None
    assert isinstance(budgets.output, OutputBudget)
    return budgets.output.limit_bytes


def _overflow_policy(budgets: Budgets) -> OverflowPolicy | None:
    if budgets.output is UNBUDGETED:
        return None
    assert isinstance(budgets.output, OutputBudget)
    return budgets.output.overflow_policy


def _validate_argv(argv: Sequence[str]) -> tuple[str, ...]:
    """Argv-only: nothing here is ever interpreted by a shell."""
    if isinstance(argv, str | bytes) or not isinstance(argv, Sequence) or not argv:
        raise DeclarationError("command must be a nonempty sequence of strings")
    for argument in argv:
        if not isinstance(argument, str):
            raise DeclarationError("command must be a nonempty sequence of strings")
        if "\0" in argument:
            raise DeclarationError("command arguments must not contain NUL")
    if not argv[0]:
        raise DeclarationError("command program must not be empty")
    return tuple(argv)


def _validated_input(input_text: str, budgets: Budgets) -> bytes:
    """Input budgets are enforced before spawn: never a wasted spawn."""
    if not isinstance(input_text, str):
        raise DeclarationError("input must be text")
    payload = input_text.encode("utf-8")
    if budgets.input is not UNBUDGETED and len(payload) > budgets.input:
        raise DeclarationError(
            f"input of {len(payload)} bytes exceeds the declared "
            f"{budgets.input}-byte input budget"
        )
    return payload


def _materialize_environment(grant: EnvironmentGrant) -> dict[str, str]:
    """Build exactly what the child receives from the frozen grant."""
    if grant.kind is GrantKind.OVERLAY:
        base = dict(os.environ)
        present = sorted(name for name in grant.exclusions if name in base)
        if present:
            raise DeclarationError(
                "overlay exclusions are present in the parent environment: "
                + ", ".join(present)
            )
        base.update(grant.resolved)
        return base
    return dict(grant.resolved)


def _validate_invocation_size(
    argv: tuple[str, ...], child_environment: Mapping[str, str]
) -> None:
    """Reject an oversized argv-plus-environment pre-spawn, never an E2BIG."""
    total = sum(len(argument.encode("utf-8")) + 1 for argument in argv)
    total += sum(
        len(name.encode("utf-8")) + len(value.encode("utf-8")) + 2
        for name, value in child_environment.items()
    )
    if total > INVOCATION_AGGREGATE_BOUND_BYTES:
        raise DeclarationError(
            f"argv plus granted environment is {total} bytes, over the "
            f"{INVOCATION_AGGREGATE_BOUND_BYTES}-byte invocation bound"
        )


def _validate_program_resolvable(
    program: str, child_environment: Mapping[str, str]
) -> None:
    """Resolution is execvp-style against the *granted* environment's PATH.

    Only the unresolvable-by-construction case is a caller error; an
    advisory pre-check never decides the outcome, so a program that this
    passes and the spawn rejects still lands as ENOENT absence.
    """
    if os.sep in program or (os.altsep is not None and os.altsep in program):
        return
    if "PATH" not in child_environment:
        raise DeclarationError(
            f"relative program {program!r} cannot resolve: the grant declares no PATH"
        )
