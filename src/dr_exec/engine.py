"""The one private call-scoped execution path: spawn through teardown.

Every production execution follows this sequence and no other: validate
the declaration, prepare durable state, create the scratch workspace,
launch one fresh child through the library-owned bootstrap, exchange
transport bytes concurrently, enforce the declared workload budgets, tear
down the original process group and reap the direct child, finalize the
record, and return one result.

Two invariants shape the code rather than the prose. First, teardown and
reaping happen on *every* post-spawn exit path, including a raise caused
by machinery failure: the post-spawn body is wrapped so that returning and
raising leave through the same lifecycle work. Second, the durable record
is mandatory and ordered: ``prepare`` precedes the spawn, ``mark_running``
follows only a successful spawn, and exactly one ``finalize`` runs on every
path that produces a ``CompletedExecution`` -- including recognized
pre-spawn outcomes, which finalize from ``prepared`` without ever launching
a child.

A machinery failure that prevents a trustworthy result raises instead of
finalizing, and that is deliberate: join exhaustion, a bootstrap that could
not launch, and a store or thread failure of an unexpected type all leave
the call with no result worth recording, so the latest lifecycle state that
was successfully published -- ``prepared`` or ``running`` -- stays as the
durable record rather than being completed with a manufactured outcome.

Recording degradation never replaces an execution outcome. A post-start
recording failure becomes a degraded receipt naming the latest lifecycle
state that remains valid on disk, and the result the engine computed is
returned unchanged alongside it.

Attribution is best-effort diagnosis, not causal proof. The precedence is
pinned -- spawn absence, output budget, wall-clock budget, exit-status
interpretation -- and a recorded output violation beats a deadline or a
clean exit.
"""

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
from uuid import uuid4

from dr_serialize import IdentityDocument, Sha256Digest

from dr_exec._identity import (
    _build_env_grant_record,
    _build_executor_config_identity,
    _build_executor_identity,
    _canonical_declaration_digest,
)
from dr_exec._protocol import (
    ProtocolStreamResult,
    read_protocol_stream,
    request_identity_digest,
    request_transport_bytes,
)
from dr_exec._provenance import _executor_source_snapshot
from dr_exec._retention import PayloadRetention, StreamRetention
from dr_exec._spawn import (
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
from dr_exec.cancel import CancelToken
from dr_exec.declare import (
    EnvGrant,
    ExecutionJob,
    ExecutorSelfBudgets,
    FiniteByteLimit,
    FiniteDurationLimit,
    FiniteOutput,
    TrustedCommandTarget,
    UntrustedCommandTarget,
    UntrustedPythonTarget,
)
from dr_exec.errors import DeclarationError, ExecutorFailure
from dr_exec.kinds import (
    BudgetAxis,
    FailureOwner,
    OutputOverflowPolicy,
    ProtocolFailureCode,
    RecordState,
)
from dr_exec.names import AttemptId, ExecutionId
from dr_exec.protocols import RunStore, Runtime
from dr_exec.record import (
    BudgetExceededOutcome,
    CancelledOutcome,
    CompletedExecution,
    DegradedRecordReceipt,
    ExecutionAttribution,
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
    UntrustedCommandTargetRecord,
    UntrustedPythonTargetRecord,
)
from dr_exec.store import FinalizableRun, PreparedRun

# The supported platform. macOS process-group, session, and descriptor
# semantics are what the lifecycle claim rests on, so an unsupported
# platform is refused at the declaration boundary rather than producing a
# result whose containment claim does not hold there.
SUPPORTED_PLATFORM: Final = "darwin"

# The scratch workspace's directory prefix. One fresh directory per run,
# removed on every exit path.
SCRATCH_DIRECTORY_PREFIX: Final = "dr-exec-run-"

# Transport read size. A drain detail, never a limit: retention and the
# aggregate output budget are what bound what is kept and produced.
_DRAIN_CHUNK_BYTES: Final = 65536

# The watchdog cadence for observing a child that has no finite wall-time
# budget. It bounds how long the reaper waits inside one poll, never how
# long the run may take: an unbudgeted time axis makes no bounded-return
# promise, and no elapsed time here is ever evidence about the child.
_REAP_POLL_SECONDS: Final = 0.05


@dataclass(frozen=True, slots=True)
class _ResolvedTarget:
    """One target reduced to what the spawn and the record both need."""

    executable: str
    argv: tuple[str, ...]
    stdin_bytes: bytes
    record: ExecutionTargetRecord
    request_id_sha256: Sha256Digest | None
    wants_protocol: bool


class _ExecutionScheduler:
    """Non-public scheduler implementation placeholder."""


def _now() -> datetime:
    return datetime.now(UTC)


def _finite_ns(budget: object, /) -> int | None:
    return budget.max_ns if isinstance(budget, FiniteDurationLimit) else None


def _finite_bytes(budget: object, /) -> int | None:
    return budget.max_bytes if isinstance(budget, FiniteByteLimit) else None


def _validate_platform() -> None:
    if sys.platform != SUPPORTED_PLATFORM:
        raise DeclarationError(
            f"dr-exec v1 executes only on {SUPPORTED_PLATFORM}"
        )


def _granted_environment(grant: EnvGrant, /) -> dict[str, str]:
    """Materialize the exact environment the child receives.

    Values were snapshotted when the grant was constructed, so nothing
    here consults the parent's live environment: the grant is the whole
    inherited state.
    """
    return {variable.name: variable.value for variable in grant.variables}


def _resolve_executable(
    argv: tuple[str, ...],
    environment: dict[str, str],
    /,
) -> str:
    """Resolve argv[0] against the granted ``PATH``, and only it.

    Absent a granted ``PATH``, only an absolute executable resolves; a
    relative executable with no granted ``PATH`` has no defensible
    meaning and is a pre-spawn declaration error rather than a spawn
    attempt that would consult the parent's ambient search path. A name
    that resolves nowhere is left to the spawn, which reports absence as
    an outcome rather than raising.
    """
    name = argv[0]
    if Path(name).is_absolute():
        return name
    granted_path = environment.get("PATH")
    if granted_path is None:
        raise DeclarationError(
            "a relative executable requires a granted PATH: " + name
        )
    resolved = shutil.which(name, path=granted_path)
    return resolved if resolved is not None else name


def _target_of(job: ExecutionJob, runtime: Runtime, /) -> _ResolvedTarget:
    """Reduce one declared target to its spawn and record evidence.

    The declaration itself carries argv, stdin, source, and the request
    payload, so only its digest reaches durable evidence; the live values
    stay here.

    The Python target is the one kind whose invocation the engine does not
    compose: the runtime owns the fixed ``-I -c <wrapper>`` command and the
    wrapper that embeds the declared ``driver_source`` as data, and the
    engine only maps its transports. Its stdin is the canonical request
    document rather than declared raw bytes, and it is the one target that
    inherits the protected descriptor.
    """
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
        case UntrustedPythonTarget():
            prepared = runtime.prepare(job.target)
            return _ResolvedTarget(
                executable=prepared.argv[0],
                argv=prepared.argv,
                stdin_bytes=request_transport_bytes(prepared.request),
                record=UntrustedPythonTargetRecord(
                    canonical_declaration_sha256=digest,
                    request_id_sha256=request_identity_digest(
                        prepared.request
                    ),
                    containment_profile=job.target.containment_profile,
                    runtime=prepared.runtime_record,
                ),
                request_id_sha256=request_identity_digest(prepared.request),
                wants_protocol=True,
            )


def _validate_input_budget(job: ExecutionJob, stdin_bytes: bytes, /) -> None:
    """Compare declared input length with the budget before any spawn.

    Input bounds are the one workload axis checked before a child exists,
    so an over-budget input never costs a spawn.
    """
    limit = _finite_bytes(job.budgets.input_bytes)
    if limit is not None and len(stdin_bytes) > limit:
        raise DeclarationError(
            f"declared input of {len(stdin_bytes)} bytes exceeds the "
            f"{limit}-byte input budget"
        )


@contextmanager
def _scratch_workspace() -> Iterator[Path]:
    """Create one fresh scratch directory and remove it on every path.

    Cleanup runs on every exit path and never replaces an otherwise
    trustworthy result, so removal is best effort and cannot raise out of
    this contextmanager. V1 exposes no narration surface for the failure
    it absorbs, which is the honest limit of the claim: cleanup is
    attempted on every path, and a failure to remove is silent rather
    than reported.
    """
    directory = Path(tempfile.mkdtemp(prefix=SCRATCH_DIRECTORY_PREFIX))
    try:
        yield directory
    finally:
        shutil.rmtree(directory, ignore_errors=True)


@dataclass(slots=True)
class _DrainState:
    """Everything the concurrent transport threads accumulate."""

    retention: PayloadRetention
    overflow: Event = field(default_factory=Event)
    protocol_result: ProtocolStreamResult | None = None


def _feed_stdin(descriptor: int, payload: bytes, /) -> None:
    """Write the declared input and close, so the child reads to EOF.

    A child that exits without reading its input closes the pipe first,
    which is an ordinary outcome rather than a machinery failure, so the
    broken pipe is absorbed here. Writing to the raw descriptor keeps
    every transport on the same footing: no buffered object's internal
    lock stands between a blocked write and the descriptor behind it.
    """
    try:
        offset = 0
        while offset < len(payload):
            offset += os.write(descriptor, payload[offset:])
    except OSError:
        pass
    finally:
        with suppress(OSError):
            os.close(descriptor)


@dataclass(slots=True)
class _OutputPump:
    """One selector-driven pump over every output descriptor at once.

    Concurrent draining is what keeps a full pipe from deadlocking the
    run, and one selector is what makes that draining interruptible: a
    thread blocked inside a buffered read cannot be released by closing
    the object it is reading, because the close waits on the same lock.
    Selecting instead means the pump always returns to a point where it
    can observe the release gate, so a child that escaped the process
    group can never pin the parent to a descriptor forever.

    Protocol bytes are forwarded into a second pipe rather than parsed
    here: the protected stream's reader owns frame acquisition and its
    finite self-budgets, and forwarding gives that reader a real EOF when
    the pump releases -- so it, too, ends without being interrupted
    mid-frame.
    """

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
        """Drain until every output descriptor reaches EOF, or release.

        Draining continues past the aggregate output bound: production
        counts must stay exact through EOF, and under the fail policy it
        is the engine, not this pump, that acts on the crossing.
        """
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
    """Return what is ready, or ``None`` once the descriptor is done.

    A would-block is nothing available rather than an end, so the pump
    returns to its selector instead of retiring a descriptor that simply
    had no bytes yet.
    """
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
    """Read the forwarded protected stream to its terminal outcome."""
    with os.fdopen(descriptor, "rb") as reader:
        state.protocol_result = read_protocol_stream(
            reader,
            request_id_sha256=request_id_sha256,
            self_budgets=self_budgets,
        )


def _read_setup_status(descriptor: int, startup_ns: int | None, /) -> bytes:
    """Read the setup status through EOF within the startup budget.

    The status pipe reaching EOF is what says the payload was reached, so
    this read gates the whole attempt: nothing downstream may run until
    the helper either reports a setup failure or execs. A finite startup
    budget is the executor's own deadline on that gate -- a helper that
    stalls before exec cannot hold the call open past it. With no declared
    startup budget there is no deadline, which is exactly what an
    unbudgeted axis promises.

    Ownership stays with the caller: the descriptor is neither closed nor
    left non-blocking here, and expiry raises rather than returning a
    truncated status that would be parsed as a payload that started.
    """
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
    """Release descriptors this frame owns, exactly once each."""
    for descriptor in descriptors:
        with suppress(OSError):
            os.close(descriptor)


def _started_thread(target: Callable[[], None], name: str, /) -> Thread:
    thread = Thread(target=target, name=name, daemon=True)
    thread.start()
    return thread


@dataclass(frozen=True, slots=True)
class _StopReason:
    """Why the engine stopped waiting on a child that was still alive."""

    axis: BudgetAxis | None
    cancelled: bool


@dataclass(slots=True)
class _AttemptObservation:
    """What the engine learned about a live child, as it learned it.

    Observation accumulates here rather than being returned, because the
    frame that owns teardown must be able to act on a partial observation:
    a machinery failure part way through still has to know whether the
    helper leads a process group before it signals one.

    ``state`` is what separates the two completions. It exists exactly
    when the payload was reached, so a ``None`` here means the attempt
    stopped at a setup failure that ``setup_failure`` names.

    ``recording_failures`` carries the post-start publications that did
    not land. An attempt continues through those, so they would otherwise
    be invisible: they are what makes the caller's receipt degraded even
    when the finalize that follows succeeds.
    """

    prepared: PreparedRun
    setup_failure: SetupFailure | None = None
    leads_group: bool = True
    state: _DrainState | None = None
    stop: _StopReason | None = None
    running: FinalizableRun | None = None
    recording_failures: tuple[RecordingFailure, ...] = ()

    def reached_payload(self) -> _DrainState | None:
        """The drain state of an attempt whose payload actually ran."""
        return None if self.setup_failure is not None else self.state

    def latest_run(self) -> FinalizableRun:
        """The latest handle published, which is the prepared one until
        ``mark_running`` succeeds.
        """
        return self.prepared if self.running is None else self.running


def _await_child(
    process: subprocess.Popen[bytes],
    state: _DrainState,
    /,
    *,
    deadline_ns: int | None,
    fail_on_overflow: bool,
    cancellation: CancelToken | None,
) -> _StopReason | None:
    """Observe the child until it exits or the engine must stop it.

    ``None`` means the child exited on its own. Otherwise the returned
    reason names what the engine acted on; the caller performs the
    teardown. Each wait lasts one watchdog poll, shortened to what remains
    of the wall-time deadline when one is declared, so a declared budget
    is acted on at the deadline rather than a poll late -- an unbudgeted
    time axis makes no bounded-return promise, and elapsed time is never
    treated as evidence about the child.
    """
    while True:
        if process.poll() is not None:
            return None
        if fail_on_overflow and state.overflow.is_set():
            return _StopReason(axis=BudgetAxis.PAYLOAD_OUTPUT, cancelled=False)
        if cancellation is not None and cancellation.cancelled:
            return _StopReason(axis=None, cancelled=True)
        timeout = _REAP_POLL_SECONDS
        if deadline_ns is not None:
            remaining_ns = deadline_ns - time.monotonic_ns()
            if remaining_ns <= 0:
                return _StopReason(axis=BudgetAxis.WALL_TIME, cancelled=False)
            timeout = min(timeout, remaining_ns / 1e9)
        with suppress(subprocess.TimeoutExpired):
            process.wait(timeout=timeout)


def _tear_down(
    process: subprocess.Popen[bytes],
    self_budgets: ExecutorSelfBudgets,
    /,
    *,
    leads_group: bool = True,
) -> int:
    """Signal the original process group, escalate, and reap the child.

    Signalling targets the group the bootstrap created, so ordinary
    descendants go with the leader; a descendant that made its own
    session escapes, which the containment claim already says. The direct
    child is always reaped, so no attempt leaves a zombie behind.

    ``leads_group`` is false on the one path where the helper is known not
    to lead a group of its own: its ``setsid`` failed, so it never left
    the parent's group and its pid is not a group id. Signalling by that
    number would target whatever unrelated group happens to hold it, so
    this path reaps the direct child and signals nothing -- there is no
    original group to tear down, and the helper died before reaching the
    payload, so it has no descendants to contain.

    The group is signalled on every post-spawn path, including one where
    the direct child already exited on its own. A leader's exit says
    nothing about the group it led: a background descendant it forked is
    still in that group and still holding the inherited pipes. Signalling
    only a live leader would let an ordinary clean return leave survivors
    behind, which is precisely the case the lifecycle claim exists to
    exclude.

    Escalation follows the same reasoning one step further. It fires when
    the graceful signal did not end the direct child within the
    configured termination deadline, and again after the reap if anything
    still answers for the group -- a survivor that ignored the graceful
    signal has already shown that waiting on it proves nothing. With no
    declared termination budget there is no grace period to observe, so
    the first escalation is immediate: an unbudgeted axis installs no
    executor policy limit, and a graceful signal with no bounded grace
    period is a wait with no end.
    """
    started_ns = time.monotonic_ns()
    if not leads_group:
        # The same graceful-then-escalate policy, aimed at the one process
        # this path can name: there is no group, so there is nothing else
        # to reach.
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
    if not _reaped_within(process, _finite_ns(self_budgets.termination_time)):
        signal_process_group(process.pid, ESCALATION_SIGNAL)
    # Reap before probing: an unreaped leader is still a group member, so
    # the probe would report the group alive on the strength of the
    # zombie the parent has simply not collected yet.
    process.wait()
    if _group_survives(process.pid):
        signal_process_group(process.pid, ESCALATION_SIGNAL)
    return time.monotonic_ns() - started_ns


def _group_survives(pid: int, /) -> bool:
    """Report whether anything still answers for the original process group.

    Signal zero is the existence probe: it runs the kernel's permission
    and membership checks and delivers nothing. A group outlives its
    leader as long as one member remains, so this is what distinguishes
    "the run is over" from "the leader left survivors behind". A reaped
    leader with no survivors makes the group gone and the probe false.
    """
    return signal_process_group(pid, 0)


def _reaped_within(
    process: subprocess.Popen[bytes],
    termination_ns: int | None,
    /,
) -> bool:
    """Report whether the child was reaped within its termination budget.

    With no declared termination budget there is no deadline to wait out,
    so the answer is ``False`` rather than an invented grace period: an
    unbudgeted axis installs no executor policy limit, and a graceful
    signal with no bounded grace period is a wait with no end.
    """
    if termination_ns is None:
        return False
    try:
        process.wait(timeout=termination_ns / 1_000_000_000)
    except subprocess.TimeoutExpired:
        return False
    return True


@dataclass(slots=True)
class _Transports:
    """The parent ends of one child's transports and their threads.

    Every descriptor here belongs to this attempt, and ``close`` is what
    guarantees none outlives it: the pump is released first so nothing is
    reading a descriptor at the moment it goes away. A descriptor handed
    to a thread that closes it is taken out of this frame first, so a
    handoff that never happened -- because a thread never started -- still
    leaves the descriptor here for ``close`` to release.

    ``adopt`` is what keeps that true of threads too: a started thread is
    registered before the next one is started, so a start that raises
    still leaves every already-started thread joinable and releasable.
    """

    stdin_write: int | None
    stdout_read: int
    stderr_read: int
    protocol_read: int | None
    status_read: int
    release_read: int
    release_write: int
    protocol_forward_read: int | None
    protocol_forward_write: int | None
    threads: tuple[Thread, ...] = ()

    def take_stdin(self) -> int:
        """Hand the stdin write end to the feed thread that closes it."""
        descriptor = self.stdin_write
        if descriptor is None:  # pragma: no cover - one call per attempt
            raise ExecutorFailure("the stdin transport was already taken")
        self.stdin_write = None
        return descriptor

    def take_protocol_reader(self) -> int:
        """Hand the forwarded protected stream to its reader thread."""
        descriptor = self.protocol_forward_read
        if descriptor is None:  # pragma: no cover - one call per attempt
            raise ExecutorFailure("the protocol transport was already taken")
        self.protocol_forward_read = None
        return descriptor

    def take_protocol_forward_write(self) -> int | None:
        """Hand the forward pipe's write end to the pump that closes it.

        Closing this end is what gives the protocol reader its EOF, and
        the pump is the only component that can close it at the right
        moment -- after the last forwarded byte. Until the pump holds it
        this frame does, so a pump that never ran cannot strand the
        reader on a write end nobody closes.
        """
        descriptor = self.protocol_forward_write
        self.protocol_forward_write = None
        return descriptor

    def adopt(self, thread: Thread, /) -> None:
        """Register one started thread as this frame's to join and release."""
        self.threads = (*self.threads, thread)

    def release(self) -> None:
        """Wake the pump so it stops reading and lets its thread end."""
        with suppress(OSError):
            os.write(self.release_write, b"\0")

    def join(self, self_budgets: ExecutorSelfBudgets, /) -> None:
        """Join the transport threads, or refuse to invent a result.

        This only waits and raises: releasing the pump and closing the
        parent ends belong to ``close``, which ``_run_spawned``'s
        ``finally: transports.close()`` runs on every exit path, this
        raise included.

        After group teardown the inherited pipes should reach EOF. If a
        finite join budget expires first, output and measurements are no
        longer trustworthy, so the call raises rather than reporting
        numbers it cannot stand behind. With no declared join budget there
        is no deadline, so an escaped descriptor holder can hold the call
        -- which is exactly what an unbudgeted join axis promises.
        """
        join_ns = _finite_ns(self_budgets.join_time)
        deadline = None if join_ns is None else time.monotonic_ns() + join_ns
        for thread in self.threads:
            remaining = (
                None
                if deadline is None
                else max(0.0, (deadline - time.monotonic_ns()) / 1e9)
            )
            thread.join(remaining)
        if any(thread.is_alive() for thread in self.threads):
            raise ExecutorFailure(
                "payload transports did not reach EOF within the join budget"
            )

    def close(self) -> None:
        """Release the pump, wait for its threads, then free every end.

        The wait carries no deadline of its own: ``join_time`` is the one
        axis that decides how long transports may hold the call, and a
        second finite limit here would be one no declaration can spell.
        Waiting is what makes the closes below safe -- a thread still
        reading a descriptor this frame closed would be reading one the
        kernel may have already handed to something else.
        """
        self.release()
        for thread in self.threads:
            thread.join()
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
    """Classify one bootstrap setup failure into its recognized outcome.

    Spawn absence is ``ENOENT`` reported by the payload ``exec`` and
    nothing else. The reporting stage is load-bearing here: an earlier
    setup step can fail with the same errno for an unrelated reason -- a
    missing scratch directory, say -- and reporting that as a missing
    executable would name the wrong thing absent. Every other setup
    failure preserves its errno as a machine-attributed spawn failure.
    """
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
    """Interpret one reaped exit status as data, never as a verdict."""
    if returncode < 0:
        return SignaledOutcome(signal_number=-returncode)
    return ExitedOutcome(exit_code=returncode)


def _attribute(outcome: ExecutionOutcome, /) -> ExecutionAttribution:
    """Classify one outcome's owner from the evidence, best effort.

    ``owner`` is a diagnostic classification, not causal proof and not a
    retry guarantee. Where the evidence does not distinguish a payload
    fault from an executor one -- an ordinary nonzero exit is the common
    case -- the classification stays with the payload that produced it,
    and where there is no failure at all the owner is explicitly none.
    """
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
            return ExecutionAttribution(
                owner=FailureOwner.PAYLOAD,
                detail=f"the payload exceeded its {outcome.axis} budget",
            )
        case ProtocolFailedOutcome():
            # An oversized frame is the executor's own finite self-budget
            # stopping the stream, so it is attributed to the executor
            # rather than to the payload: an executor limit must not
            # masquerade as a payload crash. Every other protocol code
            # describes a stream the payload actually emitted -- bad
            # bytes, a frame out of position, a mismatched identity, a
            # repeated sequence, or a stream that simply stopped -- and
            # stays with the payload that produced it.
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


def _empty_payload_outputs() -> PayloadOutputs:
    """The payload evidence of an attempt whose child never ran."""
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
    """Finalize, absorbing a machinery failure into a degraded receipt.

    ``RunStore.finalize`` already answers with a receipt for the failures
    it owns. What is absorbed here is the residual: a store whose
    finalization raises at all still must not replace the execution
    outcome, so the raise becomes degradation rather than the call's
    result.

    ``prior_failures`` are the post-start publications that already
    degraded before this finalize. They are carried into the receipt
    whether or not the finalize itself succeeds, because a record that
    reached ``finalized`` by way of a publication that never landed is
    still a degraded record, and the caller can only know that from here.
    """
    try:
        receipt = store.finalize(run, result)
    except ExecutorFailure:
        return _degraded_receipt(run, "finalize", prior_failures)
    if not prior_failures:
        return receipt
    return DegradedRecordReceipt(
        execution_id=receipt.execution_id,
        record_dir=receipt.record_dir,
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
    """Name the latest state the handle proves, without touching disk.

    The store's own degraded receipt consults the record; this one cannot,
    because the store is the thing that just failed. A handle proves only
    a lower bound on what was published, so it is the claim made here.
    """
    return DegradedRecordReceipt(
        execution_id=run.execution_id,
        record_dir=run.record_dir,
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
    """One call-scoped attempt. Nothing here outlives the call.

    Every mutable process, scratch, recording, and I/O value is created
    inside ``run`` and referenced only from its frame, so concurrent calls
    in one parent share nothing but the immutable runtime, store, and
    self-budgets they were handed.
    """

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
        """Execute one job and return its one completion.

        Validation and platform refusal happen before anything durable
        exists. Every path that returns a completion finalizes exactly
        once; a machinery failure that leaves no trustworthy result raises
        instead, leaving the latest published lifecycle state on disk. From
        the spawn onward every exit path, returning or raising, tears down
        the group and reaps the child.
        """
        _validate_platform()
        target = _target_of(job, self.runtime)
        _validate_input_budget(job, target.stdin_bytes)
        environment = _granted_environment(job.env)
        executable = _resolve_executable(target.argv, environment)

        execution_id = ExecutionId(
            job_id=job.job_id,
            attempt_id=AttemptId(uuid4()),
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
        """Record one recognized pre-child outcome without a spawn.

        The attempt never had a child, so its measurements describe an
        empty window at the moment the outcome was recognized rather than
        a fabricated execution.
        """
        moment = _now()
        result = ExecutionResult(
            execution_id=prepared.execution_id,
            outcome=outcome,
            attribution=_attribute(outcome),
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
        """Launch the child and carry the attempt through teardown.

        The pipes are created here and every descriptor is accounted for:
        the parent's copies of the child ends are closed as soon as the
        child holds its own, and every parent end is closed by
        ``_Transports.close`` on every exit path, return or raise.
        """
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
            # No child exists, so nothing needs teardown; what does need
            # doing is releasing every descriptor this frame owns, both
            # ends included, before the machinery failure leaves.
            _close_descriptors(child_ends)
            transports.close()
            raise ExecutorFailure(
                "could not start the execution bootstrap"
            ) from error
        # The child holds its own copies now, so the parent's copies of
        # the child ends must go: otherwise nothing ever sees EOF.
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
        """Everything between a live child and a finalized record.

        The whole body is inside the ``try`` whose ``finally`` tears down
        and reaps, because a live child exists from the moment this is
        entered: a machinery failure anywhere here -- a store that raises
        an unexpected type, a thread that cannot start, a startup budget
        that expires -- leaves through the same lifecycle work an ordinary
        return does. Teardown is what the ``finally`` owns and nothing
        else does; the descriptors are owned one frame out, by
        ``_run_spawned``'s ``finally: transports.close()``.

        The setup status is read first: until the status pipe reaches EOF
        the payload has not been reached, so nothing downstream can
        confuse a setup failure with a payload that exited immediately.
        A setup failure still tears down and reaps -- the helper is a
        real child either way -- and joins the transports through the
        declared ``join_time`` before it completes, so that axis is the
        only thing that ever bounds a transport wait.
        """
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
        """Record what the live child did, into the caller's observation.

        Nothing is returned, because the caller must be able to tear down
        and complete from a partial observation: whatever this reached
        before a machinery failure is already recorded where the caller
        can see it.
        """
        setup_failure = parse_setup_status(
            _read_setup_status(
                transports.status_read,
                _finite_ns(self.self_budgets.startup_time),
            )
        )
        if setup_failure is not None:
            observation.setup_failure = setup_failure
            # A helper whose ``setsid`` failed never left the parent's
            # group, so its pid is not a group id to signal.
            observation.leads_group = (
                setup_failure.stage != SETUP_STAGE_SESSION
            )
            return
        state = _DrainState(
            retention=PayloadRetention.for_budget(job.budgets.payload_output)
        )
        observation.state = state
        # Draining comes first, before any further parent-side work. The
        # status pipe reaches EOF at the payload's ``exec``, so the payload
        # is already running and already able to fill a pipe buffer; any
        # parent step taken before the transports are live blocks the child
        # in the kernel and charges that stall to the payload's wall-clock
        # budget. ``mark_running`` in particular is a durable publish.
        self._start_transports(target, transports, state)
        self._mark_running(observation, process, started_at)
        observation.stop = _await_child(
            process,
            state,
            deadline_ns=self._deadline_ns(job, started_ns),
            fail_on_overflow=_fails_on_overflow(job),
            cancellation=cancellation,
        )

    def _mark_running(
        self,
        observation: _AttemptObservation,
        process: subprocess.Popen[bytes],
        started_at: datetime,
        /,
    ) -> None:
        """Publish the ``running`` manifest, degrading rather than failing.

        This is a post-start publication: its failure is recording
        degradation, so the attempt continues from the prepared handle,
        which remains the latest valid state on disk. Continuing is not
        forgetting -- the failure is accumulated on the observation so the
        caller's receipt names it, because a degradation the caller cannot
        read is one that did not happen as far as the caller knows.
        """
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
        """Feed input and drain every output channel concurrently.

        Sequential handling of these pipes deadlocks as soon as one fills,
        so the input feed and the output pump run at once and neither
        waits on the other. A thread that closes the descriptor it was
        handed takes it out of ``transports`` first; everything else is
        closed once, by ``_Transports.close``.

        Starting a thread is the one step here that can fail on its own --
        a parent that cannot allocate another OS thread raises -- so each
        start is published before the next is attempted, and a descriptor
        is handed over only once its thread is running. A partial start
        therefore leaves every started thread joinable and every
        descriptor owned by exactly one closer.
        """
        payload = target.stdin_bytes
        # The feed thread takes the stdin write end, because closing it
        # is what gives the child its EOF; handing ownership over is what
        # keeps that close from also happening in ``close``.
        self._adopt_started(
            transports,
            lambda descriptor: _feed_stdin(descriptor, payload),
            "dr-exec-stdin",
            take=transports.take_stdin,
        )
        # The pump closes the forward pipe's write end when it stops, which
        # is what gives the protocol reader its EOF.
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
        """Start one transport thread and register it before returning.

        The descriptor is taken before the start, because the thread body
        needs it, and released here if the start fails: an unstarted
        thread closes nothing, and the descriptor is already out of
        ``transports``, so this is the only remaining closer.
        """
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
        """Select one outcome from the recorded evidence, by precedence.

        The pinned order after teardown is spawn absence, output budget,
        wall-clock budget, then exit-status interpretation. Spawn absence
        never reaches here -- it is settled before the child is awaited --
        so this is the rest of the ladder, with a recorded output
        violation beating both the deadline and a clean exit. A protocol
        failure is read after the child's own status because it describes
        a stream that the child's exit already ended.
        """
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
        """Assemble the result and finalize the record exactly once.

        Duration spans the spawn through the reap on the monotonic clock,
        with parent setup excluded and teardown measured separately. Any
        post-start recording degradation the attempt already absorbed is
        carried into the receipt alongside whatever the finalize reports.
        """
        result = ExecutionResult(
            execution_id=run.execution_id,
            outcome=outcome,
            attribution=_attribute(outcome),
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
    """The one private entry point into the engine.

    Public entry points delegate here; nothing else in v1 spawns a child.
    """
    return _EngineCall(
        runtime=runtime,
        run_store=run_store,
        self_budgets=self_budgets,
    ).run(job, cancellation=cancellation)


__all__ = ["SUPPORTED_PLATFORM"]
