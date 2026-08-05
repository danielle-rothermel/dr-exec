"""The single-run engine against real children, for command targets.

Every case here spawns a real macOS child and synchronizes on explicit
files, FIFO gates, and terminal outcomes. Nothing waits on elapsed time or
treats the passage of time as evidence: where a case must observe a child
that is deliberately still running, it releases the child through a gate it
created and then reads the terminal outcome. Deadlines appear only as
watchdogs, and every case carries one so a hung child cannot hang the
suite.

macOS process semantics -- sessions, process groups, group-targeted
teardown, direct-child reaping -- are what these cases exercise, so they
are marked and skipped off darwin. Their passing on macOS is the
qualification evidence for the lifecycle claims.
"""

from __future__ import annotations

import errno
import fcntl
import os
import signal
import subprocess
import sys
import threading
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest
from dr_serialize import build_identity_document
from support.process import (
    Gate,
    finish_threaded_calls,
    start_threaded_calls,
)

import dr_exec.execution.engine
import dr_exec.execution.spawn
from dr_exec import (
    BudgetAxis,
    BudgetExceededOutcome,
    Budgets,
    CancelledOutcome,
    CancelToken,
    CompleteRecordReceipt,
    ContainmentProfile,
    DeclarationError,
    DegradedRecordReceipt,
    DirectoryRunStore,
    EnvGrant,
    ExecutionJob,
    ExecutionResult,
    ExecutorFailure,
    ExecutorSelfBudgets,
    ExitedOutcome,
    FailureOwner,
    FinalizedRecord,
    FiniteByteLimit,
    FiniteDurationLimit,
    FiniteOutput,
    IsolatedHostPythonRuntime,
    JobId,
    OutputOverflowPolicy,
    PayloadRetentionBudget,
    PreparedRecord,
    ProcessRecord,
    RealRecordReceipt,
    RecordState,
    RunningRecord,
    RunRecord,
    RunStore,
    SignaledOutcome,
    SpawnAbsentOutcome,
    SpawnFailedOutcome,
    StreamRetentionBudget,
    TrustedCommandTarget,
    TrustedCommandTargetRecord,
    UntrustedCommandTarget,
    UntrustedCommandTargetRecord,
    UntrustedPythonTarget,
)
from dr_exec.execution.engine import SCRATCH_DIRECTORY_PREFIX, run_execution
from dr_exec.execution.spawn import (
    SETUP_STAGE_CHDIR,
)
from dr_exec.recording.store import FinalizableRun, PreparedRun, RunningRun

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

    from dr_exec.recording.models import CompletedExecution

requires_macos = pytest.mark.skipif(
    sys.platform != "darwin",
    reason="real macOS process semantics",
)
pytestmark = [
    pytest.mark.integration,
    pytest.mark.subprocess,
    pytest.mark.platform_macos,
]

# Watchdog only. It bounds a case that would otherwise hang the suite; no
# case ever asserts on it, and no case uses its non-expiry as evidence.
# Watchdogs expressed as budgets. A test that needs the engine itself to
# stop an intentionally-immortal child declares one of these; the case
# then asserts on the terminal outcome, never on how long it took.
WATCHDOG_WALL_TIME = FiniteDurationLimit(max_ns=5_000_000_000)
WATCHDOG_JOIN_TIME = FiniteDurationLimit(max_ns=5_000_000_000)
ESCAPEE_JOIN_TIME = FiniteDurationLimit(max_ns=500_000_000)

# An input far past any pipe buffer a kernel offers, so a child that never
# reads it leaves the feed mid-payload rather than absorbing it all.
UNREADABLE_STDIN_BYTES = 8 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class Harness:
    """One temporary record root plus the runtime and budgets to run it."""

    store: DirectoryRunStore
    root: Path
    runtime: IsolatedHostPythonRuntime

    def run(
        self,
        argv: tuple[str, ...],
        /,
        *,
        stdin: bytes = b"",
        env: EnvGrant | None = None,
        budgets: Budgets | None = None,
        self_budgets: ExecutorSelfBudgets | None = None,
        cancellation: CancelToken | None = None,
        untrusted: bool = False,
    ) -> CompletedExecution:
        target = (
            UntrustedCommandTarget(
                argv=argv,
                stdin=stdin,
                containment_profile=(ContainmentProfile.PROCESS_BOUNDARY_ONLY),
            )
            if untrusted
            else TrustedCommandTarget(argv=argv, stdin=stdin)
        )
        job = ExecutionJob(
            job_id=JobId(uuid4()),
            target=target,
            env=env if env is not None else EnvGrant.none(),
            budgets=budgets if budgets is not None else Budgets.unbudgeted(),
        )
        return self.execute(
            job,
            self_budgets=self_budgets,
            cancellation=cancellation,
        )

    def execute(
        self,
        job: ExecutionJob,
        /,
        *,
        self_budgets: ExecutorSelfBudgets | None = None,
        cancellation: CancelToken | None = None,
        store: RunStore | None = None,
    ) -> CompletedExecution:
        return run_execution(
            job,
            runtime=self.runtime,
            run_store=self.store if store is None else store,
            self_budgets=(
                ExecutorSelfBudgets.unbudgeted()
                if self_budgets is None
                else self_budgets
            ),
            cancellation=cancellation,
        )

    def only_record_dir(self) -> Path:
        (directory,) = sorted(self.root.iterdir())
        return directory


def record_dir_of(completed: CompletedExecution, /) -> Path:
    """Read the record location a real receipt always carries.

    A production call never yields the fake receipt, so narrowing here
    keeps the assertion about the record rather than about which receipt
    variant arrived.
    """
    receipt = completed.record_receipt
    assert isinstance(receipt, CompleteRecordReceipt | DegradedRecordReceipt)
    return receipt.record_dir


def degraded_receipt_of(
    completed: CompletedExecution, /
) -> DegradedRecordReceipt:
    receipt = completed.record_receipt
    assert isinstance(receipt, DegradedRecordReceipt)
    return receipt


def finalized_record(
    store: DirectoryRunStore, record_dir: Path, /
) -> FinalizedRecord:
    record = store.load(record_dir)
    assert isinstance(record, FinalizedRecord)
    return record


@pytest.fixture
def harness(
    tmp_path: Path, host_runtime: IsolatedHostPythonRuntime
) -> Harness:
    root = tmp_path / "records"
    root.mkdir()
    return Harness(
        store=DirectoryRunStore(root=root),
        root=root,
        runtime=host_runtime,
    )


def python_command(source: str, /) -> tuple[str, ...]:
    """One real child that is a fresh isolated interpreter, not a shell."""
    return (sys.executable, "-I", "-c", source)


# --- Recognized outcomes -------------------------------------------------


@requires_macos
def test_a_clean_exit_returns_exit_data_with_no_failure_owner(
    harness: Harness,
) -> None:
    completed = harness.run(python_command("pass"))

    assert completed.result.outcome == ExitedOutcome(exit_code=0)
    assert completed.result.attribution.owner is FailureOwner.NONE


@requires_macos
def test_a_nonzero_exit_stays_raw_data_attributed_to_the_payload(
    harness: Harness,
) -> None:
    completed = harness.run(python_command("raise SystemExit(7)"))

    assert completed.result.outcome == ExitedOutcome(exit_code=7)
    assert completed.result.attribution.owner is FailureOwner.PAYLOAD


@requires_macos
def test_a_signalled_child_reports_its_signal_number(
    harness: Harness,
) -> None:
    completed = harness.run(
        python_command(
            "import os, signal; os.kill(os.getpid(), signal.SIGKILL)"
        )
    )

    assert completed.result.outcome == SignaledOutcome(
        signal_number=signal.SIGKILL
    )


@requires_macos
def test_a_missing_executable_is_spawn_absence_not_a_raise(
    harness: Harness,
) -> None:
    """Absence is a recognized outcome, so it is data rather than an error.

    It also names the executable that was missing, which the durable
    record deliberately drops.
    """
    completed = harness.run(("/nonexistent/definitely-not-here",))

    assert completed.result.outcome == SpawnAbsentOutcome(
        executable="/nonexistent/definitely-not-here"
    )
    assert completed.result.attribution.owner is FailureOwner.EXECUTOR
    assert isinstance(completed.record_receipt, CompleteRecordReceipt)


@requires_macos
def test_a_non_executable_file_is_a_spawn_failure_preserving_its_errno(
    harness: Harness, tmp_path: Path
) -> None:
    unreadable = tmp_path / "not-executable"
    unreadable.write_text("#!/bin/sh\n")
    unreadable.chmod(0o600)

    completed = harness.run((unreadable.as_posix(),))

    outcome = completed.result.outcome
    assert isinstance(outcome, SpawnFailedOutcome)
    assert outcome.errno == errno.EACCES
    assert completed.result.attribution.owner is FailureOwner.MACHINE


@requires_macos
def test_a_bootstrap_setup_failure_names_the_stage_that_failed(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A setup stage before `exec` is a spawn failure, never absence.

    The scratch directory is removed between its creation and the spawn,
    so the helper's `chdir` fails with the same `ENOENT` a missing
    executable would report. Classifying on the errno alone would name
    the wrong thing missing.
    """
    original = dr_exec.execution.engine._scratch_workspace

    @contextmanager
    def vanishing_scratch() -> Iterator[Path]:
        with original() as directory:
            directory.rmdir()
            yield directory

    monkeypatch.setattr(
        dr_exec.execution.engine, "_scratch_workspace", vanishing_scratch
    )
    completed = harness.run(python_command("pass"))

    outcome = completed.result.outcome
    assert isinstance(outcome, SpawnFailedOutcome)
    assert outcome.errno == errno.ENOENT
    assert outcome.error_message == SETUP_STAGE_CHDIR


# --- Transports ----------------------------------------------------------


@requires_macos
def test_declared_stdin_reaches_the_child_and_is_followed_by_eof(
    harness: Harness,
) -> None:
    completed = harness.run(
        python_command(
            "import sys; sys.stdout.write(sys.stdin.buffer.read().decode())"
        ),
        stdin=b"exactly these bytes",
    )

    assert completed.result.outcome == ExitedOutcome(exit_code=0)
    assert completed.result.payload_outputs.stdout.head == (
        b"exactly these bytes"
    )
    assert completed.result.measurements.input_bytes == len(
        b"exactly these bytes"
    )


@requires_macos
def test_payload_stdout_and_stderr_stay_separate_raw_byte_channels(
    harness: Harness,
) -> None:
    """No decoding, no newline normalization, no interleaving."""
    completed = harness.run(
        python_command(
            "import sys\n"
            "sys.stdout.buffer.write(b'out\\r\\n\\x00\\xff')\n"
            "sys.stderr.buffer.write(b'err\\r\\n')\n"
        )
    )

    assert completed.result.payload_outputs.stdout.head == b"out\r\n\x00\xff"
    assert completed.result.payload_outputs.stderr.head == b"err\r\n"


@requires_macos
def test_both_streams_drain_concurrently_past_one_pipe_buffer(
    harness: Harness,
) -> None:
    """Sequential draining would deadlock here; concurrent draining does not.

    Each stream writes far more than a pipe buffer holds, so a parent that
    drained one to EOF before starting the other could never finish.
    """
    volume = 1 << 20
    completed = harness.run(
        python_command(
            "import sys\n"
            f"sys.stdout.buffer.write(b'o' * {volume})\n"
            f"sys.stderr.buffer.write(b'e' * {volume})\n"
        )
    )

    assert completed.result.outcome == ExitedOutcome(exit_code=0)
    assert completed.result.payload_outputs.stdout.produced_bytes == volume
    assert completed.result.payload_outputs.stderr.produced_bytes == volume
    assert completed.result.payload_outputs.stdout.head == b"o" * volume


# --- Containment and inherited state -------------------------------------


@requires_macos
def test_a_command_child_excludes_a_high_inheritable_parent_descriptor(
    harness: Harness,
) -> None:
    """``close_fds`` excludes inheritable descriptors above a low scan."""
    seed_read, seed_write = os.pipe()
    high_descriptor = fcntl.fcntl(seed_read, fcntl.F_DUPFD, 512)
    os.set_inheritable(high_descriptor, True)
    try:
        assert high_descriptor >= 512
        assert os.get_inheritable(high_descriptor)
        completed = harness.run(
            python_command(
                "import os, sys\n"
                "try:\n"
                f"    os.fstat({high_descriptor})\n"
                "except OSError:\n"
                "    inherited = False\n"
                "else:\n"
                "    inherited = True\n"
                "sys.stdout.write(repr(inherited))\n"
            )
        )
    finally:
        os.close(high_descriptor)
        os.close(seed_write)
        os.close(seed_read)

    assert completed.result.payload_outputs.stdout.head == b"False"


@requires_macos
def test_the_child_receives_only_the_granted_environment(
    harness: Harness,
) -> None:
    """The grant is the whole inherited environment dr-exec installs.

    CPython and macOS's own runtime add a small fixed set of variables
    inside the child after exec; those are platform artifacts rather than
    values dr-exec passed, and a plain interpreter started with an empty
    environment shows the same ones. What this pins is that no parent
    variable outside the grant survives.
    """
    os.environ["DR_EXEC_TEST_AMBIENT_SECRET"] = "must not reach the child"
    try:
        completed = harness.run(
            python_command(
                "import json, os, sys\n"
                "sys.stdout.write(json.dumps(sorted(os.environ)))\n"
            ),
            env=EnvGrant.fixed({"GRANTED": "value"}),
        )
    finally:
        del os.environ["DR_EXEC_TEST_AMBIENT_SECRET"]

    names = completed.result.payload_outputs.stdout.head.decode()
    assert "GRANTED" in names
    assert "DR_EXEC_TEST_AMBIENT_SECRET" not in names


@requires_macos
def test_a_named_grant_snapshots_values_when_the_grant_is_built(
    harness: Harness,
) -> None:
    """Live-grant snapshot semantics: the grant, not the run, reads os.environ."""
    os.environ["DR_EXEC_TEST_SNAPSHOT"] = "at construction"
    try:
        grant = EnvGrant.named(["DR_EXEC_TEST_SNAPSHOT"])
        os.environ["DR_EXEC_TEST_SNAPSHOT"] = "changed after construction"
        completed = harness.run(
            python_command(
                "import os, sys\n"
                "sys.stdout.write(os.environ['DR_EXEC_TEST_SNAPSHOT'])\n"
            ),
            env=grant,
        )
    finally:
        del os.environ["DR_EXEC_TEST_SNAPSHOT"]

    assert completed.result.payload_outputs.stdout.head == b"at construction"


@requires_macos
def test_the_child_runs_in_a_fresh_scratch_directory_removed_afterwards(
    harness: Harness,
) -> None:
    completed = harness.run(
        python_command("import os, sys; sys.stdout.write(os.getcwd())")
    )

    reported = Path(
        completed.result.payload_outputs.stdout.head.decode()
    ).resolve()
    assert reported.name.startswith(SCRATCH_DIRECTORY_PREFIX)
    assert not reported.exists()


@requires_macos
def test_two_runs_never_share_a_scratch_directory(
    harness: Harness,
) -> None:
    source = "import os, sys; sys.stdout.write(os.getcwd())"
    first = harness.run(python_command(source))
    second = harness.run(python_command(source))

    assert (
        first.result.payload_outputs.stdout.head
        != second.result.payload_outputs.stdout.head
    )


@requires_macos
def test_the_child_leads_a_fresh_session_and_process_group(
    harness: Harness,
) -> None:
    completed = harness.run(
        python_command(
            "import os, sys\n"
            "sys.stdout.write(repr((os.getpid(), os.getpgrp(), os.getsid(0))))\n"
        )
    )

    pid, pgid, sid = eval(
        completed.result.payload_outputs.stdout.head.decode()
    )
    assert pid == pgid == sid
    assert pgid != os.getpgrp()


# --- Argv resolution -----------------------------------------------------


@requires_macos
def test_a_relative_executable_resolves_through_the_granted_path(
    harness: Harness, tmp_path: Path
) -> None:
    tool_dir = tmp_path / "bin"
    tool_dir.mkdir()
    tool = tool_dir / "dr-exec-test-tool"
    tool.write_text("#!/bin/sh\nprintf resolved\n")
    tool.chmod(0o700)

    completed = harness.run(
        ("dr-exec-test-tool",),
        env=EnvGrant.fixed({"PATH": tool_dir.as_posix()}),
    )

    assert completed.result.payload_outputs.stdout.head == b"resolved"


@requires_macos
def test_a_relative_executable_without_a_granted_path_is_refused(
    harness: Harness,
) -> None:
    """Refused before anything durable exists, and before any spawn."""
    with pytest.raises(DeclarationError, match="granted PATH"):
        harness.run(("dr-exec-test-tool",))

    assert list(harness.root.iterdir()) == []


@requires_macos
def test_an_absolute_executable_resolves_without_any_granted_path(
    harness: Harness,
) -> None:
    completed = harness.run(python_command("pass"))

    assert completed.result.outcome == ExitedOutcome(exit_code=0)


@requires_macos
def test_a_relative_granted_path_entry_is_refused_before_anything_durable(
    harness: Harness, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A relative entry names nothing the child could reach.

    The tool really exists at ``<cwd>/bin/<name>``, so the search would
    succeed and hand back a relative hit; the child chdirs to its fresh
    scratch directory before ``exec``, where that hit resolves to
    nothing. Reading the entry against the parent's location instead
    would be the ambient cwd the engine never consults, so the
    declaration is refused rather than resolved.
    """
    tool_dir = tmp_path / "bin"
    tool_dir.mkdir()
    tool = tool_dir / "dr-exec-test-tool"
    tool.write_text("#!/bin/sh\nprintf resolved\n")
    tool.chmod(0o700)
    monkeypatch.chdir(tmp_path)

    with pytest.raises(DeclarationError, match="absolute entries"):
        harness.run(
            ("dr-exec-test-tool",),
            env=EnvGrant.fixed({"PATH": "bin"}),
        )

    assert list(harness.root.iterdir()) == []


@requires_macos
def test_an_empty_granted_path_entry_is_refused_before_anything_durable(
    harness: Harness, tmp_path: Path
) -> None:
    """An empty entry is the current directory, spelled shorter."""
    tool_dir = tmp_path / "bin"
    tool_dir.mkdir()

    with pytest.raises(DeclarationError, match="absolute entries"):
        harness.run(
            ("dr-exec-test-tool",),
            env=EnvGrant.fixed({"PATH": f"{tool_dir.as_posix()}:"}),
        )

    assert list(harness.root.iterdir()) == []


@requires_macos
def test_a_relative_name_absent_from_the_granted_path_is_spawn_absence(
    harness: Harness, tmp_path: Path
) -> None:
    empty = tmp_path / "empty-bin"
    empty.mkdir()

    completed = harness.run(
        ("dr-exec-test-missing",),
        env=EnvGrant.fixed({"PATH": empty.as_posix()}),
    )

    assert completed.result.outcome == SpawnAbsentOutcome(
        executable="dr-exec-test-missing"
    )


@requires_macos
def test_argv_reaches_the_child_verbatim_without_shell_interpretation(
    harness: Harness,
) -> None:
    hostile = "$(echo pwned); `id`; a b\t*"
    completed = harness.run(
        (
            sys.executable,
            "-I",
            "-c",
            "import sys; sys.stdout.write(sys.argv[1])",
            hostile,
        )
    )

    assert completed.result.payload_outputs.stdout.head == hostile.encode()


# --- Platform and target validation --------------------------------------


def test_an_unsupported_platform_is_refused_before_anything_durable(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The refusal is the declaration boundary, so it holds off darwin too."""
    monkeypatch.setattr(sys, "platform", "linux")

    with pytest.raises(DeclarationError, match="darwin"):
        harness.run(python_command("pass"))

    assert list(harness.root.iterdir()) == []


@requires_macos
def test_every_declared_target_kind_runs_through_the_one_engine_path(
    harness: Harness,
) -> None:
    """All three target kinds reach a completion through the same path.

    The Python target's own behavior is qualified separately; what this
    pins is that the engine's target dispatch is total over the declared
    union, so no declared kind is left refused.
    """
    python_job = ExecutionJob(
        job_id=JobId(uuid4()),
        target=UntrustedPythonTarget(
            driver_source="def dr_exec_main(request, emit): pass",
            request=build_identity_document(
                schema="dr_exec.test_request",
                schema_version=1,
                payload={},
            ),
            containment_profile=ContainmentProfile.PROCESS_BOUNDARY_ONLY,
        ),
        env=EnvGrant.none(),
        budgets=Budgets.unbudgeted(),
    )
    completions = [
        harness.run(python_command("pass")),
        harness.run(python_command("pass"), untrusted=True),
        harness.execute(python_job),
    ]

    for completed in completions:
        assert completed.result.outcome == ExitedOutcome(exit_code=0)
        assert isinstance(completed.record_receipt, CompleteRecordReceipt)


@requires_macos
def test_an_untrusted_command_records_its_containment_profile(
    harness: Harness,
) -> None:
    completed = harness.run(python_command("pass"), untrusted=True)

    record = finalized_record(harness.store, record_dir_of(completed))
    target_record = record.declaration.target
    assert isinstance(target_record, UntrustedCommandTargetRecord)
    assert target_record.containment_profile is (
        ContainmentProfile.PROCESS_BOUNDARY_ONLY
    )


# --- Budgets -------------------------------------------------------------


@requires_macos
def test_an_over_budget_input_is_refused_before_any_spawn(
    harness: Harness,
) -> None:
    with pytest.raises(DeclarationError, match="input budget"):
        harness.run(
            python_command("pass"),
            stdin=b"0123456789",
            budgets=Budgets(input_bytes=FiniteByteLimit(max_bytes=4)),
        )

    assert list(harness.root.iterdir()) == []


@requires_macos
def test_an_input_exactly_at_its_budget_is_within_it(
    harness: Harness,
) -> None:
    completed = harness.run(
        python_command(
            "import sys; sys.stdout.write(sys.stdin.buffer.read().decode())"
        ),
        stdin=b"0123",
        budgets=Budgets(input_bytes=FiniteByteLimit(max_bytes=4)),
    )

    assert completed.result.outcome == ExitedOutcome(exit_code=0)
    assert completed.result.measurements.input_bytes == 4


@requires_macos
def test_wall_time_overflow_terminates_an_otherwise_immortal_child(
    harness: Harness, tmp_path: Path
) -> None:
    """The child never exits on its own, so only the budget can end it.

    Its announcement on stdout is the evidence it really started, so the
    outcome is a budget termination rather than a child that died on its
    own before the deadline mattered.
    """
    del tmp_path
    completed = harness.run(
        python_command(
            "import sys, time\n"
            "sys.stdout.write('running')\n"
            "sys.stdout.flush()\n"
            "while True:\n"
            "    time.sleep(3600)\n"
        ),
        budgets=Budgets(wall_time=FiniteDurationLimit(max_ns=500_000_000)),
    )

    assert completed.result.outcome == BudgetExceededOutcome(
        axis=BudgetAxis.WALL_TIME
    )
    assert completed.result.payload_outputs.stdout.head == b"running"


@requires_macos
def test_an_unbudgeted_run_retains_every_produced_byte(
    harness: Harness,
) -> None:
    volume = 4096
    completed = harness.run(
        python_command(f"import sys; sys.stdout.buffer.write(b'x' * {volume})")
    )

    stdout = completed.result.payload_outputs.stdout
    assert stdout.head == b"x" * volume
    assert stdout.tail == b""
    assert stdout.dropped_bytes == 0
    assert stdout.produced_bytes == volume


def finite_output(
    *,
    policy: OutputOverflowPolicy,
    stdout_head: int,
    stdout_tail: int,
    stderr_head: int,
    stderr_tail: int,
) -> Budgets:
    total = stdout_head + stdout_tail + stderr_head + stderr_tail
    return Budgets(
        payload_output=FiniteOutput(
            max_bytes=total,
            overflow_policy=policy,
            retention=PayloadRetentionBudget(
                stdout=StreamRetentionBudget(
                    head_bytes=stdout_head, tail_bytes=stdout_tail
                ),
                stderr=StreamRetentionBudget(
                    head_bytes=stderr_head, tail_bytes=stderr_tail
                ),
            ),
        )
    )


@requires_macos
def test_output_exactly_at_the_aggregate_budget_is_retained_whole(
    harness: Harness,
) -> None:
    budgets = finite_output(
        policy=OutputOverflowPolicy.FAIL,
        stdout_head=4,
        stdout_tail=2,
        stderr_head=2,
        stderr_tail=0,
    )
    completed = harness.run(
        python_command(
            "import sys\n"
            "sys.stdout.buffer.write(b'abcdef')\n"
            "sys.stderr.buffer.write(b'XY')\n"
        ),
        budgets=budgets,
    )

    assert completed.result.outcome == ExitedOutcome(exit_code=0)
    stdout = completed.result.payload_outputs.stdout
    assert stdout.head + stdout.tail == b"abcdef"
    assert stdout.dropped_bytes == 0


@requires_macos
def test_marked_truncation_keeps_head_and_tail_and_exact_counts(
    harness: Harness,
) -> None:
    """The retained segments are the declaration's, not the drain's.

    Head and tail stay separate values: no marker is inserted, and nothing
    represents them as contiguous output.
    """
    budgets = finite_output(
        policy=OutputOverflowPolicy.MARKED_TRUNCATION,
        stdout_head=3,
        stdout_tail=3,
        stderr_head=1,
        stderr_tail=1,
    )
    completed = harness.run(
        python_command(
            "import sys\n"
            "sys.stdout.buffer.write(bytes(range(48, 68)))\n"
            "sys.stderr.buffer.write(b'ABCDE')\n"
        ),
        budgets=budgets,
    )

    stdout = completed.result.payload_outputs.stdout
    stderr = completed.result.payload_outputs.stderr
    assert stdout.head == bytes(range(48, 51))
    assert stdout.tail == bytes(range(65, 68))
    assert stdout.produced_bytes == 20
    assert stdout.dropped_bytes == 14
    assert stderr.head == b"A"
    assert stderr.tail == b"E"
    assert stderr.produced_bytes == 5
    assert stderr.dropped_bytes == 3
    assert completed.result.outcome == BudgetExceededOutcome(
        axis=BudgetAxis.PAYLOAD_OUTPUT
    )


@requires_macos
def test_marked_truncation_counts_production_through_eof(
    harness: Harness,
) -> None:
    """Overflow does not stop the drain, so the counts stay exact."""
    volume = 1 << 18
    budgets = finite_output(
        policy=OutputOverflowPolicy.MARKED_TRUNCATION,
        stdout_head=8,
        stdout_tail=8,
        stderr_head=0,
        stderr_tail=0,
    )
    completed = harness.run(
        python_command(
            f"import sys; sys.stdout.buffer.write(b'z' * {volume})"
        ),
        budgets=budgets,
    )

    stdout = completed.result.payload_outputs.stdout
    assert stdout.produced_bytes == volume
    assert stdout.dropped_bytes == volume - 16
    assert completed.result.outcome == BudgetExceededOutcome(
        axis=BudgetAxis.PAYLOAD_OUTPUT
    )


@requires_macos
def test_the_fail_policy_terminates_a_child_that_would_never_exit(
    harness: Harness, tmp_path: Path
) -> None:
    """Only the output budget can end this child; the wall clock is a watchdog."""
    budgets = finite_output(
        policy=OutputOverflowPolicy.FAIL,
        stdout_head=8,
        stdout_tail=0,
        stderr_head=0,
        stderr_tail=0,
    )
    budgets = Budgets(
        wall_time=WATCHDOG_WALL_TIME,
        payload_output=budgets.payload_output,
    )
    completed = harness.run(
        python_command(
            "import sys, time\n"
            "sys.stdout.buffer.write(b'y' * 4096)\n"
            "sys.stdout.flush()\n"
            "while True:\n"
            "    time.sleep(3600)\n"
        ),
        budgets=budgets,
    )

    assert completed.result.outcome == BudgetExceededOutcome(
        axis=BudgetAxis.PAYLOAD_OUTPUT
    )
    assert completed.result.payload_outputs.stdout.head == b"y" * 8


@requires_macos
def test_a_recorded_output_violation_beats_a_clean_exit(
    harness: Harness,
) -> None:
    """The child exits zero; the violation is what the outcome reports."""
    budgets = finite_output(
        policy=OutputOverflowPolicy.MARKED_TRUNCATION,
        stdout_head=2,
        stdout_tail=0,
        stderr_head=0,
        stderr_tail=0,
    )
    completed = harness.run(
        python_command("import sys; sys.stdout.buffer.write(b'abcdef')"),
        budgets=budgets,
    )

    assert completed.result.outcome == BudgetExceededOutcome(
        axis=BudgetAxis.PAYLOAD_OUTPUT
    )


@requires_macos
def test_a_recorded_output_violation_beats_the_wall_clock_deadline(
    harness: Harness,
) -> None:
    """Both bounds are crossed; the pinned precedence picks the output one."""
    budgets = finite_output(
        policy=OutputOverflowPolicy.MARKED_TRUNCATION,
        stdout_head=2,
        stdout_tail=0,
        stderr_head=0,
        stderr_tail=0,
    )
    completed = harness.run(
        python_command(
            "import sys, time\n"
            "sys.stdout.buffer.write(b'abcdef')\n"
            "sys.stdout.flush()\n"
            "while True:\n"
            "    time.sleep(3600)\n"
        ),
        budgets=Budgets(
            wall_time=FiniteDurationLimit(max_ns=500_000_000),
            payload_output=budgets.payload_output,
        ),
    )

    assert completed.result.outcome == BudgetExceededOutcome(
        axis=BudgetAxis.PAYLOAD_OUTPUT
    )


# --- Teardown and reaping ------------------------------------------------


@requires_macos
def test_teardown_reaches_the_original_process_group(
    harness: Harness, tmp_path: Path
) -> None:
    """A forked descendant in the group goes with the leader.

    The direct child announces the descendant's pid on stdout and flushes
    before either process sleeps, so the pid this case checks belongs to a
    process that provably existed and shared the group.
    """
    del tmp_path
    completed = harness.run(
        python_command(
            "import os, sys, time\n"
            "descendant = os.fork()\n"
            "if descendant == 0:\n"
            "    while True:\n"
            "        time.sleep(3600)\n"
            "sys.stdout.write(str(descendant))\n"
            "sys.stdout.flush()\n"
            "while True:\n"
            "    time.sleep(3600)\n"
        ),
        budgets=Budgets(wall_time=FiniteDurationLimit(max_ns=500_000_000)),
    )

    assert completed.result.outcome == BudgetExceededOutcome(
        axis=BudgetAxis.WALL_TIME
    )
    descendant = int(completed.result.payload_outputs.stdout.head.decode())
    with pytest.raises(ProcessLookupError):
        os.kill(descendant, 0)


@requires_macos
def test_the_direct_child_is_reaped_so_no_zombie_remains(
    harness: Harness,
) -> None:
    completed = harness.run(python_command("raise SystemExit(3)"))
    del completed

    # Nothing this parent spawned is left to wait on. `ECHILD` is the
    # kernel's own statement that every direct child was reaped.
    with pytest.raises(ChildProcessError):
        os.waitpid(-1, os.WNOHANG)


@requires_macos
def test_a_clean_exit_still_tears_down_the_group_it_led(
    harness: Harness,
) -> None:
    """Teardown is unconditional, so a clean return leaves no survivors.

    The direct child forks a descendant that outlives it and then exits
    zero. Signalling only a live leader would let this ordinary
    successful return leave a background process behind, which is exactly
    what the lifecycle claim excludes.

    The descendant keeps stdout open, so the call cannot return until the
    inherited pipe reaches EOF -- which happens only once the descendant
    is gone. The return is therefore evidence that teardown reached it,
    with no window between the signal and the check.
    """
    completed = harness.run(
        python_command(
            "import os, sys, time\n"
            "descendant = os.fork()\n"
            "if descendant == 0:\n"
            "    while True:\n"
            "        time.sleep(3600)\n"
            "sys.stdout.write(str(descendant))\n"
            "sys.stdout.flush()\n"
        ),
        self_budgets=ExecutorSelfBudgets(join_time=WATCHDOG_JOIN_TIME),
    )

    assert completed.result.outcome == ExitedOutcome(exit_code=0)
    descendant = int(completed.result.payload_outputs.stdout.head.decode())
    with pytest.raises(ProcessLookupError):
        os.kill(descendant, 0)


@requires_macos
def test_a_descendant_that_leaves_the_session_escapes_the_claim(
    harness: Harness, tmp_path: Path
) -> None:
    """The honest limit of the containment claim, pinned as behavior.

    The escapee also holds the inherited pipes open, so this is the join
    exhaustion path: no trustworthy result exists and the call raises,
    leaving the latest lifecycle state incomplete on disk.
    """
    gate = Gate.create(tmp_path, "escapee")
    escapee_pids: list[int] = []
    collector = threading.Thread(
        target=lambda: escapee_pids.append(int(gate.receive())),
        daemon=True,
    )
    collector.start()
    escapee_was_alive = False
    try:
        with pytest.raises(ExecutorFailure, match="join budget"):
            harness.run(
                python_command(
                    "import os, time\n"
                    "if os.fork() == 0:\n"
                    "    os.setsid()\n"
                    f"    gate = open({str(gate.path)!r}, 'w')\n"
                    "    gate.write(str(os.getpid()))\n"
                    "    gate.close()\n"
                    "    while True:\n"
                    "        time.sleep(3600)\n"
                    "while True:\n"
                    "    time.sleep(3600)\n"
                ),
                budgets=Budgets(
                    wall_time=FiniteDurationLimit(max_ns=500_000_000)
                ),
                self_budgets=ExecutorSelfBudgets(join_time=ESCAPEE_JOIN_TIME),
            )
    finally:
        collector.join()
        for escapee in escapee_pids:
            with suppress(ProcessLookupError):
                os.kill(escapee, 0)
                escapee_was_alive = True
            with suppress(ProcessLookupError):
                os.kill(escapee, signal.SIGKILL)

    assert len(escapee_pids) == 1
    assert escapee_was_alive

    record = harness.store.load(harness.only_record_dir())
    assert record.state is RecordState.RUNNING


@requires_macos
def test_an_escapee_holding_a_full_stdin_pipe_still_returns_the_join_failure(
    harness: Harness, tmp_path: Path
) -> None:
    """The escapee cannot pin the call to a payload it will never read.

    An input larger than the pipe buffer only drains as fast as the child
    reads it, and this child's escaped descendant holds the read end
    without ever reading. The feed thread is therefore still mid-payload
    when the join budget expires, and the raise below is what says it was
    released rather than waited on: the failure can only surface if
    ``close`` -- which joins the transport threads with no deadline of its
    own -- reached them all and returned.
    """
    gate = Gate.create(tmp_path, "escapee")
    escapee_pids: list[int] = []
    collector = threading.Thread(
        target=lambda: escapee_pids.append(int(gate.receive())),
        daemon=True,
    )
    collector.start()
    escapee_was_alive = False
    try:
        with pytest.raises(ExecutorFailure, match="join budget"):
            harness.run(
                python_command(
                    "import os, time\n"
                    "if os.fork() == 0:\n"
                    "    os.setsid()\n"
                    f"    gate = open({str(gate.path)!r}, 'w')\n"
                    "    gate.write(str(os.getpid()))\n"
                    "    gate.close()\n"
                    "    while True:\n"
                    "        time.sleep(3600)\n"
                    "while True:\n"
                    "    time.sleep(3600)\n"
                ),
                stdin=b"x" * UNREADABLE_STDIN_BYTES,
                budgets=Budgets(
                    wall_time=FiniteDurationLimit(max_ns=500_000_000)
                ),
                self_budgets=ExecutorSelfBudgets(join_time=ESCAPEE_JOIN_TIME),
            )
    finally:
        collector.join()
        for escapee in escapee_pids:
            with suppress(ProcessLookupError):
                os.kill(escapee, 0)
                escapee_was_alive = True
            with suppress(ProcessLookupError):
                os.kill(escapee, signal.SIGKILL)

    assert len(escapee_pids) == 1
    assert escapee_was_alive

    record = harness.store.load(harness.only_record_dir())
    assert record.state is RecordState.RUNNING


# --- Cancellation --------------------------------------------------------


@requires_macos
def test_pre_spawn_cancellation_records_without_launching_a_child(
    harness: Harness, tmp_path: Path
) -> None:
    """A cancelled call cannot have run its payload, which would leave a mark."""
    marker = tmp_path / "the-child-ran"
    token = CancelToken()
    token.cancel()

    completed = harness.run(
        python_command(
            f"import pathlib; pathlib.Path({str(marker)!r}).write_text('ran')"
        ),
        cancellation=token,
    )

    assert completed.result.outcome == CancelledOutcome()
    assert completed.result.attribution.owner is FailureOwner.NONE
    assert not marker.exists()
    record = harness.store.load(record_dir_of(completed))
    assert record.state is RecordState.FINALIZED


@requires_macos
def test_post_spawn_cancellation_tears_down_and_returns_cancelled(
    harness: Harness, tmp_path: Path
) -> None:
    """Cancellation is observed only after the child proves it is running.

    The gate is what orders the two: the canceller's read returns exactly
    when the child announces itself, so the token is set while a real
    child is alive rather than at some hoped-for moment.
    """
    gate = Gate.create(tmp_path, "started")
    token = CancelToken()
    canceller = threading.Thread(
        target=lambda: (gate.receive(), token.cancel()),
        daemon=True,
    )
    canceller.start()

    completed = harness.run(
        python_command(
            "import os, time\n"
            f"gate = open({str(gate.path)!r}, 'w')\n"
            "gate.write(str(os.getpid()))\n"
            "gate.close()\n"
            "while True:\n"
            "    time.sleep(3600)\n"
        ),
        budgets=Budgets(wall_time=WATCHDOG_WALL_TIME),
        cancellation=token,
    )
    canceller.join()

    assert completed.result.outcome == CancelledOutcome()
    record = harness.store.load(record_dir_of(completed))
    assert record.state is RecordState.FINALIZED


# --- Recording lifecycle -------------------------------------------------


@requires_macos
def test_a_completed_run_finalizes_with_digest_matching_sidecars(
    harness: Harness,
) -> None:
    completed = harness.run(
        python_command(
            "import sys\n"
            "sys.stdout.buffer.write(b'stdout evidence')\n"
            "sys.stderr.buffer.write(b'stderr evidence')\n"
        )
    )

    assert isinstance(completed.record_receipt, CompleteRecordReceipt)
    record = harness.store.load(record_dir_of(completed))
    assert isinstance(record, FinalizedRecord)
    stdout_path = (
        record_dir_of(completed) / record.outputs.stdout.relative_path
    )
    assert stdout_path.read_bytes() == b"stdout evidence"


@requires_macos
def test_the_record_carries_the_declaration_digest_but_never_argv(
    harness: Harness,
) -> None:
    """Secret-safe durable evidence: the digest, not the invocation."""
    secret = "a-secret-argument"
    completed = harness.run(
        (sys.executable, "-I", "-c", "pass", secret),
    )

    manifest = (record_dir_of(completed) / "record.json").read_text()
    assert secret not in manifest
    assert sys.executable not in manifest
    record = finalized_record(harness.store, record_dir_of(completed))
    target_record = record.declaration.target
    assert isinstance(target_record, TrustedCommandTargetRecord)
    assert len(target_record.canonical_declaration_sha256) == 64


@requires_macos
def test_the_record_names_granted_variables_but_never_their_values(
    harness: Harness,
) -> None:
    completed = harness.run(
        python_command("pass"),
        env=EnvGrant.fixed({"GRANTED_NAME": "the-secret-value"}),
    )

    manifest = (record_dir_of(completed) / "record.json").read_text()
    assert "GRANTED_NAME" in manifest
    assert "the-secret-value" not in manifest


@requires_macos
def test_a_spawn_absence_finalizes_directly_from_prepared(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No child started, so `running` is never published for this attempt."""
    marked: list[PreparedRun] = []
    original = DirectoryRunStore.mark_running

    def record_call(
        store: DirectoryRunStore,
        prepared_run: PreparedRun,
        process: ProcessRecord,
        /,
    ) -> RunningRun:
        marked.append(prepared_run)
        return original(store, prepared_run, process)

    monkeypatch.setattr(DirectoryRunStore, "mark_running", record_call)
    completed = harness.run(("/nonexistent/definitely-not-here",))

    assert marked == []
    record = harness.store.load(record_dir_of(completed))
    assert record.state is RecordState.FINALIZED


@requires_macos
def test_the_running_manifest_is_published_while_the_child_is_alive(
    harness: Harness, tmp_path: Path
) -> None:
    """Read ``running`` only after its publication returns, with a live child."""
    arrived = Gate.create(tmp_path, "arrived")
    release = Gate.create(tmp_path, "release")
    marked_running = threading.Event()
    observing_store = _MarkRunningObservedStore(
        delegate=harness.store,
        marked_running=marked_running,
    )
    (call,) = start_threaded_calls(
        (
            lambda: harness.execute(
                ExecutionJob(
                    job_id=JobId(uuid4()),
                    target=TrustedCommandTarget(
                        argv=python_command(
                            "import os, sys\n"
                            f"with open({str(arrived.path)!r}, 'w') as gate:\n"
                            "    gate.write(str(os.getpid()))\n"
                            f"open({str(release.path)!r}).read()\n"
                            "sys.stdout.write(str(os.getpid()))\n"
                        )
                    ),
                    env=EnvGrant.none(),
                    budgets=Budgets(wall_time=WATCHDOG_WALL_TIME),
                ),
                store=observing_store,
            ),
        )
    )

    try:
        child_pid = int(arrived.receive())
        assert marked_running.wait(timeout=5), (
            "mark_running publication watchdog fired"
        )
        (record_dir,) = sorted(harness.root.iterdir())
        record = observing_store.load(record_dir)
    finally:
        release.release()
        (completed,) = finish_threaded_calls((call,))
    assert isinstance(record, RunningRecord)
    assert record.process.pid == child_pid
    assert completed.result.outcome == ExitedOutcome(exit_code=0)
    assert (
        completed.result.payload_outputs.stdout.head == str(child_pid).encode()
    )


@dataclass(frozen=True, slots=True)
class _MarkRunningObservedStore:
    """Delegate storage and signal only after ``mark_running`` returns."""

    delegate: DirectoryRunStore
    marked_running: threading.Event

    def prepare(self, record: PreparedRecord, /) -> PreparedRun:
        return self.delegate.prepare(record)

    def mark_running(
        self,
        prepared_run: PreparedRun,
        process: ProcessRecord,
        /,
    ) -> RunningRun:
        running_run = self.delegate.mark_running(prepared_run, process)
        self.marked_running.set()
        return running_run

    def finalize(
        self,
        run: FinalizableRun,
        result: ExecutionResult,
        /,
    ) -> RealRecordReceipt:
        return self.delegate.finalize(run, result)

    def load(self, record_dir: Path, /) -> RunRecord:
        return self.delegate.load(record_dir)


@dataclass(frozen=True, slots=True)
class _GatedMarkingStore(DirectoryRunStore):
    """A conforming store whose `running` publish waits on an explicit gate.

    A slow ``mark_running`` is an ordinary implementation -- a contended
    disk, a network mount, a cold cache -- so the engine must already be
    draining the child before it publishes. The gate makes the ordering
    exact rather than probable: the publish returns only once the test
    releases it, and the test releases it only after the child has proved
    it pushed more than one pipe buffer through.
    """

    gate: Gate

    def mark_running(
        self,
        prepared_run: PreparedRun,
        process: ProcessRecord,
        /,
    ) -> RunningRun:
        self.gate.receive()
        # Not ``super()``: ``slots=True`` rebuilds the class, so the
        # zero-argument form's closure cell names the pre-rebuild class.
        return DirectoryRunStore.mark_running(self, prepared_run, process)


@requires_macos
def test_the_running_publish_does_not_stall_a_child_that_fills_a_pipe(
    tmp_path: Path,
    host_runtime: IsolatedHostPythonRuntime,
) -> None:
    """Draining is live across the durable `running` publish.

    macOS pipes hold 64 KiB, so a child writing more than that blocks in
    the kernel until someone reads. If the parent published the `running`
    manifest before starting the transports, the child would be stalled for
    the whole publish and charged for it against its own wall-clock budget.

    The two gates pin the ordering with no timing at all: the publish waits
    on `proceed`, and `proceed` is released only after the child announces
    on `written`, which it can only reach once the full oversized write has
    completed. Under the wrong order the two waits are a deadlock the
    case's watchdog reports; under the right order the child streams
    through and exits cleanly with every byte retained.
    """
    produced_bytes = 200_000
    assert produced_bytes > 64 * 1024
    root = tmp_path / "gated"
    root.mkdir()
    proceed = Gate.create(tmp_path, "proceed")
    written = Gate.create(tmp_path, "written")
    store = _GatedMarkingStore(root=root, gate=proceed)

    def release_after_the_oversized_write() -> None:
        written.receive()
        proceed.release()

    watcher = threading.Thread(
        target=release_after_the_oversized_write, daemon=True
    )
    watcher.start()
    completed = Harness(
        store=store,
        root=root,
        runtime=host_runtime,
    ).execute(
        ExecutionJob(
            job_id=JobId(uuid4()),
            target=TrustedCommandTarget(
                argv=python_command(
                    "import sys\n"
                    f"sys.stdout.buffer.write(b'x' * {produced_bytes})\n"
                    "sys.stdout.flush()\n"
                    f"open({str(written.path)!r}, 'w').write('done')\n"
                )
            ),
            env=EnvGrant.none(),
            budgets=Budgets(wall_time=WATCHDOG_WALL_TIME),
        ),
    )
    watcher.join()

    assert completed.result.outcome == ExitedOutcome(exit_code=0)
    assert completed.result.attribution.owner is FailureOwner.NONE
    stdout = completed.result.payload_outputs.stdout
    assert stdout.produced_bytes == produced_bytes
    (record_dir,) = sorted(root.iterdir())
    assert store.load(record_dir).state is RecordState.FINALIZED


@requires_macos
def test_declared_stdin_larger_than_a_pipe_buffer_survives_the_publish(
    tmp_path: Path,
    host_runtime: IsolatedHostPythonRuntime,
) -> None:
    """The same ordering in the input direction, which fails identically.

    A child that reads its whole stdin first cannot see EOF until the feed
    thread has pushed every byte, and the feed thread blocks after one pipe
    buffer. The gate releases the `running` publish only once the child has
    echoed a full oversized stdin back, so the publish provably overlapped
    a live feed rather than preceding it.
    """
    stdin_bytes = b"y" * 200_000
    root = tmp_path / "gated-stdin"
    root.mkdir()
    proceed = Gate.create(tmp_path, "proceed")
    echoed = Gate.create(tmp_path, "echoed")
    store = _GatedMarkingStore(root=root, gate=proceed)

    def release_after_the_full_read() -> None:
        echoed.receive()
        proceed.release()

    watcher = threading.Thread(target=release_after_the_full_read, daemon=True)
    watcher.start()
    completed = Harness(
        store=store,
        root=root,
        runtime=host_runtime,
    ).execute(
        ExecutionJob(
            job_id=JobId(uuid4()),
            target=TrustedCommandTarget(
                argv=python_command(
                    "import sys\n"
                    "received = sys.stdin.buffer.read()\n"
                    f"open({str(echoed.path)!r}, 'w').write(str(len(received)))\n"
                    "sys.stdout.buffer.write(str(len(received)).encode())\n"
                ),
                stdin=stdin_bytes,
            ),
            env=EnvGrant.none(),
            budgets=Budgets(wall_time=WATCHDOG_WALL_TIME),
        ),
    )
    watcher.join()

    assert completed.result.outcome == ExitedOutcome(exit_code=0)
    assert completed.result.payload_outputs.stdout.head == (
        str(len(stdin_bytes)).encode()
    )


# --- Recording degradation -----------------------------------------------


class _UnwritableStore(DirectoryRunStore):
    """A store whose finalization always fails, and only its finalization."""

    def finalize(
        self,
        run: FinalizableRun,
        result: ExecutionResult,
        /,
    ) -> RealRecordReceipt:
        raise ExecutorFailure("finalization refused by the test")


@requires_macos
def test_finalization_failure_degrades_the_receipt_not_the_outcome(
    harness: Harness, tmp_path: Path
) -> None:
    root = tmp_path / "degraded"
    root.mkdir()
    store = _UnwritableStore(root=root)

    completed = harness.execute(
        ExecutionJob(
            job_id=JobId(uuid4()),
            target=TrustedCommandTarget(
                argv=python_command("raise SystemExit(5)")
            ),
            env=EnvGrant.none(),
            budgets=Budgets.unbudgeted(),
        ),
        store=store,
    )

    assert completed.result.outcome == ExitedOutcome(exit_code=5)
    receipt = degraded_receipt_of(completed)
    assert receipt.latest_state is RecordState.RUNNING
    assert receipt.failures[0].operation == "finalize"


class _UnmarkableStore(DirectoryRunStore):
    """A store whose post-start `running` publication always fails."""

    def mark_running(
        self,
        prepared_run: PreparedRun,
        process: ProcessRecord,
        /,
    ) -> RunningRun:
        raise ExecutorFailure("running publication refused by the test")


@requires_macos
def test_a_failed_running_publication_degrades_the_receipt_by_name(
    harness: Harness, tmp_path: Path
) -> None:
    """Post-start degradation, so the attempt continues and finalizes.

    The finalize that follows succeeds, so the record on disk reaches
    ``finalized`` -- but the caller is still told the ``running``
    publication never landed, by name, rather than being handed a
    complete receipt that hides it.
    """
    root = tmp_path / "unmarkable"
    root.mkdir()
    store = _UnmarkableStore(root=root)

    completed = harness.execute(
        ExecutionJob(
            job_id=JobId(uuid4()),
            target=TrustedCommandTarget(argv=python_command("pass")),
            env=EnvGrant.none(),
            budgets=Budgets.unbudgeted(),
        ),
        store=store,
    )

    assert completed.result.outcome == ExitedOutcome(exit_code=0)
    (record_dir,) = sorted(root.iterdir())
    assert store.load(record_dir).state is RecordState.FINALIZED
    receipt = degraded_receipt_of(completed)
    assert [failure.operation for failure in receipt.failures] == [
        "mark_running"
    ]
    assert receipt.latest_state is RecordState.FINALIZED


class _UnpreparableStore(DirectoryRunStore):
    """A store that cannot prepare, so no attempt may start."""

    def prepare(self, record: PreparedRecord, /) -> PreparedRun:
        raise ExecutorFailure("preparation refused by the test")


@requires_macos
def test_prepare_failure_prevents_the_spawn_and_raises(
    harness: Harness, tmp_path: Path
) -> None:
    marker = tmp_path / "the-child-ran"
    root = tmp_path / "unpreparable"
    root.mkdir()

    with pytest.raises(ExecutorFailure, match="preparation refused"):
        harness.execute(
            ExecutionJob(
                job_id=JobId(uuid4()),
                target=TrustedCommandTarget(
                    argv=python_command(
                        "import pathlib; "
                        f"pathlib.Path({str(marker)!r}).write_text('ran')"
                    )
                ),
                env=EnvGrant.none(),
                budgets=Budgets.unbudgeted(),
            ),
            store=_UnpreparableStore(root=root),
        )

    assert not marker.exists()


# --- Measurements --------------------------------------------------------


@requires_macos
def test_measurements_describe_the_attempt_the_engine_observed(
    harness: Harness,
) -> None:
    """Not a clock assertion: these are the invariants among the numbers."""
    completed = harness.run(
        python_command("import sys; sys.stdout.buffer.write(b'ok')"),
        stdin=b"input",
    )

    measurements = completed.result.measurements
    assert measurements.finished_at >= measurements.started_at
    assert measurements.duration_ns >= measurements.teardown_duration_ns
    assert measurements.input_bytes == len(b"input")
    assert measurements.protocol_bytes_received == 0


# --- Concurrency ---------------------------------------------------------


@requires_macos
def test_concurrent_calls_keep_their_attempts_fully_separate(
    harness: Harness, tmp_path: Path
) -> None:
    """All children overlap while records, scratch, and outputs stay distinct."""
    call_count = 8
    callers_ready = threading.Barrier(call_count + 1)
    arrivals = tuple(
        Gate.create(tmp_path, f"arrival-{index}")
        for index in range(call_count)
    )
    releases = tuple(
        Gate.create(tmp_path, f"release-{index}")
        for index in range(call_count)
    )

    def run_one(index: int, /) -> CompletedExecution:
        callers_ready.wait()
        return harness.run(
            python_command(
                "import os, sys\n"
                f"with open({str(arrivals[index].path)!r}, 'w') as gate:\n"
                f"    gate.write('command-{index}')\n"
                f"open({str(releases[index].path)!r}).read()\n"
                f"sys.stdout.write('command-{index}:' + os.getcwd())\n"
            ),
            budgets=Budgets(wall_time=WATCHDOG_WALL_TIME),
        )

    calls = start_threaded_calls(
        tuple(
            lambda index=index: run_one(index) for index in range(call_count)
        )
    )
    try:
        callers_ready.wait()
        assert tuple(gate.receive() for gate in arrivals) == tuple(
            f"command-{index}" for index in range(call_count)
        )
    finally:
        for gate in releases:
            gate.release()
        completions = finish_threaded_calls(calls)

    for index, completed in enumerate(completions):
        assert completed.result.outcome == ExitedOutcome(exit_code=0)
        assert completed.result.attribution.owner is FailureOwner.NONE
        assert completed.result.payload_outputs.stdout.head.startswith(
            f"command-{index}:".encode()
        )
    outputs = {
        completed.result.payload_outputs.stdout.head
        for completed in completions
    }
    assert len(outputs) == call_count
    record_dirs = {record_dir_of(completed) for completed in completions}
    assert len(record_dirs) == call_count
    attempt_ids = {
        completed.result.execution_id.attempt_id for completed in completions
    }
    assert len(attempt_ids) == call_count


@requires_macos
def test_every_call_gets_a_fresh_child_and_a_distinct_attempt_id(
    harness: Harness,
) -> None:
    job = ExecutionJob(
        job_id=JobId(uuid4()),
        target=TrustedCommandTarget(
            argv=python_command(
                "import os, sys; sys.stdout.write(str(os.getpid()))"
            )
        ),
        env=EnvGrant.none(),
        budgets=Budgets.unbudgeted(),
    )
    first = harness.execute(job)
    second = harness.execute(job)

    assert (
        first.result.execution_id.job_id == second.result.execution_id.job_id
    )
    assert (
        first.result.execution_id.attempt_id
        != second.result.execution_id.attempt_id
    )
    assert (
        first.result.payload_outputs.stdout.head
        != second.result.payload_outputs.stdout.head
    )


# --- Post-spawn machinery failure ----------------------------------------


@requires_macos
def test_bootstrap_launch_failure_closes_every_attempt_resource(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failed ``Popen`` leaves prepared state and no owned resources."""
    scratch_paths: list[Path] = []
    original_scratch = dr_exec.execution.engine._scratch_workspace

    @contextmanager
    def observed_scratch() -> Iterator[Path]:
        with original_scratch() as scratch:
            scratch_paths.append(scratch)
            yield scratch

    def failing_launch(
        *,
        executable: str,
        argv: tuple[str, ...],
        environment: dict[str, str],
        scratch_directory: str,
        descriptor_map: tuple[tuple[int, int], ...],
        status_write: int,
    ) -> subprocess.Popen[bytes]:
        del (
            executable,
            argv,
            environment,
            scratch_directory,
            descriptor_map,
            status_write,
        )
        raise OSError(errno.EMFILE, "synthetic launch failure")

    monkeypatch.setattr(
        dr_exec.execution.engine, "_scratch_workspace", observed_scratch
    )
    monkeypatch.setattr(
        dr_exec.execution.engine, "launch_bootstrap", failing_launch
    )
    before = len(os.listdir("/dev/fd"))

    with pytest.raises(ExecutorFailure, match="could not start") as raised:
        harness.run(python_command("pass"))

    assert isinstance(raised.value.__cause__, OSError)
    assert len(os.listdir("/dev/fd")) == before
    assert len(scratch_paths) == 1
    assert not scratch_paths[0].exists()
    record = harness.store.load(harness.only_record_dir())
    assert record.state is RecordState.PREPARED
    with pytest.raises(ChildProcessError):
        os.waitpid(-1, os.WNOHANG)


@requires_macos
def test_a_started_output_worker_failure_raises_after_lifecycle_cleanup(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A dead pump cannot turn missing output into a clean completion."""

    def failing_run(_: object) -> None:
        raise RuntimeError("synthetic output failure")

    monkeypatch.setattr(
        dr_exec.execution.engine._OutputPump, "run", failing_run
    )
    before = len(os.listdir("/dev/fd"))

    with pytest.raises(
        ExecutorFailure, match="output transport worker"
    ) as exc:
        harness.run(
            python_command("import time; time.sleep(3600)"),
            budgets=Budgets(wall_time=FiniteDurationLimit(max_ns=500_000_000)),
            self_budgets=ExecutorSelfBudgets(join_time=WATCHDOG_JOIN_TIME),
        )

    assert isinstance(exc.value.__cause__, RuntimeError)
    assert len(os.listdir("/dev/fd")) == before
    record = harness.store.load(harness.only_record_dir())
    assert record.state is RecordState.RUNNING
    with pytest.raises(ChildProcessError):
        os.waitpid(-1, os.WNOHANG)


@requires_macos
def test_an_escaped_stdin_oserror_remains_ordinary_transport_behavior(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A child closing stdin early is not executor machinery failure."""

    def failing_feed(*_: object) -> None:
        raise OSError(errno.EPIPE, "synthetic closed stdin")

    monkeypatch.setattr(dr_exec.execution.engine, "_feed", failing_feed)

    completed = harness.run(python_command("pass"), stdin=b"unread input")

    assert completed.result.outcome == ExitedOutcome(exit_code=0)


@requires_macos
def test_a_store_failure_after_the_spawn_still_reaps_the_direct_child(
    harness: Harness,
) -> None:
    """A live child exists from the spawn on, so every raise reaps it.

    `mark_running` is the first thing that runs against a live child, and
    a store that raises an unexpected type is a machinery failure rather
    than the recording degradation the engine absorbs. The raise must
    still leave through teardown: `ECHILD` is the kernel's own statement
    that nothing this parent spawned is left unreaped.
    """

    class ExplodingStore(DirectoryRunStore):
        def mark_running(
            self, prepared: PreparedRun, process: ProcessRecord, /
        ) -> RunningRun:
            raise RuntimeError("the store failed in an unexpected way")

    job = ExecutionJob(
        job_id=JobId(uuid4()),
        target=TrustedCommandTarget(argv=python_command("pass")),
        env=EnvGrant.none(),
        budgets=Budgets.unbudgeted(),
    )
    with pytest.raises(RuntimeError, match="unexpected way"):
        harness.execute(job, store=ExplodingStore(root=harness.root))

    with pytest.raises(ChildProcessError):
        os.waitpid(-1, os.WNOHANG)


@requires_macos
def test_a_thread_that_cannot_start_still_tears_down_the_group(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Thread exhaustion mid-start leaves no survivor and no zombie.

    The child forks a descendant that would outlive it, then blocks on a
    gate it never receives, so the descendant is provably alive when the
    engine's own machinery fails. The protocol reader is the last thread
    started, so failing that start exercises the window where earlier
    threads are already running against descriptors the frame still owns.
    """
    started: list[str] = []
    original = dr_exec.execution.engine._started_thread

    def failing_start(
        target: Callable[[], None], name: str, /
    ) -> dr_exec.execution.engine._TransportWorker:
        started.append(name)
        if name == "dr-exec-output":
            raise RuntimeError("can't start new thread")
        return original(target, name)

    monkeypatch.setattr(
        dr_exec.execution.engine, "_started_thread", failing_start
    )

    with pytest.raises(RuntimeError, match="can't start new thread"):
        harness.run(
            python_command(
                "import os, time\n"
                "if os.fork() == 0:\n"
                "    while True:\n"
                "        time.sleep(3600)\n"
                "while True:\n"
                "    time.sleep(3600)\n"
            )
        )

    assert started == ["dr-exec-stdin", "dr-exec-output"]
    with pytest.raises(ChildProcessError):
        os.waitpid(-1, os.WNOHANG)


@requires_macos
def test_a_partial_transport_start_leaks_no_descriptor(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Every descriptor the attempt opened is closed on the failing path.

    A protocol target is what makes this the interesting case: it opens
    the forward pipe whose write end the pump would have taken and whose
    read end the reader would have taken, and the failing start is the
    reader's, so neither handoff completes. The count of this process's
    own open descriptors is taken after one attempt has warmed every lazy
    allocation and again after several more; a write end nobody owned, a
    taken read end nobody closed, or an unclosed selector would each show
    as a positive delta.
    """
    original = dr_exec.execution.engine._started_thread

    def failing_start(
        target: Callable[[], None], name: str, /
    ) -> dr_exec.execution.engine._TransportWorker:
        if name == "dr-exec-protocol":
            raise RuntimeError("can't start new thread")
        return original(target, name)

    monkeypatch.setattr(
        dr_exec.execution.engine, "_started_thread", failing_start
    )

    def attempt() -> None:
        job = ExecutionJob(
            job_id=JobId(uuid4()),
            target=UntrustedPythonTarget(
                driver_source="",
                request=build_identity_document(
                    schema="dr_exec.test_request",
                    schema_version=1,
                    payload={"a": 1},
                ),
                containment_profile=ContainmentProfile.PROCESS_BOUNDARY_ONLY,
            ),
            env=EnvGrant.none(),
            budgets=Budgets.unbudgeted(),
        )
        with pytest.raises(RuntimeError, match="can't start new thread"):
            harness.execute(job)

    attempt()
    before = len(os.listdir("/dev/fd"))
    for _ in range(8):
        attempt()

    assert len(os.listdir("/dev/fd")) == before


# --- Executor self-budgets ------------------------------------------------


@requires_macos
def test_a_stalled_bootstrap_is_stopped_by_the_startup_budget(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A helper that never reaches the payload cannot hold the call open.

    The status pipe reaching EOF is what says the payload was reached, so
    a helper stalled before `exec` gates the whole attempt. The stall here
    is a real one -- an extra open copy of the status write end that no
    payload will ever close -- rather than a slow child, so the budget is
    what ends the wait and nothing else can.

    The failure is `ExecutorFailure` rather than a budget outcome: this is
    the executor's own limit on its own machinery, and a payload that
    never ran cannot own it.
    """
    stalls: list[int] = []
    original = dr_exec.execution.engine.launch_bootstrap

    def stalling_launch(
        *,
        executable: str,
        argv: tuple[str, ...],
        environment: dict[str, str],
        scratch_directory: str,
        descriptor_map: tuple[tuple[int, int], ...],
        status_write: int,
    ) -> subprocess.Popen[bytes]:
        # A duplicate the payload's `exec` cannot close-on-exec, so the
        # parent's read never sees EOF no matter what the child does.
        stalls.append(os.dup(status_write))
        return original(
            executable=executable,
            argv=argv,
            environment=environment,
            scratch_directory=scratch_directory,
            descriptor_map=descriptor_map,
            status_write=status_write,
        )

    monkeypatch.setattr(
        dr_exec.execution.engine, "launch_bootstrap", stalling_launch
    )

    try:
        with pytest.raises(ExecutorFailure, match="startup budget"):
            harness.run(
                python_command("pass"),
                self_budgets=ExecutorSelfBudgets(
                    startup_time=FiniteDurationLimit(max_ns=500_000_000)
                ),
            )
    finally:
        for descriptor in stalls:
            os.close(descriptor)

    with pytest.raises(ChildProcessError):
        os.waitpid(-1, os.WNOHANG)


@requires_macos
def test_a_helper_stopped_before_setsid_is_still_reaped(
    harness: Harness, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Teardown before the group exists still ends the call.

    The gate holds the helper at its first statement, so the startup
    budget expires while the child is still short of `setsid`: it remains
    in the parent's own group, where its pid names no group and every
    group-targeted signal fails with ESRCH. Signalling only the group
    there would leave the unbounded reap that follows waiting forever, so
    the case asserts the executor's own failure arrives and that the
    direct child was collected.

    The parent holds both ends of the gate FIFO open for the whole run,
    so the child's own open returns at once and it blocks in `read`.
    That is what makes the hold survivable from the parent's side: a
    child that teardown kills while gated never opens the FIFO, and the
    parent still has no peer to wait for when it lets go.
    """
    gate_path = tmp_path / "before-setsid"
    os.mkfifo(gate_path)
    gate_read = os.open(gate_path, os.O_RDONLY | os.O_NONBLOCK)
    gate_write = os.open(gate_path, os.O_WRONLY)
    original = dr_exec.execution.spawn.spawn_bootstrap_source

    def gated_source(
        *,
        executable: str,
        argv: tuple[str, ...],
        scratch_directory: str,
        descriptor_map: tuple[tuple[int, int], ...],
        status_descriptor: int,
    ) -> str:
        # A blocking read at the very top of the helper, ahead of every
        # line the parent renders and of `setsid` itself.
        prologue = f"open({gate_path.as_posix()!r}).read(1)\n"
        return prologue + original(
            executable=executable,
            argv=argv,
            scratch_directory=scratch_directory,
            descriptor_map=descriptor_map,
            status_descriptor=status_descriptor,
        )

    monkeypatch.setattr(
        dr_exec.execution.spawn, "spawn_bootstrap_source", gated_source
    )

    try:
        with pytest.raises(ExecutorFailure, match="startup budget"):
            harness.run(
                python_command("import time; time.sleep(120)"),
                self_budgets=ExecutorSelfBudgets(
                    startup_time=FiniteDurationLimit(max_ns=200_000_000),
                    termination_time=FiniteDurationLimit(max_ns=200_000_000),
                ),
            )
    finally:
        os.close(gate_write)
        os.close(gate_read)

    with pytest.raises(ChildProcessError):
        os.waitpid(-1, os.WNOHANG)


@requires_macos
def test_an_unbudgeted_startup_axis_installs_no_deadline(
    harness: Harness,
) -> None:
    """The default budget adds no limit, so an ordinary run is unaffected."""
    completed = harness.run(
        python_command("pass"),
        self_budgets=ExecutorSelfBudgets.unbudgeted(),
    )

    assert completed.result.outcome == ExitedOutcome(exit_code=0)


@requires_macos
def test_a_setup_failure_after_setsid_still_tears_down_the_group(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Every other setup stage is past `setsid`, so the group is real."""
    signalled: list[tuple[int, int]] = []
    original = dr_exec.execution.engine.signal_process_group

    def recording_signal(pid: int, number: int, /) -> bool:
        signalled.append((pid, number))
        return original(pid, number)

    monkeypatch.setattr(
        dr_exec.execution.engine, "signal_process_group", recording_signal
    )

    original_scratch = dr_exec.execution.engine._scratch_workspace

    @contextmanager
    def vanishing_scratch() -> Iterator[Path]:
        with original_scratch() as directory:
            directory.rmdir()
            yield directory

    monkeypatch.setattr(
        dr_exec.execution.engine, "_scratch_workspace", vanishing_scratch
    )

    completed = harness.run(python_command("pass"))

    outcome = completed.result.outcome
    assert isinstance(outcome, SpawnFailedOutcome)
    assert outcome.error_message == SETUP_STAGE_CHDIR
    assert [number for _, number in signalled][:1] == [signal.SIGTERM]
