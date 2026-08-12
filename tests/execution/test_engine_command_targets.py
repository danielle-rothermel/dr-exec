from __future__ import annotations

import errno
import fcntl
import os
import signal
import subprocess
import sys
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest
from dr_serialize import build_identity_document
from support.process import (
    Gate,
    cleanup_exact_pids,
    exact_pid_exists,
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
    OutputArtifactRecord,
    OutputOverflowPolicy,
    PayloadRetentionBudget,
    PreparedRecord,
    ProcessRecord,
    RealRecordReceipt,
    RecordState,
    RunningRecord,
    RunRecord,
    RunRecordReference,
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
from dr_exec.core.model import canonical_model_bytes
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
    pytest.mark.usefixtures("process_watchdog"),
]

WATCHDOG_WALL_TIME = FiniteDurationLimit(max_ns=5_000_000_000)
WATCHDOG_JOIN_TIME = FiniteDurationLimit(max_ns=5_000_000_000)
ESCAPEE_JOIN_TIME = FiniteDurationLimit(max_ns=500_000_000)

UNREADABLE_STDIN_BYTES = 8 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class Harness:
    store: DirectoryRunStore
    root: Path
    runtime: IsolatedHostPythonRuntime
    prepared_runs: list[PreparedRun] = dataclass_field(default_factory=list)

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
            run_store=(
                _CapturingRunStore(self.store, self.prepared_runs)
                if store is None
                else store
            ),
            self_budgets=(
                ExecutorSelfBudgets.unbudgeted()
                if self_budgets is None
                else self_budgets
            ),
            cancellation=cancellation,
        )

    def only_record_reference(self) -> RunRecordReference:
        (prepared,) = self.prepared_runs
        return prepared.reference


@dataclass(frozen=True, slots=True)
class _CapturingRunStore:
    delegate: DirectoryRunStore
    prepared_runs: list[PreparedRun]

    def prepare(self, record: PreparedRecord, /) -> PreparedRun:
        prepared = self.delegate.prepare(record)
        self.prepared_runs.append(prepared)
        return prepared

    def mark_running(
        self,
        prepared_run: PreparedRun,
        process: ProcessRecord,
        /,
    ) -> RunningRun:
        return self.delegate.mark_running(prepared_run, process)

    def finalize(
        self,
        run: FinalizableRun,
        result: ExecutionResult,
        /,
    ) -> RealRecordReceipt:
        return self.delegate.finalize(run, result)

    def load(self, reference: RunRecordReference, /) -> RunRecord:
        return self.delegate.load(reference)

    def read_artifact(
        self,
        reference: RunRecordReference,
        artifact: OutputArtifactRecord,
        /,
        *,
        max_bytes: int,
    ) -> bytes:
        return self.delegate.read_artifact(
            reference, artifact, max_bytes=max_bytes
        )


def reference_of(completed: CompletedExecution, /) -> RunRecordReference:
    receipt = completed.record_receipt
    assert isinstance(receipt, CompleteRecordReceipt | DegradedRecordReceipt)
    return receipt.reference


def degraded_receipt_of(
    completed: CompletedExecution, /
) -> DegradedRecordReceipt:
    receipt = completed.record_receipt
    assert isinstance(receipt, DegradedRecordReceipt)
    return receipt


def finalized_record(
    store: DirectoryRunStore, reference: RunRecordReference, /
) -> FinalizedRecord:
    record = store.load(reference)
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
    return (sys.executable, "-I", "-c", source)


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


@requires_macos
def test_a_command_child_excludes_a_high_inheritable_parent_descriptor(
    harness: Harness,
) -> None:
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


def test_an_unsupported_platform_is_refused_before_anything_durable(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(sys, "platform", "linux")

    with pytest.raises(DeclarationError, match="darwin"):
        harness.run(python_command("pass"))

    assert list(harness.root.iterdir()) == []


@requires_macos
def test_every_declared_target_kind_runs_through_the_one_engine_path(
    harness: Harness,
) -> None:
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

    record = finalized_record(harness.store, reference_of(completed))
    target_record = record.declaration.target
    assert isinstance(target_record, UntrustedCommandTargetRecord)
    assert target_record.containment_profile is (
        ContainmentProfile.PROCESS_BOUNDARY_ONLY
    )


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
    assert completed.result.attribution.owner is FailureOwner.EXECUTOR
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


@requires_macos
def test_teardown_reaches_the_original_process_group(
    harness: Harness, tmp_path: Path
) -> None:
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
def test_exact_pid_cleanup_runs_when_the_case_body_fails() -> None:

    class _ForcedFailure(Exception):
        pass

    process = subprocess.Popen(
        (
            sys.executable,
            "-I",
            "-c",
            "import signal; signal.pause()",
        ),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    pid = process.pid

    with pytest.raises(_ForcedFailure), cleanup_exact_pids() as cleanup:
        cleanup.append(pid)
        assert exact_pid_exists(pid)
        raise _ForcedFailure("forced after PID registration")

    assert not exact_pid_exists(pid)
    process.wait()


@requires_macos
def test_finite_termination_budget_allows_a_cooperative_term_exit(
    harness: Harness,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ready = Gate.create(tmp_path, "cooperative-ready")
    handled = tmp_path / "term-handled"
    token = CancelToken()
    group_signals: list[int] = []
    original_signal_group = dr_exec.execution.engine.signal_process_group

    def record_group_signal(pid: int, number: int, /) -> bool:
        group_signals.append(number)
        return original_signal_group(pid, number)

    monkeypatch.setattr(
        dr_exec.execution.engine,
        "signal_process_group",
        record_group_signal,
    )

    def cancel_ready_child() -> int:
        pid = int(ready.receive())
        token.cancel()
        return pid

    (canceller,) = start_threaded_calls((cancel_ready_child,))
    try:
        completed = harness.run(
            python_command(
                "import os, signal\n"
                "def handle_term(_number, _frame):\n"
                f"    with open({str(handled)!r}, 'w') as marker:\n"
                "        marker.write(str(os.getpid()))\n"
                "    raise SystemExit(0)\n"
                "signal.signal(signal.SIGTERM, handle_term)\n"
                f"with open({str(ready.path)!r}, 'w') as gate:\n"
                "    gate.write(str(os.getpid()))\n"
                "signal.pause()\n"
            ),
            budgets=Budgets(wall_time=WATCHDOG_WALL_TIME),
            self_budgets=ExecutorSelfBudgets(
                termination_time=FiniteDurationLimit(max_ns=1_000_000_000)
            ),
            cancellation=token,
        )
    finally:
        (child_pid,) = finish_threaded_calls((canceller,))

    assert completed.result.outcome == CancelledOutcome()
    assert handled.read_text() == str(child_pid)
    assert [
        number
        for number in group_signals
        if number in {signal.SIGTERM, signal.SIGKILL}
    ] == [signal.SIGTERM]


@requires_macos
def test_finite_termination_budget_escalates_a_term_ignoring_child(
    harness: Harness,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ready = Gate.create(tmp_path, "ignoring-ready")
    token = CancelToken()
    group_signals: list[int] = []
    original_signal_group = dr_exec.execution.engine.signal_process_group

    def record_group_signal(pid: int, number: int, /) -> bool:
        group_signals.append(number)
        return original_signal_group(pid, number)

    monkeypatch.setattr(
        dr_exec.execution.engine,
        "signal_process_group",
        record_group_signal,
    )

    def cancel_ready_child() -> int:
        pid = int(ready.receive())
        token.cancel()
        return pid

    (canceller,) = start_threaded_calls((cancel_ready_child,))
    try:
        completed = harness.run(
            python_command(
                "import os, signal\n"
                "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
                f"with open({str(ready.path)!r}, 'w') as gate:\n"
                "    gate.write(str(os.getpid()))\n"
                "signal.pause()\n"
            ),
            budgets=Budgets(wall_time=WATCHDOG_WALL_TIME),
            self_budgets=ExecutorSelfBudgets(
                termination_time=FiniteDurationLimit(max_ns=100_000_000)
            ),
            cancellation=token,
        )
    finally:
        (child_pid,) = finish_threaded_calls((canceller,))

    assert completed.result.outcome == CancelledOutcome()
    assert not exact_pid_exists(child_pid)
    assert [
        number
        for number in group_signals
        if number in {signal.SIGTERM, signal.SIGKILL}
    ] == [signal.SIGTERM, signal.SIGKILL]


@requires_macos
def test_a_descendant_that_leaves_the_session_escapes_the_claim(
    harness: Harness, tmp_path: Path
) -> None:
    gate = Gate.create(tmp_path, "escapee")
    (collector,) = start_threaded_calls((lambda: int(gate.receive()),))
    escapee_pid: int | None = None
    with cleanup_exact_pids() as cleanup:
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
                    self_budgets=ExecutorSelfBudgets(
                        join_time=ESCAPEE_JOIN_TIME
                    ),
                )
        finally:
            (escapee_pid,) = finish_threaded_calls((collector,))
            cleanup.append(escapee_pid)
        assert exact_pid_exists(escapee_pid)

    assert not exact_pid_exists(escapee_pid)

    record = harness.store.load(harness.only_record_reference())
    assert record.state is RecordState.RUNNING


@requires_macos
def test_an_escapee_holding_a_full_stdin_pipe_still_returns_the_join_failure(
    harness: Harness, tmp_path: Path
) -> None:
    gate = Gate.create(tmp_path, "escapee")
    (collector,) = start_threaded_calls((lambda: int(gate.receive()),))
    escapee_pid: int | None = None
    with cleanup_exact_pids() as cleanup:
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
                    self_budgets=ExecutorSelfBudgets(
                        join_time=ESCAPEE_JOIN_TIME
                    ),
                )
        finally:
            (escapee_pid,) = finish_threaded_calls((collector,))
            cleanup.append(escapee_pid)
        assert exact_pid_exists(escapee_pid)

    assert not exact_pid_exists(escapee_pid)

    record = harness.store.load(harness.only_record_reference())
    assert record.state is RecordState.RUNNING


@requires_macos
def test_pre_spawn_cancellation_records_without_launching_a_child(
    harness: Harness, tmp_path: Path
) -> None:
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
    record = harness.store.load(reference_of(completed))
    assert record.state is RecordState.FINALIZED


@requires_macos
def test_post_spawn_cancellation_tears_down_and_returns_cancelled(
    harness: Harness, tmp_path: Path
) -> None:
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
    record = harness.store.load(reference_of(completed))
    assert record.state is RecordState.FINALIZED


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
    record = harness.store.load(reference_of(completed))
    assert isinstance(record, FinalizedRecord)
    assert (
        harness.store.read_artifact(
            reference_of(completed),
            record.outputs.stdout,
            max_bytes=record.outputs.stdout.size_bytes,
        )
        == b"stdout evidence"
    )


@requires_macos
def test_the_record_carries_the_declaration_digest_but_never_argv(
    harness: Harness,
) -> None:
    secret = "a-secret-argument"
    completed = harness.run(
        (sys.executable, "-I", "-c", "pass", secret),
    )

    manifest = canonical_model_bytes(
        harness.store.load(reference_of(completed))
    ).decode()
    assert secret not in manifest
    assert sys.executable not in manifest
    record = finalized_record(harness.store, reference_of(completed))
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

    manifest = canonical_model_bytes(
        harness.store.load(reference_of(completed))
    ).decode()
    assert "GRANTED_NAME" in manifest
    assert "the-secret-value" not in manifest


@requires_macos
def test_a_spawn_absence_finalizes_directly_from_prepared(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
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
    record = harness.store.load(reference_of(completed))
    assert record.state is RecordState.FINALIZED


@requires_macos
def test_the_running_manifest_is_published_while_the_child_is_alive(
    harness: Harness, tmp_path: Path
) -> None:
    arrived = Gate.create(tmp_path, "arrived")
    release = Gate.create(tmp_path, "release")
    marked_running = threading.Event()
    observing_store = _MarkRunningObservedStore(
        delegate=harness.store,
        marked_running=marked_running,
        prepared_runs=[],
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
        (prepared,) = observing_store.prepared_runs
        record = observing_store.load(prepared.reference)
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
    delegate: DirectoryRunStore
    marked_running: threading.Event
    prepared_runs: list[PreparedRun]

    def prepare(self, record: PreparedRecord, /) -> PreparedRun:
        prepared = self.delegate.prepare(record)
        self.prepared_runs.append(prepared)
        return prepared

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

    def load(self, reference: RunRecordReference, /) -> RunRecord:
        return self.delegate.load(reference)

    def read_artifact(
        self,
        reference: RunRecordReference,
        artifact: OutputArtifactRecord,
        /,
        *,
        max_bytes: int,
    ) -> bytes:
        return self.delegate.read_artifact(
            reference, artifact, max_bytes=max_bytes
        )


@dataclass(frozen=True, slots=True)
class _GatedMarkingStore(DirectoryRunStore):
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
    produced_bytes = 200_000
    assert produced_bytes > 64 * 1024
    root = tmp_path / "gated"
    root.mkdir()
    written = Gate.create(tmp_path, "written")
    store = _GatedMarkingStore(root=root, gate=written)
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
    assert completed.result.outcome == ExitedOutcome(exit_code=0)
    assert completed.result.attribution.owner is FailureOwner.NONE
    stdout = completed.result.payload_outputs.stdout
    assert stdout.produced_bytes == produced_bytes
    assert store.load(reference_of(completed)).state is RecordState.FINALIZED


@requires_macos
def test_declared_stdin_larger_than_a_pipe_buffer_survives_the_publish(
    tmp_path: Path,
    host_runtime: IsolatedHostPythonRuntime,
) -> None:
    stdin_bytes = b"y" * 200_000
    root = tmp_path / "gated-stdin"
    root.mkdir()
    echoed = Gate.create(tmp_path, "echoed")
    store = _GatedMarkingStore(root=root, gate=echoed)
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
    assert completed.result.outcome == ExitedOutcome(exit_code=0)
    assert completed.result.payload_outputs.stdout.head == (
        str(len(stdin_bytes)).encode()
    )


class _UnwritableStore(DirectoryRunStore):
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
    assert store.load(reference_of(completed)).state is RecordState.FINALIZED
    receipt = degraded_receipt_of(completed)
    assert [failure.operation for failure in receipt.failures] == [
        "mark_running"
    ]
    assert receipt.latest_state is RecordState.FINALIZED


class _UnpreparableStore(DirectoryRunStore):
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


@requires_macos
def test_measurements_describe_the_attempt_the_engine_observed(
    harness: Harness,
) -> None:
    completed = harness.run(
        python_command("import sys; sys.stdout.buffer.write(b'ok')"),
        stdin=b"input",
    )

    measurements = completed.result.measurements
    assert measurements.finished_at >= measurements.started_at
    assert measurements.duration_ns >= measurements.teardown_duration_ns
    assert measurements.input_bytes == len(b"input")
    assert measurements.protocol_bytes_received == 0


@requires_macos
def test_concurrent_calls_keep_their_attempts_fully_separate(
    harness: Harness, tmp_path: Path
) -> None:
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
    references = {reference_of(completed) for completed in completions}
    assert len(references) == call_count
    attempt_ids = {
        completed.result.execution_id.attempt_id for completed in completions
    }
    assert len(attempt_ids) == call_count


@requires_macos
def test_every_call_gets_a_fresh_child_and_distinct_job_ids_get_distinct_attempt_ids(
    harness: Harness,
) -> None:
    target = TrustedCommandTarget(
        argv=python_command(
            "import os, sys; sys.stdout.write(str(os.getpid()))"
        )
    )
    first_job = ExecutionJob(
        job_id=JobId(uuid4()),
        target=target,
        env=EnvGrant.none(),
        budgets=Budgets.unbudgeted(),
    )
    second_job = ExecutionJob(
        job_id=JobId(uuid4()),
        target=target,
        env=EnvGrant.none(),
        budgets=Budgets.unbudgeted(),
    )
    first = harness.execute(first_job)
    second = harness.execute(second_job)

    assert (
        first.result.execution_id.attempt_id
        != second.result.execution_id.attempt_id
    )
    assert (
        first.result.payload_outputs.stdout.head
        != second.result.payload_outputs.stdout.head
    )


@requires_macos
def test_bootstrap_launch_failure_closes_every_attempt_resource(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
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
    record = harness.store.load(harness.only_record_reference())
    assert record.state is RecordState.PREPARED
    with pytest.raises(ChildProcessError):
        os.waitpid(-1, os.WNOHANG)


@requires_macos
def test_a_started_output_worker_failure_raises_after_lifecycle_cleanup(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:

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
    record = harness.store.load(harness.only_record_reference())
    assert record.state is RecordState.RUNNING
    with pytest.raises(ChildProcessError):
        os.waitpid(-1, os.WNOHANG)


@requires_macos
def test_an_escaped_stdin_oserror_remains_ordinary_transport_behavior(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:

    def failing_feed(*_: object) -> None:
        raise OSError(errno.EPIPE, "synthetic closed stdin")

    monkeypatch.setattr(dr_exec.execution.engine, "_feed", failing_feed)

    completed = harness.run(python_command("pass"), stdin=b"unread input")

    assert completed.result.outcome == ExitedOutcome(exit_code=0)


@requires_macos
def test_a_store_failure_after_the_spawn_still_reaps_the_direct_child(
    harness: Harness,
) -> None:

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


@requires_macos
def test_a_stalled_bootstrap_is_stopped_by_the_startup_budget(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
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
    completed = harness.run(
        python_command("pass"),
        self_budgets=ExecutorSelfBudgets.unbudgeted(),
    )

    assert completed.result.outcome == ExitedOutcome(exit_code=0)


@requires_macos
def test_a_setup_failure_after_setsid_still_tears_down_the_group(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
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
