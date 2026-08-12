from __future__ import annotations

import errno
import json
import os
import signal
from contextlib import suppress
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, cast

import pytest

from dr_exec import (
    BudgetAxis,
    BudgetExceededOutcome,
    Budgets,
    CancelledOutcome,
    ExecutorFailure,
    ExecutorSelfBudgets,
    ExitedOutcome,
    FailureOwner,
    FiniteDurationLimit,
    FiniteOutput,
    OutputOverflowPolicy,
    PayloadRetentionBudget,
    ProtocolFailedOutcome,
    ProtocolFailureCode,
    SignaledOutcome,
    SpawnAbsentOutcome,
    SpawnFailedOutcome,
    StreamRetentionBudget,
    UnbudgetedOutput,
)
from dr_exec.execution.engine import (
    _attribute,
    _DrainState,
    _OutputPump,
    _spawn_outcome,
    _started_thread,
    _tear_down,
)
from dr_exec.execution.retention import PayloadRetention
from dr_exec.execution.spawn import (
    PAYLOAD_PROTOCOL_DESCRIPTOR,
    PAYLOAD_STDERR_DESCRIPTOR,
    PAYLOAD_STDIN_DESCRIPTOR,
    PAYLOAD_STDOUT_DESCRIPTOR,
    SETUP_FAILURE_EXIT_CODE,
    SETUP_STAGE_CHDIR,
    SETUP_STAGE_EXEC,
    SETUP_STAGE_SESSION,
    SPAWN_HELPER_ARGUMENTS,
    STATUS_ERRNO_KEY,
    STATUS_STAGE_KEY,
    SetupFailure,
    parse_setup_status,
    spawn_bootstrap_source,
)

if TYPE_CHECKING:
    import subprocess

    from dr_exec.declarations.models import OutputBudget
    from dr_exec.recording.models import ExecutionOutcome


def finite_output_budget(
    *,
    policy: OutputOverflowPolicy = OutputOverflowPolicy.MARKED_TRUNCATION,
    stdout_head: int = 4,
    stdout_tail: int = 4,
    stderr_head: int = 2,
    stderr_tail: int = 2,
) -> OutputBudget:
    return FiniteOutput(
        max_bytes=stdout_head + stdout_tail + stderr_head + stderr_tail,
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


@dataclass(slots=True)
class _ProcessProbe:
    pid: int = 4321
    signals: list[int] = field(default_factory=list)
    wait_calls: int = 0

    def send_signal(self, number: int, /) -> None:
        self.signals.append(number)

    def wait(self, timeout: float | None = None) -> int:
        del timeout
        self.wait_calls += 1
        return 0


def test_an_unbudgeted_stream_retains_every_byte_in_one_segment() -> None:
    retention = PayloadRetention.for_budget(UnbudgetedOutput())
    retention.stdout.offer(b"abc")
    retention.stdout.offer(b"def")

    stream = retention.stdout.snapshot()
    assert stream.head == b"abcdef"
    assert stream.tail == b""
    assert stream.dropped_bytes == 0
    assert stream.produced_bytes == 6


@pytest.mark.parametrize("chunk_size", [1, 2, 3, 5, 7, 64])
def test_retention_is_identical_however_the_bytes_arrive(
    chunk_size: int,
) -> None:
    produced = bytes(range(48, 78))
    retention = PayloadRetention.for_budget(finite_output_budget())
    for offset in range(0, len(produced), chunk_size):
        retention.stdout.offer(produced[offset : offset + chunk_size])

    stream = retention.stdout.snapshot()
    assert stream.head == produced[:4]
    assert stream.tail == produced[-4:]
    assert stream.produced_bytes == len(produced)
    assert stream.dropped_bytes == len(produced) - 8


def test_a_zero_tail_allocation_drops_everything_past_the_head() -> None:
    budget = finite_output_budget(stdout_head=3, stdout_tail=0)
    retention = PayloadRetention.for_budget(budget)
    retention.stdout.offer(b"abcdefgh")

    stream = retention.stdout.snapshot()
    assert stream.head == b"abc"
    assert stream.tail == b""
    assert stream.dropped_bytes == 5


def test_a_zero_head_allocation_keeps_only_the_tail() -> None:
    budget = finite_output_budget(stdout_head=0, stdout_tail=3)
    retention = PayloadRetention.for_budget(budget)
    retention.stdout.offer(b"abcdefgh")

    stream = retention.stdout.snapshot()
    assert stream.head == b""
    assert stream.tail == b"fgh"
    assert stream.dropped_bytes == 5


def test_production_at_exactly_the_aggregate_budget_has_not_overflowed() -> (
    None
):
    budget = finite_output_budget(
        stdout_head=2, stdout_tail=2, stderr_head=2, stderr_tail=2
    )
    retention = PayloadRetention.for_budget(budget)
    retention.stdout.offer(b"abcd")
    retention.stderr.offer(b"ABCD")

    assert retention.produced_bytes == 8
    assert not retention.overflowed


def test_one_byte_past_the_aggregate_budget_overflows() -> None:
    budget = finite_output_budget(
        stdout_head=2, stdout_tail=2, stderr_head=2, stderr_tail=2
    )
    retention = PayloadRetention.for_budget(budget)
    retention.stdout.offer(b"abcd")
    retention.stderr.offer(b"ABCDE")

    assert retention.overflowed


def test_the_aggregate_budget_spans_both_streams_together() -> None:
    budget = finite_output_budget(
        stdout_head=4, stdout_tail=0, stderr_head=4, stderr_tail=0
    )
    retention = PayloadRetention.for_budget(budget)
    retention.stdout.offer(b"abcde")
    assert not retention.overflowed

    retention.stderr.offer(b"ABCD")
    assert retention.overflowed


def test_retained_and_dropped_bytes_always_equal_production() -> None:
    retention = PayloadRetention.for_budget(finite_output_budget())
    retention.stdout.offer(bytes(range(100)))
    retention.stderr.offer(bytes(range(50)))

    outputs = retention.snapshot()
    for stream in (outputs.stdout, outputs.stderr):
        assert (
            len(stream.head) + len(stream.tail) + stream.dropped_bytes
            == stream.produced_bytes
        )


def test_the_default_budgets_install_no_output_retention_limit() -> None:
    retention = PayloadRetention.for_budget(
        Budgets.unbudgeted().payload_output
    )

    assert retention.max_total_bytes is None
    assert not retention.overflowed


def test_the_helper_embeds_every_declared_value_as_an_inert_literal() -> None:
    hostile = "'\"\\\n#'''" + '"""'
    source = spawn_bootstrap_source(
        executable=hostile,
        argv=(hostile, hostile),
        scratch_directory=hostile,
        descriptor_map=((7, 0),),
        status_descriptor=9,
    )

    namespace: dict[str, object] = {}
    bindings, _, _ = source.partition("\nimport json")
    exec(bindings, namespace)  # noqa: S102 - the embedding is under test
    assert namespace["_DR_EXEC_EXECUTABLE"] == hostile
    assert namespace["_DR_EXEC_ARGV"] == [hostile, hostile]
    assert namespace["_DR_EXEC_SCRATCH_DIRECTORY"] == hostile
    assert namespace["_DR_EXEC_DESCRIPTOR_MAP"] == [[7, 0]]


def test_the_helper_renders_the_pinned_literals_once_each() -> None:
    source = spawn_bootstrap_source(
        executable="/bin/true",
        argv=("/bin/true",),
        scratch_directory="/tmp",
        descriptor_map=((7, 0),),
        status_descriptor=9,
    )

    assert source.count("_DR_EXEC_STATUS_DESCRIPTOR = ") == 1
    assert source.count("_DR_EXEC_SETUP_FAILURE_EXIT_CODE = ") == 1
    assert "_DR_EXEC_HIGHEST_TARGET_DESCRIPTOR = 3\n" in source


def test_the_child_observable_spawn_literals_are_exactly_pinned() -> None:
    assert SPAWN_HELPER_ARGUMENTS == ("-I", "-c")
    assert STATUS_STAGE_KEY == "stage"
    assert STATUS_ERRNO_KEY == "errno"
    assert SETUP_FAILURE_EXIT_CODE == 127
    assert PAYLOAD_STDIN_DESCRIPTOR == 0
    assert PAYLOAD_STDOUT_DESCRIPTOR == 1
    assert PAYLOAD_STDERR_DESCRIPTOR == 2
    assert PAYLOAD_PROTOCOL_DESCRIPTOR == 3


def test_an_empty_status_pipe_means_the_payload_exec_succeeded() -> None:
    assert parse_setup_status(b"") is None


def test_a_status_line_reports_its_stage_and_errno() -> None:
    line = (
        json.dumps(
            {
                STATUS_STAGE_KEY: SETUP_STAGE_CHDIR,
                STATUS_ERRNO_KEY: errno.EACCES,
            }
        ).encode()
        + b"\n"
    )

    assert parse_setup_status(line) == SetupFailure(
        stage=SETUP_STAGE_CHDIR, errno=errno.EACCES
    )


@pytest.mark.parametrize(
    "line",
    [
        b"not json at all\n",
        b"[1, 2, 3]\n",
        b"\xff\xfe\n",
        b'{"unexpected": "shape"}\n',
    ],
)
def test_an_unreadable_status_line_is_still_a_setup_failure(
    line: bytes,
) -> None:
    failure = parse_setup_status(line)

    assert failure is not None
    assert failure.errno is None


def test_a_status_line_without_an_errno_reports_none() -> None:
    line = json.dumps({STATUS_STAGE_KEY: SETUP_STAGE_EXEC}).encode() + b"\n"

    assert parse_setup_status(line) == SetupFailure(
        stage=SETUP_STAGE_EXEC, errno=None
    )


def test_enoent_from_the_payload_exec_is_spawn_absence() -> None:
    outcome = _spawn_outcome(
        SetupFailure(stage=SETUP_STAGE_EXEC, errno=errno.ENOENT),
        "/declared/executable",
    )

    assert outcome == SpawnAbsentOutcome(executable="/declared/executable")


def test_enoent_from_an_earlier_setup_stage_is_not_spawn_absence() -> None:
    outcome = _spawn_outcome(
        SetupFailure(stage=SETUP_STAGE_CHDIR, errno=errno.ENOENT),
        "/declared/executable",
    )

    assert outcome == SpawnFailedOutcome(
        errno=errno.ENOENT, error_message=SETUP_STAGE_CHDIR
    )


def test_a_setup_failure_preserves_its_errno_as_a_spawn_failure() -> None:
    outcome = _spawn_outcome(
        SetupFailure(stage=SETUP_STAGE_SESSION, errno=errno.EPERM),
        "/declared/executable",
    )

    assert isinstance(outcome, SpawnFailedOutcome)
    assert outcome.errno == errno.EPERM


def test_a_setup_failure_with_no_reported_errno_still_classifies() -> None:
    outcome = _spawn_outcome(
        SetupFailure(stage=SETUP_STAGE_EXEC, errno=None),
        "/declared/executable",
    )

    assert outcome == SpawnFailedOutcome(
        errno=0, error_message=SETUP_STAGE_EXEC
    )


@pytest.mark.parametrize(
    ("outcome", "owner"),
    [
        (ExitedOutcome(exit_code=0), FailureOwner.NONE),
        (ExitedOutcome(exit_code=1), FailureOwner.PAYLOAD),
        (SignaledOutcome(signal_number=9), FailureOwner.PAYLOAD),
        (SpawnAbsentOutcome(executable="/x"), FailureOwner.EXECUTOR),
        (
            SpawnFailedOutcome(errno=13, error_message="exec"),
            FailureOwner.MACHINE,
        ),
        (
            BudgetExceededOutcome(axis=BudgetAxis.WALL_TIME),
            FailureOwner.PAYLOAD,
        ),
        (
            ProtocolFailedOutcome(
                failure_code=ProtocolFailureCode.INCOMPLETE_STREAM,
                failure_detail="stopped",
                accepted_output_count=0,
            ),
            FailureOwner.PAYLOAD,
        ),
        (
            ProtocolFailedOutcome(
                failure_code=ProtocolFailureCode.OVERSIZED_FRAME,
                failure_detail="bounded",
                accepted_output_count=0,
            ),
            FailureOwner.EXECUTOR,
        ),
        (CancelledOutcome(), FailureOwner.NONE),
    ],
)
def test_every_recognized_outcome_gets_one_evidence_based_owner(
    outcome: ExecutionOutcome, owner: FailureOwner
) -> None:
    assert _attribute(outcome).owner is owner


def test_a_transport_worker_captures_non_exception_base_failures() -> None:

    def stop_thread() -> None:
        raise SystemExit("synthetic worker exit")

    worker = _started_thread(stop_thread, "dr-exec-test")
    worker.thread.join(timeout=1)

    assert not worker.thread.is_alive()
    with pytest.raises(ExecutorFailure, match="test transport worker") as exc:
        worker.raise_if_failed()
    assert isinstance(exc.value.__cause__, SystemExit)


def test_session_stage_teardown_signals_only_the_direct_child(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = _ProcessProbe()
    group_signals: list[tuple[int, int]] = []
    monkeypatch.setattr(
        "dr_exec.execution.engine.signal_process_group",
        lambda pid, number: group_signals.append((pid, number)),
    )
    monkeypatch.setattr(
        "dr_exec.execution.engine._reaped_within",
        lambda *_: True,
    )

    _tear_down(
        cast("subprocess.Popen[bytes]", process),
        ExecutorSelfBudgets(
            termination_time=FiniteDurationLimit(max_ns=1_000_000)
        ),
        leads_group=False,
    )

    assert group_signals == []
    assert process.signals == [signal.SIGTERM]
    assert process.wait_calls == 1


def test_unbudgeted_termination_grace_waits_for_sigterm_before_sigkill(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = _ProcessProbe()
    group_signals: list[tuple[int, int]] = []
    monkeypatch.setattr(
        "dr_exec.execution.engine.signal_process_group",
        lambda pid, number: group_signals.append((pid, number)),
    )
    monkeypatch.setattr(
        "dr_exec.execution.engine._group_survives",
        lambda *_: False,
    )

    _tear_down(
        cast("subprocess.Popen[bytes]", process),
        ExecutorSelfBudgets.unbudgeted(),
    )

    assert [number for _, number in group_signals] == [signal.SIGTERM]
    assert process.signals == [signal.SIGTERM]
    assert process.wait_calls == 2


@pytest.mark.parametrize(
    ("reaped_after_term", "expected_signals"),
    [
        pytest.param(True, [signal.SIGTERM], id="cooperative"),
        pytest.param(
            False,
            [signal.SIGTERM, signal.SIGKILL],
            id="escalated",
        ),
    ],
)
def test_teardown_escalation_follows_the_termination_result(
    monkeypatch: pytest.MonkeyPatch,
    reaped_after_term: bool,
    expected_signals: list[int],
) -> None:
    process = _ProcessProbe()
    group_signals: list[tuple[int, int]] = []
    monkeypatch.setattr(
        "dr_exec.execution.engine.signal_process_group",
        lambda pid, number: group_signals.append((pid, number)) or True,
    )
    monkeypatch.setattr(
        "dr_exec.execution.engine._reaped_within",
        lambda *_: reaped_after_term,
    )
    monkeypatch.setattr(
        "dr_exec.execution.engine._group_survives",
        lambda *_: False,
    )

    _tear_down(
        cast("subprocess.Popen[bytes]", process),
        ExecutorSelfBudgets(
            termination_time=FiniteDurationLimit(max_ns=1_000_000)
        ),
    )

    assert [number for _, number in group_signals] == expected_signals
    assert process.signals == expected_signals
    assert process.wait_calls == 1


def test_a_pump_that_cannot_register_still_closes_what_it_owns() -> None:
    forward_read, forward_write = os.pipe()
    release_read, release_write = os.pipe()
    stdout_read, stdout_write = os.pipe()
    stderr_read, stderr_write = os.pipe()
    for descriptor in (
        forward_read,
        release_write,
        stdout_write,
        stderr_write,
    ):
        os.close(descriptor)
    # A descriptor closed out from under the pump makes registration fail
    # the way an already-invalid one would.
    os.close(stderr_read)

    pump = _OutputPump(
        state=_DrainState(
            retention=PayloadRetention.for_budget(UnbudgetedOutput())
        ),
        stdout_descriptor=stdout_read,
        stderr_descriptor=stderr_read,
        protocol_descriptor=None,
        protocol_forward=forward_write,
        release_descriptor=release_read,
    )
    with pytest.raises(OSError):
        pump.run()

    with pytest.raises(OSError) as raised:
        os.fstat(forward_write)
    assert raised.value.errno == errno.EBADF

    for descriptor in (stdout_read, release_read):
        with suppress(OSError):
            os.close(descriptor)
