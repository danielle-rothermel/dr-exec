"""Retention arithmetic and spawn-bootstrap rendering, in isolation.

These are pure units: retention is byte arithmetic and the bootstrap is
source rendering, so both are exercised without a child. They are what
pins the properties the real-child cases cannot isolate -- that retention
is independent of chunk boundaries, and that no declared value can become
helper syntax.
"""

from __future__ import annotations

import errno
import json
from typing import TYPE_CHECKING

import pytest

from dr_exec import (
    BudgetAxis,
    BudgetExceededOutcome,
    Budgets,
    CancelledOutcome,
    ExitedOutcome,
    FailureOwner,
    FiniteOutput,
    OutputOverflowPolicy,
    PayloadRetentionBudget,
    SignaledOutcome,
    SpawnAbsentOutcome,
    SpawnFailedOutcome,
    StreamRetentionBudget,
    UnbudgetedOutput,
)
from dr_exec._retention import PayloadRetention
from dr_exec._spawn import (
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
from dr_exec.engine import _attribute, _spawn_outcome

if TYPE_CHECKING:
    from dr_exec.declare import OutputBudget
    from dr_exec.record import ExecutionOutcome


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


# --- Retention -----------------------------------------------------------


def test_an_unbudgeted_stream_retains_every_byte_in_one_segment() -> None:
    """No declared budget means no split for a reader to rejoin."""
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
    """Retention is declaration-pinned, not a record of drain scheduling."""
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
    """A run that produces exactly the declared total stayed within it."""
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
    """Neither stream alone crosses it; their sum does."""
    budget = finite_output_budget(
        stdout_head=4, stdout_tail=0, stderr_head=4, stderr_tail=0
    )
    retention = PayloadRetention.for_budget(budget)
    retention.stdout.offer(b"abcde")
    assert not retention.overflowed

    retention.stderr.offer(b"ABCD")
    assert retention.overflowed


def test_retained_and_dropped_bytes_always_equal_production() -> None:
    """The record model enforces this, so the counts must satisfy it."""
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


# --- Spawn bootstrap rendering -------------------------------------------


def test_the_helper_embeds_every_declared_value_as_an_inert_literal() -> None:
    """A hostile argv or scratch path stays data, never helper syntax."""
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
    """These are what the child and its status line spell.

    Every other case reads them symbolically and so would follow a
    rename; only spelling the values out catches silent drift.
    """
    assert SPAWN_HELPER_ARGUMENTS == ("-I", "-c")
    assert STATUS_STAGE_KEY == "stage"
    assert STATUS_ERRNO_KEY == "errno"
    assert SETUP_FAILURE_EXIT_CODE == 127
    assert PAYLOAD_STDIN_DESCRIPTOR == 0
    assert PAYLOAD_STDOUT_DESCRIPTOR == 1
    assert PAYLOAD_STDERR_DESCRIPTOR == 2
    assert PAYLOAD_PROTOCOL_DESCRIPTOR == 3


# --- Setup status classification -----------------------------------------


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
    """Never mistaken for a successful start, which is the dangerous read."""
    failure = parse_setup_status(line)

    assert failure is not None
    assert failure.errno is None


def test_a_status_line_without_an_errno_reports_none() -> None:
    line = json.dumps({STATUS_STAGE_KEY: SETUP_STAGE_EXEC}).encode() + b"\n"

    assert parse_setup_status(line) == SetupFailure(
        stage=SETUP_STAGE_EXEC, errno=None
    )


# --- Spawn outcome classification ----------------------------------------


def test_enoent_from_the_payload_exec_is_spawn_absence() -> None:
    outcome = _spawn_outcome(
        SetupFailure(stage=SETUP_STAGE_EXEC, errno=errno.ENOENT),
        "/declared/executable",
    )

    assert outcome == SpawnAbsentOutcome(executable="/declared/executable")


def test_enoent_from_an_earlier_setup_stage_is_not_spawn_absence() -> None:
    """The same errno from `chdir` means a missing directory, not a tool.

    Reporting it as absence would name the wrong thing missing, which is
    exactly the diagnostic the caller would act on.
    """
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


# --- Attribution ---------------------------------------------------------


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
        (CancelledOutcome(), FailureOwner.NONE),
    ],
)
def test_every_recognized_outcome_gets_one_evidence_based_owner(
    outcome: ExecutionOutcome, owner: FailureOwner
) -> None:
    """Attribution is total over the closed outcome union, and diagnostic.

    It is a classification of the evidence, not causal proof: an ordinary
    nonzero exit is attributed to the payload that produced it because no
    stronger evidence exists, not because the payload was proven at fault.
    """
    assert _attribute(outcome).owner is owner
