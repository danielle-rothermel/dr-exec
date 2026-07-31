"""Scripted results the engine could never have produced are refused.

The rule these tests defend: a test that passes against the fake cannot be
wrong about the contract. Each rejection here mirrors an invariant the engine
holds on the real path, so scripting an impossible outcome fails at the fake
rather than encoding a false belief in a consumer's test suite.
"""

from __future__ import annotations

import errno

import pytest

from dr_exec.declare import (
    PROCESS_BOUNDARY_ONLY,
    Budgets,
    ExitPolicy,
    ExitVerdict,
    OutputBudget,
    OverflowPolicy,
    Records,
    StreamBounds,
)
from dr_exec.fake import FakeExecutor, ScriptError
from dr_exec.record import (
    Attribution,
    BudgetAxis,
    Measurements,
    Outcome,
    RunResult,
    TruncationMark,
)

from .conftest import QUICK, outcome_result, payload_result


def _returned(result: RunResult, **call_arguments: object) -> RunResult:
    fake = FakeExecutor()
    fake.enqueue(result)
    arguments = {"budgets": QUICK, "records": Records.none(), **call_arguments}
    return fake.run_tool(["/bin/echo"], **arguments)  # type: ignore[arg-type]


def _refused(result: RunResult, **call_arguments: object) -> str:
    with pytest.raises(ScriptError) as raised:
        _returned(result, **call_arguments)
    return str(raised.value)


class TestAttributionShape:
    def test_a_budget_outcome_must_name_an_axis(self) -> None:
        with pytest.raises(ValueError, match="exactly one violated axis"):
            Outcome(attribution=Attribution.BUDGET)

    def test_an_axis_without_a_budget_attribution_is_refused(self) -> None:
        with pytest.raises(ValueError, match="exactly one violated axis"):
            Outcome(
                attribution=Attribution.PAYLOAD, violated_axis=BudgetAxis.WALL_CLOCK
            )

    def test_a_budget_outcome_carries_a_returncode(self) -> None:
        outcome = Outcome(
            attribution=Attribution.BUDGET, violated_axis=BudgetAxis.WALL_CLOCK
        )

        assert "carries a returncode" in _refused(outcome_result(outcome))

    def test_a_budget_outcome_with_a_signal_returncode_is_accepted(self) -> None:
        outcome = Outcome(
            attribution=Attribution.BUDGET, violated_axis=BudgetAxis.WALL_CLOCK
        )

        result = _returned(outcome_result(outcome, returncode=-9))

        assert result.outcome.violated_axis is BudgetAxis.WALL_CLOCK
        assert result.returncode == -9


class TestAbsenceAndMachine:
    def test_an_absence_outcome_has_no_returncode(self) -> None:
        outcome = Outcome(attribution=Attribution.ABSENCE, spawn_errno=errno.ENOENT)

        assert "never spawned" in _refused(outcome_result(outcome, returncode=127))

    def test_an_absence_outcome_carries_the_errno_that_decided_it(self) -> None:
        outcome = Outcome(attribution=Attribution.ABSENCE)

        assert "spawn errno" in _refused(outcome_result(outcome))

    def test_an_absence_outcome_captured_no_output(self) -> None:
        result = RunResult(
            returncode=None,
            stdout="ghost output",
            stderr="",
            truncation=TruncationMark(),
            measurements=Measurements(
                duration_seconds=0.0,
                teardown_seconds=0.0,
                stdout_bytes_produced=12,
                stderr_bytes_produced=0,
                input_bytes=0,
            ),
            outcome=Outcome(attribution=Attribution.ABSENCE, spawn_errno=errno.ENOENT),
        )

        assert "captured" in _refused(result)

    def test_a_well_formed_absence_outcome_is_accepted(self) -> None:
        outcome = Outcome(attribution=Attribution.ABSENCE, spawn_errno=errno.ENOENT)

        assert _returned(outcome_result(outcome)).outcome is outcome

    def test_a_machine_outcome_keeps_its_errno(self) -> None:
        outcome = Outcome(attribution=Attribution.MACHINE, spawn_errno=errno.EACCES)

        assert _returned(outcome_result(outcome)).outcome.spawn_errno == errno.EACCES

    def test_a_payload_outcome_may_not_carry_a_spawn_errno(self) -> None:
        outcome = Outcome(
            attribution=Attribution.PAYLOAD,
            spawn_errno=errno.ENOENT,
            exit_verdict=ExitVerdict.REPORT_ONLY.value,
        )

        assert "spawn errno belongs" in _refused(outcome_result(outcome, returncode=0))


class TestExitVerdict:
    def test_a_payload_verdict_must_match_the_declared_policy(self) -> None:
        policy = ExitPolicy(name="candidate", verdicts={0: ExitVerdict.SUCCESS})

        message = _refused(
            payload_result(returncode=0, exit_verdict=ExitVerdict.FAILURE.value),
            exit_policy=policy,
        )

        assert "'success'" in message

    def test_the_matching_verdict_is_accepted(self) -> None:
        policy = ExitPolicy(name="candidate", verdicts={0: ExitVerdict.SUCCESS})

        result = _returned(
            payload_result(returncode=0, exit_verdict=ExitVerdict.SUCCESS.value),
            exit_policy=policy,
        )

        assert result.outcome.exit_verdict == ExitVerdict.SUCCESS.value

    def test_a_non_payload_outcome_carries_no_exit_verdict(self) -> None:
        outcome = Outcome(
            attribution=Attribution.BUDGET,
            violated_axis=BudgetAxis.OUTPUT,
            exit_verdict=ExitVerdict.FAILURE.value,
        )

        assert "exit verdict belongs" in _refused(
            outcome_result(outcome, returncode=-9)
        )


class TestCaptureAccounting:
    def test_truncation_requires_a_bound_to_have_been_crossed(self) -> None:
        result = payload_result(
            truncation=TruncationMark(stdout_bytes_dropped=10),
            stdout_bytes_produced=10,
        )

        assert "truncation mark requires" in _refused(result)

    def test_truncation_is_accepted_under_a_declared_output_budget(self) -> None:
        budgets = Budgets(
            wall_clock=1.0,
            output=OutputBudget(
                limit_bytes=4, overflow_policy=OverflowPolicy.MARKED_TRUNCATION
            ),
        )
        result = payload_result(
            stdout="abcd",
            truncation=TruncationMark(stdout_bytes_dropped=6),
            stdout_bytes_produced=10,
        )

        assert _returned(result, budgets=budgets).truncation.stdout_bytes_dropped == 6

    def test_bytes_produced_never_undercount_what_was_retained(self) -> None:
        result = payload_result(stdout="abcdef", stdout_bytes_produced=2)

        assert "retained more" in _refused(result)

    def test_measurements_never_go_negative(self) -> None:
        result = payload_result(stderr_bytes_produced=-1)

        assert "never a negative" in _refused(result)

    def test_input_bytes_must_match_the_declared_input(self) -> None:
        result = payload_result(input_bytes=0)

        assert "input bytes" in _refused(result, input_text="fed")

    def test_matching_input_bytes_are_accepted(self) -> None:
        result = payload_result(input_bytes=3)

        assert _returned(result, input_text="fed").measurements.input_bytes == 3


class TestStreamBoundedRuns:
    def test_truncation_is_accepted_under_declared_stream_bounds(self) -> None:
        fake = FakeExecutor()
        fake.enqueue(
            payload_result(
                stdout="ab",
                truncation=TruncationMark(stdout_bytes_dropped=8),
                stdout_bytes_produced=10,
            )
        )

        result = fake.run_untrusted_python(
            "print(1)",
            profile=PROCESS_BOUNDARY_ONLY,
            budgets=QUICK,
            records=Records.none(),
            stream_bounds=StreamBounds(stdout_bytes=2),
        )

        assert result.truncation.stdout_bytes_dropped == 8
