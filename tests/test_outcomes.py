"""Behavior tests for outcomes, truncation marks, and results."""

from __future__ import annotations

import pytest

from dr_exec.errors import DeclarationError
from dr_exec.record import (
    Attribution,
    BudgetAxis,
    Measurements,
    Outcome,
    RunResult,
    TruncationMark,
    new_run_id,
)


class TestOutcome:
    def test_budget_attribution_names_its_axis(self) -> None:
        outcome = Outcome(
            attribution=Attribution.BUDGET, violated_axis=BudgetAxis.WALL_CLOCK
        )
        assert outcome.violated_axis is BudgetAxis.WALL_CLOCK

    def test_budget_attribution_without_an_axis_is_unconstructable(self) -> None:
        with pytest.raises(DeclarationError, match="violated axis"):
            Outcome(attribution=Attribution.BUDGET)

    def test_non_budget_attribution_with_an_axis_is_unconstructable(self) -> None:
        with pytest.raises(DeclarationError, match="violated axis"):
            Outcome(attribution=Attribution.PAYLOAD, violated_axis=BudgetAxis.OUTPUT)

    def test_machine_attribution_preserves_the_spawn_errno(self) -> None:
        outcome = Outcome(attribution=Attribution.MACHINE, spawn_errno=13)
        assert outcome.spawn_errno == 13

    def test_absence_is_its_own_attribution(self) -> None:
        outcome = Outcome(attribution=Attribution.ABSENCE)
        assert outcome.attribution is Attribution.ABSENCE
        assert outcome.violated_axis is None

    def test_outcome_is_frozen(self) -> None:
        outcome = Outcome(attribution=Attribution.PAYLOAD)
        with pytest.raises(AttributeError):
            outcome.attribution = Attribution.EXECUTOR


class TestTruncationMark:
    def test_no_drops_by_default(self) -> None:
        mark = TruncationMark()
        assert mark.stdout_bytes_dropped == 0
        assert mark.stderr_bytes_dropped == 0
        assert mark.any_dropped is False

    def test_any_dropped_reports_either_stream(self) -> None:
        assert TruncationMark(stdout_bytes_dropped=1).any_dropped is True
        assert TruncationMark(stderr_bytes_dropped=1).any_dropped is True


class TestRunResult:
    def test_returncode_is_raw_including_signal_values(self) -> None:
        result = RunResult(
            returncode=-11,
            stdout="",
            stderr="Segmentation fault",
            truncation=TruncationMark(),
            measurements=Measurements(
                duration_seconds=0.1,
                teardown_seconds=0.001,
                stdout_bytes_produced=0,
                stderr_bytes_produced=18,
                input_bytes=0,
            ),
            outcome=Outcome(attribution=Attribution.PAYLOAD),
        )
        assert result.returncode == -11
        assert result.outcome.attribution is Attribution.PAYLOAD

    def test_absent_program_has_no_returncode(self) -> None:
        result = RunResult(
            returncode=None,
            stdout="",
            stderr="",
            truncation=TruncationMark(),
            measurements=Measurements(
                duration_seconds=0.0,
                teardown_seconds=0.0,
                stdout_bytes_produced=0,
                stderr_bytes_produced=0,
                input_bytes=0,
            ),
            outcome=Outcome(attribution=Attribution.ABSENCE),
        )
        assert result.returncode is None


class TestMeasurements:
    def test_bytes_produced_can_exceed_a_truncation_bound(self) -> None:
        measurements = Measurements(
            duration_seconds=1.0,
            teardown_seconds=0.01,
            stdout_bytes_produced=5000,
            stderr_bytes_produced=0,
            input_bytes=10,
        )
        mark = TruncationMark(stdout_bytes_dropped=4000)
        assert measurements.stdout_bytes_produced - mark.stdout_bytes_dropped == 1000


class TestRunId:
    def test_is_uuid4_hex(self) -> None:
        run_id = new_run_id()
        assert len(run_id) == 32
        assert set(run_id) <= set("0123456789abcdef")

    def test_is_collision_free_across_calls(self) -> None:
        assert len({new_run_id() for _ in range(100)}) == 100
