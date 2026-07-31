"""Per-stream capture bounds: the seam a protocol channel needs.

Plain runs keep the single shared bound; a run that declares per-stream
bounds measures each stream against its own, so a flood on one can never
consume the other's budget.
"""

from __future__ import annotations

import sys

from dr_exec.declare import (
    PROCESS_BOUNDARY_ONLY,
    Budgets,
    OutputBudget,
    OverflowPolicy,
    Records,
    StreamBounds,
)
from dr_exec.record import Attribution, BudgetAxis
from dr_exec.run import run_untrusted_python

_BOTH_STREAMS_SOURCE = (
    "import sys\n"
    "sys.stdout.write('o' * 4000)\n"
    "sys.stderr.write('e' * 4000)\n"
    "sys.stdout.flush()\n"
    "sys.stderr.flush()\n"
)


def _run(source: str, *, budgets: Budgets, stream_bounds: StreamBounds | None):
    return run_untrusted_python(
        source,
        profile=PROCESS_BOUNDARY_ONLY,
        budgets=budgets,
        records=Records.none(),
        stream_bounds=stream_bounds,
    )


class TestSharedBoundRemainsTheDefault:
    def test_without_stream_bounds_both_streams_draw_from_one_bound(self) -> None:
        result = _run(
            _BOTH_STREAMS_SOURCE,
            budgets=Budgets(
                wall_clock=10.0,
                output=OutputBudget(
                    limit_bytes=5000,
                    overflow_policy=OverflowPolicy.MARKED_TRUNCATION,
                ),
            ),
            stream_bounds=None,
        )

        assert len(result.stdout) + len(result.stderr) == 5000
        assert result.truncation.any_dropped is True


class TestPerStreamBounds:
    def test_each_stream_is_measured_against_its_own_declared_bound(self) -> None:
        result = _run(
            _BOTH_STREAMS_SOURCE,
            budgets=Budgets(
                wall_clock=10.0,
                output=OutputBudget(
                    limit_bytes=100, overflow_policy=OverflowPolicy.MARKED_TRUNCATION
                ),
            ),
            stream_bounds=StreamBounds(stdout_bytes=4000, stderr_bytes=1000),
        )

        assert len(result.stdout) == 4000
        assert len(result.stderr) == 1000
        assert result.truncation.stdout_bytes_dropped == 0
        assert result.truncation.stderr_bytes_dropped == 3000

    def test_a_flood_on_one_stream_never_shrinks_the_other_stream_capture(
        self,
    ) -> None:
        result = _run(
            "import sys\n"
            "sys.stderr.write('e' * 200000)\n"
            "sys.stdout.write('o' * 100)\n"
            "sys.stdout.flush()\n",
            budgets=Budgets(wall_clock=10.0),
            stream_bounds=StreamBounds(stdout_bytes=1000, stderr_bytes=2000),
        )

        assert result.stdout == "o" * 100
        assert len(result.stderr) == 2000
        assert result.measurements.stderr_bytes_produced == 200000

    def test_an_unset_axis_falls_back_to_the_shared_bound(self) -> None:
        result = _run(
            _BOTH_STREAMS_SOURCE,
            budgets=Budgets(
                wall_clock=10.0,
                output=OutputBudget(
                    limit_bytes=500, overflow_policy=OverflowPolicy.MARKED_TRUNCATION
                ),
            ),
            stream_bounds=StreamBounds(stdout_bytes=4000),
        )

        assert len(result.stdout) == 4000
        assert len(result.stderr) == 500

    def test_crossing_a_per_stream_bound_under_fail_is_an_output_budget_outcome(
        self,
    ) -> None:
        result = _run(
            "import sys, time\n"
            "sys.stderr.write('e' * 40000)\n"
            "sys.stderr.flush()\n"
            "time.sleep(30)\n",
            budgets=Budgets(
                wall_clock=10.0,
                output=OutputBudget(
                    limit_bytes=1024, overflow_policy=OverflowPolicy.FAIL
                ),
            ),
            stream_bounds=StreamBounds(stdout_bytes=1024, stderr_bytes=1024),
        )

        assert result.outcome.attribution is Attribution.BUDGET
        assert result.outcome.violated_axis is BudgetAxis.OUTPUT


def test_the_interpreter_under_test_is_the_one_that_runs() -> None:
    result = _run(
        "import sys; sys.stdout.write(sys.executable)",
        budgets=Budgets(wall_clock=10.0),
        stream_bounds=None,
    )

    assert result.stdout == sys.executable
