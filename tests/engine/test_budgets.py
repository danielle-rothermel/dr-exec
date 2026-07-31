"""Budget enforcement, attribution precedence, and byte-denominated bounds."""

from __future__ import annotations

import sys
from collections.abc import Callable

import pytest

from dr_exec import engine
from dr_exec.declare import (
    REPORT_ONLY,
    Budgets,
    ExitVerdict,
    OutputBudget,
    OverflowPolicy,
    Records,
)
from dr_exec.errors import DeclarationError
from dr_exec.record import Attribution, BudgetAxis, Outcome, RunResult
from dr_exec.run import run_tool

from .conftest import output_budget

_INPUT_BOUND_BYTES = 256 * 1024

_FLOOD_STDOUT_SOURCE = (
    "import sys\nsys.stdout.buffer.write(b'x' * 40000)\nsys.stdout.buffer.flush()\n"
)


class TestWallClockBudget:
    def test_an_expired_deadline_is_a_wall_clock_budget_outcome(
        self, run_python: Callable[..., RunResult]
    ) -> None:
        result = run_python(
            "import time; time.sleep(60)", budgets=Budgets(wall_clock=0.2)
        )

        assert result.outcome.attribution is Attribution.BUDGET
        assert result.outcome.violated_axis is BudgetAxis.WALL_CLOCK

    def test_an_unbudgeted_wall_clock_lets_a_quick_run_finish(
        self, run_python: Callable[..., RunResult]
    ) -> None:
        result = run_python("print('done')", budgets=Budgets())

        assert result.stdout == "done\n"
        assert result.outcome.attribution is Attribution.PAYLOAD


class TestOutputBudgetFail:
    def test_flooding_past_a_fail_bound_is_an_output_budget_outcome(
        self, run_python: Callable[..., RunResult]
    ) -> None:
        result = run_python(
            _FLOOD_STDOUT_SOURCE,
            budgets=output_budget(1024, OverflowPolicy.FAIL),
        )

        assert result.outcome.attribution is Attribution.BUDGET
        assert result.outcome.violated_axis is BudgetAxis.OUTPUT

    def test_output_captured_before_the_bound_is_retained_and_marked(
        self, run_python: Callable[..., RunResult]
    ) -> None:
        result = run_python(
            _FLOOD_STDOUT_SOURCE,
            budgets=output_budget(1024, OverflowPolicy.FAIL),
        )

        assert result.stdout == "x" * 1024
        assert result.truncation.any_dropped is True

    def test_the_bound_is_shared_by_stdout_and_stderr(
        self, run_python: Callable[..., RunResult]
    ) -> None:
        result = run_python(
            "import os\nos.write(1, b'x' * 600)\nos.write(2, b'y' * 600)\n",
            budgets=output_budget(1024, OverflowPolicy.FAIL),
        )

        assert result.outcome.attribution is Attribution.BUDGET
        assert result.outcome.violated_axis is BudgetAxis.OUTPUT
        assert len(result.stdout) + len(result.stderr) == 1024


class TestOutputBudgetMarkedTruncation:
    def test_a_flooding_child_runs_to_completion_and_the_drop_is_marked(
        self, run_python: Callable[..., RunResult]
    ) -> None:
        result = run_python(
            "import sys\n"
            "sys.stdout.buffer.write(b'x' * 200000)\n"
            "sys.stdout.buffer.flush()\n"
            "raise SystemExit(0)\n",
            budgets=output_budget(1024, OverflowPolicy.MARKED_TRUNCATION),
        )

        assert result.returncode == 0
        assert result.outcome.attribution is Attribution.PAYLOAD
        assert result.stdout == "x" * 1024
        assert result.truncation.stdout_bytes_dropped == 200000 - 1024

    def test_a_payload_writing_far_past_the_bound_is_never_blocked(
        self, run_python: Callable[..., RunResult]
    ) -> None:
        # Far more than any pipe buffer: if capture stopped draining at the
        # bound, the payload would block on write and hit the deadline.
        result = run_python(
            "import sys\n"
            "sys.stdout.buffer.write(b'x' * 4_000_000)\n"
            "sys.stdout.buffer.flush()\n"
            "sys.stderr.write('finished')\n"
            "raise SystemExit(0)\n",
            budgets=output_budget(64, OverflowPolicy.MARKED_TRUNCATION),
        )

        assert result.returncode == 0
        assert result.outcome.attribution is Attribution.PAYLOAD
        assert result.measurements.stdout_bytes_produced == 4_000_000
        assert result.measurements.stderr_bytes_produced == len("finished")
        assert len(result.stdout) + len(result.stderr) == 64

    def test_bytes_produced_counts_past_the_bound(
        self, run_python: Callable[..., RunResult]
    ) -> None:
        result = run_python(
            "import sys\n"
            "sys.stdout.buffer.write(b'x' * 200000)\n"
            "sys.stdout.buffer.flush()\n",
            budgets=output_budget(1024, OverflowPolicy.MARKED_TRUNCATION),
        )

        assert result.measurements.stdout_bytes_produced == 200000


class TestAttributionPrecedenceOrder:
    """The pinned order, over the flag states that discriminate it.

    ``_supervise`` returns on the first enforcement hit, so both flags set
    at once is reachable only through the post-loop race re-check — a state
    no timing-based test can construct reliably. Deciding attribution is a
    pure function of the recorded flags, so the order is pinned here
    directly, where the discriminating state can simply be built.
    """

    @staticmethod
    def _decide(
        *, deadline_expired: bool, output_overflowed: bool, returncode: int = 0
    ) -> Outcome:
        return engine._decide_attribution(
            returncode=returncode,
            enforcement=engine._Enforcement(
                deadline_expired=deadline_expired,
                output_overflowed=output_overflowed,
            ),
            exit_policy=REPORT_ONLY,
        )

    def test_an_overflow_that_also_expired_the_deadline_is_an_output_outcome(
        self,
    ) -> None:
        outcome = self._decide(deadline_expired=True, output_overflowed=True)

        assert outcome.attribution is Attribution.BUDGET
        assert outcome.violated_axis is BudgetAxis.OUTPUT

    def test_an_overflow_alone_is_an_output_outcome(self) -> None:
        outcome = self._decide(deadline_expired=False, output_overflowed=True)

        assert outcome.violated_axis is BudgetAxis.OUTPUT

    def test_a_deadline_alone_is_a_wall_clock_outcome(self) -> None:
        outcome = self._decide(deadline_expired=True, output_overflowed=False)

        assert outcome.violated_axis is BudgetAxis.WALL_CLOCK

    def test_a_recorded_violation_beats_a_clean_exit(self) -> None:
        outcome = self._decide(
            deadline_expired=False, output_overflowed=True, returncode=0
        )

        assert outcome.attribution is Attribution.BUDGET
        assert outcome.exit_verdict is None

    def test_neither_flag_is_exit_status_interpretation(self) -> None:
        outcome = self._decide(
            deadline_expired=False, output_overflowed=False, returncode=3
        )

        assert outcome.attribution is Attribution.PAYLOAD
        assert outcome.exit_verdict is ExitVerdict.REPORT_ONLY

    def test_a_channel_fault_sits_below_the_budget_axes(self) -> None:
        # The bound the caller declared is what ended the run; a stranded
        # drain is that kill's consequence, never the attribution.
        outcome = engine._decide_attribution(
            returncode=-9,
            enforcement=engine._Enforcement(
                output_overflowed=True,
                ipc_fault=engine._IpcFault(
                    side=engine._IpcSide.STDOUT, error=OSError("severed")
                ),
            ),
            exit_policy=REPORT_ONLY,
        )

        assert outcome.attribution is Attribution.BUDGET

    def test_a_channel_fault_sits_above_exit_status_interpretation(self) -> None:
        outcome = engine._decide_attribution(
            returncode=0,
            enforcement=engine._Enforcement(
                ipc_fault=engine._IpcFault(
                    side=engine._IpcSide.STDOUT, error=OSError("severed")
                )
            ),
            exit_policy=REPORT_ONLY,
        )

        assert outcome.attribution is Attribution.CHANNEL
        assert outcome.exit_verdict is None


class TestAttributionPrecedence:
    def test_a_recorded_overflow_beats_a_clean_exit_that_raced_it(
        self, run_python: Callable[..., RunResult]
    ) -> None:
        result = run_python(
            "import sys\n"
            "sys.stdout.buffer.write(b'x' * 400000)\n"
            "sys.stdout.buffer.flush()\n"
            "raise SystemExit(0)\n",
            budgets=output_budget(1024, OverflowPolicy.FAIL),
        )

        assert result.outcome.attribution is Attribution.BUDGET
        assert result.outcome.violated_axis is BudgetAxis.OUTPUT

    def test_an_overflow_under_a_short_deadline_is_an_output_outcome(
        self, run_python: Callable[..., RunResult]
    ) -> None:
        # An integration companion to the flag-level ordering tests: the
        # overflow lands first here, so this pins the reachable arm, not the
        # both-flags-set state.
        result = run_python(
            "import sys, time\n"
            "sys.stdout.buffer.write(b'x' * 400000)\n"
            "sys.stdout.buffer.flush()\n"
            "time.sleep(60)\n",
            budgets=Budgets(
                wall_clock=0.3,
                output=OutputBudget(
                    limit_bytes=1024, overflow_policy=OverflowPolicy.FAIL
                ),
            ),
        )

        assert result.outcome.attribution is Attribution.BUDGET
        assert result.outcome.violated_axis is BudgetAxis.OUTPUT


class TestByteDenominatedBounds:
    def test_the_bound_counts_bytes_and_a_split_character_decodes_to_replacement(
        self, run_python: Callable[..., RunResult]
    ) -> None:
        # Four ASCII bytes then a three-byte character; a five-byte bound
        # splits the multibyte character mid-sequence.
        result = run_python(
            "import sys\n"
            "sys.stdout.buffer.write(b'abcd' + '\\u4e16'.encode('utf-8'))\n"
            "sys.stdout.buffer.flush()\n",
            budgets=output_budget(5, OverflowPolicy.MARKED_TRUNCATION),
        )

        assert result.stdout == "abcd\ufffd"
        assert result.truncation.stdout_bytes_dropped == 2
        assert result.measurements.stdout_bytes_produced == 7


class TestInputBudget:
    def test_oversized_input_is_rejected_before_any_spawn(self) -> None:
        with pytest.raises(DeclarationError, match="input budget"):
            run_tool(
                [sys.executable, "-I", "-c", "pass"],
                budgets=Budgets(wall_clock=5.0, input=16),
                records=Records.none(),
                input_text="x" * 17,
            )

    def test_input_at_the_declared_bound_round_trips_without_deadlock(
        self, run_python: Callable[..., RunResult]
    ) -> None:
        payload = "x" * _INPUT_BOUND_BYTES

        result = run_python(
            "import sys\n"
            "sys.stdout.write('ready\\n')\n"
            "sys.stdout.flush()\n"
            "data = sys.stdin.buffer.read()\n"
            "sys.stdout.write(str(len(data)))\n",
            budgets=Budgets(wall_clock=20.0, input=_INPUT_BOUND_BYTES),
            input_text=payload,
        )

        assert result.stdout == f"ready\n{_INPUT_BOUND_BYTES}"
        assert result.measurements.input_bytes == _INPUT_BOUND_BYTES
