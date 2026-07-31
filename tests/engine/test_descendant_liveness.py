"""No survivors: a grandchild is observably dead before the call returns.

Every exit path — deadline, enforced overflow, and a normal exit whose
grandchild outlives it — kills the whole group inside the termination
self-budget.

A descendant that calls ``setsid`` leaves the run's group and is beyond what
group-targeted teardown can reach; containment that holds a payload to the
process tree is use case 5's job, and v1 ships one profile that declares it
restricts nothing beyond the process boundary. What v1 does guarantee for
that payload shape is that it never costs the caller its result: the escaped
descendant holds the inherited pipes open, and the run still returns an
attributed outcome rather than an exception.
"""

from __future__ import annotations

import os
import signal
import sys
from collections.abc import Callable
from pathlib import Path

from dr_exec.declare import Budgets, OverflowPolicy, Records
from dr_exec.record import Attribution, BudgetAxis, RunResult
from dr_exec.run import run_tool

from .conftest import output_budget, requires_posix_groups, wait_until_dead

pytestmark = requires_posix_groups

_GRANDCHILD_SLEEP_SECONDS = 60


def _spawn_grandchild_source(
    pid_path: Path, then: str, *, detached: bool = False
) -> str:
    return (
        "import pathlib, subprocess, sys, time\n"
        "child = subprocess.Popen([sys.executable, '-c', "
        f"'import time; time.sleep({_GRANDCHILD_SLEEP_SECONDS})'], "
        f"start_new_session={detached!r})\n"
        f"pathlib.Path({str(pid_path)!r}).write_text(str(child.pid))\n"
        f"{then}\n"
    )


def _grandchild_pid(pid_path: Path) -> int:
    return int(pid_path.read_text())


def _reap(pid: int) -> None:
    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        return


class TestDescendantsDieOnEveryExitPath:
    def test_a_deadline_kills_the_grandchild(
        self, tmp_path: Path, run_python: Callable[..., RunResult]
    ) -> None:
        pid_path = tmp_path / "descendant.pid"
        source = _spawn_grandchild_source(pid_path, "time.sleep(60)")

        result = run_python(source, budgets=Budgets(wall_clock=0.3))

        assert result.outcome.attribution is Attribution.BUDGET
        assert result.outcome.violated_axis is BudgetAxis.WALL_CLOCK
        assert wait_until_dead(_grandchild_pid(pid_path), within_seconds=2.0)

    def test_an_enforced_overflow_kills_the_grandchild(
        self, tmp_path: Path, run_python: Callable[..., RunResult]
    ) -> None:
        pid_path = tmp_path / "descendant.pid"
        source = _spawn_grandchild_source(
            pid_path,
            "sys.stdout.buffer.write(b'x' * 40000)\n"
            "sys.stdout.buffer.flush()\n"
            "time.sleep(60)",
        )

        result = run_python(source, budgets=output_budget(1024, OverflowPolicy.FAIL))

        assert result.outcome.attribution is Attribution.BUDGET
        assert result.outcome.violated_axis is BudgetAxis.OUTPUT
        assert wait_until_dead(_grandchild_pid(pid_path), within_seconds=2.0)

    def test_a_normal_exit_still_kills_the_orphaned_grandchild(
        self, tmp_path: Path, run_python: Callable[..., RunResult]
    ) -> None:
        pid_path = tmp_path / "descendant.pid"
        source = _spawn_grandchild_source(pid_path, "raise SystemExit(0)")

        result = run_python(source, budgets=Budgets(wall_clock=10.0))

        assert result.returncode == 0
        assert result.outcome.attribution is Attribution.PAYLOAD
        assert wait_until_dead(_grandchild_pid(pid_path), within_seconds=2.0)


class TestADetachedDescendantNeverCostsTheResult:
    """A ``setsid`` descendant is outside the group, so it holds the pipes.

    Group-targeted teardown cannot reach it — once the leader exits it
    reparents to init, with no group, session, or parent link left to find
    it by — so the drains never see EOF. The run still has a result: the
    child was reaped, its bytes are captured, and the unfinished drain is
    reported as a channel fault on the outcome rather than raised.
    """

    def test_a_normal_exit_with_a_detached_descendant_still_returns_a_result(
        self, tmp_path: Path, run_python: Callable[..., RunResult]
    ) -> None:
        pid_path = tmp_path / "descendant.pid"
        source = _spawn_grandchild_source(
            pid_path,
            "print('COMPLETE-PAYLOAD-OUTPUT')\nsys.stdout.flush()\nraise SystemExit(0)",
            detached=True,
        )

        result = run_python(source, budgets=Budgets(wall_clock=10.0))
        _reap(_grandchild_pid(pid_path))

        assert result.returncode == 0
        assert "COMPLETE-PAYLOAD-OUTPUT" in result.stdout
        assert result.outcome.attribution is Attribution.CHANNEL

    def test_a_deadline_with_a_detached_descendant_stays_a_budget_outcome(
        self, tmp_path: Path, run_python: Callable[..., RunResult]
    ) -> None:
        pid_path = tmp_path / "descendant.pid"
        source = _spawn_grandchild_source(pid_path, "time.sleep(60)", detached=True)

        result = run_python(source, budgets=Budgets(wall_clock=0.3))
        _reap(_grandchild_pid(pid_path))

        # The bound the caller declared is what ended the run; the stranded
        # drain is a consequence of it, never the attribution.
        assert result.outcome.attribution is Attribution.BUDGET
        assert result.outcome.violated_axis is BudgetAxis.WALL_CLOCK

    def test_an_enforced_overflow_with_a_detached_descendant_stays_a_budget_outcome(
        self, tmp_path: Path, run_python: Callable[..., RunResult]
    ) -> None:
        pid_path = tmp_path / "descendant.pid"
        source = _spawn_grandchild_source(
            pid_path,
            "sys.stdout.buffer.write(b'x' * 40000)\n"
            "sys.stdout.buffer.flush()\n"
            "time.sleep(60)",
            detached=True,
        )

        result = run_python(source, budgets=output_budget(1024, OverflowPolicy.FAIL))
        _reap(_grandchild_pid(pid_path))

        assert result.outcome.attribution is Attribution.BUDGET
        assert result.outcome.violated_axis is BudgetAxis.OUTPUT


class TestSignalDeathFidelity:
    def test_a_self_inflicted_signal_death_stays_payload_attributed(self) -> None:
        result = run_tool(
            [
                sys.executable,
                "-I",
                "-c",
                "import os, signal; os.kill(os.getpid(), signal.SIGSEGV)",
            ],
            budgets=Budgets(wall_clock=10.0),
            records=Records.none(),
        )

        assert result.returncode == -11
        assert result.outcome.attribution is Attribution.PAYLOAD

    def test_an_executor_kill_is_never_payload_attributed(
        self, run_python: Callable[..., RunResult]
    ) -> None:
        result = run_python(
            "import time; time.sleep(60)", budgets=Budgets(wall_clock=0.2)
        )

        assert result.returncode is not None
        assert result.returncode < 0
        assert result.outcome.attribution is Attribution.BUDGET
