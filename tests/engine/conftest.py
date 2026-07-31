"""Shared helpers for the spawn-path suite.

This repo owns lifecycle correctness, so these tests execute real children.
They use tiny deadlines and small declared bounds to stay fast.
"""

from __future__ import annotations

import os
import sys
import time
from collections.abc import Callable

import pytest

from dr_exec.declare import (
    Budgets,
    EnvironmentGrant,
    OutputBudget,
    OverflowPolicy,
    Records,
)
from dr_exec.record import RunResult
from dr_exec.run import run_tool

requires_posix_groups = pytest.mark.skipif(
    not hasattr(os, "killpg"),
    reason="real process groups require a POSIX platform",
)


def process_is_alive(pid: int) -> bool:
    """POSIX liveness without /proc: signal 0 plus a reap of our own child."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def wait_until_dead(pid: int, *, within_seconds: float) -> bool:
    deadline = time.monotonic() + within_seconds
    while time.monotonic() < deadline:
        if not process_is_alive(pid):
            return True
        time.sleep(0.01)
    return not process_is_alive(pid)


def output_budget(limit_bytes: int, policy: OverflowPolicy) -> Budgets:
    return Budgets(
        wall_clock=10.0,
        output=OutputBudget(limit_bytes=limit_bytes, overflow_policy=policy),
    )


@pytest.fixture
def run_python() -> Callable[..., RunResult]:
    """Run Python source through the trusted-tool path with no record.

    The engine is what these tests exercise; the trusted entry point is the
    thinnest way in.
    """

    def _run(
        source: str,
        *,
        budgets: Budgets | None = None,
        input_text: str = "",
        environment: EnvironmentGrant | None = None,
        records: Records | None = None,
    ) -> RunResult:
        return run_tool(
            [sys.executable, "-I", "-c", source],
            budgets=budgets if budgets is not None else Budgets(wall_clock=10.0),
            records=records if records is not None else Records.none(),
            input_text=input_text,
            environment=(
                environment if environment is not None else EnvironmentGrant.none()
            ),
        )

    return _run
