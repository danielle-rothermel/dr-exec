"""Shared builders for the fake's suite.

Nothing here spawns: the fake's whole point is that these tests exercise the
contract without a child, and the declarations they build are the same ones
the real entry points build.
"""

from __future__ import annotations

from dr_exec.batch import BatchItem, BatchRequest
from dr_exec.declare import Budgets, ExitVerdict
from dr_exec.record import (
    Attribution,
    Measurements,
    Outcome,
    RunResult,
    TruncationMark,
)

QUICK = Budgets(wall_clock=10.0)

BODY_SOURCE = "def run_item(item_id, payload):\n    return {'seen': payload}\n"


def payload_result(
    *,
    returncode: int = 0,
    stdout: str = "",
    stderr: str = "",
    input_bytes: int = 0,
    exit_verdict: str = ExitVerdict.REPORT_ONLY.value,
    truncation: TruncationMark | None = None,
    stdout_bytes_produced: int | None = None,
    stderr_bytes_produced: int | None = None,
) -> RunResult:
    """A payload-attributed result whose measurements agree with its capture."""
    return RunResult(
        returncode=returncode,
        stdout=stdout,
        stderr=stderr,
        truncation=truncation if truncation is not None else TruncationMark(),
        measurements=Measurements(
            duration_seconds=0.01,
            teardown_seconds=0.0,
            stdout_bytes_produced=(
                len(stdout.encode("utf-8"))
                if stdout_bytes_produced is None
                else stdout_bytes_produced
            ),
            stderr_bytes_produced=(
                len(stderr.encode("utf-8"))
                if stderr_bytes_produced is None
                else stderr_bytes_produced
            ),
            input_bytes=input_bytes,
        ),
        outcome=Outcome(attribution=Attribution.PAYLOAD, exit_verdict=exit_verdict),
    )


def outcome_result(outcome: Outcome, *, returncode: int | None = None) -> RunResult:
    """A result carrying an arbitrary outcome and nothing else."""
    return RunResult(
        returncode=returncode,
        stdout="",
        stderr="",
        truncation=TruncationMark(),
        measurements=Measurements(
            duration_seconds=0.01,
            teardown_seconds=0.0,
            stdout_bytes_produced=0,
            stderr_bytes_produced=0,
            input_bytes=0,
        ),
        outcome=outcome,
    )


def request(*item_ids: str, config: object = None) -> BatchRequest:
    return BatchRequest(
        items=tuple(
            BatchItem(item_id=item_id, payload=index)
            for index, item_id in enumerate(item_ids)
        ),
        body_source=BODY_SOURCE,
        item_schema="int",
        config=config if config is not None else {"seed": 3},
    )
