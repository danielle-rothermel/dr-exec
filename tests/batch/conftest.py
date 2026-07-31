"""Shared helpers for the batch suite.

The driver kit's meaning is what a real child does with it, so these tests
execute real children through the untrusted-Python entry point. They use
small item counts and tight bounds to stay fast.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

import pytest

from dr_exec.batch import (
    BatchItem,
    BatchRequest,
    BatchResult,
    ProtocolChannelBudget,
    run_batch,
)
from dr_exec.declare import (
    PROCESS_BOUNDARY_ONLY,
    Budgets,
    OutputBudget,
    OverflowPolicy,
    Records,
)

DEFAULT_WALL_CLOCK_SECONDS = 20.0
DEFAULT_CHANNEL_BUDGET = ProtocolChannelBudget()


def items(*item_ids: str, payload: Any = None) -> tuple[BatchItem, ...]:
    return tuple(BatchItem(item_id=item_id, payload=payload) for item_id in item_ids)


def payload_budgets(
    limit_bytes: int,
    policy: OverflowPolicy = OverflowPolicy.MARKED_TRUNCATION,
    *,
    wall_clock: float = DEFAULT_WALL_CLOCK_SECONDS,
) -> Budgets:
    return Budgets(
        wall_clock=wall_clock,
        output=OutputBudget(limit_bytes=limit_bytes, overflow_policy=policy),
    )


@pytest.fixture
def execute_batch() -> Callable[..., BatchResult]:
    """Run one batch in a real child with no record written."""

    def _execute(
        body_source: str,
        *,
        batch_items: Sequence[BatchItem],
        config: Any = None,
        item_schema: str = "opaque",
        budgets: Budgets | None = None,
        channel_budget: ProtocolChannelBudget = DEFAULT_CHANNEL_BUDGET,
    ) -> BatchResult:
        request = BatchRequest(
            items=tuple(batch_items),
            body_source=body_source,
            item_schema=item_schema,
            config=config,
            channel_budget=channel_budget,
        )
        return run_batch(
            request,
            profile=PROCESS_BOUNDARY_ONLY,
            budgets=(
                budgets
                if budgets is not None
                else Budgets(wall_clock=DEFAULT_WALL_CLOCK_SECONDS)
            ),
            records=Records.none(),
        )

    return _execute
