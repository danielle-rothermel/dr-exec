"""Source composition and the per-stream bounds the protocol channel needs."""

from __future__ import annotations

from collections.abc import Callable

import pytest

from dr_exec.batch import (
    BatchItem,
    BatchRequest,
    BatchResult,
    ProtocolChannelBudget,
    _channel_bounds,
    run_batch,
)
from dr_exec.declare import (
    PROCESS_BOUNDARY_ONLY,
    SOURCE_BOUND_BYTES,
    Budgets,
    OutputBudget,
    OverflowPolicy,
    Records,
    StreamBounds,
)
from dr_exec.errors import DeclarationError

from .conftest import items

_ECHO_BODY = "def run_item(item_id, payload):\n    return payload\n"


def _request(body_source: str = _ECHO_BODY, **overrides: object) -> BatchRequest:
    fields: dict[str, object] = {
        "items": items("a"),
        "body_source": body_source,
        "item_schema": "opaque",
        "config": {"seed": 1},
    }
    fields.update(overrides)
    return BatchRequest(**fields)  # type: ignore[arg-type]


class TestComposedSource:
    def test_the_composed_source_is_valid_python(self) -> None:
        compile(_request().driver_source(), "<composed>", "exec")

    def test_the_body_is_carried_as_data_not_spliced_as_text(self) -> None:
        source = _request("def run_item(i, p):\n    return 'quote\"and\\\\slash'\n")

        compile(source.driver_source(), "<composed>", "exec")

    def test_a_body_over_the_source_bound_is_a_declaration_error(self) -> None:
        oversized = "x = '" + "y" * SOURCE_BOUND_BYTES + "'\n" + _ECHO_BODY

        with pytest.raises(DeclarationError, match="exceeds the"):
            run_batch(
                _request(oversized),
                profile=PROCESS_BOUNDARY_ONLY,
                budgets=Budgets(wall_clock=5.0),
                records=Records.none(),
            )

    def test_the_declared_item_schema_reaches_the_body(
        self, execute_batch: Callable[..., BatchResult]
    ) -> None:
        result = execute_batch(
            "def run_item(item_id, payload):\n"
            "    return {'schema': _KIT_ITEM_SCHEMA}\n",
            batch_items=items("a"),
            item_schema="{x: int}",
        )

        assert result.results[0].payload == {"schema": "{x: int}"}

    def test_a_body_seeing_its_item_payloads_verbatim_across_the_boundary(
        self, execute_batch: Callable[..., BatchResult]
    ) -> None:
        payload = {"unicode": "héllo", "quote": 'a"b', "nested": [1, {"k": None}]}
        result = execute_batch(
            _ECHO_BODY,
            batch_items=(BatchItem(item_id="a", payload=payload),),
        )

        assert result.results[0].payload == payload


class TestChannelBounds:
    def test_stdout_gets_the_channel_budget_and_stderr_the_run_output_budget(
        self,
    ) -> None:
        request = _request(items=items("a", "b"))
        budgets = Budgets(
            output=OutputBudget(
                limit_bytes=4096, overflow_policy=OverflowPolicy.MARKED_TRUNCATION
            )
        )

        bounds = _channel_bounds(request, budgets)

        assert bounds == StreamBounds(
            stdout_bytes=ProtocolChannelBudget().channel_bytes_for(2),
            stderr_bytes=4096,
        )

    def test_an_unbudgeted_output_axis_leaves_stderr_unbounded(self) -> None:
        bounds = _channel_bounds(_request(), Budgets())

        assert bounds.stderr_bytes is None
        assert bounds.stdout_bytes == ProtocolChannelBudget().channel_bytes_for(1)


class TestStreamBoundsDeclaration:
    def test_a_nonpositive_bound_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="positive integer of bytes"):
            StreamBounds(stdout_bytes=0)

    def test_unset_axes_mean_the_shared_output_bound_still_governs(self) -> None:
        assert StreamBounds() == StreamBounds(stdout_bytes=None, stderr_bytes=None)
