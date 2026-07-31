"""Source composition and the per-stream bounds the protocol channel needs."""

from __future__ import annotations

from collections.abc import Callable

import pytest

from dr_exec.batch import (
    BatchItem,
    BatchRequest,
    BatchResult,
    ProtocolChannelBudget,
    channel_bounds_for,
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


class TestLargeBatchesCrossViaStdin:
    def test_a_batch_far_past_the_source_bound_runs_end_to_end(
        self, execute_batch: Callable[..., BatchResult]
    ) -> None:
        # 300 items each carrying ~1 KiB of payload is ~300 KiB of item data —
        # over three times the source bound. Inlined in the driver source this
        # was a DeclarationError before the child ever spawned; delivered on
        # stdin it is bounded only by the input budget, so the real child
        # sweeps every item.
        blob = "x" * 1024
        batch_items = tuple(
            BatchItem(item_id=f"i{n}", payload={"blob": blob, "n": n})
            for n in range(300)
        )
        assert (
            len(_request(items=batch_items).items_input_text().encode("utf-8"))
            > SOURCE_BOUND_BYTES
        )

        result = execute_batch(
            "def run_item(item_id, payload):\n    return {'n': payload['n']}\n",
            batch_items=batch_items,
            channel_budget=ProtocolChannelBudget(),
        )

        assert result.completion_seen is True
        assert result.complete is True
        assert result.missing_item_ids == ()
        assert len(result.results) == 300
        assert result.results_by_item_id["i0"].payload == {"n": 0}
        assert result.results_by_item_id["i299"].payload == {"n": 299}

    def test_the_composed_source_is_independent_of_payload_size(self) -> None:
        small = items("a", "b", payload={"k": 1})
        large = items("a", "b", payload={"k": "x" * 100_000})

        assert len(_request(items=small).driver_source().encode("utf-8")) == len(
            _request(items=large).driver_source().encode("utf-8")
        )

    def test_the_composed_source_stays_under_the_bound_for_a_large_batch(
        self,
    ) -> None:
        # Only the item-id list rides in the prelude the source binds; the
        # payloads — the bulk of a real batch — cross on stdin, so even a
        # 2000-item batch composes to a source far under the bound.
        batch_items = items(*[f"item-{n:04d}" for n in range(2000)])

        source_bytes = len(_request(items=batch_items).driver_source().encode("utf-8"))

        assert source_bytes < SOURCE_BOUND_BYTES


class TestInputBudgetBoundsTheBatch:
    def test_an_over_input_budget_batch_is_a_clean_pre_spawn_declaration_error(
        self,
    ) -> None:
        batch_items = tuple(
            BatchItem(item_id=f"i{n}", payload={"blob": "y" * 512}) for n in range(100)
        )
        request = _request(items=batch_items)
        input_bytes = len(request.items_input_text().encode("utf-8"))
        budget = input_bytes // 2

        with pytest.raises(DeclarationError, match="input budget"):
            run_batch(
                request,
                profile=PROCESS_BOUNDARY_ONLY,
                budgets=Budgets(wall_clock=5.0, input=budget),
                records=Records.none(),
            )

    def test_a_batch_within_the_declared_input_budget_runs(
        self, execute_batch: Callable[..., BatchResult]
    ) -> None:
        batch_items = items("a", "b", payload={"k": 1})
        request = _request(items=batch_items)
        input_bytes = len(request.items_input_text().encode("utf-8"))

        result = execute_batch(
            _ECHO_BODY,
            batch_items=batch_items,
            budgets=Budgets(wall_clock=5.0, input=input_bytes),
        )

        assert result.complete is True
        assert [item.item_id for item in result.results] == ["a", "b"]


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

        bounds = channel_bounds_for(request, budgets)

        assert bounds == StreamBounds(
            stdout_bytes=ProtocolChannelBudget().channel_bytes_for(2),
            stderr_bytes=4096,
        )

    def test_an_unbudgeted_output_axis_leaves_stderr_unbounded(self) -> None:
        bounds = channel_bounds_for(_request(), Budgets())

        assert bounds.stderr_bytes is None
        assert bounds.stdout_bytes == ProtocolChannelBudget().channel_bytes_for(1)


class TestStreamBoundsDeclaration:
    def test_a_nonpositive_bound_is_rejected(self) -> None:
        with pytest.raises(DeclarationError, match="positive integer of bytes"):
            StreamBounds(stdout_bytes=0)

    def test_unset_axes_mean_the_shared_output_bound_still_governs(self) -> None:
        assert StreamBounds() == StreamBounds(stdout_bytes=None, stderr_bytes=None)
