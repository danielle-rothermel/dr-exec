"""Batch faking: the parent's API, scripted per-item results, real accounting.

The fake does not fake the accounting. Scripted item results are rendered as
the transcript a conforming driver would have written and run through the
real parent-side accounting, so a script that breaks the protocol's rules
fails here exactly as a broken driver would.
"""

from __future__ import annotations

import inspect

import pytest

from dr_exec.batch import BatchResult, ItemResult, run_batch
from dr_exec.declare import (
    PROCESS_BOUNDARY_ONLY,
    SOURCE_BOUND_BYTES,
    Budgets,
    OutputBudget,
    OverflowPolicy,
    Records,
)
from dr_exec.errors import DeclarationError, ProtocolFailure
from dr_exec.fake import (
    EntryPoint,
    FakeExecutor,
    RecordedBatchCall,
    ScriptedBatch,
    ScriptError,
    UnscriptedCall,
)
from dr_exec.record import Attribution, BudgetAxis, Outcome

from .conftest import QUICK, outcome_result, payload_result, request


def _batch(fake: FakeExecutor, *item_ids: str, budgets: Budgets = QUICK) -> BatchResult:
    return fake.run_batch(
        request(*item_ids),
        profile=PROCESS_BOUNDARY_ONLY,
        budgets=budgets,
        records=Records.none(),
    )


class TestParentApiParity:
    def test_the_fake_matches_the_real_run_batch_signature(self) -> None:
        real = inspect.signature(run_batch)
        fake = inspect.signature(FakeExecutor.run_batch)

        assert list(fake.parameters)[1:] == list(real.parameters)
        assert fake.return_annotation == real.return_annotation

    def test_an_oversized_composed_driver_is_refused_as_it_is_for_real(self) -> None:
        fake = FakeExecutor()
        oversized = request("a")
        oversized = type(oversized)(
            items=oversized.items,
            body_source="x = '" + "y" * SOURCE_BOUND_BYTES + "'\n",
            item_schema=oversized.item_schema,
            config=oversized.config,
        )
        fake.enqueue_batch(ScriptedBatch(run=payload_result()))

        with pytest.raises(DeclarationError, match="composed driver source"):
            fake.run_batch(
                oversized,
                profile=PROCESS_BOUNDARY_ONLY,
                budgets=QUICK,
                records=Records.none(),
            )


class TestScriptedResults:
    def test_a_complete_batch_reports_every_item(self) -> None:
        fake = FakeExecutor()
        fake.enqueue_batch(
            ScriptedBatch(
                run=payload_result(),
                results=(
                    ItemResult(item_id="a", payload={"score": 1}),
                    ItemResult(item_id="b", payload={"score": 0}),
                ),
            )
        )

        result = _batch(fake, "a", "b")

        assert result.complete
        assert result.results_by_item_id["a"].payload == {"score": 1}
        assert result.missing_item_ids == ()
        assert result.results_emitted_claim == 2

    def test_a_partial_batch_names_the_missing_items(self) -> None:
        fake = FakeExecutor()
        fake.enqueue_batch(
            ScriptedBatch(
                run=outcome_result(
                    Outcome(
                        attribution=Attribution.BUDGET,
                        violated_axis=BudgetAxis.WALL_CLOCK,
                    ),
                    returncode=-9,
                ),
                results=(ItemResult(item_id="a", payload={"score": 1}),),
                completion_seen=False,
            )
        )

        result = _batch(fake, "a", "b")

        assert not result.complete
        assert result.missing_item_ids == ("b",)
        assert result.run.outcome.violated_axis is BudgetAxis.WALL_CLOCK

    def test_a_behavioral_script_sees_the_request_and_the_declared_run(self) -> None:
        seen: list[RecordedBatchCall] = []
        fake = FakeExecutor()

        def script(call: RecordedBatchCall) -> ScriptedBatch:
            seen.append(call)
            return ScriptedBatch(
                run=payload_result(),
                results=tuple(
                    ItemResult(item_id=item.item_id, payload={"echo": item.payload})
                    for item in call.request.items
                ),
            )

        fake.script_batches_with(script)
        result = _batch(fake, "a", "b", "c")

        (call,) = seen
        assert call.request.item_ids == ("a", "b", "c")
        assert call.call.profile is PROCESS_BOUNDARY_ONLY
        assert "run_item" in call.call.source
        assert result.complete
        assert result.results_by_item_id["c"].payload == {"echo": 2}

    def test_an_unscripted_batch_names_the_remedy(self) -> None:
        with pytest.raises(UnscriptedCall) as raised:
            _batch(FakeExecutor(), "a")

        assert "script_batches_with" in str(raised.value)
        assert "enqueue_batch" in str(raised.value)

    def test_a_batch_script_returning_something_else_is_refused(self) -> None:
        fake = FakeExecutor()
        fake.script_batches_with(lambda call: payload_result())  # type: ignore[arg-type,return-value]

        with pytest.raises(ScriptError, match="not a ScriptedBatch"):
            _batch(fake, "a")


class TestAccountingIsReal:
    def test_a_duplicate_scripted_item_fails_accounting(self) -> None:
        fake = FakeExecutor()
        fake.enqueue_batch(
            ScriptedBatch(
                run=payload_result(),
                results=(
                    ItemResult(item_id="a", payload=1),
                    ItemResult(item_id="a", payload=2),
                ),
            )
        )

        with pytest.raises(ProtocolFailure, match="more than once"):
            _batch(fake, "a", "b")

    def test_an_unknown_scripted_item_fails_accounting(self) -> None:
        fake = FakeExecutor()
        fake.enqueue_batch(
            ScriptedBatch(
                run=payload_result(), results=(ItemResult(item_id="ghost", payload=1),)
            )
        )

        with pytest.raises(ProtocolFailure, match="unknown item id"):
            _batch(fake, "a")

    def test_a_dishonest_completion_claim_survives_to_the_consumer(self) -> None:
        fake = FakeExecutor()
        fake.enqueue_batch(
            ScriptedBatch(
                run=payload_result(),
                results=(ItemResult(item_id="a", payload=1),),
                results_emitted_claim=99,
            )
        )

        result = _batch(fake, "a")

        assert result.results_emitted_claim == 99
        assert len(result.results) == 1

    def test_scripted_results_must_be_item_results(self) -> None:
        fake = FakeExecutor()
        fake.enqueue_batch(
            ScriptedBatch(run=payload_result(), results=({"item_id": "a"},))  # type: ignore[arg-type]
        )

        with pytest.raises(ScriptError, match="ItemResult"):
            _batch(fake, "a")

    def test_the_channel_byte_count_is_the_transcript_the_fake_wrote(self) -> None:
        fake = FakeExecutor()
        fake.enqueue_batch(
            ScriptedBatch(
                run=payload_result(stderr="payload noise"),
                results=(ItemResult(item_id="a", payload={"score": 1}),),
            )
        )

        result = _batch(fake, "a")

        produced = result.run.measurements.stdout_bytes_produced
        assert produced == len(result.run.stdout.encode("utf-8"))
        assert result.run.measurements.stderr_bytes_produced == len(b"payload noise")


class TestBatchOutcomeValidation:
    def test_the_scripted_run_obeys_the_same_invariants(self) -> None:
        fake = FakeExecutor()
        fake.enqueue_batch(
            ScriptedBatch(
                run=outcome_result(
                    Outcome(
                        attribution=Attribution.BUDGET,
                        violated_axis=BudgetAxis.OUTPUT,
                    )
                )
            )
        )

        with pytest.raises(ScriptError, match="carries a returncode"):
            _batch(fake, "a")

    def test_a_batch_call_is_recorded_on_both_surfaces(self) -> None:
        fake = FakeExecutor()
        fake.enqueue_batch(ScriptedBatch(run=payload_result()))

        _batch(fake, "a")

        assert fake.last_batch_call.request.item_ids == ("a",)
        assert fake.calls_for(EntryPoint.RUN_BATCH) == (fake.last_call,)
        assert fake.last_call.entry_point is EntryPoint.RUN_BATCH

    def test_last_batch_call_on_a_fake_that_never_ran_is_a_test_bug(self) -> None:
        with pytest.raises(AssertionError, match="no batch call has been recorded"):
            _ = FakeExecutor().last_batch_call

    def test_the_declared_channel_split_is_recorded(self) -> None:
        fake = FakeExecutor()
        fake.enqueue_batch(ScriptedBatch(run=payload_result()))
        budgets = Budgets(
            wall_clock=5.0,
            output=OutputBudget(
                limit_bytes=512, overflow_policy=OverflowPolicy.MARKED_TRUNCATION
            ),
        )

        _batch(fake, "a", "b", budgets=budgets)

        bounds = fake.last_batch_call.call.stream_bounds
        assert bounds is not None
        assert bounds.stderr_bytes == 512
        assert bounds.stdout_bytes == request(
            "a", "b"
        ).channel_budget.channel_bytes_for(2)
