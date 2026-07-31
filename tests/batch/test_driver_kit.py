"""The driver kit's behavior in a real child.

Protocol-stdout protection, incremental delivery, item failures as data, and
load-phase fan-out only mean something when a real interpreter runs the
composed source, so every test here spawns.
"""

from __future__ import annotations

from collections.abc import Callable

from dr_exec.batch import BatchResult, ProtocolChannelBudget
from dr_exec.declare import Budgets, OverflowPolicy
from dr_exec.record import Attribution, BudgetAxis

from .conftest import items, payload_budgets

_ECHO_BODY = "def run_item(item_id, payload):\n    return {'echoed': payload}\n"


class TestHappyPath:
    def test_every_item_produces_one_result_and_a_completion_line(
        self, execute_batch: Callable[..., BatchResult]
    ) -> None:
        result = execute_batch(_ECHO_BODY, batch_items=items("a", "b", "c", payload=7))

        assert [item.item_id for item in result.results] == ["a", "b", "c"]
        assert result.completion_seen is True
        assert result.results_emitted_claim == 3
        assert result.complete is True
        assert result.missing_item_ids == ()

    def test_the_body_return_value_arrives_verbatim(
        self, execute_batch: Callable[..., BatchResult]
    ) -> None:
        result = execute_batch(
            "def run_item(item_id, payload):\n"
            "    return {'nested': {'n': payload * 2}, 'id': item_id}\n",
            batch_items=items("only", payload=21),
        )

        assert result.results[0].payload == {"nested": {"n": 42}, "id": "only"}
        assert result.results[0].error_text is None

    def test_the_run_exits_zero_and_is_payload_attributed(
        self, execute_batch: Callable[..., BatchResult]
    ) -> None:
        result = execute_batch(_ECHO_BODY, batch_items=items("a"))

        assert result.run.returncode == 0
        assert result.run.outcome.attribution is Attribution.PAYLOAD


class TestProtocolStdoutProtection:
    def test_a_payload_print_lands_on_stderr_and_leaves_the_protocol_clean(
        self, execute_batch: Callable[..., BatchResult]
    ) -> None:
        result = execute_batch(
            "def run_item(item_id, payload):\n"
            "    print('payload noise for ' + item_id)\n"
            "    return {'ok': True}\n",
            batch_items=items("a", "b"),
        )

        assert result.run.stderr == "payload noise for a\npayload noise for b\n"
        assert result.complete is True
        assert [item.payload for item in result.results] == [{"ok": True}, {"ok": True}]

    def test_a_payload_write_to_sys_stdout_also_lands_on_stderr(
        self, execute_batch: Callable[..., BatchResult]
    ) -> None:
        result = execute_batch(
            "import sys\n"
            "def run_item(item_id, payload):\n"
            "    sys.stdout.write('direct\\n')\n"
            "    sys.__stdout__.write('dunder\\n')\n"
            "    return {'ok': True}\n",
            batch_items=items("a"),
        )

        assert result.run.stderr == "direct\ndunder\n"
        assert result.complete is True


class TestItemFailuresAreData:
    def test_a_raising_body_becomes_an_error_result_and_the_sweep_continues(
        self, execute_batch: Callable[..., BatchResult]
    ) -> None:
        result = execute_batch(
            "def run_item(item_id, payload):\n"
            "    if item_id == 'bad':\n"
            "        raise ValueError('item blew up')\n"
            "    return {'ok': True}\n",
            batch_items=items("good", "bad", "also_good"),
        )

        by_id = result.results_by_item_id
        assert result.complete is True
        assert by_id["good"].payload == {"ok": True}
        assert by_id["also_good"].payload == {"ok": True}
        assert "ValueError: item blew up" in (by_id["bad"].error_text or "")

    def test_a_body_that_exits_the_process_still_leaves_earlier_results(
        self, execute_batch: Callable[..., BatchResult]
    ) -> None:
        result = execute_batch(
            "import os\n"
            "def run_item(item_id, payload):\n"
            "    if item_id == 'second':\n"
            "        os._exit(3)\n"
            "    return {'ok': True}\n",
            batch_items=items("first", "second", "third"),
        )

        assert [item.item_id for item in result.results] == ["first"]
        assert result.missing_item_ids == ("second", "third")
        assert result.completion_seen is False
        assert result.run.returncode == 3

    def test_an_unjsonable_return_value_becomes_that_items_error_result(
        self, execute_batch: Callable[..., BatchResult]
    ) -> None:
        result = execute_batch(
            "def run_item(item_id, payload):\n"
            "    if item_id == 'bad':\n"
            "        return {'obj': object()}\n"
            "    return {'ok': True}\n",
            batch_items=items("bad", "good"),
        )

        assert result.complete is True
        assert "not JSON serializable" in (
            result.results_by_item_id["bad"].error_text or ""
        )
        assert result.results_by_item_id["good"].payload == {"ok": True}


class TestLoadPhaseFailure:
    def test_a_syntactically_invalid_body_fans_out_one_error_per_item(
        self, execute_batch: Callable[..., BatchResult]
    ) -> None:
        result = execute_batch("this is not python(", batch_items=items("a", "b", "c"))

        assert result.run.returncode == 0
        assert result.complete is True
        assert [item.item_id for item in result.results] == ["a", "b", "c"]
        for item in result.results:
            assert "SyntaxError" in (item.error_text or "")

    def test_a_body_raising_at_load_time_fans_out_one_error_per_item(
        self, execute_batch: Callable[..., BatchResult]
    ) -> None:
        result = execute_batch(
            "raise RuntimeError('import-time boom')\n"
            "def run_item(item_id, payload):\n"
            "    return {}\n",
            batch_items=items("a", "b"),
        )

        assert result.complete is True
        for item in result.results:
            assert "RuntimeError: import-time boom" in (item.error_text or "")

    def test_a_body_defining_no_hook_fans_out_one_error_per_item(
        self, execute_batch: Callable[..., BatchResult]
    ) -> None:
        result = execute_batch(
            "def something_else():\n    return 1\n", batch_items=items("a")
        )

        assert result.complete is True
        assert "defines no run_item hook" in (result.results[0].error_text or "")


class TestIncrementalDelivery:
    def test_a_deadline_mid_batch_keeps_completed_items_as_trusted_partials(
        self, execute_batch: Callable[..., BatchResult]
    ) -> None:
        result = execute_batch(
            "import time\n"
            "def run_item(item_id, payload):\n"
            "    time.sleep(0.3)\n"
            "    return {'ok': item_id}\n",
            batch_items=items("i0", "i1", "i2", "i3", "i4", "i5"),
            budgets=Budgets(wall_clock=1.0),
        )

        assert result.run.outcome.attribution is Attribution.BUDGET
        assert result.run.outcome.violated_axis is BudgetAxis.WALL_CLOCK
        assert result.completion_seen is False
        assert result.complete is False
        # Delivered results are a prefix of the declared order: the deadline
        # costs the unfinished tail, never a result already produced.
        delivered = [item.item_id for item in result.results]
        assert delivered
        assert delivered == list(result.request.item_ids[: len(delivered)])
        assert set(result.missing_item_ids).issuperset({"i4", "i5"})

    def test_missing_items_are_reported_never_synthesized(
        self, execute_batch: Callable[..., BatchResult]
    ) -> None:
        result = execute_batch(
            "import time\n"
            "def run_item(item_id, payload):\n"
            "    if item_id == 'slow':\n"
            "        time.sleep(30)\n"
            "    return {'ok': True}\n",
            batch_items=items("fast", "slow", "never"),
            budgets=Budgets(wall_clock=1.0),
        )

        assert [item.item_id for item in result.results] == ["fast"]
        assert result.missing_item_ids == ("slow", "never")
        assert result.results_emitted_claim is None


class TestChannelBudgetIsolation:
    def test_a_stderr_flood_costs_its_own_stream_never_the_protocol_results(
        self, execute_batch: Callable[..., BatchResult]
    ) -> None:
        result = execute_batch(
            "import sys\n"
            "def run_item(item_id, payload):\n"
            "    sys.stderr.write('z' * 50000)\n"
            "    return {'ok': item_id}\n",
            batch_items=items("a", "b"),
            budgets=payload_budgets(2000),
        )

        assert result.run.truncation.stderr_bytes_dropped > 0
        assert result.run.truncation.stdout_bytes_dropped == 0
        assert result.complete is True
        assert [item.payload for item in result.results] == [
            {"ok": "a"},
            {"ok": "b"},
        ]

    def test_a_flooding_item_under_fail_is_a_budget_outcome_with_partials_kept(
        self, execute_batch: Callable[..., BatchResult]
    ) -> None:
        result = execute_batch(
            "import sys\n"
            "def run_item(item_id, payload):\n"
            "    if item_id == 'loud':\n"
            "        sys.stderr.write('z' * 200000)\n"
            "        sys.stderr.flush()\n"
            "        import time; time.sleep(5)\n"
            "    return {'ok': item_id}\n",
            batch_items=items("quiet", "loud", "later"),
            budgets=payload_budgets(2000, OverflowPolicy.FAIL),
        )

        assert result.run.outcome.attribution is Attribution.BUDGET
        assert result.run.outcome.violated_axis is BudgetAxis.OUTPUT
        assert [item.item_id for item in result.results] == ["quiet"]
        assert result.missing_item_ids == ("loud", "later")

    def test_results_produced_after_a_stderr_flood_trips_fail_still_arrive(
        self, execute_batch: Callable[..., BatchResult]
    ) -> None:
        # The first item crosses the payload stream's FAIL bound, then every
        # item runs to completion and the child exits on its own terms. The
        # protocol channel's own bound was never the constraint, so every
        # result line the child produced has to reach the parent: a flood on
        # one stream costs only the noisy item, never the batch.
        result = execute_batch(
            "import sys\n"
            "def run_item(item_id, payload):\n"
            "    if item_id == 'loud':\n"
            "        sys.stderr.write('z' * 8000)\n"
            "        sys.stderr.flush()\n"
            "    return {'ok': item_id}\n",
            batch_items=items("loud", "after-one", "after-two"),
            budgets=payload_budgets(2000, OverflowPolicy.FAIL),
        )

        assert [item.item_id for item in result.results] == [
            "loud",
            "after-one",
            "after-two",
        ]
        assert result.complete is True
        assert result.run.truncation.stdout_bytes_dropped == 0
        assert result.run.measurements.stdout_bytes_produced > 0

    def test_an_over_budget_item_result_becomes_that_items_error_result(
        self, execute_batch: Callable[..., BatchResult]
    ) -> None:
        result = execute_batch(
            "def run_item(item_id, payload):\n"
            "    return {'blob': 'x' * (5000 if item_id == 'big' else 3)}\n",
            batch_items=items("big", "small"),
            channel_budget=ProtocolChannelBudget(
                item_result_bytes=1000, frame_bytes=1000
            ),
        )

        assert result.complete is True
        assert "per-item protocol result budget" in (
            result.results_by_item_id["big"].error_text or ""
        )
        assert result.results_by_item_id["small"].payload == {"blob": "xxx"}
