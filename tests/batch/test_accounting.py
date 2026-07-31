"""Parent-side accounting over synthetic transcripts.

Accounting is a pure function of the transcript, so these tests feed one
directly: a real child cannot be made to emit a duplicate id on demand, and
the rule the parent enforces is what matters, not how a child broke.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from dr_exec.batch import (
    BatchItem,
    BatchRequest,
    BatchResult,
    ProtocolChannelBudget,
    account_transcript,
)
from dr_exec.errors import DeclarationError, ProtocolFailure
from dr_exec.record import (
    Attribution,
    Measurements,
    Outcome,
    RunResult,
    TruncationMark,
)

_CONFIG = {"seed": 7}


def _request(*item_ids: str) -> BatchRequest:
    return BatchRequest(
        items=tuple(BatchItem(item_id=item_id, payload=0) for item_id in item_ids),
        body_source="def run_item(item_id, payload):\n    return payload\n",
        item_schema="int",
        config=_CONFIG,
    )


def _run(stdout: str) -> RunResult:
    return RunResult(
        returncode=0,
        stdout=stdout,
        stderr="",
        truncation=TruncationMark(),
        measurements=Measurements(
            duration_seconds=0.1,
            teardown_seconds=0.0,
            stdout_bytes_produced=len(stdout.encode("utf-8")),
            stderr_bytes_produced=0,
            input_bytes=0,
        ),
        outcome=Outcome(attribution=Attribution.PAYLOAD, exit_verdict="report_only"),
    )


def _line(**fields: Any) -> str:
    return json.dumps(fields) + "\n"


def _result_line(item_id: str, payload: Any = None) -> str:
    return _line(kind="result", item_id=item_id, payload=payload)


def _complete_line(count: int) -> str:
    return _line(kind="complete", results_emitted=count)


def _transcript(request: BatchRequest, *lines: str) -> str:
    return json.dumps(request.prelude()) + "\n" + "".join(lines)


def _accounted(request: BatchRequest, transcript: str) -> BatchResult:
    return account_transcript(request=request, run=_run(transcript))


class TestPreludeVerification:
    def test_an_empty_transcript_is_a_protocol_failure(self) -> None:
        with pytest.raises(ProtocolFailure, match="no protocol lines"):
            _accounted(_request("a"), "")

    def test_a_first_line_that_is_not_a_prelude_is_a_protocol_failure(self) -> None:
        with pytest.raises(ProtocolFailure, match="first protocol line is not"):
            _accounted(_request("a"), _result_line("a"))

    def test_a_config_digest_mismatch_trusts_nothing(self) -> None:
        request = _request("a", "b")
        forged = dict(request.prelude())
        forged["config_digest"] = "0" * 64
        transcript = json.dumps(forged) + "\n" + _result_line("a") + _result_line("b")

        with pytest.raises(ProtocolFailure, match="different config digest") as failure:
            _accounted(request, transcript)

        assert failure.value.results == ()

    def test_an_item_set_mismatch_trusts_nothing(self) -> None:
        request = _request("a", "b")
        forged = dict(request.prelude())
        forged["item_ids"] = ["a"]
        transcript = json.dumps(forged) + "\n" + _result_line("a")

        with pytest.raises(ProtocolFailure, match="different item set") as failure:
            _accounted(request, transcript)

        assert failure.value.results == ()

    def test_a_reordered_item_set_is_accepted_because_the_check_is_set_equal(
        self,
    ) -> None:
        request = _request("a", "b")
        reordered = dict(request.prelude())
        reordered["item_ids"] = ["b", "a"]
        transcript = json.dumps(reordered) + "\n" + _result_line("a")

        assert [item.item_id for item in _accounted(request, transcript).results] == [
            "a"
        ]

    def test_a_protocol_version_mismatch_is_a_protocol_failure(self) -> None:
        request = _request("a")
        forged = dict(request.prelude())
        forged["protocol"] = 99

        with pytest.raises(ProtocolFailure, match="echoed protocol version"):
            _accounted(request, json.dumps(forged) + "\n")

    def test_an_unparsable_prelude_is_a_protocol_failure(self) -> None:
        with pytest.raises(ProtocolFailure, match="unparsable protocol line"):
            _accounted(_request("a"), "{not json\n")


class TestResultAccounting:
    def test_exactly_one_result_per_item_plus_a_terminal_line_is_complete(self) -> None:
        request = _request("a", "b")
        result = _accounted(
            request,
            _transcript(
                request, _result_line("a", 1), _result_line("b", 2), _complete_line(2)
            ),
        )

        assert result.complete is True
        assert result.results_emitted_claim == 2
        assert result.results_by_item_id["a"].payload == 1

    def test_a_duplicate_item_id_is_a_protocol_failure_carrying_partials(self) -> None:
        request = _request("a", "b")

        with pytest.raises(ProtocolFailure, match="more than once") as failure:
            _accounted(
                request, _transcript(request, _result_line("a"), _result_line("a"))
            )

        assert [item.item_id for item in failure.value.results] == ["a"]

    def test_an_unknown_item_id_is_a_protocol_failure_carrying_partials(self) -> None:
        request = _request("a", "b")

        with pytest.raises(ProtocolFailure, match="unknown item id") as failure:
            _accounted(
                request, _transcript(request, _result_line("a"), _result_line("zzz"))
            )

        assert [item.item_id for item in failure.value.results] == ["a"]

    def test_a_shape_invalid_line_is_a_protocol_failure_carrying_partials(self) -> None:
        request = _request("a", "b")

        with pytest.raises(ProtocolFailure, match="unparsable") as failure:
            _accounted(request, _transcript(request, _result_line("a"), "garbage\n"))

        assert [item.item_id for item in failure.value.results] == ["a"]

    def test_a_result_line_without_an_item_id_is_a_protocol_failure(self) -> None:
        request = _request("a")

        with pytest.raises(ProtocolFailure, match="no item id"):
            _accounted(request, _transcript(request, _line(kind="result", payload=1)))

    def test_a_result_line_without_a_payload_is_a_protocol_failure(self) -> None:
        request = _request("a")

        with pytest.raises(ProtocolFailure, match="no payload"):
            _accounted(request, _transcript(request, _line(kind="result", item_id="a")))

    def test_an_unknown_line_kind_is_a_protocol_failure(self) -> None:
        request = _request("a")

        with pytest.raises(ProtocolFailure, match="unknown line kind"):
            _accounted(request, _transcript(request, _line(kind="chatter")))

    def test_a_non_object_line_is_a_protocol_failure(self) -> None:
        request = _request("a")

        with pytest.raises(ProtocolFailure, match="not a JSON object"):
            _accounted(request, _transcript(request, "[1, 2]\n"))

    def test_a_completion_line_without_a_count_is_a_protocol_failure(self) -> None:
        request = _request("a")

        with pytest.raises(ProtocolFailure, match="no results_emitted count"):
            _accounted(
                request, _transcript(request, _result_line("a"), _line(kind="complete"))
            )


class TestMissingItemsAreNotSynthesized:
    def test_a_missing_terminal_line_leaves_results_trusted_and_incomplete(
        self,
    ) -> None:
        request = _request("a", "b")
        result = _accounted(request, _transcript(request, _result_line("a")))

        assert [item.item_id for item in result.results] == ["a"]
        assert result.missing_item_ids == ("b",)
        assert result.completion_seen is False
        assert result.complete is False
        assert result.results_emitted_claim is None

    def test_a_truncated_trailing_line_is_a_cut_not_a_fault(self) -> None:
        request = _request("a", "b")
        transcript = _transcript(request, _result_line("a")) + '{"kind":"resu'

        result = _accounted(request, transcript)

        assert [item.item_id for item in result.results] == ["a"]
        assert result.missing_item_ids == ("b",)

    def test_a_prelude_only_transcript_yields_no_results_and_no_failure(self) -> None:
        request = _request("a")
        result = _accounted(request, _transcript(request))

        assert result.results == ()
        assert result.missing_item_ids == ("a",)


class TestRequestDeclarations:
    def test_a_request_with_no_items_is_a_declaration_error(self) -> None:
        with pytest.raises(DeclarationError, match="at least one item"):
            _request()

    def test_a_duplicate_declared_item_id_is_a_declaration_error(self) -> None:
        with pytest.raises(DeclarationError, match="duplicate batch item id"):
            _request("a", "a")

    def test_an_empty_body_source_is_a_declaration_error(self) -> None:
        with pytest.raises(DeclarationError, match="driver body source"):
            BatchRequest(
                items=(BatchItem(item_id="a", payload=0),),
                body_source="   ",
                item_schema="int",
                config=_CONFIG,
            )

    def test_a_nonpositive_channel_bound_is_a_declaration_error(self) -> None:
        with pytest.raises(DeclarationError, match="positive integer of bytes"):
            ProtocolChannelBudget(item_result_bytes=0)

    def test_the_channel_bound_scales_with_the_item_count(self) -> None:
        budget = ProtocolChannelBudget(item_result_bytes=100, frame_bytes=10)

        assert budget.channel_bytes_for(4) == 420
