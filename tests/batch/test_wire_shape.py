"""Golden tests over the batch wire format's literals and canonicalization.

These assertions are the contract: a key, a kind, or the digest's
canonicalization changes only by contract revision. Each set assertion is
written out member by member so a silent addition or rename fails here
first.
"""

from __future__ import annotations

import hashlib
import json

from dr_exec.batch import (
    BODY_HOOK_NAME,
    CLIP_MARKER,
    PROTOCOL_VERSION,
    BatchItem,
    BatchRequest,
    WireKey,
    WireKind,
    config_digest_of,
)


def test_wire_key_literals() -> None:
    assert WireKey.KIND.value == "kind"
    assert WireKey.PROTOCOL.value == "protocol"
    assert WireKey.ITEM_IDS.value == "item_ids"
    assert WireKey.CONFIG_DIGEST.value == "config_digest"
    assert WireKey.ITEM_ID.value == "item_id"
    assert WireKey.PAYLOAD.value == "payload"
    assert WireKey.RESULTS_EMITTED.value == "results_emitted"
    assert WireKey.ERROR.value == "error"


def test_wire_key_member_set_is_exactly_the_eight_keys() -> None:
    assert {member.value for member in WireKey} == {
        "kind",
        "protocol",
        "item_ids",
        "config_digest",
        "item_id",
        "payload",
        "results_emitted",
        "error",
    }


def test_wire_kind_literals() -> None:
    assert WireKind.PRELUDE.value == "prelude"
    assert WireKind.RESULT.value == "result"
    assert WireKind.COMPLETE.value == "complete"


def test_wire_kind_member_set_is_exactly_the_three_line_kinds() -> None:
    assert {member.value for member in WireKind} == {"prelude", "result", "complete"}


def test_protocol_version_is_one() -> None:
    assert PROTOCOL_VERSION == 1


def test_body_hook_name_is_run_item() -> None:
    assert BODY_HOOK_NAME == "run_item"


def test_clip_marker_literal() -> None:
    assert CLIP_MARKER == "...[clipped]"


class TestConfigDigest:
    def test_canonicalization_is_pinned_to_a_known_hex(self) -> None:
        digest = config_digest_of({"b": 2, "a": [1, {"d": 4, "c": 3}]})

        assert digest == (
            "b90ecf34c980b7ce791e11520e8f83c0c20ddbc78be1ff95bd85fb7708edb05a"
        )

    def test_the_digest_is_sha256_over_sorted_compact_utf8_json(self) -> None:
        config = {"b": 2, "a": [1, {"d": 4, "c": 3}]}
        canonical = '{"a":[1,{"c":3,"d":4}],"b":2}'

        assert json.dumps(config, sort_keys=True, separators=(",", ":")) == canonical
        assert (
            config_digest_of(config)
            == hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        )

    def test_key_order_does_not_change_the_digest(self) -> None:
        assert config_digest_of({"a": 1, "b": 2}) == config_digest_of({"b": 2, "a": 1})

    def test_a_value_change_changes_the_digest(self) -> None:
        assert config_digest_of({"a": 1}) != config_digest_of({"a": 2})


class TestPreludeShape:
    def test_the_prelude_carries_exactly_the_four_identity_keys(self) -> None:
        request = _request()

        assert request.prelude() == {
            "kind": "prelude",
            "protocol": 1,
            "item_ids": ["alpha", "beta"],
            "config_digest": config_digest_of({"seed": 7}),
        }

    def test_item_ids_echo_the_declared_order(self) -> None:
        assert _request().prelude()["item_ids"] == ["alpha", "beta"]


def _request() -> BatchRequest:
    return BatchRequest(
        items=(
            BatchItem(item_id="alpha", payload=1),
            BatchItem(item_id="beta", payload=2),
        ),
        body_source="def run_item(item_id, payload):\n    return payload\n",
        item_schema="int",
        config={"seed": 7},
    )
