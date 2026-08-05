"""Strict read and write behavior for the validated serialization paths.

``require_canonical_json_bytes`` is the shared front half of every read
boundary, so its bounds, its strict-decoder taxonomy, and its canonical
equality check are exercised directly. The closed-model tail belongs to
the real boundaries, so ``decode_frame`` stands in for it here.
"""

from __future__ import annotations

import pytest
from dr_serialize import (
    DuplicateJsonKeyError,
    InvalidUtf8Error,
    JsonByteLimitError,
    JsonDepthLimitError,
    JsonSyntaxError,
    NonFiniteJsonNumberError,
    Sha256Digest,
    StrictJsonDecodeError,
)

from dr_exec import ExecutionId
from dr_exec.core.kinds import ProtocolFailureCode
from dr_exec.core.model import (
    NonCanonicalBytesError,
    canonical_model_bytes,
    require_canonical_json_bytes,
)
from dr_exec.runtime.protocol import ProtocolViolation, decode_frame
from dr_exec.runtime.wire import ProtocolPrelude

READ_MAX_BYTES = 4096
READ_MAX_DEPTH = 32


def _read(data: bytes) -> None:
    require_canonical_json_bytes(
        data,
        max_bytes=READ_MAX_BYTES,
        max_depth=READ_MAX_DEPTH,
    )


def test_canonical_write_then_read_round_trips(
    execution_id: ExecutionId,
) -> None:
    data = canonical_model_bytes(execution_id)

    _read(data)

    assert ExecutionId.model_validate_json(data, strict=True) == execution_id


def test_read_rejects_non_canonical_key_order(
    execution_id: ExecutionId,
) -> None:
    reordered = (
        b'{"job_id":"0189d3f4-1c2b-7e3a-9f10-2b3c4d5e6f70",'
        b'"attempt_id":"0189d3f4-1c2b-7e3a-9f10-2b3c4d5e6f71"}'
    )
    assert reordered != canonical_model_bytes(execution_id)
    with pytest.raises(NonCanonicalBytesError):
        _read(reordered)


def test_read_rejects_insignificant_whitespace(
    execution_id: ExecutionId,
) -> None:
    spaced = canonical_model_bytes(execution_id).replace(b",", b", ")
    with pytest.raises(NonCanonicalBytesError):
        _read(spaced)


@pytest.mark.parametrize(
    ("data", "expected_error"),
    [
        pytest.param(
            b'{"job_id":"x","job_id":"y"}',
            DuplicateJsonKeyError,
            id="duplicate-key",
        ),
        pytest.param(b"\xff\xfe", InvalidUtf8Error, id="invalid-utf8"),
        pytest.param(b'{"a":1}trailing', JsonSyntaxError, id="trailing-data"),
        pytest.param(b'{"a":NaN}', NonFiniteJsonNumberError, id="non-finite"),
    ],
)
def test_read_surfaces_shared_decoder_taxonomy(
    data: bytes,
    expected_error: type[StrictJsonDecodeError],
) -> None:
    with pytest.raises(expected_error):
        _read(data)


def test_read_enforces_the_declared_byte_bound(
    execution_id: ExecutionId,
) -> None:
    data = canonical_model_bytes(execution_id)
    with pytest.raises(JsonByteLimitError):
        require_canonical_json_bytes(
            data,
            max_bytes=len(data) - 1,
            max_depth=READ_MAX_DEPTH,
        )


def test_read_enforces_the_declared_depth_bound(
    execution_id: ExecutionId,
) -> None:
    data = canonical_model_bytes(execution_id)
    with pytest.raises(JsonDepthLimitError):
        require_canonical_json_bytes(
            data,
            max_bytes=READ_MAX_BYTES,
            max_depth=0,
        )


def test_a_real_boundary_validates_the_same_canonical_bytes() -> None:
    """The closed-model tail runs on the bytes the front half verified."""
    prelude = ProtocolPrelude(
        version=1,
        request_id_sha256=Sha256Digest("a" * 64),
    )
    data = canonical_model_bytes(prelude)

    assert decode_frame(data, max_depth=READ_MAX_DEPTH) == prelude


def test_a_real_boundary_rejects_a_valid_document_of_the_wrong_model(
    execution_id: ExecutionId,
) -> None:
    data = canonical_model_bytes(execution_id)

    with pytest.raises(ProtocolViolation) as raised:
        decode_frame(data, max_depth=READ_MAX_DEPTH)

    assert raised.value.code == ProtocolFailureCode.MALFORMED_FRAME


def test_a_real_boundary_rejects_extra_fields() -> None:
    """An extra key in canonical position still fails model validation."""
    prelude = ProtocolPrelude(
        version=1,
        request_id_sha256=Sha256Digest("a" * 64),
    )
    data = canonical_model_bytes(prelude).replace(
        b'{"kind"', b'{"extra":1,"kind"', 1
    )

    with pytest.raises(ProtocolViolation) as raised:
        decode_frame(data, max_depth=READ_MAX_DEPTH)

    assert raised.value.code == ProtocolFailureCode.MALFORMED_FRAME
