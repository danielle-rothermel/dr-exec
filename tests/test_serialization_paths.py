"""Strict read and write behavior for the validated serialization paths."""

from __future__ import annotations

import pytest
from dr_serialize import (
    DuplicateJsonKeyError,
    InvalidUtf8Error,
    JsonByteLimitError,
    JsonDepthLimitError,
    JsonSyntaxError,
    NonFiniteJsonNumberError,
    StrictJsonDecodeError,
)

from dr_exec import ExecutionId, OutputArtifactRecord
from dr_exec._model import (
    NonCanonicalBytesError,
    canonical_model_bytes,
    validate_canonical_model_bytes,
)

READ_MAX_BYTES = 4096
READ_MAX_DEPTH = 32


def test_canonical_write_then_read_round_trips(
    execution_id: ExecutionId,
) -> None:
    data = canonical_model_bytes(execution_id)
    assert (
        validate_canonical_model_bytes(
            ExecutionId,
            data,
            max_bytes=READ_MAX_BYTES,
            max_depth=READ_MAX_DEPTH,
        )
        == execution_id
    )


def test_read_rejects_non_canonical_key_order(
    execution_id: ExecutionId,
) -> None:
    reordered = (
        b'{"job_id":"0189d3f4-1c2b-7e3a-9f10-2b3c4d5e6f70",'
        b'"attempt_id":"0189d3f4-1c2b-7e3a-9f10-2b3c4d5e6f71"}'
    )
    assert reordered != canonical_model_bytes(execution_id)
    with pytest.raises(NonCanonicalBytesError):
        validate_canonical_model_bytes(
            ExecutionId,
            reordered,
            max_bytes=READ_MAX_BYTES,
            max_depth=READ_MAX_DEPTH,
        )


def test_read_rejects_insignificant_whitespace(
    execution_id: ExecutionId,
) -> None:
    spaced = canonical_model_bytes(execution_id).replace(b",", b", ")
    with pytest.raises(NonCanonicalBytesError):
        validate_canonical_model_bytes(
            ExecutionId,
            spaced,
            max_bytes=READ_MAX_BYTES,
            max_depth=READ_MAX_DEPTH,
        )


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
        validate_canonical_model_bytes(
            ExecutionId,
            data,
            max_bytes=READ_MAX_BYTES,
            max_depth=READ_MAX_DEPTH,
        )


def test_read_enforces_the_declared_byte_bound(
    execution_id: ExecutionId,
) -> None:
    data = canonical_model_bytes(execution_id)
    with pytest.raises(JsonByteLimitError):
        validate_canonical_model_bytes(
            ExecutionId,
            data,
            max_bytes=len(data) - 1,
            max_depth=READ_MAX_DEPTH,
        )


def test_read_enforces_the_declared_depth_bound(
    execution_id: ExecutionId,
) -> None:
    data = canonical_model_bytes(execution_id)
    with pytest.raises(JsonDepthLimitError):
        validate_canonical_model_bytes(
            ExecutionId,
            data,
            max_bytes=READ_MAX_BYTES,
            max_depth=0,
        )


def test_read_rejects_a_valid_document_of_the_wrong_model(
    execution_id: ExecutionId,
) -> None:
    data = canonical_model_bytes(execution_id)
    with pytest.raises(ValueError, match="validation error"):
        validate_canonical_model_bytes(
            OutputArtifactRecord,
            data,
            max_bytes=READ_MAX_BYTES,
            max_depth=READ_MAX_DEPTH,
        )


def test_read_rejects_extra_fields(execution_id: ExecutionId) -> None:
    """An extra key in canonical position still fails model validation."""
    data = canonical_model_bytes(execution_id).replace(
        b',"job_id"', b',"extra":1,"job_id"', 1
    )
    with pytest.raises(ValueError, match="validation error"):
        validate_canonical_model_bytes(
            ExecutionId,
            data,
            max_bytes=READ_MAX_BYTES,
            max_depth=READ_MAX_DEPTH,
        )
