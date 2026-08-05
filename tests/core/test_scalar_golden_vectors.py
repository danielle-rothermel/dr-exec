"""Golden vectors pinning every scalar wire spelling.

These bytes are the contract, not an observation of current output. A
failure here means persisted identity is changing meaning: revert the
drift, or deliberately revise the standing contract and bump the schema
version with new vectors. Never regenerate expected bytes to match.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import pytest
from dr_serialize import IdentityDocument, Sha256Digest
from pydantic import ValidationError

from dr_exec import (
    AttemptId,
    Budgets,
    ContainmentProfile,
    EnvGrantKind,
    EnvGrantRecord,
    ExecutionId,
    ExecutionMeasurements,
    FiniteByteLimit,
    FiniteDurationLimit,
    FiniteOutput,
    JobId,
    OutputArtifactRecord,
    OutputOverflowPolicy,
    PayloadRetentionBudget,
    StreamRetentionBudget,
    TrustedCommandTarget,
    UntrustedPythonTarget,
)
from dr_exec.core.model import ContractModel, canonical_model_bytes
from dr_exec.runtime.wire import (
    ProtocolComplete,
    ProtocolOutput,
    ProtocolPrelude,
)

ALL_ZERO_DIGEST_WITH_TRAILING_F = Sha256Digest("0" * 63 + "f")
ALL_A_DIGEST = Sha256Digest("a" * 64)
ALL_E_DIGEST = Sha256Digest("e" * 64)


def _uuid_scalar_model() -> ContractModel:
    return ExecutionId(
        job_id=JobId(UUID("0189d3f4-1c2b-7e3a-9f10-2b3c4d5e6f70")),
        attempt_id=AttemptId(UUID("0189d3f4-1c2b-7e3a-9f10-2b3c4d5e6f71")),
    )


def _bytes_and_unicode_scalar_model() -> ContractModel:
    return TrustedCommandTarget(
        argv=("/usr/bin/env", "printf", "aéb"),
        stdin=b"\x00\x01\xfe\xff",
    )


def _timestamp_and_duration_scalar_model() -> ContractModel:
    return ExecutionMeasurements(
        started_at=datetime(2026, 8, 5, 12, 34, 56, 7, tzinfo=UTC),
        finished_at=datetime(2026, 8, 5, 12, 34, 57, 890123, tzinfo=UTC),
        duration_ns=1883123000,
        teardown_duration_ns=250000,
        input_bytes=41,
        protocol_bytes_received=123,
    )


def _path_and_digest_scalar_model() -> ContractModel:
    return OutputArtifactRecord(
        relative_path=Path("stdout.bin"),
        size_bytes=7,
        sha256=ALL_ZERO_DIGEST_WITH_TRAILING_F,
    )


def _enum_and_integer_scalar_model() -> ContractModel:
    return Budgets(
        wall_time=FiniteDurationLimit(max_ns=5_000_000_000),
        input_bytes=FiniteByteLimit(max_bytes=1024),
        payload_output=FiniteOutput(
            max_bytes=100,
            overflow_policy=OutputOverflowPolicy.MARKED_TRUNCATION,
            retention=PayloadRetentionBudget(
                stdout=StreamRetentionBudget(head_bytes=40, tail_bytes=10),
                stderr=StreamRetentionBudget(head_bytes=30, tail_bytes=20),
            ),
        ),
    )


def _env_grant_record_model() -> ContractModel:
    return EnvGrantRecord(
        kind=EnvGrantKind.OVERLAY,
        var_names=("A", "B"),
        excluded_var_names=("C",),
        canonical_values_sha256=ALL_A_DIGEST,
    )


def _non_ascii_env_grant_record_model() -> ContractModel:
    """Names whose canonical order differs from Python's `sorted()`.

    `canonical_sorted_values` orders by canonical JSON text, in which a
    non-ASCII character is its `\\uXXXX` escape, so `é` sorts before `a`
    where `sorted()` puts it after. An all-ASCII vector cannot tell the
    two rules apart, so only this one pins the persisted ordering.
    """
    return EnvGrantRecord(
        kind=EnvGrantKind.OVERLAY,
        var_names=("Z", "é", "a"),
        excluded_var_names=("Q", "ü"),
        canonical_values_sha256=ALL_A_DIGEST,
    )


SCALAR_VECTORS = (
    pytest.param(
        _uuid_scalar_model,
        b'{"attempt_id":"0189d3f4-1c2b-7e3a-9f10-2b3c4d5e6f71",'
        b'"job_id":"0189d3f4-1c2b-7e3a-9f10-2b3c4d5e6f70"}',
        id="uuid",
    ),
    pytest.param(
        _bytes_and_unicode_scalar_model,
        b'{"argv":["/usr/bin/env","printf","a\\u00e9b"],'
        b'"kind":"trusted_command","stdin":"AAH-_w=="}',
        id="bytes-and-unicode",
    ),
    pytest.param(
        _timestamp_and_duration_scalar_model,
        b'{"duration_ns":1883123000,'
        b'"finished_at":"2026-08-05T12:34:57.890123Z","input_bytes":41,'
        b'"protocol_bytes_received":123,'
        b'"started_at":"2026-08-05T12:34:56.000007Z",'
        b'"teardown_duration_ns":250000}',
        id="timestamp-and-duration",
    ),
    pytest.param(
        _path_and_digest_scalar_model,
        b'{"relative_path":"stdout.bin","sha256":"0000000000000000000000'
        b'00000000000000000000000000000000000000000f","size_bytes":7}',
        id="path-and-sha256",
    ),
    pytest.param(
        _enum_and_integer_scalar_model,
        b'{"cpu_time":{"kind":"unbudgeted"},"disk_bytes":{"kind":'
        b'"unbudgeted"},"file_size_bytes":{"kind":"unbudgeted"},'
        b'"input_bytes":{"kind":"finite","max_bytes":1024},'
        b'"memory_bytes":{"kind":"unbudgeted"},"open_file_count":{"kind":'
        b'"unbudgeted"},"payload_output":{"kind":"finite","max_bytes":100,'
        b'"overflow_policy":"marked_truncation","retention":{"stderr":'
        b'{"head_bytes":30,"tail_bytes":20},"stdout":{"head_bytes":40,'
        b'"tail_bytes":10}}},"process_count":{"kind":"unbudgeted"},'
        b'"wall_time":{"kind":"finite","max_ns":5000000000}}',
        id="enum-and-integer",
    ),
    pytest.param(
        _env_grant_record_model,
        b'{"canonical_values_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
        b'aaaaaaaaaaaaaaaaaaaaaaaaaaaaa","excluded_var_names":["C"],'
        b'"kind":"overlay","var_names":["A","B"]}',
        id="env-grant-record",
    ),
    pytest.param(
        _non_ascii_env_grant_record_model,
        b'{"canonical_values_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
        b'aaaaaaaaaaaaaaaaaaaaaaaaaaaaa",'
        b'"excluded_var_names":["Q","\\u00fc"],'
        b'"kind":"overlay","var_names":["Z","\\u00e9","a"]}',
        id="env-grant-record-non-ascii",
    ),
)


@pytest.mark.parametrize(("build_model", "expected"), SCALAR_VECTORS)
def test_scalar_wire_spelling_is_pinned(
    build_model: Callable[[], ContractModel],
    expected: bytes,
) -> None:
    assert canonical_model_bytes(build_model()) == expected


@pytest.mark.parametrize(("build_model", "expected"), SCALAR_VECTORS)
def test_the_pinned_scalar_spelling_reads_back(
    build_model: Callable[[], ContractModel],
    expected: bytes,
) -> None:
    """Each written vector is accepted by the read path that wrote it."""
    model = build_model()
    assert type(model).model_validate_json(expected, strict=True) == model


# --- rejected alternate spellings ----------------------------------------
#
# The read path accepts exactly one spelling per scalar. These are the
# forms the underlying parsers would otherwise accept and silently
# normalize, which would let distinct durable documents read back as one
# value -- or, for a seventh fractional digit, let a loaded value differ
# from the bytes on disk without any error.

GOOD_UUID = "0189d3f4-1c2b-7e3a-9f10-2b3c4d5e6f70"

REJECTED_UUID_SPELLINGS = (
    pytest.param(GOOD_UUID.upper(), id="uppercase"),
    pytest.param(GOOD_UUID.replace("-", ""), id="unhyphenated"),
    pytest.param(f"urn:uuid:{GOOD_UUID}", id="urn"),
    pytest.param(f"{{{GOOD_UUID}}}", id="braced"),
)

REJECTED_TIMESTAMP_SPELLINGS = (
    pytest.param("2026-08-05T12:34:56Z", id="no-fractional-digits"),
    pytest.param("2026-08-05T12:34:56.1Z", id="one-fractional-digit"),
    pytest.param("2026-08-05T12:34:56.000007+00:00", id="offset-not-z"),
    pytest.param("2026-08-05T12:34:56.0000071Z", id="seven-digits"),
    pytest.param("2026-08-05T12:34:56.000007", id="naive"),
    pytest.param("2026-08-05T12:34:56.000007+02:00", id="non-utc-offset"),
)

REJECTED_BYTES_SPELLINGS = (
    pytest.param("AAH+/w==", id="standard-alphabet-padded"),
    pytest.param("AAH-_w", id="url-safe-unpadded"),
    pytest.param("AAH+/w", id="standard-alphabet-unpadded"),
    # The final character of a padded group carries bits the encoder
    # never sets, so these all decode to bytes whose own encoding is a
    # different string: `AB==`, `AC==`, and `AP==` decode to `AA==`'s
    # `b"\x00"`, and `AAF=` decodes to `AAE=`'s `b"\x00\x01"`.
    pytest.param("AB==", id="two-char-trailing-bits-low"),
    pytest.param("AC==", id="two-char-trailing-bits-mid"),
    pytest.param("AP==", id="two-char-trailing-bits-high"),
    pytest.param("AAF=", id="three-char-trailing-bits"),
)


@pytest.mark.parametrize("spelling", REJECTED_UUID_SPELLINGS)
def test_a_non_pinned_uuid_spelling_is_rejected(spelling: str) -> None:
    document = json.dumps({"job_id": spelling, "attempt_id": GOOD_UUID})
    with pytest.raises(ValidationError):
        ExecutionId.model_validate_json(document, strict=True)


@pytest.mark.parametrize("spelling", REJECTED_TIMESTAMP_SPELLINGS)
def test_a_non_pinned_timestamp_spelling_is_rejected(spelling: str) -> None:
    document = json.dumps(
        {
            "started_at": spelling,
            "finished_at": "2026-08-05T12:34:57.890123Z",
            "duration_ns": 1,
            "teardown_duration_ns": 0,
            "input_bytes": 0,
            "protocol_bytes_received": 0,
        }
    )
    with pytest.raises(ValidationError):
        ExecutionMeasurements.model_validate_json(document, strict=True)


@pytest.mark.parametrize("spelling", REJECTED_BYTES_SPELLINGS)
def test_a_non_pinned_bytes_spelling_is_rejected(spelling: str) -> None:
    document = json.dumps(
        {"kind": "trusted_command", "argv": ["/bin/echo"], "stdin": spelling}
    )
    with pytest.raises(ValidationError):
        TrustedCommandTarget.model_validate_json(document, strict=True)


def test_untrusted_python_target_wire_spelling_is_pinned(
    request_document: IdentityDocument,
) -> None:
    target = UntrustedPythonTarget(
        driver_source="def dr_exec_main(request, emit):\n    return None\n",
        request=request_document,
        containment_profile=ContainmentProfile.PROCESS_BOUNDARY_ONLY,
    )
    assert canonical_model_bytes(target) == (
        b'{"containment_profile":"process_boundary_only","driver_source":'
        b'"def dr_exec_main(request, emit):\\n    return None\\n",'
        b'"kind":"untrusted_python","request":{"payload":{"a":[1,'
        b'{"z":null}],"b":2},"schema":"dr_exec.test_request",'
        b'"schema_version":1}}'
    )


def test_protocol_prelude_wire_spelling_is_pinned() -> None:
    frame = ProtocolPrelude(version=1, request_id_sha256=ALL_E_DIGEST)
    assert canonical_model_bytes(frame) == (
        b'{"kind":"prelude","request_id_sha256":"eeeeeeeeeeeeeeeeeeeeeeee'
        b'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee","version":1}'
    )


def test_protocol_output_wire_spelling_is_pinned(
    request_document: IdentityDocument,
) -> None:
    frame = ProtocolOutput(
        version=1,
        sequence=0,
        document=request_document,
    )
    assert canonical_model_bytes(frame) == (
        b'{"document":{"payload":{"a":[1,{"z":null}],"b":2},"schema":'
        b'"dr_exec.test_request","schema_version":1},"kind":"output",'
        b'"sequence":0,"version":1}'
    )


def test_protocol_complete_wire_spelling_is_pinned() -> None:
    frame = ProtocolComplete(version=1, output_count=1)
    assert canonical_model_bytes(frame) == (
        b'{"kind":"complete","output_count":1,"version":1}'
    )
