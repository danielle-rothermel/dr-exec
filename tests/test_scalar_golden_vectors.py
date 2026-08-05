"""Golden vectors pinning every scalar wire spelling.

These bytes are the contract, not an observation of current output. A
failure here means persisted identity is changing meaning: revert the
drift, or deliberately revise the standing contract and bump the schema
version with new vectors. Never regenerate expected bytes to match.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import pytest
from dr_serialize import IdentityDocument

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
from dr_exec._model import ContractModel, canonical_model_bytes
from dr_exec._wire import ProtocolComplete, ProtocolOutput, ProtocolPrelude

ALL_ZERO_DIGEST_WITH_TRAILING_F = "0" * 63 + "f"
ALL_A_DIGEST = "a" * 64
ALL_E_DIGEST = "e" * 64


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
)


@pytest.mark.parametrize(("build_model", "expected"), SCALAR_VECTORS)
def test_scalar_wire_spelling_is_pinned(
    build_model: Callable[[], ContractModel],
    expected: bytes,
) -> None:
    assert canonical_model_bytes(build_model()) == expected


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
    frame = ProtocolPrelude(request_id_sha256=ALL_E_DIGEST)
    assert canonical_model_bytes(frame) == (
        b'{"kind":"prelude","request_id_sha256":"eeeeeeeeeeeeeeeeeeeeeeee'
        b'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee","version":1}'
    )


def test_protocol_output_wire_spelling_is_pinned(
    request_document: IdentityDocument,
) -> None:
    frame = ProtocolOutput(sequence=0, document=request_document)
    assert canonical_model_bytes(frame) == (
        b'{"document":{"payload":{"a":[1,{"z":null}],"b":2},"schema":'
        b'"dr_exec.test_request","schema_version":1},"kind":"output",'
        b'"sequence":0,"version":1}'
    )


def test_protocol_complete_wire_spelling_is_pinned() -> None:
    frame = ProtocolComplete(output_count=1)
    assert canonical_model_bytes(frame) == (
        b'{"kind":"complete","output_count":1,"version":1}'
    )
