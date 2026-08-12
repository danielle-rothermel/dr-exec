from __future__ import annotations

import pytest
from dr_serialize import (
    build_identity_document,
    canonical_identity_json_bytes,
    identity_document_hash,
)
from pydantic import ValidationError

from dr_exec import (
    ContainmentProfile,
    EnvGrant,
    EnvGrantKind,
    EnvVar,
    ExecutorSelfBudgets,
    FiniteByteLimit,
    FiniteCountLimit,
    FiniteDurationLimit,
    TrustedCommandTarget,
    TrustedPythonTarget,
    UntrustedCommandTarget,
    UntrustedPythonTarget,
)
from dr_exec.recording.identity import (
    _canonical_env_values_digest,
    build_env_grant_record,
    build_executor_config_identity,
    build_executor_identity,
    canonical_declaration_digest,
    validate_executor_config_identity,
    validate_executor_identity,
)
from dr_exec.recording.provenance import ExecutorSourceSnapshot
from dr_exec.runtime.identity import (
    IsolatedHostRuntimeIdentityPayload,
    build_isolated_host_runtime_identity,
)

CLEAN_SNAPSHOT = ExecutorSourceSnapshot(
    package_version="0.1.0",
    source_commit="0" * 40,
    source_state="clean",
    session_id=None,
)
UNKNOWN_SNAPSHOT = ExecutorSourceSnapshot(
    package_version="0.1.0",
    source_commit=None,
    source_state="unknown",
    session_id="0189d3f4-1c2b-7e3a-9f10-2b3c4d5e6f73",
)
RUNTIME_PAYLOAD = IsolatedHostRuntimeIdentityPayload(
    kind="isolated_host_python",
    resolved_executable="/opt/py/bin/python3.13",
    implementation="cpython",
    python_version="3.13.2",
    cache_tag="cpython-313",
    platform="darwin",
)


def test_clean_executor_identity_is_pinned() -> None:
    document = build_executor_identity(CLEAN_SNAPSHOT)
    assert canonical_identity_json_bytes(document) == (
        b'{"payload":{"kind":"process_executor","package_version":"0.1.0",'
        b'"session_id":null,"source_commit":"00000000000000000000000000'
        b'00000000000000","source_state":"clean"},"schema":'
        b'"dr_exec.executor","schema_version":1}'
    )
    assert identity_document_hash(document) == (
        "4dc688091b7afc597f17e50c660f8ecf06c0331db9113cf35c2a058cd03231fb"
    )


def test_unknown_executor_identity_is_pinned() -> None:
    document = build_executor_identity(UNKNOWN_SNAPSHOT)
    assert canonical_identity_json_bytes(document) == (
        b'{"payload":{"kind":"process_executor","package_version":"0.1.0",'
        b'"session_id":"0189d3f4-1c2b-7e3a-9f10-2b3c4d5e6f73",'
        b'"source_commit":null,"source_state":"unknown"},"schema":'
        b'"dr_exec.executor","schema_version":1}'
    )
    assert identity_document_hash(document) == (
        "4f4f3795fcd773ec56b36f6b341235d32e3e27029164e9424ebafdde74e8f96a"
    )


def test_unbudgeted_executor_config_identity_is_pinned() -> None:
    document = build_executor_config_identity(ExecutorSelfBudgets.unbudgeted())
    assert canonical_identity_json_bytes(document) == (
        b'{"payload":{"join_time":{"kind":"unbudgeted"},"json_depth":{"kind":'
        b'"unbudgeted"},"protocol_frame_bytes":{"kind":"unbudgeted"},'
        b'"protocol_output_count":{"kind":"unbudgeted"},"protocol_total_bytes":'
        b'{"kind":"unbudgeted"},"startup_time":{"kind":"unbudgeted"},'
        b'"termination_time":{"kind":"unbudgeted"}},"schema":'
        b'"dr_exec.executor_config","schema_version":1}'
    )
    assert identity_document_hash(document) == (
        "b3cb5e10816f98a5965300e144dbf07b02aa73f39a25ccecd86638d7e2a77e93"
    )


def test_representative_finite_executor_config_identity_is_pinned() -> None:
    document = build_executor_config_identity(
        ExecutorSelfBudgets(
            protocol_frame_bytes=FiniteByteLimit(max_bytes=1024),
            protocol_output_count=FiniteCountLimit(max_count=3),
            startup_time=FiniteDurationLimit(max_ns=5_000_000_000),
        )
    )
    assert canonical_identity_json_bytes(document) == (
        b'{"payload":{"join_time":{"kind":"unbudgeted"},"json_depth":{"kind":'
        b'"unbudgeted"},"protocol_frame_bytes":{"kind":"finite","max_bytes":1024},'
        b'"protocol_output_count":{"kind":"finite","max_count":3},'
        b'"protocol_total_bytes":{"kind":"unbudgeted"},"startup_time":{"kind":'
        b'"finite","max_ns":5000000000},"termination_time":{"kind":'
        b'"unbudgeted"}},"schema":"dr_exec.executor_config","schema_version":1}'
    )
    assert identity_document_hash(document) == (
        "9bee8d00b961b2ff935270b47e251a0cff7c25c4a8e1ef841a68ee79f4dc2a1e"
    )


def test_isolated_host_runtime_identity_is_pinned() -> None:
    document = build_isolated_host_runtime_identity(RUNTIME_PAYLOAD)
    assert canonical_identity_json_bytes(document) == (
        b'{"payload":{"cache_tag":"cpython-313","implementation":"cpython",'
        b'"kind":"isolated_host_python","platform":"darwin",'
        b'"python_version":"3.13.2","resolved_executable":'
        b'"/opt/py/bin/python3.13"},"schema":'
        b'"dr_exec.isolated_host_python_runtime","schema_version":1}'
    )
    assert identity_document_hash(document) == (
        "dba9d30d4fb7d032604cd2026dce35dc0c41d3e8cd7dc68f1bf81915d29f9567"
    )


def test_canonical_environment_value_digest_is_pinned() -> None:
    grant = EnvGrant(
        kind=EnvGrantKind.FIXED,
        variables=(EnvVar("PATH", "/usr/bin"), EnvVar("LANG", "C.UTF-8")),
    )
    assert _canonical_env_values_digest(grant) == (
        "18241bf7da43f7ef34cf34dfdac15c6fbc7b0a4a336dd5cbff99db34430799c3"
    )


def test_empty_environment_value_digest_is_pinned() -> None:
    assert _canonical_env_values_digest(EnvGrant.none()) == (
        "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"
    )


def test_trusted_command_declaration_digest_is_pinned() -> None:
    target = TrustedCommandTarget(argv=("/bin/echo", "hi"))
    assert canonical_declaration_digest(target) == (
        "f84544a13ed0a6209f3dfcc7a95e2afaa38d876fe50dec59943711a9c97985e5"
    )


def test_untrusted_command_declaration_digest_is_pinned() -> None:
    target = UntrustedCommandTarget(
        argv=("/usr/bin/env", "printf", "hi"),
        stdin=b"input",
        containment_profile=ContainmentProfile.PROCESS_BOUNDARY_ONLY,
    )
    assert canonical_declaration_digest(target) == (
        "cf06ac433f9a8bd85e6894738540bec9573ed0991139fb09767dc82b32ee3265"
    )


def test_untrusted_python_declaration_digest_is_pinned() -> None:
    request = build_identity_document(
        schema="dr_exec.test_request",
        schema_version=1,
        payload={"count": 2, "label": "x"},
    )
    target = UntrustedPythonTarget(
        driver_source=(
            "def dr_exec_main(request, emit):\n    emit(request)\n"
        ),
        request=request,
        containment_profile=ContainmentProfile.PROCESS_BOUNDARY_ONLY,
    )
    assert canonical_declaration_digest(target) == (
        "efd92cff97fbb61af1c3923f8e2eff55ea02ee98eb4e728cf586d570f4659cc0"
    )


def test_trusted_python_declaration_digest_is_pinned() -> None:
    request = build_identity_document(
        schema="dr_exec.test_request",
        schema_version=1,
        payload={"count": 2, "label": "x"},
    )
    target = TrustedPythonTarget(
        driver_source=(
            "def dr_exec_main(request, emit):\n    emit(request)\n"
        ),
        request=request,
    )
    assert canonical_declaration_digest(target) == (
        "af2ac384a6371c9d1250abe823ff9caf8c308c0715bac6e70647777a41594ce1"
    )


@pytest.mark.parametrize(
    "payload",
    [
        pytest.param({"kind": "process_executor"}, id="incomplete"),
        pytest.param(
            {
                "kind": "process_executor",
                "package_version": "0.1.0",
                "source_commit": "0" * 40,
                "source_state": "clean",
                "session_id": None,
                "extra": 1,
            },
            id="extra-key",
        ),
        pytest.param(
            {
                "package_version": "0.1.0",
                "source_commit": "0" * 40,
                "source_state": "clean",
                "session_id": None,
            },
            id="missing-kind",
        ),
    ],
)
def test_executor_identity_rejects_incomplete_or_open_payloads(
    payload: dict[str, object],
) -> None:
    document = build_identity_document(
        schema="dr_exec.executor",
        schema_version=1,
        payload=payload,
    )

    with pytest.raises(ValidationError):
        validate_executor_identity(document)


def test_executor_config_identity_rejects_a_foreign_schema_version() -> None:
    document = build_identity_document(
        schema="dr_exec.executor_config",
        schema_version=2,
        payload=ExecutorSelfBudgets.unbudgeted().model_dump(mode="json"),
    )

    with pytest.raises(ValueError, match="schema version 1"):
        validate_executor_config_identity(document)


def test_environment_value_digest_ignores_declaration_order() -> None:
    ascending = EnvGrant.fixed({"A": "1", "B": "2"})
    descending = EnvGrant.fixed({"B": "2", "A": "1"})
    assert _canonical_env_values_digest(
        ascending
    ) == _canonical_env_values_digest(descending)


def test_environment_value_digest_separates_names_from_values() -> None:
    split_early = EnvGrant.fixed({"A": "1", "AB": "2"})
    split_late = EnvGrant.fixed({"A": "1A", "B": "2"})
    assert _canonical_env_values_digest(
        split_early
    ) != _canonical_env_values_digest(split_late)


def test_env_grant_record_is_secret_free() -> None:
    grant = EnvGrant(
        kind=EnvGrantKind.FIXED,
        variables=(
            EnvVar("TOKEN", "super-secret-value"),
            EnvVar("LANG", "C.UTF-8"),
        ),
    )
    record = build_env_grant_record(grant)
    assert record.var_names == ("LANG", "TOKEN")
    projected = record.model_dump_json()
    assert "super-secret-value" not in projected
    assert "C.UTF-8" not in projected


def test_env_grant_names_use_canonical_not_local_ordering() -> None:
    names = ("Z", "é", "a")
    grant = EnvGrant(
        kind=EnvGrantKind.OVERLAY,
        variables=tuple(EnvVar(name, "value") for name in names),
        excluded_var_names=("Q", "ü"),
    )

    record = build_env_grant_record(grant)

    assert record.var_names == ("Z", "é", "a")
    assert record.var_names != tuple(sorted(names))
    assert record.excluded_var_names == ("Q", "ü")
