"""Golden vectors pinning identity documents and their digests.

Identity documents and digests are persisted identity: every byte and
every hexadecimal character here is the contract. A failure means stored
identity changed meaning; resolve it by reverting the drift or by a
deliberate standing-contract and schema-version revision, never by
regenerating the expected values.
"""

from __future__ import annotations

from dr_serialize import (
    canonical_identity_json_bytes,
    identity_document_hash,
)

from dr_exec import (
    EnvGrant,
    EnvGrantKind,
    EnvVar,
    ExecutorSelfBudgets,
    TrustedCommandTarget,
)
from dr_exec._identity import (
    _build_env_grant_record,
    _build_executor_config_identity,
    _build_executor_identity,
    _build_isolated_host_runtime_identity,
    _canonical_declaration_digest,
    _canonical_env_values_digest,
    _IsolatedHostRuntimeIdentityPayload,
)
from dr_exec._provenance import ExecutorSourceSnapshot

CLEAN_SNAPSHOT = ExecutorSourceSnapshot(
    package_version="0.1.0",
    source_commit="0" * 40,
    source_state="clean",
    session_id=None,
)
DIRTY_SNAPSHOT = ExecutorSourceSnapshot(
    package_version="0.1.0",
    source_commit="1" * 40,
    source_state="dirty",
    session_id="0189d3f4-1c2b-7e3a-9f10-2b3c4d5e6f72",
)
UNKNOWN_SNAPSHOT = ExecutorSourceSnapshot(
    package_version="0.1.0",
    source_commit=None,
    source_state="unknown",
    session_id="0189d3f4-1c2b-7e3a-9f10-2b3c4d5e6f73",
)
RUNTIME_PAYLOAD = _IsolatedHostRuntimeIdentityPayload(
    resolved_executable="/opt/py/bin/python3.13",
    implementation="cpython",
    python_version="3.13.2",
    cache_tag="cpython-313",
    platform="darwin",
)


def test_clean_executor_identity_is_pinned() -> None:
    document = _build_executor_identity(CLEAN_SNAPSHOT)
    assert canonical_identity_json_bytes(document) == (
        b'{"payload":{"kind":"process_executor","package_version":"0.1.0",'
        b'"session_id":null,"source_commit":"00000000000000000000000000'
        b'00000000000000","source_state":"clean"},"schema":'
        b'"dr_exec.executor","schema_version":1}'
    )
    assert identity_document_hash(document) == (
        "4dc688091b7afc597f17e50c660f8ecf06c0331db9113cf35c2a058cd03231fb"
    )


def test_dirty_executor_identity_is_pinned() -> None:
    document = _build_executor_identity(DIRTY_SNAPSHOT)
    assert canonical_identity_json_bytes(document) == (
        b'{"payload":{"kind":"process_executor","package_version":"0.1.0",'
        b'"session_id":"0189d3f4-1c2b-7e3a-9f10-2b3c4d5e6f72",'
        b'"source_commit":"111111111111111111111111111111111111111'
        b'1","source_state":"dirty"},"schema":"dr_exec.executor",'
        b'"schema_version":1}'
    )
    assert identity_document_hash(document) == (
        "7a1865e9e5395efa9ee476435b6be7a30c172efe79280ccfdc4598075fcbdd2f"
    )


def test_unknown_executor_identity_is_pinned() -> None:
    document = _build_executor_identity(UNKNOWN_SNAPSHOT)
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
    document = _build_executor_config_identity(
        ExecutorSelfBudgets.unbudgeted()
    )
    assert canonical_identity_json_bytes(document) == (
        b'{"payload":{"failure_detail_bytes":{"kind":"unbudgeted"},'
        b'"join_time":{"kind":"unbudgeted"},"json_depth":{"kind":'
        b'"unbudgeted"},"manifest_bytes":{"kind":"unbudgeted"},'
        b'"narration_bytes":{"kind":"unbudgeted"},"protocol_frame_bytes":'
        b'{"kind":"unbudgeted"},"protocol_output_count":{"kind":'
        b'"unbudgeted"},"protocol_total_bytes":{"kind":"unbudgeted"},'
        b'"recording_failure_count":{"kind":"unbudgeted"},"startup_time":'
        b'{"kind":"unbudgeted"},"termination_time":{"kind":"unbudgeted"}},'
        b'"schema":"dr_exec.executor_config","schema_version":1}'
    )
    assert identity_document_hash(document) == (
        "7ea3f81b8d80bd9336b20632c588eabe4fefc72a7eb4eabef8a8cfa0d5887f4e"
    )


def test_isolated_host_runtime_identity_is_pinned() -> None:
    document = _build_isolated_host_runtime_identity(RUNTIME_PAYLOAD)
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


def test_canonical_declaration_digest_is_pinned() -> None:
    target = TrustedCommandTarget(argv=("/bin/echo", "hi"))
    assert _canonical_declaration_digest(target) == (
        "f84544a13ed0a6209f3dfcc7a95e2afaa38d876fe50dec59943711a9c97985e5"
    )


def test_environment_value_digest_ignores_declaration_order() -> None:
    ascending = EnvGrant.fixed({"A": "1", "B": "2"})
    descending = EnvGrant.fixed({"B": "2", "A": "1"})
    assert _canonical_env_values_digest(
        ascending
    ) == _canonical_env_values_digest(descending)


def test_environment_value_digest_separates_names_from_values() -> None:
    """Concatenation-style digests collide; the canonical payload must not."""
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
    record = _build_env_grant_record(grant)
    assert record.var_names == ("LANG", "TOKEN")
    projected = record.model_dump_json()
    assert "super-secret-value" not in projected
    assert "C.UTF-8" not in projected


def test_env_grant_names_use_canonical_not_local_ordering() -> None:
    """Persisted names order by canonical JSON text, not code point.

    These two orderings coincide for ASCII and diverge otherwise, so a
    non-ASCII name is the only thing that distinguishes them.
    """
    names = ("Z", "é", "a")
    grant = EnvGrant(
        kind=EnvGrantKind.OVERLAY,
        variables=tuple(EnvVar(name, "value") for name in names),
        excluded_var_names=("Q", "ü"),
    )

    record = _build_env_grant_record(grant)

    assert record.var_names == ("Z", "é", "a")
    assert record.var_names != tuple(sorted(names))
    assert record.excluded_var_names == ("Q", "ü")
