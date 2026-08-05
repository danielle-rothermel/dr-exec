"""Secret-free projections and the IdentityDocument wire form."""

from __future__ import annotations

from pathlib import Path

from dr_serialize import IdentityDocument, build_identity_document

from dr_exec import (
    ContainmentProfile,
    EnvGrant,
    RuntimeKind,
    RuntimeRecord,
    TrustedCommandTarget,
    UntrustedCommandTargetRecord,
    UntrustedPythonTarget,
    UntrustedPythonTargetRecord,
)
from dr_exec.core.model import canonical_model_bytes
from dr_exec.recording.identity import (
    _build_env_grant_record,
    _canonical_declaration_digest,
)

SECRET_ARGUMENT = "hunter2-argv-secret"
SECRET_SOURCE = "SECRET_SOURCE_LITERAL"
SECRET_STDIN = b"hunter2-stdin-secret"
SECRET_ENV_VALUE = "hunter2-env-secret"
SECRET_REQUEST_VALUE = "hunter2-request-secret"
RUNTIME_EXECUTABLE = Path("/opt/py/bin/python3.13")


def _stub_runtime_record() -> RuntimeRecord:
    return RuntimeRecord(
        kind=RuntimeKind.ISOLATED_HOST_PYTHON,
        resolved_executable=RUNTIME_EXECUTABLE,
        id_doc=build_identity_document(
            schema="dr_exec.isolated_host_python_runtime",
            schema_version=1,
            payload={
                "kind": "isolated_host_python",
                "resolved_executable": RUNTIME_EXECUTABLE.as_posix(),
                "implementation": "cpython",
                "python_version": "3.13.2",
                "cache_tag": "cpython-313",
                "platform": "darwin",
            },
        ),
    )


def test_target_records_expose_only_a_declaration_digest() -> None:
    target = TrustedCommandTarget(
        argv=("/bin/echo", SECRET_ARGUMENT),
        stdin=SECRET_STDIN,
    )
    record = UntrustedCommandTargetRecord(
        canonical_declaration_sha256=_canonical_declaration_digest(target),
        containment_profile=ContainmentProfile.PROCESS_BOUNDARY_ONLY,
    )
    projected = canonical_model_bytes(record)
    assert SECRET_ARGUMENT.encode() not in projected
    assert SECRET_STDIN not in projected
    assert record.canonical_declaration_sha256.encode() in projected


def test_python_target_records_expose_no_source_or_request_payload(
    request_document: IdentityDocument,
) -> None:
    secret_request = build_identity_document(
        schema=request_document.schema,
        schema_version=request_document.schema_version,
        payload={"secret": SECRET_REQUEST_VALUE},
    )
    target = UntrustedPythonTarget(
        driver_source=f"# {SECRET_SOURCE}\ndef dr_exec_main(r, e): ...\n",
        request=secret_request,
        containment_profile=ContainmentProfile.PROCESS_BOUNDARY_ONLY,
    )
    declaration_digest = _canonical_declaration_digest(target)
    assert SECRET_SOURCE.encode() in canonical_model_bytes(target)
    record = UntrustedPythonTargetRecord(
        canonical_declaration_sha256=declaration_digest,
        request_id_sha256=declaration_digest,
        containment_profile=ContainmentProfile.PROCESS_BOUNDARY_ONLY,
        runtime=_stub_runtime_record(),
    )
    projected = canonical_model_bytes(record)
    assert SECRET_SOURCE.encode() not in projected
    assert SECRET_REQUEST_VALUE.encode() not in projected


def test_environment_records_never_carry_values() -> None:
    grant = EnvGrant.fixed({"TOKEN": SECRET_ENV_VALUE, "LANG": "C.UTF-8"})
    projected = canonical_model_bytes(_build_env_grant_record(grant))
    assert SECRET_ENV_VALUE.encode() not in projected
    assert b"C.UTF-8" not in projected
    assert b'"var_names":["LANG","TOKEN"]' in projected


def test_identity_document_fields_keep_the_three_field_wire_form(
    request_document: IdentityDocument,
) -> None:
    """A bare IdentityDocument annotation would spell the key `_payload`."""
    target = UntrustedPythonTarget(
        driver_source="def dr_exec_main(r, e): ...\n",
        request=request_document,
        containment_profile=ContainmentProfile.PROCESS_BOUNDARY_ONLY,
    )
    projected = canonical_model_bytes(target)
    assert b'"_payload"' not in projected
    assert b'"request":{"payload":' in projected
    assert b'"schema":"dr_exec.test_request"' in projected
    assert b'"schema_version":1' in projected


def test_identity_document_fields_round_trip_through_the_wire_form(
    request_document: IdentityDocument,
) -> None:
    target = UntrustedPythonTarget(
        driver_source="def dr_exec_main(r, e): ...\n",
        request=request_document,
        containment_profile=ContainmentProfile.PROCESS_BOUNDARY_ONLY,
    )
    restored = UntrustedPythonTarget.model_validate_json(
        canonical_model_bytes(target), strict=True
    )
    assert restored.request == request_document
    assert restored == target
