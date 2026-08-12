from __future__ import annotations

from dr_serialize import IdentityDocument

from dr_exec import (
    ContainmentProfile,
    EnvGrant,
    TrustedPythonTarget,
    UntrustedPythonTarget,
)
from dr_exec.core.model import canonical_model_bytes
from dr_exec.recording.identity import build_env_grant_record

SECRET_ENV_VALUE = "hunter2-env-secret"


def test_environment_records_never_carry_values() -> None:
    grant = EnvGrant.fixed({"TOKEN": SECRET_ENV_VALUE, "LANG": "C.UTF-8"})
    projected = canonical_model_bytes(build_env_grant_record(grant))
    assert SECRET_ENV_VALUE.encode() not in projected
    assert b"C.UTF-8" not in projected
    assert b'"var_names":["LANG","TOKEN"]' in projected


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


def test_trusted_identity_document_fields_round_trip_through_the_wire_form(
    request_document: IdentityDocument,
) -> None:
    target = TrustedPythonTarget(
        driver_source="def dr_exec_main(r, e): ...\n",
        request=request_document,
    )
    restored = TrustedPythonTarget.model_validate_json(
        canonical_model_bytes(target), strict=True
    )
    assert restored.request == request_document
    assert restored == target
