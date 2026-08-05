"""Identity contract for the isolated host Python runtime."""

from __future__ import annotations

from pathlib import PurePosixPath
from typing import Literal

from dr_serialize import IdentityDocument, build_identity_document
from pydantic import field_validator

from dr_exec.core.identity import (
    IDENTITY_SCHEMA_VERSION,
    NonemptyString,
    _identity_payload,
    _require_identity_role,
)
from dr_exec.core.model import ContractModel

ISOLATED_HOST_RUNTIME_IDENTITY_SCHEMA = "dr_exec.isolated_host_python_runtime"


def _validate_normalized_absolute_posix_path(value: str) -> str:
    path = PurePosixPath(value)
    if (
        not path.is_absolute()
        or path.as_posix() != value
        or any(part in {".", ".."} for part in path.parts)
    ):
        raise ValueError(
            "resolved_executable must be a normalized absolute POSIX path"
        )
    return value


class _IsolatedHostRuntimeIdentityPayload(ContractModel):
    kind: Literal["isolated_host_python"]
    resolved_executable: str
    implementation: NonemptyString
    python_version: NonemptyString
    cache_tag: NonemptyString
    platform: NonemptyString

    _validated_resolved_executable = field_validator("resolved_executable")(
        _validate_normalized_absolute_posix_path
    )


def _isolated_host_runtime_identity_payload(
    document: IdentityDocument,
) -> _IsolatedHostRuntimeIdentityPayload:
    payload = _require_identity_role(
        document,
        schema=ISOLATED_HOST_RUNTIME_IDENTITY_SCHEMA,
    )
    return _IsolatedHostRuntimeIdentityPayload.model_validate(payload)


def _validate_isolated_host_runtime_identity(
    document: IdentityDocument,
) -> IdentityDocument:
    _isolated_host_runtime_identity_payload(document)
    return document


def _build_isolated_host_runtime_identity(
    payload: _IsolatedHostRuntimeIdentityPayload,
    /,
) -> IdentityDocument:
    return build_identity_document(
        schema=ISOLATED_HOST_RUNTIME_IDENTITY_SCHEMA,
        schema_version=IDENTITY_SCHEMA_VERSION,
        payload=_identity_payload(payload),
    )
