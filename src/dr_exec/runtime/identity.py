from __future__ import annotations

from pathlib import PurePosixPath
from typing import Literal

from dr_serialize import IdentityDocument, build_identity_document
from pydantic import field_validator

from dr_exec.core.identity import (
    IDENTITY_SCHEMA_VERSION,
    NonemptyString,
    identity_payload,
    require_identity_role,
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


class IsolatedHostRuntimeIdentityPayload(ContractModel):
    kind: Literal["isolated_host_python"]
    resolved_executable: str
    implementation: NonemptyString
    python_version: NonemptyString
    cache_tag: NonemptyString
    platform: NonemptyString

    _validated_resolved_executable = field_validator("resolved_executable")(
        _validate_normalized_absolute_posix_path
    )


def isolated_host_runtime_identity_payload(
    document: IdentityDocument,
) -> IsolatedHostRuntimeIdentityPayload:
    payload = require_identity_role(
        document,
        schema=ISOLATED_HOST_RUNTIME_IDENTITY_SCHEMA,
    )
    return IsolatedHostRuntimeIdentityPayload.model_validate(payload)


def build_isolated_host_runtime_identity(
    payload: IsolatedHostRuntimeIdentityPayload,
    /,
) -> IdentityDocument:
    return build_identity_document(
        schema=ISOLATED_HOST_RUNTIME_IDENTITY_SCHEMA,
        schema_version=IDENTITY_SCHEMA_VERSION,
        payload=identity_payload(payload),
    )


__all__ = [
    "ISOLATED_HOST_RUNTIME_IDENTITY_SCHEMA",
    "IsolatedHostRuntimeIdentityPayload",
    "build_isolated_host_runtime_identity",
    "isolated_host_runtime_identity_payload",
]
