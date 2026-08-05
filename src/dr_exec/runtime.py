from __future__ import annotations

import os
import stat
from dataclasses import dataclass
from pathlib import Path

from dr_serialize import IdentityDocument
from pydantic import field_validator, model_validator

from dr_exec._identity import (
    _isolated_host_runtime_identity_payload,
    _validate_isolated_host_runtime_identity,
)
from dr_exec._model import ContractModel, IdentityDocumentField
from dr_exec.declare import UntrustedPythonTarget
from dr_exec.kinds import RuntimeKind


class RuntimeRecord(ContractModel):
    kind: RuntimeKind
    resolved_executable: Path
    id_doc: IdentityDocumentField

    _validated_identity = field_validator("id_doc")(
        _validate_isolated_host_runtime_identity
    )

    @field_validator("resolved_executable")
    @classmethod
    def executable_must_be_absolute(cls, executable: Path) -> Path:
        if not executable.is_absolute():
            raise ValueError("resolved_executable must be absolute")
        return executable

    @model_validator(mode="after")
    def fields_must_match_identity(self) -> RuntimeRecord:
        payload = _isolated_host_runtime_identity_payload(self.id_doc)
        if self.kind.value != payload.kind:
            raise ValueError("runtime kind does not match identity")
        if self.resolved_executable.as_posix() != payload.resolved_executable:
            raise ValueError("resolved executable does not match identity")
        return self


@dataclass(frozen=True, slots=True)
class PreparedPythonProcess:
    argv: tuple[str, ...]
    request: IdentityDocument
    runtime_record: RuntimeRecord


@dataclass(frozen=True, slots=True)
class IsolatedHostPythonRuntime:
    executable: Path

    def __post_init__(self) -> None:
        try:
            resolved = self.executable.resolve(strict=True)
            mode = resolved.stat().st_mode
        except (OSError, RuntimeError) as error:
            raise ValueError(
                f"unable to resolve runtime executable: {self.executable}"
            ) from error
        if not stat.S_ISREG(mode):
            raise ValueError(
                f"runtime executable must be a regular file: {resolved}"
            )
        if os.name == "posix" and not os.access(resolved, os.X_OK):
            raise ValueError(
                f"runtime executable is not executable: {resolved}"
            )
        object.__setattr__(self, "executable", resolved)

    def prepare(
        self,
        target: UntrustedPythonTarget,
        /,
    ) -> PreparedPythonProcess:
        raise NotImplementedError(
            "IsolatedHostPythonRuntime.prepare is not implemented"
        )

    def describe(self) -> RuntimeRecord:
        raise NotImplementedError(
            "IsolatedHostPythonRuntime.describe is not implemented"
        )


__all__ = [
    "IsolatedHostPythonRuntime",
    "PreparedPythonProcess",
    "RuntimeRecord",
]
