from __future__ import annotations

import os
import stat
from dataclasses import dataclass, field
from pathlib import Path

from dr_serialize import Sha256Digest
from pydantic import model_validator

from dr_exec.core.kinds import RuntimeKind
from dr_exec.core.model import ContractModel, IdentityDocumentField
from dr_exec.declarations.models import (
    TrustedPythonTarget,
    UntrustedPythonTarget,
)
from dr_exec.declarations.transport import (
    request_transport_bytes,
    request_transport_digest,
)
from dr_exec.runtime.bootstrap import (
    ISOLATED_INVOCATION_ARGUMENTS,
    driver_wrapper_source,
)
from dr_exec.runtime.identity import (
    _build_isolated_host_runtime_identity,
    _isolated_host_runtime_identity_payload,
    _IsolatedHostRuntimeIdentityPayload,
)
from dr_exec.runtime.probe import probe_interpreter


class RuntimeRecord(ContractModel):
    kind: RuntimeKind
    resolved_executable: Path
    id_doc: IdentityDocumentField

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
    request_bytes: bytes
    request_id_sha256: Sha256Digest
    runtime_record: RuntimeRecord


@dataclass(frozen=True, slots=True)
class IsolatedHostPythonRuntime:
    """Resolved host Python invoked with ``-I``.

    The probe describes the host interpreter; it does not verify interpreter
    bytes, installed packages, or runtime closure.
    """

    executable: Path
    _runtime_record: RuntimeRecord = field(
        init=False, repr=False, compare=False
    )

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
        object.__setattr__(
            self,
            "_runtime_record",
            _describe_resolved_executable(resolved),
        )

    def prepare(
        self,
        target: TrustedPythonTarget | UntrustedPythonTarget,
        /,
    ) -> PreparedPythonProcess:
        request_bytes = request_transport_bytes(target.request)
        return PreparedPythonProcess(
            argv=(
                self.executable.as_posix(),
                *ISOLATED_INVOCATION_ARGUMENTS,
                driver_wrapper_source(target.driver_source),
            ),
            request_bytes=request_bytes,
            request_id_sha256=request_transport_digest(request_bytes),
            runtime_record=self._runtime_record,
        )

    def describe(self) -> RuntimeRecord:
        return self._runtime_record


def _describe_resolved_executable(executable: Path, /) -> RuntimeRecord:
    facts = probe_interpreter(executable)
    payload = _IsolatedHostRuntimeIdentityPayload(
        kind=RuntimeKind.ISOLATED_HOST_PYTHON.value,
        resolved_executable=executable.as_posix(),
        implementation=facts["implementation"],
        python_version=facts["python_version"],
        cache_tag=facts["cache_tag"],
        platform=facts["platform"],
    )
    return RuntimeRecord(
        kind=RuntimeKind(payload.kind),
        resolved_executable=executable,
        id_doc=_build_isolated_host_runtime_identity(payload),
    )


__all__ = [
    "IsolatedHostPythonRuntime",
    "PreparedPythonProcess",
    "RuntimeRecord",
]
