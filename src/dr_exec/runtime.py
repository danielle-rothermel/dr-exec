from __future__ import annotations

import os
import stat
from dataclasses import dataclass, field
from pathlib import Path

from dr_serialize import IdentityDocument
from pydantic import model_validator

from dr_exec._bootstrap import (
    ISOLATED_INVOCATION_ARGUMENTS,
    driver_wrapper_source,
)
from dr_exec._identity import (
    _build_isolated_host_runtime_identity,
    _isolated_host_runtime_identity_payload,
    _IsolatedHostRuntimeIdentityPayload,
)
from dr_exec._model import ContractModel, IdentityDocumentField
from dr_exec._probe import probe_interpreter
from dr_exec.declare import UntrustedPythonTarget
from dr_exec.kinds import RuntimeKind


class RuntimeRecord(ContractModel):
    kind: RuntimeKind
    resolved_executable: Path
    id_doc: IdentityDocumentField

    @model_validator(mode="after")
    def fields_must_match_identity(self) -> RuntimeRecord:
        """Validate the runtime identity and bind the fields to it.

        Parsing the payload is the identity's own validation, and the
        payload's ``resolved_executable`` is already a normalized
        absolute POSIX path, so requiring the field to equal it is the
        single check that establishes both.
        """
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
    """One resolved host interpreter, probed once at construction.

    Runtime identity includes the resolved absolute executable path, so
    equal interpreter builds reached through different paths compare as
    distinct runtimes.
    """

    executable: Path
    # Fully derived from the resolved executable, so it carries no
    # comparison information and stays out of equality and hashing.
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
        target: UntrustedPythonTarget,
        /,
    ) -> PreparedPythonProcess:
        """Prepare the fixed isolated invocation for one Python target.

        The command is always ``<executable> -I -c <wrapper-source>``; the
        consumer's ``driver_source`` is embedded as data, never composed
        into argv or interpreted by a shell.
        """
        return PreparedPythonProcess(
            argv=(
                self.executable.as_posix(),
                *ISOLATED_INVOCATION_ARGUMENTS,
                driver_wrapper_source(target.driver_source),
            ),
            request=target.request,
            runtime_record=self._runtime_record,
        )

    def describe(self) -> RuntimeRecord:
        """Return the runtime record retained from construction."""
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
