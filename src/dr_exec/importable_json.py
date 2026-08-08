from __future__ import annotations

import keyword
from functools import cache
from typing import Final

from dr_serialize import (
    IdentityDocument,
    Jsonable,
    build_identity_document,
    validate_strict_json,
)
from pydantic import field_validator

from dr_exec.core.kinds import ContainmentProfile
from dr_exec.core.model import ContractModel
from dr_exec.core.names import JobId
from dr_exec.declarations.models import (
    Budgets,
    EnvGrant,
    ExecutionJob,
    TrustedPythonTarget,
    UntrustedPythonTarget,
)
from dr_exec.recording.models import CompletedExecution, ExitedOutcome

# Persisted-format contract: these fixed envelope literals are shared by the
# request and result positions and are pinned by golden tests.
_ENVELOPE_SCHEMA: Final = "dr_exec.importable_json"
_ENVELOPE_SCHEMA_VERSION: Final = 1


class ImportableEntryPoint(ContractModel):
    """One absolute module and one exact module-level attribute."""

    module_name: str
    attribute_name: str

    @field_validator("module_name")
    @classmethod
    def module_name_must_be_absolute(cls, value: str) -> str:
        parts = value.split(".")
        if not parts or any(
            not part.isidentifier() or keyword.iskeyword(part)
            for part in parts
        ):
            raise ValueError(
                "module_name must be an absolute dotted Python module name"
            )
        return value

    @field_validator("attribute_name")
    @classmethod
    def attribute_name_must_be_exact(cls, value: str) -> str:
        if not value.isidentifier() or keyword.iskeyword(value):
            raise ValueError("attribute_name must be one Python identifier")
        return value


class ImportableJsonResultError(ValueError):
    """A completion is not one successful importable-JSON result."""


def build_trusted_importable_json_job(
    job_id: JobId,
    entry_point: ImportableEntryPoint,
    request: Jsonable,
    /,
    *,
    env: EnvGrant,
    budgets: Budgets | None = None,
) -> ExecutionJob:
    """Build one trusted importable JSON process job without spawning it."""

    return ExecutionJob(
        job_id=job_id,
        target=TrustedPythonTarget(
            driver_source=_driver_source(entry_point),
            request=_envelope(request),
        ),
        env=env,
        budgets=Budgets.unbudgeted() if budgets is None else budgets,
    )


def build_untrusted_importable_json_job(
    job_id: JobId,
    entry_point: ImportableEntryPoint,
    request: Jsonable,
    /,
    *,
    env: EnvGrant,
    budgets: Budgets | None = None,
) -> ExecutionJob:
    """Build one process-boundary-only untrusted importable JSON job."""

    return ExecutionJob(
        job_id=job_id,
        target=UntrustedPythonTarget(
            driver_source=_driver_source(entry_point),
            request=_envelope(request),
            containment_profile=ContainmentProfile.PROCESS_BOUNDARY_ONLY,
        ),
        env=env,
        budgets=Budgets.unbudgeted() if budgets is None else budgets,
    )


def parse_importable_json_result(completed: CompletedExecution, /) -> Jsonable:
    """Return the one matching JSON result or reject the completion."""

    outcome = completed.result.outcome
    if not isinstance(outcome, ExitedOutcome) or outcome.exit_code != 0:
        raise ImportableJsonResultError(
            "the importable JSON execution did not exit cleanly with code zero"
        )
    outputs = completed.result.protocol_outputs
    if len(outputs) != 1:
        raise ImportableJsonResultError(
            "the importable JSON execution did not produce exactly one output"
        )
    output = outputs[0]
    if (
        output.schema != _ENVELOPE_SCHEMA
        or output.schema_version != _ENVELOPE_SCHEMA_VERSION
    ):
        raise ImportableJsonResultError(
            "the importable JSON output does not use the required envelope"
        )
    return validate_strict_json(output.payload)


def _envelope(payload: Jsonable, /) -> IdentityDocument:
    validated = validate_strict_json(payload)
    return build_identity_document(
        schema=_ENVELOPE_SCHEMA,
        schema_version=_ENVELOPE_SCHEMA_VERSION,
        payload=validated,
    )


@cache
def _driver_source(entry_point: ImportableEntryPoint, /) -> str:
    # Persisted-format contract: generated bindings and source text contribute
    # to canonical target identity and are pinned verbatim by golden tests.
    return f"""import importlib as _dr_exec_importlib

_DR_EXEC_MODULE_NAME = {entry_point.module_name!r}
_DR_EXEC_ATTRIBUTE_NAME = {entry_point.attribute_name!r}
_DR_EXEC_ENVELOPE_SCHEMA = {_ENVELOPE_SCHEMA!r}
_DR_EXEC_ENVELOPE_SCHEMA_VERSION = {_ENVELOPE_SCHEMA_VERSION}


def dr_exec_main(request, emit):
    if (
        request["schema"] != _DR_EXEC_ENVELOPE_SCHEMA
        or request["schema_version"] != _DR_EXEC_ENVELOPE_SCHEMA_VERSION
    ):
        raise ValueError("request does not use the importable JSON envelope")
    module = _dr_exec_importlib.import_module(_DR_EXEC_MODULE_NAME)
    entry_point = getattr(module, _DR_EXEC_ATTRIBUTE_NAME)
    if not callable(entry_point):
        raise TypeError("the imported module attribute is not callable")
    result = entry_point(request["payload"])
    emit({{
        "schema": _DR_EXEC_ENVELOPE_SCHEMA,
        "schema_version": _DR_EXEC_ENVELOPE_SCHEMA_VERSION,
        "payload": result,
    }})
"""


__all__ = [
    "ImportableEntryPoint",
    "ImportableJsonResultError",
    "build_trusted_importable_json_job",
    "build_untrusted_importable_json_job",
    "parse_importable_json_result",
]
