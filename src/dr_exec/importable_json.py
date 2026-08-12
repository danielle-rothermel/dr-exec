from __future__ import annotations

import importlib
from functools import cache
from typing import Final

from dr_serialize import (
    IdentityDocument,
    Jsonable,
    StrictJsonError,
    build_identity_document,
    validate_strict_json,
)

from dr_exec.core.kinds import ContainmentProfile
from dr_exec.core.names import JobId
from dr_exec.declarations.models import (
    Budgets,
    EnvGrant,
    ExecutionJob,
    ImportableEntryPoint,
    InProcessImportableJsonTarget,
    TrustedPythonTarget,
    UntrustedPythonTarget,
)
from dr_exec.recording.models import CompletedExecution, ExitedOutcome

# Persisted-format contract: these fixed envelope literals are shared by the
# request and result positions and are pinned by golden tests.
ENVELOPE_SCHEMA: Final = "dr_exec.importable_json"
ENVELOPE_SCHEMA_VERSION: Final = 1


def is_importable_json_envelope(document: IdentityDocument, /) -> bool:
    """Report whether a document carries the importable JSON envelope pair.

    Each parent-side caller rejects a foreign envelope in its own error
    vocabulary, so the shared step is the comparison, not the rejection.
    """

    return (
        document.schema == ENVELOPE_SCHEMA
        and document.schema_version == ENVELOPE_SCHEMA_VERSION
    )


class ImportableJsonResultError(ValueError):
    """A completion is not one successful importable-JSON result."""


class ImportableJsonDispatchError(Exception):
    """An importable JSON entry point could not run."""


class ImportableJsonExecutorDispatchError(ImportableJsonDispatchError):
    """The executor, not the entry point, rejected dispatch."""


class ImportableJsonPayloadDispatchError(ImportableJsonDispatchError):
    """The entry point raised while running."""


class ImportableJsonPayloadResultError(ImportableJsonDispatchError):
    """The entry point returned non-strict JSON."""


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


def build_in_process_importable_json_job(
    job_id: JobId,
    entry_point: ImportableEntryPoint,
    request: Jsonable,
    /,
    *,
    budgets: Budgets | None = None,
) -> ExecutionJob:
    """Build one trusted in-process importable JSON job without spawning."""

    return ExecutionJob(
        job_id=job_id,
        target=InProcessImportableJsonTarget(
            entry_point=entry_point,
            request=_envelope(request),
        ),
        env=EnvGrant.none(),
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
    if not is_importable_json_envelope(output):
        raise ImportableJsonResultError(
            "the importable JSON output does not use the required envelope"
        )
    return validate_strict_json(output.payload)


def _invoke_importable_entry_point(
    entry_point: ImportableEntryPoint,
    request: IdentityDocument,
    /,
) -> Jsonable:
    if not is_importable_json_envelope(request):
        raise ImportableJsonExecutorDispatchError(
            "request does not use the importable JSON envelope"
        )
    try:
        payload = validate_strict_json(request.payload)
    except StrictJsonError as error:
        raise ImportableJsonExecutorDispatchError(
            "request payload is not strict JSON"
        ) from error
    try:
        module = importlib.import_module(entry_point.module_name)
    except Exception as error:
        raise ImportableJsonExecutorDispatchError(
            f"could not import {entry_point.module_name!r}"
        ) from error
    try:
        callable_entry = getattr(module, entry_point.attribute_name)
    except AttributeError as error:
        raise ImportableJsonExecutorDispatchError(
            f"{entry_point.module_name!r} has no attribute "
            f"{entry_point.attribute_name!r}"
        ) from error
    if not callable(callable_entry):
        raise ImportableJsonExecutorDispatchError(
            "the imported module attribute is not callable"
        )
    try:
        result = callable_entry(payload)
    except Exception as error:
        raise ImportableJsonPayloadDispatchError(
            "the importable JSON entry point raised"
        ) from error
    try:
        return validate_strict_json(result)
    except StrictJsonError as error:
        raise ImportableJsonPayloadResultError(
            "the importable JSON entry point returned non-strict JSON"
        ) from error


def _envelope(payload: Jsonable, /) -> IdentityDocument:
    validated = validate_strict_json(payload)
    return build_identity_document(
        schema=ENVELOPE_SCHEMA,
        schema_version=ENVELOPE_SCHEMA_VERSION,
        payload=validated,
    )


@cache
def _driver_source(entry_point: ImportableEntryPoint, /) -> str:
    # Persisted-format contract: generated bindings and source text contribute
    # to canonical target identity and are pinned verbatim by golden tests.
    return f"""import importlib as _dr_exec_importlib

_DR_EXEC_MODULE_NAME = {entry_point.module_name!r}
_DR_EXEC_ATTRIBUTE_NAME = {entry_point.attribute_name!r}
_DR_EXEC_ENVELOPE_SCHEMA = {ENVELOPE_SCHEMA!r}
_DR_EXEC_ENVELOPE_SCHEMA_VERSION = {ENVELOPE_SCHEMA_VERSION}


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
    "ENVELOPE_SCHEMA",
    "ENVELOPE_SCHEMA_VERSION",
    "ImportableEntryPoint",
    "ImportableJsonDispatchError",
    "ImportableJsonExecutorDispatchError",
    "ImportableJsonPayloadDispatchError",
    "ImportableJsonPayloadResultError",
    "ImportableJsonResultError",
    "build_in_process_importable_json_job",
    "build_trusted_importable_json_job",
    "build_untrusted_importable_json_job",
    "is_importable_json_envelope",
    "parse_importable_json_result",
]
