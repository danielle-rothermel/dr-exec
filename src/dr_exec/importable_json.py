from __future__ import annotations

import importlib
import traceback
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

# A payload's exception text is attacker-shaped data as far as this library is
# concerned: an entry point may raise with a message of any size, and the
# worker pool puts that text on a pipe the parent must read whole. The cap is
# what keeps one frame from growing without bound, so it is applied to the
# rendered detail as a whole rather than to any one of its parts.
PAYLOAD_ERROR_DETAIL_MAX_BYTES: Final = 8192
PAYLOAD_ERROR_DETAIL_TRUNCATION_MARKER: Final = "... [truncated]"
PAYLOAD_ERROR_TRACEBACK_FRAME_LIMIT: Final = 20
PAYLOAD_RAISED_DETAIL_PREFIX: Final = "the importable JSON entry point raised"


# The detail is rendered from payload-controlled objects: a payload chooses its
# exception type, its ``__str__``, and the frames its traceback walks, and any
# of those can raise. The formatter runs inside the handler that owns the
# payload failure, so an exception escaping it would replace a payload-owned
# outcome with a different one. These placeholders are what it substitutes
# instead, and they are part of the rendered shape.
PAYLOAD_ERROR_UNPRINTABLE_PLACEHOLDER: Final = "<unprintable>"
PAYLOAD_ERROR_TRACEBACK_OMITTED_MARKER: Final = "<traceback unavailable>"


def format_payload_error_detail(error: BaseException, /) -> str:
    """Render one payload exception as bounded, single-line-prefixed detail.

    The shape is ``<prefix>: <QualifiedType>: <message>\\n<traceback>``, capped
    at :data:`PAYLOAD_ERROR_DETAIL_MAX_BYTES` with an explicit marker. The
    worker-pool worker repeats this function rather than importing it, and a
    golden test pins the two copies equal.

    This function is total: every payload-controlled rendering step is guarded
    and substituted with a fixed placeholder on failure, so the diagnostic path
    can never raise and never changes which outcome the caller reports.
    """

    module_name, qualified_name = _safe_type_names(error)
    return _format_payload_error_detail(
        module_name,
        qualified_name,
        _safe_message(error),
        _safe_traceback_lines(error),
    )


def _safe_type_names(error: BaseException, /) -> tuple[str, str]:
    """Read one exception type's module and qualified name defensively."""

    try:
        error_type = type(error)
        module_name = error_type.__module__
        qualified_name = error_type.__qualname__
    except BaseException:  # noqa: BLE001 - the detail must never raise
        return (
            PAYLOAD_ERROR_UNPRINTABLE_PLACEHOLDER,
            PAYLOAD_ERROR_UNPRINTABLE_PLACEHOLDER,
        )
    if not isinstance(module_name, str) or not isinstance(qualified_name, str):
        return (
            PAYLOAD_ERROR_UNPRINTABLE_PLACEHOLDER,
            PAYLOAD_ERROR_UNPRINTABLE_PLACEHOLDER,
        )
    return module_name, qualified_name


def _safe_message(error: BaseException, /) -> str:
    """Stringify one exception, naming what failed instead of raising."""

    try:
        return str(error)
    except BaseException as failure:  # noqa: BLE001 - must never raise
        return _unprintable_message(error, failure)


def _unprintable_message(
    error: BaseException, failure: BaseException, /
) -> str:
    """Name the unrenderable exception and the one its rendering raised.

    Both names are read defensively, because the payload controls them too;
    if even that fails the placeholder degrades to a bare constant.
    """

    _, raised = _safe_type_names(error)
    _, by = _safe_type_names(failure)
    if PAYLOAD_ERROR_UNPRINTABLE_PLACEHOLDER in (raised, by):
        return PAYLOAD_ERROR_UNPRINTABLE_PLACEHOLDER
    return f"<unprintable {raised}: __str__ raised {by}>"


def _safe_traceback_lines(error: BaseException, /) -> list[str]:
    """Render a bounded traceback tail, or a marker when it cannot be read.

    Traceback rendering walks payload-controlled frames and can raise on
    exotic ones. That omits the traceback, never the whole detail.
    """

    try:
        return traceback.format_exception(
            error, limit=-PAYLOAD_ERROR_TRACEBACK_FRAME_LIMIT
        )
    except BaseException:  # noqa: BLE001 - the detail must never raise
        return [PAYLOAD_ERROR_TRACEBACK_OMITTED_MARKER + "\n"]


def _format_payload_error_detail(
    module_name: str,
    qualified_name: str,
    message: str,
    traceback_lines: list[str],
    /,
) -> str:
    qualified = (
        qualified_name
        if module_name == "builtins"
        else f"{module_name}.{qualified_name}"
    )
    headline = f"{PAYLOAD_RAISED_DETAIL_PREFIX}: {qualified}: {message}"
    detail = headline + "\n" + "".join(traceback_lines)
    return _truncate_utf8(detail)


def _truncate_utf8(text: str, /) -> str:
    """Cap ``text`` by encoded size, never splitting a UTF-8 character.

    Encoding is non-raising in both directions. A payload message may carry
    lone surrogates, or ``surrogateescape`` bytes from an OS-level error, which
    strict UTF-8 refuses to encode; those render as backslash escapes so that
    sizing and truncation stay defined for every input.
    """

    encoded = text.encode("utf-8", errors="backslashreplace")
    if len(encoded) <= PAYLOAD_ERROR_DETAIL_MAX_BYTES:
        return encoded.decode("utf-8", errors="replace")
    marker = PAYLOAD_ERROR_DETAIL_TRUNCATION_MARKER
    budget = PAYLOAD_ERROR_DETAIL_MAX_BYTES - len(marker.encode("utf-8"))
    kept = encoded[:budget].decode("utf-8", errors="ignore")
    return kept + marker


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
            format_payload_error_detail(error)
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
