from __future__ import annotations

from collections.abc import Iterable, Mapping
from pathlib import PurePosixPath
from typing import Annotated, Literal, cast
from uuid import UUID

from dr_serialize import (
    IdentityDocument,
    Jsonable,
    Sha256Digest,
    build_identity_document,
    canonical_sorted_values,
    json_hash,
)
from pydantic import StringConstraints, field_validator, model_validator

from dr_exec._model import ContractModel
from dr_exec._provenance import ExecutorSourceSnapshot
from dr_exec.declare import (
    ByteBudget,
    CountBudget,
    DurationBudget,
    EnvGrant,
    EnvGrantRecord,
    ExecutionTarget,
    ExecutorSelfBudgets,
)

# Persisted identity schema names and version. These are wire literals
# pinned by golden vectors; never derive them from module or class names.
EXECUTOR_IDENTITY_SCHEMA = "dr_exec.executor"
EXECUTOR_CONFIG_IDENTITY_SCHEMA = "dr_exec.executor_config"
ISOLATED_HOST_RUNTIME_IDENTITY_SCHEMA = "dr_exec.isolated_host_python_runtime"
IDENTITY_SCHEMA_VERSION = 1

_EXECUTOR_CONFIG_IDENTITY_KEYS = frozenset(
    {
        "protocol_frame_bytes",
        "protocol_total_bytes",
        "protocol_output_count",
        "json_depth",
        "manifest_bytes",
        "narration_bytes",
        "recording_failure_count",
        "failure_detail_bytes",
        "startup_time",
        "termination_time",
        "join_time",
    }
)
_EXECUTOR_IDENTITY_KEYS = frozenset(
    {
        "kind",
        "package_version",
        "source_commit",
        "source_state",
        "session_id",
    }
)
_ISOLATED_HOST_RUNTIME_IDENTITY_KEYS = frozenset(
    {
        "kind",
        "resolved_executable",
        "implementation",
        "python_version",
        "cache_tag",
        "platform",
    }
)
_LOWERCASE_HEXADECIMAL = frozenset("0123456789abcdef")

type _NonemptyString = Annotated[str, StringConstraints(min_length=1)]


def _validate_git_object_id(value: str | None) -> str | None:
    if value is not None and (
        len(value) not in {40, 64}
        or any(character not in _LOWERCASE_HEXADECIMAL for character in value)
    ):
        raise ValueError(
            "source_commit must be a complete lowercase Git object ID"
        )
    return value


def _validate_canonical_uuid(value: str | None) -> str | None:
    if value is None:
        return None
    try:
        parsed = UUID(value)
    except (AttributeError, ValueError) as error:
        raise ValueError("session_id must be a canonical UUID") from error
    if str(parsed) != value:
        raise ValueError("session_id must be a canonical UUID")
    return value


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


class _ExecutorIdentityPayload(ContractModel):
    kind: Literal["process_executor"] = "process_executor"
    package_version: _NonemptyString
    source_commit: str | None
    source_state: Literal["clean", "dirty", "unknown"]
    session_id: str | None

    _validated_source_commit = field_validator("source_commit")(
        _validate_git_object_id
    )
    _validated_session_id = field_validator("session_id")(
        _validate_canonical_uuid
    )

    @model_validator(mode="after")
    def provenance_must_be_complete(self) -> _ExecutorIdentityPayload:
        if self.source_state == "clean":
            if self.source_commit is None:
                raise ValueError("clean source requires source_commit")
            if self.session_id is not None:
                raise ValueError("clean source must not have session_id")
        elif self.session_id is None:
            raise ValueError("dirty or unknown source requires session_id")
        return self


class _ExecutorConfigIdentityPayload(ContractModel):
    protocol_frame_bytes: ByteBudget
    protocol_total_bytes: ByteBudget
    protocol_output_count: CountBudget
    json_depth: CountBudget
    manifest_bytes: ByteBudget
    narration_bytes: ByteBudget
    recording_failure_count: CountBudget
    failure_detail_bytes: ByteBudget
    startup_time: DurationBudget
    termination_time: DurationBudget
    join_time: DurationBudget


class _IsolatedHostRuntimeIdentityPayload(ContractModel):
    kind: Literal["isolated_host_python"] = "isolated_host_python"
    resolved_executable: str
    implementation: _NonemptyString
    python_version: _NonemptyString
    cache_tag: _NonemptyString
    platform: _NonemptyString

    _validated_resolved_executable = field_validator("resolved_executable")(
        _validate_normalized_absolute_posix_path
    )


def _require_identity_role(
    document: IdentityDocument,
    *,
    schema: str,
    payload_keys: frozenset[str],
) -> Mapping[str, object]:
    if document.schema != schema:
        raise ValueError(f"identity must use schema {schema}")
    if document.schema_version != IDENTITY_SCHEMA_VERSION:
        raise ValueError(
            f"identity must use schema version {IDENTITY_SCHEMA_VERSION}"
        )
    if not isinstance(document.payload, Mapping):
        raise ValueError(  # noqa: TRY004 - Pydantic validation error
            "identity payload must be a mapping"
        )
    if set(document.payload) != payload_keys:
        raise ValueError("identity payload has the wrong keys")
    return cast("Mapping[str, object]", document.payload)


def _validate_executor_identity(
    document: IdentityDocument,
) -> IdentityDocument:
    payload = _require_identity_role(
        document,
        schema=EXECUTOR_IDENTITY_SCHEMA,
        payload_keys=_EXECUTOR_IDENTITY_KEYS,
    )
    _ExecutorIdentityPayload.model_validate(payload)
    return document


def _validate_executor_config_identity(
    document: IdentityDocument,
) -> IdentityDocument:
    payload = _require_identity_role(
        document,
        schema=EXECUTOR_CONFIG_IDENTITY_SCHEMA,
        payload_keys=_EXECUTOR_CONFIG_IDENTITY_KEYS,
    )
    _ExecutorConfigIdentityPayload.model_validate(payload)
    ExecutorSelfBudgets.model_validate(payload)
    return document


def _isolated_host_runtime_identity_payload(
    document: IdentityDocument,
) -> _IsolatedHostRuntimeIdentityPayload:
    payload = _require_identity_role(
        document,
        schema=ISOLATED_HOST_RUNTIME_IDENTITY_SCHEMA,
        payload_keys=_ISOLATED_HOST_RUNTIME_IDENTITY_KEYS,
    )
    return _IsolatedHostRuntimeIdentityPayload.model_validate(payload)


def _validate_isolated_host_runtime_identity(
    document: IdentityDocument,
) -> IdentityDocument:
    _isolated_host_runtime_identity_payload(document)
    return document


def _identity_payload(model: ContractModel, /) -> Jsonable:
    """Project a validated payload model into its identity payload."""
    return cast("Jsonable", model.model_dump(mode="json"))


def _build_executor_identity(
    snapshot: ExecutorSourceSnapshot,
    /,
) -> IdentityDocument:
    """Build the executor identity from one executor source snapshot."""
    payload = _ExecutorIdentityPayload(
        package_version=snapshot.package_version,
        source_commit=snapshot.source_commit,
        source_state=snapshot.source_state,
        session_id=snapshot.session_id,
    )
    return build_identity_document(
        schema=EXECUTOR_IDENTITY_SCHEMA,
        schema_version=IDENTITY_SCHEMA_VERSION,
        payload=_identity_payload(payload),
    )


def _build_executor_config_identity(
    self_budgets: ExecutorSelfBudgets,
    /,
) -> IdentityDocument:
    """Build the executor-config identity from effective self-budgets."""
    return build_identity_document(
        schema=EXECUTOR_CONFIG_IDENTITY_SCHEMA,
        schema_version=IDENTITY_SCHEMA_VERSION,
        payload=_identity_payload(self_budgets),
    )


def _build_isolated_host_runtime_identity(
    payload: _IsolatedHostRuntimeIdentityPayload,
    /,
) -> IdentityDocument:
    return build_identity_document(
        schema=ISOLATED_HOST_RUNTIME_IDENTITY_SCHEMA,
        schema_version=IDENTITY_SCHEMA_VERSION,
        payload=_identity_payload(payload),
    )


def _canonical_declaration_digest(target: ExecutionTarget, /) -> Sha256Digest:
    """Digest the canonical bytes of a complete target declaration.

    The declaration itself carries argv, source, stdin, and the request
    payload, so only this digest reaches durable evidence.
    """
    return json_hash(_identity_payload(target))


def _canonical_env_values_digest(grant: EnvGrant, /) -> Sha256Digest:
    """Digest the canonical name/value payload of an environment grant.

    Values feed only this digest; they are never persisted or returned.
    """
    payload: Jsonable = {
        variable.name: variable.value for variable in grant.variables
    }
    return json_hash(payload)


def _canonical_sorted_names(names: Iterable[str], /) -> tuple[str, ...]:
    """Order variable names by canonical JSON text, never locally.

    ``canonical_sorted_values`` is policy-free and returns its inputs
    untouched, so ``str`` members stay ``str``; only its ``Jsonable``
    signature is widened, and the cast narrows it back.
    """
    return tuple(cast("list[str]", canonical_sorted_values(names)))


def _build_env_grant_record(grant: EnvGrant, /) -> EnvGrantRecord:
    """Project a live environment grant into secret-free durable evidence."""
    return EnvGrantRecord(
        kind=grant.kind,
        var_names=_canonical_sorted_names(
            variable.name for variable in grant.variables
        ),
        excluded_var_names=_canonical_sorted_names(grant.excluded_var_names),
        canonical_values_sha256=_canonical_env_values_digest(grant),
    )
