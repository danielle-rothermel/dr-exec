from __future__ import annotations

from collections.abc import Iterable
from typing import Literal, cast
from uuid import UUID

from dr_serialize import (
    IdentityDocument,
    Jsonable,
    Sha256Digest,
    build_identity_document,
    canonical_sorted_values,
    json_hash,
)
from pydantic import field_validator, model_validator

from dr_exec.core.identity import (
    IDENTITY_SCHEMA_VERSION,
    NonemptyString,
    _identity_payload,
    _require_identity_role,
)
from dr_exec.core.model import ContractModel
from dr_exec.declarations.models import (
    ByteBudget,
    CountBudget,
    DurationBudget,
    EnvGrant,
    EnvGrantRecord,
    ExecutionTarget,
    ExecutorSelfBudgets,
)
from dr_exec.recording.provenance import ExecutorSourceSnapshot

# Persisted identity schema names and version. These are wire literals
# pinned by golden vectors; never derive them from module or class names.
EXECUTOR_IDENTITY_SCHEMA = "dr_exec.executor"
EXECUTOR_CONFIG_IDENTITY_SCHEMA = "dr_exec.executor_config"
EXECUTOR_IDENTITY_KIND = "process_executor"

_LOWERCASE_HEXADECIMAL = frozenset("0123456789abcdef")


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


class _ExecutorIdentityPayload(ContractModel):
    # ``kind`` carries no default: it is a persisted payload key, so an
    # identity document that omits it must fail validation rather than
    # acquire one here.
    kind: Literal["process_executor"]
    package_version: NonemptyString
    source_commit: str | None
    source_state: Literal["clean", "unknown"]
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
            raise ValueError("unknown source requires session_id")
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


def _validate_executor_identity(
    document: IdentityDocument,
) -> IdentityDocument:
    payload = _require_identity_role(
        document,
        schema=EXECUTOR_IDENTITY_SCHEMA,
    )
    _ExecutorIdentityPayload.model_validate(payload)
    return document


def _validate_executor_config_identity(
    document: IdentityDocument,
) -> IdentityDocument:
    payload = _require_identity_role(
        document,
        schema=EXECUTOR_CONFIG_IDENTITY_SCHEMA,
    )
    _ExecutorConfigIdentityPayload.model_validate(payload)
    return document


def _build_executor_identity(
    snapshot: ExecutorSourceSnapshot,
    /,
) -> IdentityDocument:
    """Build the executor identity from one executor source snapshot."""
    payload = _ExecutorIdentityPayload(
        kind=EXECUTOR_IDENTITY_KIND,
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
