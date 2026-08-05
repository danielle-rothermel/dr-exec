"""Shared mechanics for role-specific identity documents."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Annotated, cast

from dr_serialize import IdentityDocument, Jsonable
from pydantic import StringConstraints

from dr_exec.core.model import ContractModel

IDENTITY_SCHEMA_VERSION = 1

type NonemptyString = Annotated[str, StringConstraints(min_length=1)]


def _require_identity_role(
    document: IdentityDocument,
    *,
    schema: str,
) -> Mapping[str, object]:
    """Require one identity document to carry the named role's schema."""
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
    return cast("Mapping[str, object]", document.payload)


def _identity_payload(model: ContractModel, /) -> Jsonable:
    """Project a validated payload model into its identity payload."""
    return cast("Jsonable", model.model_dump(mode="json"))
