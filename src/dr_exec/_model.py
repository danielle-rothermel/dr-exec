from __future__ import annotations

from datetime import datetime, timedelta
from typing import Annotated, Any

from dr_serialize import IdentityDocument, validate_identity_document
from pydantic import (
    AfterValidator,
    AwareDatetime,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    PlainSerializer,
)

_LOWERCASE_HEXADECIMAL = frozenset("0123456789abcdef")


class ContractModel(BaseModel):
    """Strict immutable base for dr-exec boundary models."""

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="forbid",
        frozen=True,
        ser_json_bytes="base64",
        strict=True,
        val_json_bytes="base64",
    )


def _validate_sha256_digest(value: str) -> str:
    if len(value) != 64 or any(
        character not in _LOWERCASE_HEXADECIMAL for character in value
    ):
        raise ValueError(
            "SHA-256 digest must be exactly 64 lowercase hexadecimal characters"
        )
    return value


def require_utc(value: datetime) -> datetime:
    """Reject naive and non-UTC timestamps at a boundary."""

    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("timestamp must be UTC")
    return value


def _serialize_utc(value: datetime) -> str:
    return (
        value.isoformat(timespec="microseconds").removesuffix("+00:00") + "Z"
    )


UtcDatetime = Annotated[
    AwareDatetime,
    AfterValidator(require_utc),
    PlainSerializer(_serialize_utc, return_type=str, when_used="json"),
]


def _validate_identity_document(value: Any) -> IdentityDocument:
    if isinstance(value, IdentityDocument):
        return value
    return validate_identity_document(value)


def _serialize_identity_document(value: IdentityDocument) -> dict[str, Any]:
    return value.to_json_dict()


# IdentityDocument keeps its payload private, so Pydantic introspection
# alone would spell the wire key "_payload" and reject real wire JSON;
# this alias pins the {schema, schema_version, payload} wire form.
IdentityDocumentField = Annotated[
    IdentityDocument,
    BeforeValidator(_validate_identity_document),
    PlainSerializer(
        _serialize_identity_document,
        return_type=dict,
        when_used="json",
    ),
]
