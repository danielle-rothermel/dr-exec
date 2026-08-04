from __future__ import annotations

from datetime import datetime, timedelta
from typing import Annotated

from pydantic import (
    AfterValidator,
    AwareDatetime,
    BaseModel,
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
