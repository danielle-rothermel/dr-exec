from __future__ import annotations

import re
from base64 import b64decode, urlsafe_b64encode
from datetime import datetime, timedelta
from typing import Annotated, Any, Final
from uuid import UUID

from dr_serialize import (
    CANONICAL_JSON_MAX_CONTAINER_DEPTH,
    IdentityDocument,
    Jsonable,
    SerializationError,
    canonical_json_bytes,
    decode_strict_json_bytes,
    validate_identity_document,
)
from pydantic import (
    AfterValidator,
    AwareDatetime,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    PlainSerializer,
)


class ContractModel(BaseModel):
    """Strict immutable base for serialized boundary models."""

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="forbid",
        frozen=True,
        ser_json_bytes="base64",
        strict=True,
        val_json_bytes="base64",
    )


class NonCanonicalBytesError(ValueError):
    """Input bytes differ from the decoded value's canonical re-encoding."""


STRUCTURAL_DEPTH_CEILING: Final = CANONICAL_JSON_MAX_CONTAINER_DEPTH


def canonical_model_bytes(model: ContractModel, /) -> bytes:
    """Serialize a boundary model through the canonical JSON owner."""
    projection: Jsonable = model.model_dump(mode="json")
    return canonical_json_bytes(projection)


def require_canonical_json_bytes(
    data: bytes,
    /,
    *,
    max_bytes: int,
    max_depth: int,
) -> None:
    """Bound, strictly decode, and require canonical input bytes."""
    decoded = decode_strict_json_bytes(
        data,
        max_bytes=max_bytes,
        max_depth=max_depth,
    )
    if canonical_json_bytes(decoded) != data:
        raise NonCanonicalBytesError(
            "input bytes are not canonical JSON bytes"
        )


def require_utc(value: datetime) -> datetime:

    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("timestamp must be UTC")
    return value


def _serialize_utc(value: datetime) -> str:
    return (
        value.isoformat(timespec="microseconds").removesuffix("+00:00") + "Z"
    )


_UTC_TIMESTAMP_PATTERN = re.compile(
    r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z"
)


def _require_pinned_utc_spelling(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    if not _UTC_TIMESTAMP_PATTERN.fullmatch(value):
        raise ValueError(
            "timestamp must be spelled YYYY-MM-DDTHH:MM:SS.ffffffZ"
        )
    return datetime.fromisoformat(value.removesuffix("Z") + "+00:00")


UtcDatetime = Annotated[
    AwareDatetime,
    BeforeValidator(_require_pinned_utc_spelling),
    AfterValidator(require_utc),
    PlainSerializer(_serialize_utc, return_type=str, when_used="json"),
]


_UUID_PATTERN = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
)


def _require_pinned_uuid_spelling(value: Any) -> Any:
    if isinstance(value, str) and not _UUID_PATTERN.fullmatch(value):
        raise ValueError(
            "UUID must be lowercase hyphenated 8-4-4-4-12 hexadecimal"
        )
    return value


CanonicalUuid = Annotated[
    UUID,
    BeforeValidator(_require_pinned_uuid_spelling),
]


_BASE64_MESSAGE = "bytes must be canonical padded URL-safe base64"
_URL_SAFE_ALTCHARS = b"-_"


def _require_pinned_base64_spelling(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    try:
        decoded = b64decode(
            value.encode("ascii"),
            altchars=_URL_SAFE_ALTCHARS,
            validate=True,
        )
    except ValueError as error:
        raise ValueError(_BASE64_MESSAGE) from error
    if urlsafe_b64encode(decoded).decode("ascii") != value:
        raise ValueError(_BASE64_MESSAGE)
    return decoded


Base64UrlBytes = Annotated[
    bytes,
    BeforeValidator(_require_pinned_base64_spelling),
]


def _validate_identity_document(value: Any) -> IdentityDocument:
    if isinstance(value, IdentityDocument):
        return value
    try:
        return validate_identity_document(value)
    except SerializationError as error:
        raise ValueError(f"invalid identity document: {error}") from error


def _serialize_identity_document(value: IdentityDocument) -> dict[str, Any]:
    return value.to_json_dict()


IdentityDocumentField = Annotated[
    IdentityDocument,
    BeforeValidator(_validate_identity_document),
    PlainSerializer(
        _serialize_identity_document,
        return_type=dict,
        when_used="json",
    ),
]
