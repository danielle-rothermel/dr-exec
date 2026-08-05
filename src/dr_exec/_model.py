from __future__ import annotations

import re
from base64 import b64decode, urlsafe_b64encode
from datetime import datetime, timedelta
from typing import Annotated, Any, Final
from uuid import UUID

from dr_serialize import (
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
    """Strict immutable base for dr-exec boundary models."""

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="forbid",
        frozen=True,
        ser_json_bytes="base64",
        strict=True,
        val_json_bytes="base64",
    )


class NonCanonicalBytesError(ValueError):
    """Decoded bytes are not their own canonical re-encoding."""


# The pinned structural ceiling on JSON nesting. This is not a budget and
# not a free choice: pydantic-core enforces a hard recursion guard of 254
# levels during validation, and the protocol frame and identity-document
# models wrap a payload in enough additional nesting that the deepest
# payload the real frame path validates is 198 levels (measured by binary
# search against the frame reader). 200 is therefore the feasible maximum;
# a larger ceiling would let the shared decoder accept frames that then
# fail Pydantic validation and misclassify depth overflow as
# malformed_frame instead of oversized_frame, splitting a persisted
# failure code. Raising this ceiling requires replacing Pydantic payload
# validation on the frame path, not editing this number. Bounding the
# shared decoder by it keeps depth overflow detected on the dr-serialize
# path and reported one way at every depth. Every read boundary with no
# tighter declared depth budget bounds itself here.
STRUCTURAL_DEPTH_CEILING: Final = 200


def canonical_model_bytes(model: ContractModel, /) -> bytes:
    """Project a boundary model and return its canonical JSON bytes.

    The explicit Pydantic JSON-mode projection is the secret-safe wire
    value; dr-serialize owns the final canonical bytes. Pydantic's own
    ``model_dump_json`` is never the persisted form.
    """
    projection: Jsonable = model.model_dump(mode="json")
    return canonical_json_bytes(projection)


def require_canonical_json_bytes(
    data: bytes,
    /,
    *,
    max_bytes: int,
    max_depth: int,
) -> None:
    """Bound, strictly decode, and require canonical input bytes.

    This is the shared front half of the validated read path: bounded
    strict decode followed by a canonical re-encode and a byte-for-byte
    equality check. Callers then validate the same original bytes with
    Pydantic; the decoded ``Jsonable`` never reaches it.

    Every read boundary routes through here so one pinned pipeline
    cannot drift into per-caller reimplementations. Callers own only the
    translation of the raised errors into their own taxonomy.
    """
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
    """Reject naive and non-UTC timestamps at a boundary."""

    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("timestamp must be UTC")
    return value


def _serialize_utc(value: datetime) -> str:
    return (
        value.isoformat(timespec="microseconds").removesuffix("+00:00") + "Z"
    )


# The one pinned timestamp spelling: UTC RFC 3339 with a trailing `Z` and
# exactly six fractional digits, which is exactly what `_serialize_utc`
# emits. Alternate RFC 3339 spellings the parser would otherwise accept
# -- no fraction, a shorter fraction, a `+00:00` offset -- would read back
# as bytes the write path never produced, and a seventh fractional digit
# would be silently truncated, so a loaded value would differ from the
# durable bytes it came from.
_UTC_TIMESTAMP_PATTERN = re.compile(
    r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z"
)


def _require_pinned_utc_spelling(value: Any) -> Any:
    """Parse the pinned spelling, and only it, into a real ``datetime``.

    A function validator makes the core schema behind it strict in the
    Python sense, so the string is converted here rather than handed on:
    that keeps this the only accepted textual spelling instead of leaving
    the parser's permissive RFC 3339 handling in place behind it.
    """
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


# The one pinned UUID spelling: 36 lowercase hexadecimal characters,
# hyphenated 8-4-4-4-12. Python's `UUID` also accepts uppercase,
# unhyphenated, URN, and braced forms and silently normalizes them, so
# distinct durable documents would otherwise read back as one identity.
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


# The one pinned JSON-bytes spelling: padded RFC 4648 URL-safe base64,
# which is what `ser_json_bytes="base64"` emits. Pydantic's validator also
# accepts the standard alphabet and unpadded input, so up to four distinct
# documents would otherwise decode to one value and re-digest differently
# than the bytes they came from.
_BASE64_MESSAGE = "bytes must be canonical padded URL-safe base64"
_URL_SAFE_ALTCHARS = b"-_"


def _require_pinned_base64_spelling(value: Any) -> Any:
    """Decode the pinned spelling, and only it, into real ``bytes``.

    As with the timestamp, the decode happens here so the permissive
    standard-alphabet and unpadded forms the parser would otherwise accept
    never reach a bytes field.

    Canonicity is decided by re-encoding rather than by an alphabet
    pattern: base64's final character carries unused bits that the encoder
    always emits as zero, so a pattern alone still admits spellings like
    ``AB==`` that decode to the same bytes as ``AA==``. Requiring the
    input to equal the encoder's own output for the decoded bytes leaves
    exactly one spelling per value.
    """
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
    """Validate one embedded identity document inside a boundary model.

    The shared validator raises ``SerializationError``, which Pydantic
    would let escape untranslated; re-raising as ``ValueError`` keeps an
    embedded identity failure inside the enclosing ``ValidationError``,
    where the protocol and record-load boundaries translate it into
    their own closed taxonomy.
    """
    if isinstance(value, IdentityDocument):
        return value
    try:
        return validate_identity_document(value)
    except SerializationError as error:
        raise ValueError(f"invalid identity document: {error}") from error


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
