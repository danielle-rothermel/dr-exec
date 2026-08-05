from __future__ import annotations

from datetime import datetime, timedelta
from typing import Annotated, Any

from dr_serialize import (
    IdentityDocument,
    Jsonable,
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


def validate_canonical_model_bytes[ContractModelT: ContractModel](
    model_type: type[ContractModelT],
    data: bytes,
    /,
    *,
    max_bytes: int,
    max_depth: int,
) -> ContractModelT:
    """Validate bounded canonical bytes into one boundary model.

    The closed-model tail of the shared read path, for callers whose
    target is a single ``ContractModel`` rather than a discriminated
    union.
    """
    require_canonical_json_bytes(
        data, max_bytes=max_bytes, max_depth=max_depth
    )
    return model_type.model_validate_json(data, strict=True)


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
