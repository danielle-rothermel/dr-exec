from __future__ import annotations

from typing import Annotated, Literal

from pydantic import Field, NonNegativeInt, field_validator

from dr_exec._model import (
    ContractModel,
    IdentityDocumentField,
    _validate_sha256_digest,
)
from dr_exec.kinds import ProtocolFrameKind


class ProtocolPrelude(ContractModel):
    version: Literal[1] = 1
    kind: Literal[ProtocolFrameKind.PRELUDE] = ProtocolFrameKind.PRELUDE
    request_id_sha256: str

    _validated_request_id_sha256 = field_validator("request_id_sha256")(
        _validate_sha256_digest
    )


class ProtocolOutput(ContractModel):
    version: Literal[1] = 1
    kind: Literal[ProtocolFrameKind.OUTPUT] = ProtocolFrameKind.OUTPUT
    sequence: NonNegativeInt
    document: IdentityDocumentField


class ProtocolComplete(ContractModel):
    version: Literal[1] = 1
    kind: Literal[ProtocolFrameKind.COMPLETE] = ProtocolFrameKind.COMPLETE
    output_count: NonNegativeInt


type ProtocolFrame = Annotated[
    ProtocolPrelude | ProtocolOutput | ProtocolComplete,
    Field(discriminator="kind"),
]
