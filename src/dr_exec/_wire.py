from __future__ import annotations

from typing import Annotated, Literal

from dr_serialize import Sha256Digest
from pydantic import Field, NonNegativeInt

from dr_exec._model import ContractModel, IdentityDocumentField
from dr_exec.kinds import ProtocolFrameKind


class ProtocolPrelude(ContractModel):
    version: Literal[1] = 1
    kind: Literal[ProtocolFrameKind.PRELUDE] = ProtocolFrameKind.PRELUDE
    request_id_sha256: Sha256Digest


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
