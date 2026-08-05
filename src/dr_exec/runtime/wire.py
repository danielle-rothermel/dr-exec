from __future__ import annotations

from typing import Annotated, Literal

from dr_serialize import Sha256Digest
from pydantic import Field, NonNegativeInt

from dr_exec.core.kinds import ProtocolFrameKind
from dr_exec.core.model import ContractModel, IdentityDocumentField

# Child-observable wire literal: the frame boundary. The frame version is
# spelled by each frame model's `version` field, which is what the child
# writes and the parent validates; golden byte vectors pin both.
FRAME_TERMINATOR = b"\n"


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


__all__ = [
    "FRAME_TERMINATOR",
    "ProtocolComplete",
    "ProtocolFrame",
    "ProtocolOutput",
    "ProtocolPrelude",
]
