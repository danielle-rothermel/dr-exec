from __future__ import annotations

from dr_exec.core.model import canonical_model_bytes
from dr_exec.runtime.wire import PROTOCOL_FRAME_TERMINATOR, ProtocolFrame


def encode_frame(frame: ProtocolFrame, /) -> bytes:
    """Encode one protocol frame the way a conforming child writes it."""

    return canonical_model_bytes(frame) + PROTOCOL_FRAME_TERMINATOR
