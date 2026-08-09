from __future__ import annotations

from dataclasses import dataclass, field
from typing import IO, Final

from dr_serialize import (
    IdentityDocument,
    Jsonable,
    JsonDepthLimitError,
    Sha256Digest,
    StrictJsonDecodeError,
    canonical_json_bytes,
)
from pydantic import TypeAdapter, ValidationError

from dr_exec.core.kinds import ProtocolFailureCode
from dr_exec.core.model import (
    STRUCTURAL_DEPTH_CEILING,
    NonCanonicalBytesError,
    require_canonical_json_bytes,
)
from dr_exec.declarations.models import (
    ByteBudget,
    CountBudget,
    ExecutorSelfBudgets,
    FiniteByteLimit,
    FiniteCountLimit,
)
from dr_exec.declarations.transport import (
    request_transport_bytes,
    request_transport_digest,
)
from dr_exec.runtime.wire import (
    FRAME_TERMINATOR,
    ProtocolComplete,
    ProtocolFrame,
    ProtocolOutput,
    ProtocolPrelude,
)

_FRAME_ADAPTER: TypeAdapter[ProtocolFrame] = TypeAdapter(ProtocolFrame)

_CHUNK_BYTES: Final = 65536


class ProtocolViolation(Exception):
    """Protected-protocol failure carrying a closed failure code."""

    def __init__(self, code: ProtocolFailureCode, detail: str) -> None:
        super().__init__(detail)
        self.code = code
        self.detail = detail


@dataclass(frozen=True, slots=True)
class ProtocolFailure:
    """Closed classification of one protocol failure."""

    code: ProtocolFailureCode
    detail: str


@dataclass(frozen=True, slots=True)
class ProtocolStreamResult:
    """Accepted outputs plus the stream's terminal protocol status."""

    outputs: tuple[IdentityDocument, ...]
    bytes_received: int
    failure: ProtocolFailure | None = None

    @property
    def completed(self) -> bool:
        return self.failure is None


def request_identity_digest(request: IdentityDocument, /) -> Sha256Digest:
    return request_transport_digest(request_transport_bytes(request))


def encode_frame(frame: ProtocolFrame, /) -> bytes:
    projection: Jsonable = frame.model_dump(mode="json")
    return canonical_json_bytes(projection) + FRAME_TERMINATOR


def decode_frame(frame_bytes: bytes, /, *, max_depth: int) -> ProtocolFrame:
    """Validate canonical frame bytes into the closed wire model."""

    try:
        require_canonical_json_bytes(
            frame_bytes,
            max_bytes=len(frame_bytes),
            max_depth=max_depth,
        )
    except JsonDepthLimitError as error:
        # Depth overflow is always oversized, never malformed.
        raise ProtocolViolation(
            ProtocolFailureCode.OVERSIZED_FRAME,
            "frame exceeded its structural depth budget",
        ) from error
    except NonCanonicalBytesError as error:
        raise ProtocolViolation(
            ProtocolFailureCode.MALFORMED_FRAME,
            "frame bytes are not canonical JSON bytes",
        ) from error
    except StrictJsonDecodeError as error:
        raise ProtocolViolation(
            ProtocolFailureCode.MALFORMED_FRAME,
            f"frame is not strict JSON: {type(error).__name__}",
        ) from error
    try:
        frame = _FRAME_ADAPTER.validate_json(frame_bytes, strict=True)
    except ValidationError as error:
        raise ProtocolViolation(
            ProtocolFailureCode.MALFORMED_FRAME,
            "frame is not a valid protocol frame",
        ) from error
    if "version" not in frame.model_fields_set:
        raise ProtocolViolation(
            ProtocolFailureCode.MALFORMED_FRAME,
            "frame does not contain an explicit protocol version",
        )
    return frame


def _finite_bytes(budget: ByteBudget, /) -> int | None:
    return budget.max_bytes if isinstance(budget, FiniteByteLimit) else None


def _finite_count(budget: CountBudget, /) -> int | None:
    return budget.max_count if isinstance(budget, FiniteCountLimit) else None


def _effective_depth(max_depth: int | None, /) -> int:
    if max_depth is None:
        return STRUCTURAL_DEPTH_CEILING
    return min(max_depth, STRUCTURAL_DEPTH_CEILING)


@dataclass(slots=True)
class _FrameAcquisition:
    reader: IO[bytes]
    max_frame_bytes: int | None
    max_total_bytes: int | None
    _buffer: bytearray = field(default_factory=bytearray)
    _at_eof: bool = False
    bytes_received: int = 0

    def next_frame(self, *, after_completion: bool) -> bytes | None:
        while True:
            terminator = self._buffer.find(FRAME_TERMINATOR)
            if terminator >= 0:
                frame = bytes(self._buffer[:terminator])
                del self._buffer[: terminator + 1]
                self._check_frame_budget(len(frame))
                return frame
            self._check_frame_budget(len(self._buffer))
            if self._at_eof:
                if self._buffer:
                    raise ProtocolViolation(
                        ProtocolFailureCode.UNEXPECTED_FRAME
                        if after_completion
                        else ProtocolFailureCode.INCOMPLETE_STREAM,
                        "bytes arrived after the completion frame"
                        if after_completion
                        else "stream ended without a terminating LF",
                    )
                return None
            self._fill()

    def _fill(self) -> None:
        chunk = self.reader.read(self._read_size())
        if not chunk:
            self._at_eof = True
            return
        self.bytes_received += len(chunk)
        if (
            self.max_total_bytes is not None
            and self.bytes_received > self.max_total_bytes
        ):
            raise ProtocolViolation(
                ProtocolFailureCode.OVERSIZED_FRAME,
                "protocol stream exceeded its total byte budget",
            )
        self._buffer.extend(chunk)

    def _read_size(self) -> int:
        remaining = [_CHUNK_BYTES]
        if self.max_frame_bytes is not None:
            remaining.append(self.max_frame_bytes + 1 - len(self._buffer))
        if self.max_total_bytes is not None:
            remaining.append(
                self.max_total_bytes + 1 - self.bytes_received,
            )
        return max(1, min(remaining))

    def _check_frame_budget(self, length: int, /) -> None:
        if self.max_frame_bytes is not None and length > self.max_frame_bytes:
            raise ProtocolViolation(
                ProtocolFailureCode.OVERSIZED_FRAME,
                "protocol frame exceeded its byte budget",
            )


@dataclass(slots=True)
class _StreamState:
    request_id_sha256: Sha256Digest
    max_output_count: int | None
    outputs: list[IdentityDocument] = field(default_factory=list)
    _prelude_seen: bool = False
    _completed: bool = False

    @property
    def completed(self) -> bool:
        return self._completed

    def accept(self, frame: ProtocolFrame, /) -> None:
        match frame:
            case ProtocolPrelude():
                self._accept_prelude(frame)
            case ProtocolOutput():
                self._accept_output(frame)
            case ProtocolComplete():
                self._accept_complete(frame)

    def finish(self) -> None:
        if not self._completed:
            raise ProtocolViolation(
                ProtocolFailureCode.INCOMPLETE_STREAM,
                "stream ended before its completion frame",
            )

    def _accept_prelude(self, frame: ProtocolPrelude, /) -> None:
        if self._prelude_seen:
            raise ProtocolViolation(
                ProtocolFailureCode.UNEXPECTED_FRAME,
                "prelude must appear exactly once, first",
            )
        self._prelude_seen = True
        if frame.request_id_sha256 != self.request_id_sha256:
            raise ProtocolViolation(
                ProtocolFailureCode.ID_MISMATCH,
                "prelude does not bind this request identity",
            )

    def _accept_output(self, frame: ProtocolOutput, /) -> None:
        self._require_open("output")
        if frame.sequence < len(self.outputs):
            raise ProtocolViolation(
                ProtocolFailureCode.DUPLICATE_OUTPUT,
                f"output sequence {frame.sequence} was already accepted",
            )
        if frame.sequence != len(self.outputs):
            raise ProtocolViolation(
                ProtocolFailureCode.UNEXPECTED_FRAME,
                f"output sequence {frame.sequence} is not consecutive",
            )
        if (
            self.max_output_count is not None
            and len(self.outputs) + 1 > self.max_output_count
        ):
            raise ProtocolViolation(
                ProtocolFailureCode.OVERSIZED_FRAME,
                "protocol stream exceeded its output-count budget",
            )
        self.outputs.append(frame.document)

    def _accept_complete(self, frame: ProtocolComplete, /) -> None:
        self._require_open("completion")
        if frame.output_count != len(self.outputs):
            raise ProtocolViolation(
                ProtocolFailureCode.INCOMPLETE_STREAM,
                "completion count does not match the accepted outputs",
            )
        self._completed = True

    def _require_open(self, name: str, /) -> None:
        if not self._prelude_seen:
            raise ProtocolViolation(
                ProtocolFailureCode.UNEXPECTED_FRAME,
                f"{name} frame arrived before the prelude",
            )
        if self._completed:
            raise ProtocolViolation(
                ProtocolFailureCode.UNEXPECTED_FRAME,
                f"{name} frame arrived after completion",
            )


def read_protocol_stream(
    reader: IO[bytes],
    /,
    *,
    request_id_sha256: Sha256Digest,
    self_budgets: ExecutorSelfBudgets,
) -> ProtocolStreamResult:
    """Retain accepted outputs when a later frame fails."""

    acquisition = _FrameAcquisition(
        reader=reader,
        max_frame_bytes=_finite_bytes(self_budgets.protocol_frame_bytes),
        max_total_bytes=_finite_bytes(self_budgets.protocol_total_bytes),
    )
    state = _StreamState(
        request_id_sha256=request_id_sha256,
        max_output_count=_finite_count(self_budgets.protocol_output_count),
    )
    max_depth = _finite_count(self_budgets.json_depth)
    failure: ProtocolFailure | None = None
    try:
        while True:
            frame_bytes = acquisition.next_frame(
                after_completion=state.completed
            )
            if frame_bytes is None:
                state.finish()
                break
            if state.completed:
                raise ProtocolViolation(
                    ProtocolFailureCode.UNEXPECTED_FRAME,
                    "bytes arrived after the completion frame",
                )
            state.accept(
                decode_frame(
                    frame_bytes,
                    max_depth=_effective_depth(max_depth),
                )
            )
    except ProtocolViolation as violation:
        failure = ProtocolFailure(violation.code, violation.detail)
    return ProtocolStreamResult(
        outputs=tuple(state.outputs),
        bytes_received=acquisition.bytes_received,
        failure=failure,
    )


__all__ = [
    "ProtocolFailure",
    "ProtocolStreamResult",
    "ProtocolViolation",
    "decode_frame",
    "encode_frame",
    "read_protocol_stream",
    "request_identity_digest",
]
