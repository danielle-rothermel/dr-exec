"""The protected protocol: frame codec and stream state machine.

The parent acquires LF-terminated frames from the read end of fd 3,
validates each against the closed frame models, and enforces the
prelude/output/completion state machine. Every failure is one closed
``ProtocolFailureCode``, and every previously accepted output survives a
later failure: the reader returns what it accepted alongside the failure
rather than discarding the stream.

Finite executor self-budgets bound acquisition. An unbudgeted axis gets no
executor-installed limit -- not a large one -- so an unbudgeted frame axis
scans to EOF. Depth is the one axis with a floor beneath the budget: the
pinned parsers stop recursing at ``STRUCTURAL_DEPTH_CEILING``, which is a
property of the machinery rather than executor policy, so dr-exec states
that number itself and enforces it uniformly instead of letting it leak
out of a parser as an inconsistently classified failure.
"""

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
    identity_document_hash,
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
from dr_exec.runtime.wire import (
    FRAME_TERMINATOR,
    ProtocolComplete,
    ProtocolFrame,
    ProtocolOutput,
    ProtocolPrelude,
)

_FRAME_ADAPTER: TypeAdapter[ProtocolFrame] = TypeAdapter(ProtocolFrame)

# One frame is read one byte at a time up to the LF boundary, so the read
# size is a transport detail, never a protocol limit: acquisition stops at
# the terminator or at a declared finite budget, never at this value.
_CHUNK_BYTES: Final = 65536


class ProtocolViolation(Exception):
    """One protected-protocol failure, carrying its closed failure code."""

    def __init__(self, code: ProtocolFailureCode, detail: str) -> None:
        super().__init__(detail)
        self.code = code
        self.detail = detail


@dataclass(frozen=True, slots=True)
class ProtocolFailure:
    """The closed classification of one protocol failure."""

    code: ProtocolFailureCode
    detail: str


@dataclass(frozen=True, slots=True)
class ProtocolStreamResult:
    """Everything one protected stream yielded, complete or failed.

    ``outputs`` holds every output frame accepted before ``failure``, in
    sequence order. A failed stream is never emptied: the domain owns
    result completeness, and dr-exec neither discards accepted outputs nor
    synthesizes missing ones.
    """

    outputs: tuple[IdentityDocument, ...]
    bytes_received: int
    failure: ProtocolFailure | None = None

    @property
    def completed(self) -> bool:
        return self.failure is None


def request_identity_digest(request: IdentityDocument, /) -> Sha256Digest:
    """Return the full digest of one request's canonical identity bytes.

    This is the digest the prelude must bind, computed over exactly the
    bytes the child received, so a prelude can only match a request the
    child actually read.
    """
    return identity_document_hash(request)


def encode_frame(frame: ProtocolFrame, /) -> bytes:
    """Render one frame as canonical UTF-8 JSON plus the terminator.

    Canonical JSON contains no raw line break, so the terminating LF is
    unambiguous and needs no escaping or length prefix.
    """
    projection: Jsonable = frame.model_dump(mode="json")
    return canonical_json_bytes(projection) + FRAME_TERMINATOR


def decode_frame(frame_bytes: bytes, /, *, max_depth: int) -> ProtocolFrame:
    """Validate one frame's bytes into its closed model.

    The shared read path bounds the decoder by the frame's own actual
    length and by ``max_depth`` and requires the canonical re-encode to
    reproduce the input exactly; Pydantic then validates those same
    original bytes in strict JSON mode rather than the decoded value.
    Only the mapping of its failures onto ``ProtocolFailureCode`` is
    owned here.
    """
    try:
        require_canonical_json_bytes(
            frame_bytes,
            max_bytes=len(frame_bytes),
            max_depth=max_depth,
        )
    except JsonDepthLimitError as error:
        # Depth overflow is an oversized frame rather than a malformed
        # one, whether the bound came from a finite budget or from the
        # structural ceiling. Bounding here at or below what the parser
        # behind this call can accept is what keeps that classification
        # single-valued.
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
    """Bound the decoder by the tighter of the budget and the ceiling.

    A declaration may legally spell a ``json_depth`` budget above the
    structural ceiling. Passing that budget through verbatim would let the
    parser behind the shared decoder be the component that rejects the
    frame, which reports a malformed frame -- so the same over-deep input
    would classify one way under a small budget and another way under a
    large one. Clamping keeps the depth classification single-valued
    whatever the declaration spells.
    """
    if max_depth is None:
        return STRUCTURAL_DEPTH_CEILING
    return min(max_depth, STRUCTURAL_DEPTH_CEILING)


@dataclass(slots=True)
class _FrameAcquisition:
    """Buffered LF-delimited acquisition over one protected read end.

    Scanning stops at the terminator or at a declared finite frame budget,
    so an oversized frame is refused without acquiring beyond the limit.
    """

    reader: IO[bytes]
    max_frame_bytes: int | None
    max_total_bytes: int | None
    _buffer: bytearray = field(default_factory=bytearray)
    _at_eof: bool = False
    bytes_received: int = 0

    def next_frame(self, *, after_completion: bool) -> bytes | None:
        """Return the next frame's bytes without its terminator, or None.

        ``None`` means a clean EOF on a frame boundary. Bytes present at
        EOF without a terminator are an incomplete stream -- unless the
        completion frame already arrived, in which case they are the more
        specific post-completion prohibition: once a stream has completed,
        every trailing byte is unexpected, whether or not it happens to
        be LF-terminated.
        """
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
        """Never acquire past a declared finite budget in one read.

        Both budgets cap how much this reader may pull in; with neither
        declared the read size is the plain transport chunk.
        """
        remaining = [_CHUNK_BYTES]
        if self.max_frame_bytes is not None:
            # One more byte than the budget allows makes the overflow
            # observable without the frame ever being acquired whole.
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
    """The prelude/output/completion state machine over accepted frames."""

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
        """Require that EOF arrived only after the completion frame."""
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
    """Read one complete protected stream and classify its outcome.

    Acquisition, decoding, and the state machine share one failure
    taxonomy. On failure the reader stops at the offending frame and
    returns the outputs accepted before it; it never truncates protocol
    bytes head/tail and never manufactures an output.
    """
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
