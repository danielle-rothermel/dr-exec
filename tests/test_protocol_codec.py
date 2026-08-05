"""Protected-protocol golden vectors, taxonomy, and finite budget edges."""

from __future__ import annotations

import hashlib
import io

import pytest
from dr_serialize import (
    IdentityDocument,
    Sha256Digest,
    build_identity_document,
    canonical_identity_json_bytes,
)

from dr_exec import (
    ExecutorSelfBudgets,
    FiniteByteLimit,
    FiniteCountLimit,
    ProtocolFailureCode,
    UnbudgetedLimit,
)
from dr_exec._protocol import (
    ProtocolStreamResult,
    encode_frame,
    read_protocol_stream,
    request_identity_digest,
    request_transport_bytes,
)
from dr_exec._wire import (
    FRAME_TERMINATOR,
    PROTOCOL_VERSION,
    ProtocolComplete,
    ProtocolOutput,
    ProtocolPrelude,
)

OUTPUT_SCHEMA = "dr_exec.test_output"
OTHER_DIGEST = Sha256Digest("b" * 64)


def _output_document(index: int, /) -> IdentityDocument:
    return build_identity_document(
        schema=OUTPUT_SCHEMA,
        schema_version=1,
        payload={"index": index},
    )


def _read(
    stream: bytes,
    /,
    *,
    request_id_sha256: Sha256Digest,
    self_budgets: ExecutorSelfBudgets | None = None,
) -> ProtocolStreamResult:
    return read_protocol_stream(
        io.BytesIO(stream),
        request_id_sha256=request_id_sha256,
        self_budgets=self_budgets or ExecutorSelfBudgets.unbudgeted(),
    )


def _stream(digest: Sha256Digest, output_count: int, /) -> bytes:
    frames = [ProtocolPrelude(request_id_sha256=digest)]
    frames.extend(
        ProtocolOutput(sequence=index, document=_output_document(index))
        for index in range(output_count)
    )
    frames.append(ProtocolComplete(output_count=output_count))
    return b"".join(encode_frame(frame) for frame in frames)


# --- Golden wire vectors -------------------------------------------------


def test_the_prelude_frame_has_exactly_its_pinned_bytes() -> None:
    frame = ProtocolPrelude(request_id_sha256=Sha256Digest("a" * 64))
    assert encode_frame(frame) == (
        b'{"kind":"prelude","request_id_sha256":"' + b"a" * 64 + b'",'
        b'"version":1}\n'
    )


def test_the_output_frame_has_exactly_its_pinned_bytes() -> None:
    frame = ProtocolOutput(sequence=0, document=_output_document(7))
    assert encode_frame(frame) == (
        b'{"document":{"payload":{"index":7},'
        b'"schema":"dr_exec.test_output","schema_version":1},'
        b'"kind":"output","sequence":0,"version":1}\n'
    )


def test_the_completion_frame_has_exactly_its_pinned_bytes() -> None:
    assert encode_frame(ProtocolComplete(output_count=2)) == (
        b'{"kind":"complete","output_count":2,"version":1}\n'
    )


def test_the_pinned_frame_version_is_one() -> None:
    assert PROTOCOL_VERSION == 1
    assert ProtocolPrelude(request_id_sha256=OTHER_DIGEST).version == 1


def test_frames_carry_no_raw_line_break_before_the_terminator() -> None:
    document = build_identity_document(
        schema=OUTPUT_SCHEMA,
        schema_version=1,
        payload={"text": "line\nbreak\r\nand\ttab"},
    )
    encoded = encode_frame(ProtocolOutput(sequence=0, document=document))
    assert encoded.count(FRAME_TERMINATOR) == 1
    assert encoded.endswith(FRAME_TERMINATOR)
    assert b"\r" not in encoded


def test_non_ascii_output_payloads_survive_the_round_trip() -> None:
    document = build_identity_document(
        schema=OUTPUT_SCHEMA,
        schema_version=1,
        payload={"text": "é中\U0001f600"},
    )
    digest = Sha256Digest("c" * 64)
    stream = (
        encode_frame(ProtocolPrelude(request_id_sha256=digest))
        + encode_frame(ProtocolOutput(sequence=0, document=document))
        + encode_frame(ProtocolComplete(output_count=1))
    )
    assert stream.isascii()
    result = _read(stream, request_id_sha256=digest)
    assert result.completed
    assert result.outputs[0].payload == {"text": "é中\U0001f600"}


# --- Valid streams -------------------------------------------------------


@pytest.mark.parametrize("output_count", [0, 1, 5])
def test_a_well_formed_stream_yields_its_outputs_in_order(
    output_count: int,
) -> None:
    digest = Sha256Digest("d" * 64)
    stream = _stream(digest, output_count)
    result = _read(stream, request_id_sha256=digest)
    assert result.completed
    assert result.failure is None
    assert result.bytes_received == len(stream)
    assert [document.payload for document in result.outputs] == [
        {"index": index} for index in range(output_count)
    ]


# --- Failure taxonomy ----------------------------------------------------


def test_invalid_utf8_is_a_malformed_frame() -> None:
    digest = Sha256Digest("e" * 64)
    stream = b"\xff\xfe" + FRAME_TERMINATOR
    result = _read(stream, request_id_sha256=digest)
    assert result.failure is not None
    assert result.failure.code == ProtocolFailureCode.MALFORMED_FRAME


@pytest.mark.parametrize(
    "frame",
    [
        pytest.param(b"{", id="truncated-object"),
        pytest.param(b"", id="blank-line"),
        pytest.param(b"   ", id="whitespace-only"),
        pytest.param(b"\xef\xbb\xbf{}", id="byte-order-mark"),
        pytest.param(b'{"kind":"prelude"} ', id="trailing-space"),
        pytest.param(b' {"kind":"prelude"}', id="leading-space"),
        pytest.param(b'{"a":1,"a":2}', id="duplicate-keys"),
        pytest.param(b'{"a": 1}', id="non-canonical-spacing"),
        pytest.param(b'{"b":1,"a":2}', id="unsorted-keys"),
        pytest.param(b'{"a":1.0e1}', id="non-canonical-number"),
        pytest.param(b'{"kind":"unknown","version":1}', id="unknown-kind"),
        pytest.param(b'{"kind":"complete","version":2}', id="wrong-version"),
        pytest.param(
            b'{"extra":0,"kind":"complete","output_count":0,"version":1}',
            id="extra-field",
        ),
        pytest.param(
            b'{"kind":"complete","output_count":-1,"version":1}',
            id="negative-count",
        ),
        pytest.param(b"[]", id="array-not-object"),
    ],
)
def test_a_frame_that_is_not_a_closed_canonical_model_is_malformed(
    frame: bytes,
) -> None:
    digest = Sha256Digest("f" * 64)
    result = _read(
        frame + FRAME_TERMINATOR,
        request_id_sha256=digest,
    )
    assert result.failure is not None
    assert result.failure.code == ProtocolFailureCode.MALFORMED_FRAME
    assert result.outputs == ()


def test_a_crlf_terminator_leaves_a_non_canonical_frame() -> None:
    digest = Sha256Digest("a" * 64)
    stream = (
        encode_frame(ProtocolPrelude(request_id_sha256=digest)).removesuffix(
            FRAME_TERMINATOR
        )
        + b"\r\n"
    )
    result = _read(stream, request_id_sha256=digest)
    assert result.failure is not None
    assert result.failure.code == ProtocolFailureCode.MALFORMED_FRAME


@pytest.mark.parametrize(
    "frames",
    [
        pytest.param(
            (ProtocolComplete(output_count=0),),
            id="completion-without-prelude",
        ),
        pytest.param(
            (
                ProtocolOutput(sequence=0, document=_output_document(0)),
                ProtocolComplete(output_count=1),
            ),
            id="output-without-prelude",
        ),
    ],
)
def test_a_frame_before_the_prelude_is_unexpected(
    frames: tuple[ProtocolComplete | ProtocolOutput, ...],
) -> None:
    digest = Sha256Digest("a" * 64)
    stream = b"".join(encode_frame(frame) for frame in frames)
    result = _read(stream, request_id_sha256=digest)
    assert result.failure is not None
    assert result.failure.code == ProtocolFailureCode.UNEXPECTED_FRAME


def test_a_second_prelude_is_unexpected() -> None:
    digest = Sha256Digest("a" * 64)
    prelude = encode_frame(ProtocolPrelude(request_id_sha256=digest))
    result = _read(prelude + prelude, request_id_sha256=digest)
    assert result.failure is not None
    assert result.failure.code == ProtocolFailureCode.UNEXPECTED_FRAME


def test_a_frame_after_completion_is_unexpected() -> None:
    digest = Sha256Digest("a" * 64)
    stream = _stream(digest, 1) + encode_frame(
        ProtocolOutput(sequence=1, document=_output_document(1))
    )
    result = _read(stream, request_id_sha256=digest)
    assert result.failure is not None
    assert result.failure.code == ProtocolFailureCode.UNEXPECTED_FRAME
    assert len(result.outputs) == 1


def test_a_second_completion_is_unexpected() -> None:
    digest = Sha256Digest("a" * 64)
    stream = _stream(digest, 0) + encode_frame(
        ProtocolComplete(output_count=0)
    )
    result = _read(stream, request_id_sha256=digest)
    assert result.failure is not None
    assert result.failure.code == ProtocolFailureCode.UNEXPECTED_FRAME


def test_a_prelude_binding_another_request_is_an_identity_mismatch() -> None:
    digest = Sha256Digest("a" * 64)
    result = _read(_stream(OTHER_DIGEST, 1), request_id_sha256=digest)
    assert result.failure is not None
    assert result.failure.code == ProtocolFailureCode.ID_MISMATCH
    assert result.outputs == ()


def test_a_repeated_output_sequence_is_a_duplicate() -> None:
    digest = Sha256Digest("a" * 64)
    stream = (
        encode_frame(ProtocolPrelude(request_id_sha256=digest))
        + encode_frame(
            ProtocolOutput(sequence=0, document=_output_document(0))
        )
        + encode_frame(
            ProtocolOutput(sequence=0, document=_output_document(1))
        )
    )
    result = _read(stream, request_id_sha256=digest)
    assert result.failure is not None
    assert result.failure.code == ProtocolFailureCode.DUPLICATE_OUTPUT
    assert [document.payload for document in result.outputs] == [{"index": 0}]


def test_a_skipped_output_sequence_is_unexpected() -> None:
    digest = Sha256Digest("a" * 64)
    stream = encode_frame(
        ProtocolPrelude(request_id_sha256=digest)
    ) + encode_frame(ProtocolOutput(sequence=1, document=_output_document(1)))
    result = _read(stream, request_id_sha256=digest)
    assert result.failure is not None
    assert result.failure.code == ProtocolFailureCode.UNEXPECTED_FRAME


def test_reordered_outputs_are_unexpected() -> None:
    digest = Sha256Digest("a" * 64)
    stream = (
        encode_frame(ProtocolPrelude(request_id_sha256=digest))
        + encode_frame(
            ProtocolOutput(sequence=1, document=_output_document(1))
        )
        + encode_frame(
            ProtocolOutput(sequence=0, document=_output_document(0))
        )
    )
    result = _read(stream, request_id_sha256=digest)
    assert result.failure is not None
    assert result.failure.code == ProtocolFailureCode.UNEXPECTED_FRAME


def test_eof_before_completion_is_an_incomplete_stream() -> None:
    digest = Sha256Digest("a" * 64)
    stream = encode_frame(
        ProtocolPrelude(request_id_sha256=digest)
    ) + encode_frame(ProtocolOutput(sequence=0, document=_output_document(0)))
    result = _read(stream, request_id_sha256=digest)
    assert result.failure is not None
    assert result.failure.code == ProtocolFailureCode.INCOMPLETE_STREAM
    assert len(result.outputs) == 1


def test_an_empty_stream_is_an_incomplete_stream() -> None:
    result = _read(b"", request_id_sha256=Sha256Digest("a" * 64))
    assert result.failure is not None
    assert result.failure.code == ProtocolFailureCode.INCOMPLETE_STREAM
    assert result.bytes_received == 0


def test_a_missing_terminal_lf_is_an_incomplete_stream() -> None:
    digest = Sha256Digest("a" * 64)
    stream = _stream(digest, 1).removesuffix(FRAME_TERMINATOR)
    result = _read(stream, request_id_sha256=digest)
    assert result.failure is not None
    assert result.failure.code == ProtocolFailureCode.INCOMPLETE_STREAM
    assert len(result.outputs) == 1


@pytest.mark.parametrize(
    "declared",
    [pytest.param(0, id="too-few"), pytest.param(2, id="too-many")],
)
def test_a_completion_count_mismatch_is_an_incomplete_stream(
    declared: int,
) -> None:
    digest = Sha256Digest("a" * 64)
    stream = (
        encode_frame(ProtocolPrelude(request_id_sha256=digest))
        + encode_frame(
            ProtocolOutput(sequence=0, document=_output_document(0))
        )
        + encode_frame(ProtocolComplete(output_count=declared))
    )
    result = _read(stream, request_id_sha256=digest)
    assert result.failure is not None
    assert result.failure.code == ProtocolFailureCode.INCOMPLETE_STREAM
    assert len(result.outputs) == 1


# --- Finite self-budget edges -------------------------------------------


def _frame_byte_budget(max_bytes: int, /) -> ExecutorSelfBudgets:
    return ExecutorSelfBudgets(
        protocol_frame_bytes=FiniteByteLimit(max_bytes=max_bytes)
    )


def test_a_frame_exactly_at_its_byte_budget_is_accepted() -> None:
    digest = Sha256Digest("a" * 64)
    stream = _stream(digest, 1)
    longest = max(
        len(frame) for frame in stream.split(FRAME_TERMINATOR) if frame
    )
    result = _read(
        stream,
        request_id_sha256=digest,
        self_budgets=_frame_byte_budget(longest),
    )
    assert result.completed


def test_a_frame_one_byte_over_its_budget_is_oversized() -> None:
    digest = Sha256Digest("a" * 64)
    stream = _stream(digest, 1)
    longest = max(
        len(frame) for frame in stream.split(FRAME_TERMINATOR) if frame
    )
    result = _read(
        stream,
        request_id_sha256=digest,
        self_budgets=_frame_byte_budget(longest - 1),
    )
    assert result.failure is not None
    assert result.failure.code == ProtocolFailureCode.OVERSIZED_FRAME


def test_an_oversized_frame_is_refused_without_being_acquired() -> None:
    digest = Sha256Digest("a" * 64)
    prelude = encode_frame(ProtocolPrelude(request_id_sha256=digest))
    budget = len(prelude) - 2
    stream = prelude + b"x" * 10_000_000 + FRAME_TERMINATOR
    result = _read(
        stream,
        request_id_sha256=digest,
        self_budgets=_frame_byte_budget(budget),
    )
    assert result.failure is not None
    assert result.failure.code == ProtocolFailureCode.OVERSIZED_FRAME
    assert result.bytes_received <= budget + 1


class _TrickleReader(io.RawIOBase):
    """A reader that yields one byte per call.

    Frame boundaries and budget edges must not depend on how the transport
    happens to chunk its bytes, so the pathological split is exercised
    directly rather than assumed away.
    """

    def __init__(self, data: bytes) -> None:
        self._data = data
        self._position = 0

    def readable(self) -> bool:
        return True

    def read(self, size: int = -1) -> bytes:
        del size
        if self._position >= len(self._data):
            return b""
        chunk = self._data[self._position : self._position + 1]
        self._position += 1
        return chunk


def test_frame_boundaries_do_not_depend_on_transport_chunking() -> None:
    digest = Sha256Digest("a" * 64)
    stream = _stream(digest, 2)
    result = read_protocol_stream(
        _TrickleReader(stream),
        request_id_sha256=digest,
        self_budgets=ExecutorSelfBudgets.unbudgeted(),
    )
    assert result.completed
    assert result.bytes_received == len(stream)
    assert len(result.outputs) == 2


@pytest.mark.parametrize(
    ("offset", "oversized"),
    [
        pytest.param(0, False, id="at-budget"),
        pytest.param(-1, True, id="over"),
    ],
)
def test_the_frame_byte_edge_holds_under_byte_at_a_time_reads(
    offset: int,
    oversized: bool,
) -> None:
    digest = Sha256Digest("a" * 64)
    stream = _stream(digest, 1)
    longest = max(
        len(frame) for frame in stream.split(FRAME_TERMINATOR) if frame
    )
    result = read_protocol_stream(
        _TrickleReader(stream),
        request_id_sha256=digest,
        self_budgets=_frame_byte_budget(longest + offset),
    )
    if oversized:
        assert result.failure is not None
        assert result.failure.code == ProtocolFailureCode.OVERSIZED_FRAME
    else:
        assert result.completed


def test_a_stream_exactly_at_its_total_byte_budget_is_accepted() -> None:
    digest = Sha256Digest("a" * 64)
    stream = _stream(digest, 2)
    result = _read(
        stream,
        request_id_sha256=digest,
        self_budgets=ExecutorSelfBudgets(
            protocol_total_bytes=FiniteByteLimit(max_bytes=len(stream))
        ),
    )
    assert result.completed
    assert result.bytes_received == len(stream)


def test_a_stream_one_byte_over_its_total_budget_is_oversized() -> None:
    digest = Sha256Digest("a" * 64)
    stream = _stream(digest, 2)
    result = _read(
        stream,
        request_id_sha256=digest,
        self_budgets=ExecutorSelfBudgets(
            protocol_total_bytes=FiniteByteLimit(max_bytes=len(stream) - 1)
        ),
    )
    assert result.failure is not None
    assert result.failure.code == ProtocolFailureCode.OVERSIZED_FRAME


def test_outputs_exactly_at_the_count_budget_are_accepted() -> None:
    digest = Sha256Digest("a" * 64)
    result = _read(
        _stream(digest, 3),
        request_id_sha256=digest,
        self_budgets=ExecutorSelfBudgets(
            protocol_output_count=FiniteCountLimit(max_count=3)
        ),
    )
    assert result.completed
    assert len(result.outputs) == 3


def test_one_output_over_the_count_budget_is_oversized() -> None:
    digest = Sha256Digest("a" * 64)
    result = _read(
        _stream(digest, 3),
        request_id_sha256=digest,
        self_budgets=ExecutorSelfBudgets(
            protocol_output_count=FiniteCountLimit(max_count=2)
        ),
    )
    assert result.failure is not None
    assert result.failure.code == ProtocolFailureCode.OVERSIZED_FRAME
    assert len(result.outputs) == 2


def _nested_document(depth: int, /) -> IdentityDocument:
    payload: object = 0
    for _ in range(depth):
        payload = {"n": payload}
    return build_identity_document(
        schema=OUTPUT_SCHEMA,
        schema_version=1,
        payload=payload,
    )


def _nested_stream(digest: Sha256Digest, depth: int, /) -> bytes:
    return (
        encode_frame(ProtocolPrelude(request_id_sha256=digest))
        + encode_frame(
            ProtocolOutput(sequence=0, document=_nested_document(depth))
        )
        + encode_frame(ProtocolComplete(output_count=1))
    )


# One output frame nesting a payload four objects deep is exactly six
# structural levels: the frame object, the document object, and the four
# payload objects.
_NESTED_PAYLOAD_DEPTH = 4
_NESTED_FRAME_DEPTH = 6


def test_a_frame_exactly_at_the_depth_budget_is_accepted() -> None:
    digest = Sha256Digest("a" * 64)
    result = _read(
        _nested_stream(digest, _NESTED_PAYLOAD_DEPTH),
        request_id_sha256=digest,
        self_budgets=ExecutorSelfBudgets(
            json_depth=FiniteCountLimit(max_count=_NESTED_FRAME_DEPTH)
        ),
    )
    assert result.completed


def test_a_frame_one_level_over_the_depth_budget_is_malformed() -> None:
    digest = Sha256Digest("a" * 64)
    result = _read(
        _nested_stream(digest, _NESTED_PAYLOAD_DEPTH),
        request_id_sha256=digest,
        self_budgets=ExecutorSelfBudgets(
            json_depth=FiniteCountLimit(max_count=_NESTED_FRAME_DEPTH - 1)
        ),
    )
    assert result.failure is not None
    assert result.failure.code == ProtocolFailureCode.MALFORMED_FRAME


# --- Unbudgeted axes carry no hidden finite limit ------------------------


def test_an_unbudgeted_frame_axis_accepts_a_frame_of_any_size() -> None:
    digest = Sha256Digest("a" * 64)
    document = build_identity_document(
        schema=OUTPUT_SCHEMA,
        schema_version=1,
        payload={"blob": "x" * 4_000_000},
    )
    stream = (
        encode_frame(ProtocolPrelude(request_id_sha256=digest))
        + encode_frame(ProtocolOutput(sequence=0, document=document))
        + encode_frame(ProtocolComplete(output_count=1))
    )
    budgets = ExecutorSelfBudgets.unbudgeted()
    assert isinstance(budgets.protocol_frame_bytes, UnbudgetedLimit)
    assert isinstance(budgets.protocol_total_bytes, UnbudgetedLimit)
    result = _read(stream, request_id_sha256=digest, self_budgets=budgets)
    assert result.completed
    assert result.bytes_received == len(stream)


def test_an_unbudgeted_count_axis_accepts_many_outputs() -> None:
    digest = Sha256Digest("a" * 64)
    result = _read(_stream(digest, 2048), request_id_sha256=digest)
    assert result.completed
    assert len(result.outputs) == 2048


def test_an_unbudgeted_depth_axis_accepts_deep_nesting() -> None:
    """No hidden cap stands between an unbudgeted axis and the bytes.

    The frame bytes are assembled directly rather than through an
    ``IdentityDocument``, whose deep-copy snapshot recurses. This depth is
    far past any plausible library default, so the test fails if a finite
    cap is reintroduced on this unbudgeted axis; the remaining ceiling
    belongs to the pinned parsers' own stack recursion, which is the
    machine-resource exhaustion the design leaves possible rather than an
    executor-installed limit.
    """
    digest = Sha256Digest("a" * 64)
    depth = 100
    payload = b'{"n":' * depth + b"0" + b"}" * depth
    output = (
        b'{"document":{"payload":' + payload + b","
        b'"schema":"' + OUTPUT_SCHEMA.encode() + b'","schema_version":1},'
        b'"kind":"output","sequence":0,"version":1}' + FRAME_TERMINATOR
    )
    stream = (
        encode_frame(ProtocolPrelude(request_id_sha256=digest))
        + output
        + encode_frame(ProtocolComplete(output_count=1))
    )
    result = _read(stream, request_id_sha256=digest)
    assert result.completed
    assert len(result.outputs) == 1


# --- Request transport ---------------------------------------------------


def test_the_request_transport_is_exactly_canonical_identity_bytes(
    request_document: IdentityDocument,
) -> None:
    transport = request_transport_bytes(request_document)
    assert transport == canonical_identity_json_bytes(request_document)
    assert not transport.startswith(b"\xef\xbb\xbf")
    assert not transport.endswith(FRAME_TERMINATOR)
    assert FRAME_TERMINATOR not in transport


def test_the_request_digest_covers_exactly_the_transported_bytes(
    request_document: IdentityDocument,
) -> None:
    assert (
        request_identity_digest(request_document)
        == hashlib.sha256(
            request_transport_bytes(request_document)
        ).hexdigest()
    )


def test_a_prelude_bound_to_the_request_digest_opens_its_stream(
    request_document: IdentityDocument,
) -> None:
    digest = request_identity_digest(request_document)
    result = _read(_stream(digest, 1), request_id_sha256=digest)
    assert result.completed
