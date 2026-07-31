"""The batch protocol: parent-side orchestration and the child's driver kit.

One :class:`BatchRequest` is one warm child: the sharing boundary is declared
by constructing the request, and the cross-product fan-out across outer
dimensions stays parent-side. Results leave the child incrementally as
newline-delimited JSON, prefaced by a prelude echoing the request identity,
so a result once delivered is trustable the moment it arrives and no later
truncation or death can retroactively void it.

The attribution seam sits here: this module reports raw distinguishable
state — which items produced results, how the child exited, whether it
finished on its own terms. Domain meaning (what a SIGSEGV death means for
an item that never reported) stays with the consumer.
"""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum, unique
from typing import Any, Final

from dr_exec._driver_kit import KIT_SOURCE
from dr_exec.declare import (
    HERMETIC,
    REPORT_ONLY,
    SOURCE_BOUND_BYTES,
    UNBUDGETED,
    Budgets,
    ContainmentProfile,
    EnvironmentGrant,
    ExitPolicy,
    PythonRuntime,
    Records,
    StreamBounds,
)
from dr_exec.engine import execute
from dr_exec.errors import DeclarationError, ProtocolFailure
from dr_exec.record import RunResult
from dr_exec.run import untrusted_python_declaration

PROTOCOL_VERSION: Final[int] = 1
"""The wire protocol's pinned version, echoed in every prelude."""

BODY_HOOK_NAME: Final[str] = "run_item"
"""The per-item function a caller's driver body must define."""

CLIP_MARKER: Final[str] = "...[clipped]"
"""What the kit appends when it clips an over-budget error text."""

DEFAULT_ITEM_RESULT_BOUND_BYTES: Final[int] = 64 * 1024
DEFAULT_FRAME_BOUND_BYTES: Final[int] = 8 * 1024

_logger: Final[logging.Logger] = logging.getLogger("dr_exec.batch")

_NO_ENVIRONMENT: Final[EnvironmentGrant] = EnvironmentGrant.none()
"""The default grant, as one frozen value shared by every batch call."""

__all__ = [
    "BODY_HOOK_NAME",
    "CLIP_MARKER",
    "PROTOCOL_VERSION",
    "BatchItem",
    "BatchRequest",
    "BatchResult",
    "ItemResult",
    "ProtocolChannelBudget",
    "WireKey",
    "WireKind",
    "config_digest_of",
    "run_batch",
]


@unique
class WireKey(StrEnum):
    """Every JSON key in the batch wire format.

    Consumers parse persisted protocol transcripts with these, so they are
    pinned at exact-literal level and never derived from field names. Never
    iterate this enum to build a payload.
    """

    KIND = "kind"
    PROTOCOL = "protocol"
    ITEM_IDS = "item_ids"
    CONFIG_DIGEST = "config_digest"
    ITEM_ID = "item_id"
    PAYLOAD = "payload"
    RESULTS_EMITTED = "results_emitted"
    ERROR = "error"


@unique
class WireKind(StrEnum):
    """The three line kinds, in the order the protocol produces them.

    Persisted format; never iterate to build a payload.
    """

    PRELUDE = "prelude"
    RESULT = "result"
    COMPLETE = "complete"


def config_digest_of(config: Any) -> str:
    """SHA-256 hex over canonical JSON — the pinned request-identity digest.

    Canonicalization: sorted keys, ``(",", ":")`` separators, non-ASCII left
    as-is, encoded UTF-8. The child echoes this digest in its prelude and the
    parent verifies it before trusting any result line.
    """
    canonical = json.dumps(
        config, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class BatchItem:
    """One unit of work: a caller-declared coordinate and an opaque payload.

    ``item_id`` is a dimension coordinate — an opaque string to the executor,
    meaningful only to the consumer that declared it. ``payload`` is JSON-able
    data the driver body interprets.
    """

    item_id: str
    payload: Any


@dataclass(frozen=True, slots=True)
class ProtocolChannelBudget:
    """The contract budget on the child's protocol stdout.

    Separate from the payload-stream output budget by design: a payload that
    floods its own streams costs only its own items, never the batch. The kit
    enforces ``item_result_bytes`` per result line in-child (an over-budget
    result becomes an error result, so the item is still reported), and the
    parent bounds the channel as a whole from these declarations.
    """

    item_result_bytes: int = DEFAULT_ITEM_RESULT_BOUND_BYTES
    frame_bytes: int = DEFAULT_FRAME_BOUND_BYTES

    def __post_init__(self) -> None:
        for name in ("item_result_bytes", "frame_bytes"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise DeclarationError(
                    f"protocol channel {name} must be a positive integer of bytes"
                )

    def channel_bytes_for(self, item_count: int) -> int:
        """The whole channel's bound: both frames plus every item's result."""
        return 2 * self.frame_bytes + item_count * self.item_result_bytes


@dataclass(frozen=True, slots=True)
class BatchRequest:
    """One warm child's worth of work, plus the identity the child echoes.

    Constructing a request *is* the sharing-boundary declaration: one request
    is one warm child, and the cross-product fan-out across outer dimensions
    stays parent-side.

    Budgets cross the boundary as data: the protocol channel budget is
    rehydrated in-child from this declaration, so the child enforces the same
    bound the caller wrote. ``item_schema`` crosses the same way — the driver
    body reads it as ``_KIT_ITEM_SCHEMA`` and may validate its payloads
    against it, so the schema the caller declared is the schema the body
    checks.

    Item data crosses as the child's stdin payload, not inlined in the driver
    source: :meth:`items_input_text` renders the item array the parent feeds
    as ``input_text``, so the child reads its items from stdin and the batch
    size is bounded by the declared input budget, never by the source bound.
    """

    items: tuple[BatchItem, ...]
    body_source: str
    item_schema: str
    config: Any
    channel_budget: ProtocolChannelBudget = ProtocolChannelBudget()

    def __post_init__(self) -> None:
        if not self.items:
            raise DeclarationError("a batch request must declare at least one item")
        seen: set[str] = set()
        for item in self.items:
            if not isinstance(item, BatchItem):
                raise DeclarationError("batch items must be BatchItem values")
            if not isinstance(item.item_id, str) or not item.item_id:
                raise DeclarationError("batch item ids must be nonempty strings")
            if item.item_id in seen:
                raise DeclarationError(f"duplicate batch item id {item.item_id!r}")
            seen.add(item.item_id)
        if not isinstance(self.body_source, str) or not self.body_source.strip():
            raise DeclarationError("a batch request must declare a driver body source")

    @property
    def item_ids(self) -> tuple[str, ...]:
        return tuple(item.item_id for item in self.items)

    def config_digest(self) -> str:
        return config_digest_of(self.config)

    def prelude(self) -> dict[str, Any]:
        """The line the child emits first, echoing this request's identity."""
        return {
            WireKey.KIND.value: WireKind.PRELUDE.value,
            WireKey.PROTOCOL.value: PROTOCOL_VERSION,
            WireKey.ITEM_IDS.value: list(self.item_ids),
            WireKey.CONFIG_DIGEST.value: self.config_digest(),
        }

    def items_input_text(self) -> str:
        """The item array the parent feeds as the child's stdin payload.

        Each item is one wire object of its id and payload; the child reads
        this whole array from ``sys.stdin`` and sweeps it. This is the batch's
        stdin contract — the items *are* the input payload, bounded by the
        declared input budget, so a batch never separately feeds stdin.
        """
        return json.dumps(
            [
                {
                    WireKey.ITEM_ID.value: item.item_id,
                    WireKey.PAYLOAD.value: item.payload,
                }
                for item in self.items
            ],
            separators=(",", ":"),
        )

    def driver_source(self) -> str:
        """The composed program: kit preamble, kit body, caller's body text.

        The item data is not bound here — it crosses as stdin via
        :meth:`items_input_text`, so this source is small and roughly constant
        regardless of item count.
        """
        return _compose_source(self)


@dataclass(frozen=True, slots=True)
class ItemResult:
    """One item's delivered result: the payload the driver body produced.

    ``payload`` is the body's return value verbatim; an item whose body
    raised arrives as a payload carrying the clipped traceback under the
    pinned ``error`` key. Both are results — item failures are data.
    """

    item_id: str
    payload: Any

    @property
    def error_text(self) -> str | None:
        """The clipped traceback when the body raised, else ``None``."""
        if isinstance(self.payload, Mapping):
            error = self.payload.get(WireKey.ERROR.value)
            if isinstance(error, str):
                return error
        return None


@dataclass(frozen=True, slots=True)
class BatchResult:
    """Raw distinguishable state after one batch child ran.

    Items missing at child death are *not* synthesized: ``missing_item_ids``
    names them and ``run`` carries the outcome and attribution a consumer
    needs to decide what their absence means.
    """

    request: BatchRequest
    run: RunResult
    results: tuple[ItemResult, ...]
    completion_seen: bool
    results_emitted_claim: int | None

    @property
    def results_by_item_id(self) -> dict[str, ItemResult]:
        return {result.item_id: result for result in self.results}

    @property
    def missing_item_ids(self) -> tuple[str, ...]:
        delivered = {result.item_id for result in self.results}
        return tuple(
            item_id for item_id in self.request.item_ids if item_id not in delivered
        )

    @property
    def complete(self) -> bool:
        """Every item reported and the child finished on its own terms."""
        return self.completion_seen and not self.missing_item_ids


def run_batch(
    request: BatchRequest,
    *,
    profile: ContainmentProfile,
    budgets: Budgets,
    records: Records,
    runtime: PythonRuntime = HERMETIC,
    environment: EnvironmentGrant = _NO_ENVIRONMENT,
    exit_policy: ExitPolicy = REPORT_ONLY,
) -> BatchResult:
    """Run one batch request in one child and account for its result lines.

    The child's stdout is the protocol channel, bounded by the request's
    channel budget; its stderr is the payload stream, bounded by the run's
    output budget. Delivery is captured, so the transcript is parsed after
    the run — incremental *production* is what makes partials survive, and
    the kit flushes every line as it emits it.

    The items cross to the child as its stdin payload (the input the child
    reads whole and sweeps), bounded by the declared input budget. Because
    input budgets are validated pre-spawn, a batch whose item data exceeds a
    declared input budget is a clean :class:`~dr_exec.errors.DeclarationError`
    caller error before any child exists, never a mid-run failure.

    Raises :class:`~dr_exec.errors.ProtocolFailure` when the transcript
    cannot be accounted for: a missing or mismatched prelude, a duplicate or
    unknown item id, or a shape-invalid line. The failure carries whatever
    results were validated before the fault.
    """
    # The declaration is built here and executed directly rather than routed
    # through the public entry point: per-stream bounds are scoped to the
    # protocol channel, so declaring them stays in-package.
    run = execute(
        untrusted_python_declaration(
            validated_driver_source(request),
            profile=profile,
            budgets=budgets,
            records=records,
            runtime=runtime,
            input_text=request.items_input_text(),
            environment=environment,
            exit_policy=exit_policy,
            stream_bounds=channel_bounds_for(request, budgets),
        )
    )
    return account_transcript(request=request, run=run)


def validated_driver_source(request: BatchRequest) -> str:
    """Compose the driver program and check it against the source bound.

    The bound belongs to the request, not to the spawn: an executor that
    never spawns still rejects a request whose composed driver could not be
    delivered as one argument.
    """
    source = request.driver_source()
    source_bytes = len(source.encode("utf-8"))
    if source_bytes > SOURCE_BOUND_BYTES:
        raise DeclarationError(
            f"composed driver source of {source_bytes} bytes exceeds the "
            f"{SOURCE_BOUND_BYTES}-byte source bound"
        )
    _logger.info(
        "batch of %d items, driver source %d bytes", len(request.items), source_bytes
    )
    return source


def channel_bounds_for(request: BatchRequest, budgets: Budgets) -> StreamBounds:
    """Protocol stdout gets the channel budget; payload stderr gets the run's.

    Splitting the bound is what keeps a noisy payload from voiding completed
    results: stderr's flood is accounted, attributed, and — under the
    caller's policy — enforced, while the protocol channel's own bytes stay
    unconsumed by it.
    """
    return StreamBounds(
        stdout_bytes=request.channel_budget.channel_bytes_for(len(request.items)),
        stderr_bytes=(
            None if budgets.output is UNBUDGETED else budgets.output.limit_bytes
        ),
    )


def account_transcript(*, request: BatchRequest, run: RunResult) -> BatchResult:
    """Parse the transcript, verifying identity before trusting any result.

    A final fragment with no terminating newline is a cut, not a fault: the
    child died or the channel bound landed mid-line. Dropping it keeps the
    incremental-trust rule — everything already whole stays trusted, and the
    run's outcome explains the cut.
    """
    lines = _whole_lines(run.stdout)
    if not lines:
        raise ProtocolFailure(
            "the driver emitted no protocol lines", results=(), run=run
        )

    prelude = _parsed_line(lines[0], results=(), run=run)
    _verify_prelude(prelude, request=request, run=run)

    results: list[ItemResult] = []
    seen: set[str] = set()
    expected = set(request.item_ids)
    completion_seen = False
    results_emitted_claim: int | None = None

    for raw in lines[1:]:
        line = _parsed_line(raw, results=tuple(results), run=run)
        kind = line.get(WireKey.KIND.value)
        if kind == WireKind.RESULT.value:
            item_id = line.get(WireKey.ITEM_ID.value)
            if not isinstance(item_id, str):
                raise ProtocolFailure(
                    "a result line carries no item id",
                    results=tuple(results),
                    run=run,
                )
            if item_id not in expected:
                raise ProtocolFailure(
                    f"the driver reported unknown item id {item_id!r}",
                    results=tuple(results),
                    run=run,
                )
            if item_id in seen:
                raise ProtocolFailure(
                    f"the driver reported item id {item_id!r} more than once",
                    results=tuple(results),
                    run=run,
                )
            if WireKey.PAYLOAD.value not in line:
                raise ProtocolFailure(
                    f"the result for item id {item_id!r} carries no payload",
                    results=tuple(results),
                    run=run,
                )
            seen.add(item_id)
            results.append(
                ItemResult(item_id=item_id, payload=line[WireKey.PAYLOAD.value])
            )
        elif kind == WireKind.COMPLETE.value:
            completion_seen = True
            claim = line.get(WireKey.RESULTS_EMITTED.value)
            if not isinstance(claim, int) or isinstance(claim, bool):
                raise ProtocolFailure(
                    "the completion line carries no results_emitted count",
                    results=tuple(results),
                    run=run,
                )
            results_emitted_claim = claim
        else:
            raise ProtocolFailure(
                f"the driver emitted an unknown line kind {kind!r}",
                results=tuple(results),
                run=run,
            )

    return BatchResult(
        request=request,
        run=run,
        results=tuple(results),
        completion_seen=completion_seen,
        results_emitted_claim=results_emitted_claim,
    )


def _whole_lines(transcript: str) -> list[str]:
    """Every newline-terminated, nonempty line; an unterminated tail is cut."""
    return [line for line in transcript.split("\n")[:-1] if line]


def _parsed_line(
    raw: str, *, results: tuple[ItemResult, ...], run: RunResult
) -> dict[str, Any]:
    try:
        line = json.loads(raw)
    except ValueError as parse_error:
        raise ProtocolFailure(
            f"the driver emitted an unparsable protocol line: {parse_error}",
            results=results,
            run=run,
        ) from parse_error
    if not isinstance(line, dict):
        raise ProtocolFailure(
            "a protocol line is not a JSON object", results=results, run=run
        )
    return line


def _verify_prelude(
    prelude: Mapping[str, Any], *, request: BatchRequest, run: RunResult
) -> None:
    """Identity first: nothing after the prelude is trusted until it matches."""
    if prelude.get(WireKey.KIND.value) != WireKind.PRELUDE.value:
        raise ProtocolFailure(
            "the driver's first protocol line is not a prelude", results=(), run=run
        )
    if prelude.get(WireKey.PROTOCOL.value) != PROTOCOL_VERSION:
        raise ProtocolFailure(
            f"the driver echoed protocol version "
            f"{prelude.get(WireKey.PROTOCOL.value)!r}, not {PROTOCOL_VERSION}",
            results=(),
            run=run,
        )
    echoed_ids = prelude.get(WireKey.ITEM_IDS.value)
    if not isinstance(echoed_ids, list) or set(echoed_ids) != set(request.item_ids):
        raise ProtocolFailure(
            "the driver echoed a different item set than the request declared",
            results=(),
            run=run,
        )
    if prelude.get(WireKey.CONFIG_DIGEST.value) != request.config_digest():
        raise ProtocolFailure(
            "the driver echoed a different config digest than the request declared",
            results=(),
            run=run,
        )


def _compose_source(request: BatchRequest) -> str:
    """Bind the kit's parameters as literals, then the kit, then the body.

    The body arrives as a string constant the kit ``exec``s, so a syntax
    error in consumer domain code is a load-phase failure the kit fans out
    as one error result per item — never a spawn-time surprise.

    Item data is *not* bound here: it crosses as the child's stdin payload
    (see :meth:`BatchRequest.items_input_text`), so this source stays small
    and its size is independent of the batch's item count.
    """
    bindings = {
        "_KIT_PRELUDE_JSON": json.dumps(
            json.dumps(request.prelude(), separators=(",", ":"), sort_keys=True)
        ),
        "_KIT_BODY_SOURCE": json.dumps(request.body_source),
        "_KIT_BODY_HOOK_NAME": json.dumps(BODY_HOOK_NAME),
        "_KIT_ITEM_SCHEMA": json.dumps(request.item_schema),
        "_KIT_RESULT_BOUND": repr(request.channel_budget.item_result_bytes),
        "_KIT_CLIP_MARKER": json.dumps(CLIP_MARKER),
        "_KIT_KEY_KIND": json.dumps(WireKey.KIND.value),
        "_KIT_KEY_ITEM_ID": json.dumps(WireKey.ITEM_ID.value),
        "_KIT_KEY_PAYLOAD": json.dumps(WireKey.PAYLOAD.value),
        "_KIT_KEY_ERROR": json.dumps(WireKey.ERROR.value),
        "_KIT_KEY_RESULTS_EMITTED": json.dumps(WireKey.RESULTS_EMITTED.value),
        "_KIT_KIND_RESULT": json.dumps(WireKind.RESULT.value),
        "_KIT_KIND_COMPLETE": json.dumps(WireKind.COMPLETE.value),
    }
    preamble = "\n".join(f"{name} = {value}" for name, value in bindings.items())
    return f"{preamble}\n{KIT_SOURCE}"
