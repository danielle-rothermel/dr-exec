"""The contract-enforcing fake: same declarations, same invariants, no spawn.

:class:`FakeExecutor` is the consumer's executor double, shipped with the
library so no consumer writes its own. It runs the engine's own pre-spawn
validation — a call the real executor rejects, the fake rejects identically,
with the same exception and the same message — executes nothing, and returns
results a caller scripted.

Two guards make a passing fake test evidence about the contract rather than
about the double. Scripted results are validated against the invariants the
engine guarantees, so an outcome the engine could never construct is refused
at return time. And the fake declares its own identity, refusing to claim the
production one, so a fake-produced outcome can never cache-collide with a
real run's.

Every call's full declaration is recorded as a :class:`RecordedCall` and is
assertable: adding a budget to production code changes what tests can
observe, never what they can run.
"""

from __future__ import annotations

import json
from collections import deque
from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace
from enum import StrEnum, unique
from typing import Final

from dr_exec.batch import (
    BatchRequest,
    BatchResult,
    ItemResult,
    WireKey,
    WireKind,
    account_transcript,
    channel_bounds_for,
    validated_driver_source,
)
from dr_exec.declare import (
    HERMETIC,
    REPORT_ONLY,
    Budgets,
    ContainmentProfile,
    EnvironmentGrant,
    ExitPolicy,
    OutputBudget,
    PythonRuntime,
    Records,
    StreamBounds,
)
from dr_exec.engine import Declaration, validate_declaration
from dr_exec.errors import DeclarationError, DrExecError
from dr_exec.record import (
    EXECUTOR_IDENTITY,
    FAKE_EXECUTOR_IDENTITY,
    Attribution,
    RunResult,
    TrustCategory,
)
from dr_exec.run import (
    tool_declaration,
    untrusted_command_declaration,
    untrusted_python_declaration,
)

__all__ = [
    "EntryPoint",
    "FakeExecutor",
    "RecordedBatchCall",
    "RecordedCall",
    "ScriptError",
    "ScriptedBatch",
    "UnscriptedCall",
]

_NO_ENVIRONMENT: Final[EnvironmentGrant] = EnvironmentGrant.none()


@unique
class EntryPoint(StrEnum):
    """Which entry point a recorded call came through.

    The value is the public function's own name, so a test asserting on a
    recorded call names the function the production code called.
    """

    RUN_TOOL = "run_tool"
    RUN_UNTRUSTED_PYTHON = "run_untrusted_python"
    RUN_UNTRUSTED_COMMAND = "run_untrusted_command"
    RUN_BATCH = "run_batch"


class ScriptError(DrExecError):
    """A scripted result could not have come from a real run.

    Raised at return time, before the caller sees the result: a test that
    passes against the fake cannot be wrong about the contract.
    """


class UnscriptedCall(DrExecError):
    """The fake was called with nothing scripted to answer it."""


@dataclass(frozen=True, slots=True)
class RecordedCall:
    """One call's full declaration set, exactly as the production code made it.

    ``declaration`` is the same :class:`~dr_exec.engine.Declaration` the real
    executor would have run, so an assertion here is an assertion about the
    real call. The named fields are the reading conveniences over it; nothing
    is recorded that the declaration does not already carry.
    """

    entry_point: EntryPoint
    declaration: Declaration

    @property
    def trust_category(self) -> TrustCategory:
        return self.declaration.invocation.trust_category

    @property
    def command(self) -> tuple[str, ...]:
        """The argument vector as spawned, source-carrying argv included."""
        return self.declaration.invocation.argv

    @property
    def source(self) -> str | None:
        """The untrusted Python source, or ``None`` for a command form."""
        return self.declaration.invocation.source

    @property
    def input_text(self) -> str:
        return self.declaration.invocation.input_text

    @property
    def runtime(self) -> PythonRuntime | None:
        return self.declaration.invocation.runtime

    @property
    def profile(self) -> ContainmentProfile | None:
        return self.declaration.invocation.profile

    @property
    def budgets(self) -> Budgets:
        return self.declaration.budgets

    @property
    def environment(self) -> EnvironmentGrant:
        return self.declaration.environment

    @property
    def exit_policy(self) -> ExitPolicy:
        return self.declaration.exit_policy

    @property
    def records(self) -> Records:
        return self.declaration.records

    @property
    def stream_bounds(self) -> StreamBounds | None:
        return self.declaration.stream_bounds


@dataclass(frozen=True, slots=True)
class RecordedBatchCall:
    """A batch call: the request the caller built plus the run it declared.

    ``call`` is the run the batch declared — composed driver source, channel
    stream bounds, and all — so the same assertions that work on a plain call
    work here.
    """

    request: BatchRequest
    call: RecordedCall

    @property
    def entry_point(self) -> EntryPoint:
        return self.call.entry_point


@dataclass(frozen=True, slots=True)
class ScriptedBatch:
    """What a batch script returns: the child's run plus its item results.

    The parent's accounting is not faked — the fake renders these results as
    the transcript a conforming driver would have written and runs the real
    parent-side accounting over it, so a scripted batch obeys the same
    exactly-one-result-per-item rule a real one does.
    """

    run: RunResult
    results: tuple[ItemResult, ...] = ()
    completion_seen: bool = True
    results_emitted_claim: int | None = None


type RunScript = Callable[[RecordedCall], RunResult]
type BatchScript = Callable[[RecordedBatchCall], ScriptedBatch]


class FakeExecutor:
    """An executor that validates and records everything and runs nothing.

    ``identity`` is what a consumer folding executor identity into a cache
    key or a provenance field would see. It defaults to the fake identity and
    may be narrowed to a consumer's own fake name; claiming
    :data:`~dr_exec.record.EXECUTOR_IDENTITY` is refused at construction.
    """

    def __init__(self, identity: str = FAKE_EXECUTOR_IDENTITY) -> None:
        if identity == EXECUTOR_IDENTITY:
            raise DeclarationError(
                f"a fake executor may not claim the production identity "
                f"{EXECUTOR_IDENTITY!r}"
            )
        if not isinstance(identity, str) or not identity:
            raise DeclarationError("executor identity must be a nonempty string")
        self.identity: Final[str] = identity
        self.calls: list[RecordedCall] = []
        self.batch_calls: list[RecordedBatchCall] = []
        self._script: RunScript | None = None
        self._batch_script: BatchScript | None = None
        self._queue: deque[RunResult] = deque()
        self._batch_queue: deque[ScriptedBatch] = deque()

    def script_with(self, script: RunScript) -> None:
        """Answer every run from a callable over the full declaration.

        The callable sees a :class:`RecordedCall` and returns a
        :class:`~dr_exec.record.RunResult`: doubles that inspect the payload
        and synthesize matching output are written this way, which a flat
        queue cannot express.
        """
        self._script = script

    def enqueue(self, result: RunResult) -> None:
        """Answer the next unscripted-by-callable run with ``result``, FIFO."""
        self._queue.append(result)

    def script_batches_with(self, script: BatchScript) -> None:
        """Answer every batch from a callable over the request and call."""
        self._batch_script = script

    def enqueue_batch(self, scripted: ScriptedBatch) -> None:
        """Answer the next batch with ``scripted``, FIFO."""
        self._batch_queue.append(scripted)

    @property
    def last_call(self) -> RecordedCall:
        """The most recent recorded call; a fake with none is a test bug."""
        if not self.calls:
            raise AssertionError("no call has been recorded")
        return self.calls[-1]

    def calls_for(self, entry_point: EntryPoint) -> tuple[RecordedCall, ...]:
        """Every recorded call that came through ``entry_point``, in order."""
        return tuple(call for call in self.calls if call.entry_point is entry_point)

    @property
    def last_batch_call(self) -> RecordedBatchCall:
        if not self.batch_calls:
            raise AssertionError("no batch call has been recorded")
        return self.batch_calls[-1]

    def run_tool(
        self,
        command: Sequence[str],
        *,
        budgets: Budgets,
        records: Records,
        input_text: str = "",
        environment: EnvironmentGrant = _NO_ENVIRONMENT,
        exit_policy: ExitPolicy = REPORT_ONLY,
    ) -> RunResult:
        return self._answer(
            EntryPoint.RUN_TOOL,
            tool_declaration(
                command,
                budgets=budgets,
                records=records,
                input_text=input_text,
                environment=environment,
                exit_policy=exit_policy,
            ),
        )

    def run_untrusted_python(
        self,
        source: str,
        *,
        profile: ContainmentProfile,
        budgets: Budgets,
        records: Records,
        runtime: PythonRuntime = HERMETIC,
        input_text: str = "",
        environment: EnvironmentGrant = _NO_ENVIRONMENT,
        exit_policy: ExitPolicy = REPORT_ONLY,
        stream_bounds: StreamBounds | None = None,
    ) -> RunResult:
        return self._answer(
            EntryPoint.RUN_UNTRUSTED_PYTHON,
            untrusted_python_declaration(
                source,
                profile=profile,
                budgets=budgets,
                records=records,
                runtime=runtime,
                input_text=input_text,
                environment=environment,
                exit_policy=exit_policy,
                stream_bounds=stream_bounds,
            ),
        )

    def run_untrusted_command(
        self,
        command: Sequence[str],
        *,
        profile: ContainmentProfile,
        budgets: Budgets,
        records: Records,
        input_text: str = "",
        environment: EnvironmentGrant = _NO_ENVIRONMENT,
        exit_policy: ExitPolicy = REPORT_ONLY,
    ) -> RunResult:
        return self._answer(
            EntryPoint.RUN_UNTRUSTED_COMMAND,
            untrusted_command_declaration(
                command,
                profile=profile,
                budgets=budgets,
                records=records,
                input_text=input_text,
                environment=environment,
                exit_policy=exit_policy,
            ),
        )

    def run_batch(
        self,
        request: BatchRequest,
        *,
        profile: ContainmentProfile,
        budgets: Budgets,
        records: Records,
        runtime: PythonRuntime = HERMETIC,
        environment: EnvironmentGrant = _NO_ENVIRONMENT,
        exit_policy: ExitPolicy = REPORT_ONLY,
    ) -> BatchResult:
        declaration = untrusted_python_declaration(
            validated_driver_source(request),
            profile=profile,
            budgets=budgets,
            records=records,
            runtime=runtime,
            environment=environment,
            exit_policy=exit_policy,
            stream_bounds=channel_bounds_for(request, budgets),
        )
        validate_declaration(declaration)
        # The batch's own call lands in `calls` too: a batch *is* an
        # untrusted-Python run, so assertions over declarations see it
        # without knowing which surface produced it.
        inner = RecordedCall(entry_point=EntryPoint.RUN_BATCH, declaration=declaration)
        self.calls.append(inner)
        call = RecordedBatchCall(request=request, call=inner)
        self.batch_calls.append(call)

        scripted = self._scripted_batch(call)
        run = _transcribed(request, scripted)
        _validate_result(run, declaration)
        return account_transcript(request=request, run=run)

    def _answer(self, entry_point: EntryPoint, declaration: Declaration) -> RunResult:
        validate_declaration(declaration)
        call = RecordedCall(entry_point=entry_point, declaration=declaration)
        self.calls.append(call)

        result = self._scripted_result(call)
        _validate_result(result, declaration)
        return result

    def _scripted_result(self, call: RecordedCall) -> RunResult:
        if self._script is not None:
            result = self._script(call)
            if not isinstance(result, RunResult):
                raise ScriptError(
                    f"the script returned {type(result).__name__}, not a RunResult"
                )
            return result
        if self._queue:
            return self._queue.popleft()
        raise UnscriptedCall(
            f"{call.entry_point.value} was called with nothing scripted: "
            "script_with(callable) or enqueue(result) first"
        )

    def _scripted_batch(self, call: RecordedBatchCall) -> ScriptedBatch:
        if self._batch_script is not None:
            scripted = self._batch_script(call)
            if not isinstance(scripted, ScriptedBatch):
                raise ScriptError(
                    f"the batch script returned {type(scripted).__name__}, "
                    "not a ScriptedBatch"
                )
            return scripted
        if self._batch_queue:
            return self._batch_queue.popleft()
        raise UnscriptedCall(
            "run_batch was called with nothing scripted: "
            "script_batches_with(callable) or enqueue_batch(scripted) first"
        )


def _transcribed(request: BatchRequest, scripted: ScriptedBatch) -> RunResult:
    """Render scripted item results as the transcript a driver would write.

    Going through the wire form rather than around it is what makes the
    parent-side accounting real: a script that claims two results for one
    item fails here exactly as a broken driver would.

    The protocol channel's byte count is the transcript's own, not the
    script's: the fake wrote those bytes, so it is the fake that reports
    them. A script declares the *payload* stream's counts, which it owns.
    """
    lines = [json.dumps(request.prelude(), sort_keys=True, separators=(",", ":"))]
    for result in scripted.results:
        if not isinstance(result, ItemResult):
            raise ScriptError("scripted batch results must be ItemResult values")
        lines.append(
            json.dumps(
                {
                    WireKey.KIND.value: WireKind.RESULT.value,
                    WireKey.ITEM_ID.value: result.item_id,
                    WireKey.PAYLOAD.value: result.payload,
                },
                sort_keys=True,
                separators=(",", ":"),
            )
        )
    if scripted.completion_seen:
        claim = (
            len(scripted.results)
            if scripted.results_emitted_claim is None
            else scripted.results_emitted_claim
        )
        lines.append(
            json.dumps(
                {
                    WireKey.KIND.value: WireKind.COMPLETE.value,
                    WireKey.RESULTS_EMITTED.value: claim,
                },
                sort_keys=True,
                separators=(",", ":"),
            )
        )
    transcript = "".join(f"{line}\n" for line in lines)
    transcript_bytes = len(transcript.encode("utf-8"))
    return replace(
        scripted.run,
        stdout=transcript,
        measurements=replace(
            scripted.run.measurements,
            stdout_bytes_produced=max(
                scripted.run.measurements.stdout_bytes_produced, transcript_bytes
            ),
        ),
    )


def _validate_result(result: RunResult, declaration: Declaration) -> None:
    """Refuse any outcome the engine could not have constructed.

    Each rule mirrors one the engine holds: exactly one attribution with the
    shape that attribution implies, a status wherever a child was reaped, and
    truncation marked only where a bound existed to cross.
    """
    if not isinstance(result, RunResult):
        raise ScriptError(
            f"a scripted result must be a RunResult, not {type(result).__name__}"
        )

    outcome = result.outcome
    attribution = outcome.attribution

    if attribution is Attribution.ABSENCE and outcome.spawn_errno is None:
        raise ScriptError("an absence outcome carries the spawn errno that decided it")
    if outcome.spawn_errno is not None and attribution not in (
        Attribution.ABSENCE,
        Attribution.MACHINE,
    ):
        raise ScriptError(
            f"a spawn errno belongs to an absence or machine outcome, "
            f"not {attribution.value!r}"
        )

    if attribution in (Attribution.ABSENCE, Attribution.MACHINE):
        if result.returncode is not None:
            raise ScriptError(
                f"a {attribution.value} outcome never spawned, so it has no returncode"
            )
        if result.stdout or result.stderr:
            raise ScriptError(
                f"a {attribution.value} outcome never spawned, so it captured no output"
            )
    elif result.returncode is None:
        raise ScriptError(
            f"a {attribution.value} outcome comes from a reaped child, so it "
            "carries a returncode"
        )

    if attribution is Attribution.PAYLOAD:
        expected = declaration.exit_policy.verdict_for(result.returncode).value
        if outcome.exit_verdict != expected:
            raise ScriptError(
                f"the declared exit policy makes returncode {result.returncode} "
                f"{expected!r}, not {outcome.exit_verdict!r}"
            )
    elif outcome.exit_verdict is not None:
        raise ScriptError(
            f"an exit verdict belongs to a payload outcome, not {attribution.value!r}"
        )

    _validate_capture(result, declaration)


def _validate_capture(result: RunResult, declaration: Declaration) -> None:
    """Bytes produced, retained, and dropped must agree with the bounds."""
    measurements = result.measurements
    truncation = result.truncation

    for value, name in (
        (measurements.duration_seconds, "duration_seconds"),
        (measurements.teardown_seconds, "teardown_seconds"),
        (measurements.stdout_bytes_produced, "stdout_bytes_produced"),
        (measurements.stderr_bytes_produced, "stderr_bytes_produced"),
        (measurements.input_bytes, "input_bytes"),
        (truncation.stdout_bytes_dropped, "stdout_bytes_dropped"),
        (truncation.stderr_bytes_dropped, "stderr_bytes_dropped"),
    ):
        if value < 0:
            raise ScriptError(f"{name} counts bytes or seconds, never a negative")

    declared_input = len(declaration.invocation.input_text.encode("utf-8"))
    if measurements.input_bytes != declared_input:
        raise ScriptError(
            f"the declaration feeds {declared_input} input bytes, so the result "
            f"reports {declared_input}, not {measurements.input_bytes}"
        )

    if truncation.any_dropped and not _has_output_bound(declaration):
        raise ScriptError(
            "a truncation mark requires a bounded output budget or declared "
            "stream bounds: an unbudgeted run drops nothing"
        )

    for produced, retained, stream in (
        (measurements.stdout_bytes_produced, result.stdout, "stdout"),
        (measurements.stderr_bytes_produced, result.stderr, "stderr"),
    ):
        if produced < len(retained.encode("utf-8")):
            raise ScriptError(
                f"{stream} produced {produced} bytes but retained more: bytes "
                "produced count everything the child wrote"
            )


def _has_output_bound(declaration: Declaration) -> bool:
    bounds = declaration.stream_bounds
    if bounds is not None and (
        bounds.stdout_bytes is not None or bounds.stderr_bytes is not None
    ):
        return True
    return isinstance(declaration.budgets.output, OutputBudget)
