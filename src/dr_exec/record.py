"""Run results, the durable run record, and executor identity.

A :class:`RunResult` is the in-memory answer; a :class:`RunRecord` is its
persistent shadow. Outcomes are data here: a budget violation, a signal
death, and an absent program are all values, never exception types.
"""

from __future__ import annotations

import importlib.metadata
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum, unique
from typing import Any, Final, Self

from pydantic import BaseModel, ConfigDict, Field

from dr_exec.declare import (
    UNBUDGETED,
    Budgets,
    EnvironmentGrant,
)

EXECUTOR_IDENTITY: Final[str] = f"dr-exec@{importlib.metadata.version('dr-exec')}"
"""Which machinery produced a run — for cache keys and dataset provenance.

A declared value, never inferred from callable identity: wrapping or
partial-applying an entry point must not change what a run claims to be.
This is not a *runtime* identity; platform and interpreter provenance stay
the consumer's to declare.
"""

FAKE_EXECUTOR_IDENTITY: Final[str] = (
    f"dr-exec-fake@{importlib.metadata.version('dr-exec')}"
)
"""The fake's distinct identity, so its outcomes can never cache-collide."""

RECORD_SCHEMA_VERSION: Final[int] = 1

UNBUDGETED_WIRE_VALUE: Final[str] = "unbudgeted"
"""How an unbudgeted axis serializes — a pinned persisted-format literal."""

_RECORD_TIMESTAMP_FORMAT: Final[str] = "%Y%m%dT%H%M%S%f"


@unique
class Attribution(StrEnum):
    """Which party a failure belongs to. Exactly one per run.

    These literals are persisted format: consumers write them into durable
    artifacts and cache keys, so they change only by contract revision.
    Never iterate this enum to build a payload — a wire payload names the
    members it needs.

    ``PAYLOAD`` is never assigned by elimination; an unknown cause is
    ``EXECUTOR``, and a ``CHANNEL`` claim requires evidence.
    """

    PAYLOAD = "payload"
    EXECUTOR = "executor"
    CHANNEL = "channel"
    BUDGET = "budget"
    MACHINE = "machine"
    ABSENCE = "absence"


@unique
class BudgetAxis(StrEnum):
    """Which resource axis a budget outcome violated.

    Persisted format; never iterate to build a payload.
    """

    WALL_CLOCK = "wall_clock"
    OUTPUT = "output"
    INPUT = "input"


@unique
class TrustCategory(StrEnum):
    """Who authored the payload, declared by which entry point ran.

    Persisted format; never iterate to build a payload.
    """

    TRUSTED_TOOL = "trusted_tool"
    UNTRUSTED_PYTHON = "untrusted_python"
    UNTRUSTED_COMMAND = "untrusted_command"


@unique
class RecordStatus(StrEnum):
    """How far a record got. A write failure never fails the run.

    Persisted format; never iterate to build a payload.
    """

    SPAWNED = "spawned"
    FINALIZED = "finalized"
    WRITE_FAILED = "write_failed"


@unique
class RecordKey(StrEnum):
    """Every JSON key in the record wire format.

    Consumers derive persisted cache keys from these, so they are pinned at
    exact-literal level and never derived from field names. Never iterate
    this enum to build a payload.
    """

    SCHEMA_VERSION = "schema_version"
    EXECUTOR_IDENTITY = "executor_identity"
    TRUST_CATEGORY = "trust_category"
    RUN_ID = "run_id"
    ARGV = "argv"
    SOURCE_DIGEST = "source_digest"
    INPUT_DIGEST = "input_digest"
    GRANT_KIND = "grant_kind"
    GRANT_NAMES = "grant_names"
    GRANT_EXCLUSIONS = "grant_exclusions"
    GRANT_CONTENTS_DIGEST = "grant_contents_digest"
    PROFILE_NAME = "profile_name"
    BUDGET_WALL_CLOCK_SECONDS = "budget_wall_clock_seconds"
    BUDGET_OUTPUT_BYTES = "budget_output_bytes"
    BUDGET_OUTPUT_OVERFLOW_POLICY = "budget_output_overflow_policy"
    BUDGET_INPUT_BYTES = "budget_input_bytes"
    UNBUDGETED_AXES = "unbudgeted_axes"
    RUNTIME_NAME = "runtime_name"
    RUNTIME_INTERPRETER = "runtime_interpreter"
    STARTED_AT = "started_at"
    FINISHED_AT = "finished_at"
    ATTRIBUTION = "attribution"
    VIOLATED_AXIS = "violated_axis"
    SPAWN_ERRNO = "spawn_errno"
    EXIT_VERDICT = "exit_verdict"
    RETURNCODE = "returncode"
    DURATION_SECONDS = "duration_seconds"
    TEARDOWN_SECONDS = "teardown_seconds"
    STDOUT_BYTES_PRODUCED = "stdout_bytes_produced"
    STDERR_BYTES_PRODUCED = "stderr_bytes_produced"
    INPUT_BYTES = "input_bytes"
    STDOUT_BYTES_DROPPED = "stdout_bytes_dropped"
    STDERR_BYTES_DROPPED = "stderr_bytes_dropped"
    OUTPUTS_LOCATION = "outputs_location"
    SCRATCH_PATH = "scratch_path"
    RECORD_STATUS = "record_status"


@unique
class OutputsLocation(StrEnum):
    """Where a run's output landed — the seam spooled delivery extends.

    Persisted format; never iterate to build a payload.
    """

    CAPTURED = "captured"


@dataclass(frozen=True, slots=True)
class Outcome:
    """The single attributed verdict on a run.

    ``violated_axis`` is set exactly when the attribution is ``BUDGET``;
    ``spawn_errno`` is preserved for ``MACHINE`` spawn failures so EACCES
    and ENOEXEC stay distinguishable.
    """

    attribution: Attribution
    violated_axis: BudgetAxis | None = None
    spawn_errno: int | None = None
    exit_verdict: str | None = None

    def __post_init__(self) -> None:
        if (self.attribution is Attribution.BUDGET) != (self.violated_axis is not None):
            raise ValueError("a budget attribution names exactly one violated axis")


@dataclass(frozen=True, slots=True)
class TruncationMark:
    """What a budget bound dropped, per stream, and never in-band.

    Marks are metadata: captured output stays byte-exact for what was kept.
    """

    stdout_bytes_dropped: int = 0
    stderr_bytes_dropped: int = 0

    @property
    def any_dropped(self) -> bool:
        return bool(self.stdout_bytes_dropped or self.stderr_bytes_dropped)


@dataclass(frozen=True, slots=True)
class Measurements:
    """What the run cost. Bytes counted are bytes *produced*, not retained.

    The executor keeps counting past a truncation bound, so a consumer can
    size a bound from an overflowing run. ``duration_seconds`` is
    spawn-to-reap on the monotonic clock, excluding parent-side setup.
    """

    duration_seconds: float
    teardown_seconds: float
    stdout_bytes_produced: int
    stderr_bytes_produced: int
    input_bytes: int


@dataclass(frozen=True, slots=True)
class RunResult:
    """The in-memory answer for every run that spawned.

    ``returncode`` is raw, including negative signal values, and is ``None``
    only when no status exists (absence, spawn failure). Consumers branch on
    ``outcome.attribution`` first: an executor-inflicted kill can never
    masquerade as a payload crash.
    """

    returncode: int | None
    stdout: str
    stderr: str
    truncation: TruncationMark
    measurements: Measurements
    outcome: Outcome


class RunRecord(BaseModel):
    """The durable twin of a run result, written at spawn, kept regardless.

    Field names are Python-side only; every serialized key comes from
    :class:`RecordKey`, so a field rename can never silently move the wire
    format.
    """

    model_config = ConfigDict(frozen=True, populate_by_name=True, extra="forbid")

    schema_version: int = Field(
        default=RECORD_SCHEMA_VERSION, alias=RecordKey.SCHEMA_VERSION.value
    )
    executor_identity: str = Field(alias=RecordKey.EXECUTOR_IDENTITY.value)
    trust_category: TrustCategory = Field(alias=RecordKey.TRUST_CATEGORY.value)
    run_id: str = Field(alias=RecordKey.RUN_ID.value)

    argv: tuple[str, ...] | None = Field(default=None, alias=RecordKey.ARGV.value)
    source_digest: str | None = Field(default=None, alias=RecordKey.SOURCE_DIGEST.value)
    input_digest: str = Field(alias=RecordKey.INPUT_DIGEST.value)

    grant_kind: str = Field(alias=RecordKey.GRANT_KIND.value)
    grant_names: tuple[str, ...] = Field(alias=RecordKey.GRANT_NAMES.value)
    grant_exclusions: tuple[str, ...] = Field(
        default=(), alias=RecordKey.GRANT_EXCLUSIONS.value
    )
    grant_contents_digest: str = Field(alias=RecordKey.GRANT_CONTENTS_DIGEST.value)

    profile_name: str | None = Field(default=None, alias=RecordKey.PROFILE_NAME.value)

    budget_wall_clock_seconds: float | str = Field(
        alias=RecordKey.BUDGET_WALL_CLOCK_SECONDS.value
    )
    budget_output_bytes: int | str = Field(alias=RecordKey.BUDGET_OUTPUT_BYTES.value)
    budget_output_overflow_policy: str = Field(
        alias=RecordKey.BUDGET_OUTPUT_OVERFLOW_POLICY.value
    )
    budget_input_bytes: int | str = Field(alias=RecordKey.BUDGET_INPUT_BYTES.value)
    unbudgeted_axes: tuple[str, ...] = Field(alias=RecordKey.UNBUDGETED_AXES.value)

    runtime_name: str | None = Field(default=None, alias=RecordKey.RUNTIME_NAME.value)
    runtime_interpreter: str | None = Field(
        default=None, alias=RecordKey.RUNTIME_INTERPRETER.value
    )

    started_at: str = Field(alias=RecordKey.STARTED_AT.value)
    finished_at: str | None = Field(default=None, alias=RecordKey.FINISHED_AT.value)

    attribution: Attribution | None = Field(
        default=None, alias=RecordKey.ATTRIBUTION.value
    )
    violated_axis: BudgetAxis | None = Field(
        default=None, alias=RecordKey.VIOLATED_AXIS.value
    )
    spawn_errno: int | None = Field(default=None, alias=RecordKey.SPAWN_ERRNO.value)
    exit_verdict: str | None = Field(default=None, alias=RecordKey.EXIT_VERDICT.value)
    returncode: int | None = Field(default=None, alias=RecordKey.RETURNCODE.value)

    duration_seconds: float | None = Field(
        default=None, alias=RecordKey.DURATION_SECONDS.value
    )
    teardown_seconds: float | None = Field(
        default=None, alias=RecordKey.TEARDOWN_SECONDS.value
    )
    stdout_bytes_produced: int | None = Field(
        default=None, alias=RecordKey.STDOUT_BYTES_PRODUCED.value
    )
    stderr_bytes_produced: int | None = Field(
        default=None, alias=RecordKey.STDERR_BYTES_PRODUCED.value
    )
    input_bytes: int | None = Field(default=None, alias=RecordKey.INPUT_BYTES.value)
    stdout_bytes_dropped: int = Field(
        default=0, alias=RecordKey.STDOUT_BYTES_DROPPED.value
    )
    stderr_bytes_dropped: int = Field(
        default=0, alias=RecordKey.STDERR_BYTES_DROPPED.value
    )

    outputs_location: OutputsLocation = Field(alias=RecordKey.OUTPUTS_LOCATION.value)
    scratch_path: str | None = Field(default=None, alias=RecordKey.SCRATCH_PATH.value)
    record_status: RecordStatus = Field(alias=RecordKey.RECORD_STATUS.value)

    def to_wire(self) -> dict[str, Any]:
        """Serialize under the pinned keys, every field present."""
        return self.model_dump(mode="json", by_alias=True)

    def finalized_with(
        self,
        *,
        result: RunResult,
        finished_at: datetime,
        record_status: RecordStatus = RecordStatus.FINALIZED,
    ) -> Self:
        """The exit-time twin of a spawn-time record."""
        return self.model_copy(
            update={
                "finished_at": format_record_timestamp(finished_at),
                "attribution": result.outcome.attribution,
                "violated_axis": result.outcome.violated_axis,
                "spawn_errno": result.outcome.spawn_errno,
                "exit_verdict": result.outcome.exit_verdict,
                "returncode": result.returncode,
                "duration_seconds": result.measurements.duration_seconds,
                "teardown_seconds": result.measurements.teardown_seconds,
                "stdout_bytes_produced": result.measurements.stdout_bytes_produced,
                "stderr_bytes_produced": result.measurements.stderr_bytes_produced,
                "input_bytes": result.measurements.input_bytes,
                "stdout_bytes_dropped": result.truncation.stdout_bytes_dropped,
                "stderr_bytes_dropped": result.truncation.stderr_bytes_dropped,
                "record_status": record_status,
            }
        )


@dataclass(frozen=True, slots=True)
class SerializedBudgets:
    """Budgets as they land on the wire, unbudgeted axes named explicitly."""

    wall_clock_seconds: float | str
    output_bytes: int | str
    output_overflow_policy: str
    input_bytes: int | str
    unbudgeted_axes: tuple[str, ...] = field(default=())


def serialize_budgets(budgets: Budgets) -> SerializedBudgets:
    """Render budgets for the record: every axis stated, none implied."""
    unbudgeted: list[str] = []

    if budgets.wall_clock is UNBUDGETED:
        wall_clock: float | str = UNBUDGETED_WIRE_VALUE
        unbudgeted.append(BudgetAxis.WALL_CLOCK.value)
    else:
        wall_clock = float(budgets.wall_clock)

    if budgets.output is UNBUDGETED:
        output_bytes: int | str = UNBUDGETED_WIRE_VALUE
        overflow_policy: str = UNBUDGETED_WIRE_VALUE
        unbudgeted.append(BudgetAxis.OUTPUT.value)
    else:
        output_bytes = budgets.output.limit_bytes
        overflow_policy = budgets.output.overflow_policy.value

    if budgets.input is UNBUDGETED:
        input_bytes: int | str = UNBUDGETED_WIRE_VALUE
        unbudgeted.append(BudgetAxis.INPUT.value)
    else:
        input_bytes = int(budgets.input)

    return SerializedBudgets(
        wall_clock_seconds=wall_clock,
        output_bytes=output_bytes,
        output_overflow_policy=overflow_policy,
        input_bytes=input_bytes,
        unbudgeted_axes=tuple(unbudgeted),
    )


def serialize_grant(grant: EnvironmentGrant) -> dict[str, Any]:
    """Render a grant as sorted names plus a value-sensitive digest.

    Values never land in a record: redaction is the caller's, and secrets
    stay out of durable artifacts.
    """
    return {
        RecordKey.GRANT_KIND.value: grant.kind.value,
        RecordKey.GRANT_NAMES.value: grant.declared_names,
        RecordKey.GRANT_EXCLUSIONS.value: grant.exclusions,
        RecordKey.GRANT_CONTENTS_DIGEST.value: grant.contents_digest(),
    }


def format_record_timestamp(moment: datetime) -> str:
    """ISO-8601 UTC, the pinned record timestamp form."""
    return moment.astimezone(UTC).isoformat()


def record_filename(*, started_at: datetime, run_id: str) -> str:
    """``run-<utc-timestamp>-<uuid>.json`` — collision-free under concurrency."""
    stamp = started_at.astimezone(UTC).strftime(_RECORD_TIMESTAMP_FORMAT)
    return f"run-{stamp}-{run_id}.json"


def new_run_id() -> str:
    """A fresh run identifier: uuid4 hex."""
    return uuid.uuid4().hex
