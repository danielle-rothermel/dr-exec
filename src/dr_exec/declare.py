"""Declaration types: budgets, grants, containment profiles, exit policies.

Every declaration is a frozen internal value object. A declaration answers
"what did the caller ask for", never "what happened" — results and records
live in :mod:`dr_exec.record`.
"""

from __future__ import annotations

import hashlib
import math
import os
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from enum import StrEnum, unique
from pathlib import Path
from typing import Final, Literal, Self

SOURCE_BOUND_BYTES: Final[int] = 96 * 1024
"""Pre-spawn ceiling on untrusted Python source delivered as one argument.

Derived from the platform per-argument exec limit (Linux ``MAX_ARG_STRLEN``
is 128 KiB); the gap is declared headroom, not an interior default.
"""

INVOCATION_AGGREGATE_BOUND_BYTES: Final[int] = 1024 * 1024
"""Pre-spawn ceiling on argv plus granted environment, an ``ARG_MAX`` floor."""

TERMINATION_SELF_BUDGET_SECONDS: Final[float] = 5.0
"""Executor self-budget bounding group teardown, escalation, and reap."""

IPC_JOIN_SELF_BUDGET_SECONDS: Final[float] = 1.0
"""Executor self-budget bounding the join of the feed and drain threads."""

STARTUP_SELF_BUDGET_SECONDS: Final[float] = 30.0
"""Executor self-budget bounding the spawn attempt itself."""


class _Unbudgeted:
    """The explicit absence of a budget on an axis.

    A singleton so an axis is either a declared budget or this value; there
    is no unset state and no third case.
    """

    __slots__ = ()

    _instance: _Unbudgeted | None = None

    def __new__(cls) -> _Unbudgeted:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:
        return "UNBUDGETED"

    def __reduce__(self) -> str:
        return "UNBUDGETED"


UNBUDGETED: Final[_Unbudgeted] = _Unbudgeted()


@unique
class OverflowPolicy(StrEnum):
    """What an output budget does when the bound is crossed.

    Values are persisted-format strings: they land in run records and in
    consumer cache keys. Never build a payload by iterating this enum.
    """

    FAIL = "fail"
    MARKED_TRUNCATION = "marked_truncation"


def _validate_positive_seconds(value: float, axis: str) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError(f"{axis} budget must be a finite positive number")
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"{axis} budget must be a finite positive number")
    return float(value)


def _validate_positive_bytes(value: int, axis: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{axis} budget must be a positive integer of bytes")
    if value <= 0:
        raise ValueError(f"{axis} budget must be a positive integer of bytes")
    return value


@dataclass(frozen=True, slots=True)
class OutputBudget:
    """A byte bound shared across stdout and stderr, plus its overflow policy.

    The bound counts bytes on the raw streams before decoding, so a budget
    boundary never moves because an encoding changed.
    """

    limit_bytes: int
    overflow_policy: OverflowPolicy

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "limit_bytes", _validate_positive_bytes(self.limit_bytes, "output")
        )
        if not isinstance(self.overflow_policy, OverflowPolicy):
            raise ValueError("output budget requires an OverflowPolicy")


@dataclass(frozen=True, slots=True)
class StreamBounds:
    """Per-stream capture bounds, for the one case a protocol demands them.

    Plain runs keep the single shared output bound; a run whose streams
    carry different contracts — a protocol channel on stdout, payload on
    stderr — declares a bound per stream so a flood on one can never consume
    the other's budget. ``None`` on an axis means that stream is bounded only
    by the run's declared output budget. The overflow policy stays the
    caller's declared one; these bounds change where the bound sits, never
    what crossing it means.
    """

    stdout_bytes: int | None = None
    stderr_bytes: int | None = None

    def __post_init__(self) -> None:
        for name in ("stdout_bytes", "stderr_bytes"):
            value = getattr(self, name)
            if value is None:
                continue
            object.__setattr__(self, name, _validate_positive_bytes(value, name))


type WallClockBudget = float | _Unbudgeted
type SharedOutputBudget = OutputBudget | _Unbudgeted
type InputBudget = int | _Unbudgeted


@dataclass(frozen=True, slots=True)
class Budgets:
    """The three v1-budgeted axes, each declared or explicitly unbudgeted.

    Memory, CPU time, processes, file size, and open files are visibly
    unbudgeted in v1: the run record declares them so, never silently
    unenforced.
    """

    wall_clock: WallClockBudget = UNBUDGETED
    output: SharedOutputBudget = UNBUDGETED
    input: InputBudget = UNBUDGETED

    def __post_init__(self) -> None:
        if self.wall_clock is not UNBUDGETED:
            object.__setattr__(
                self,
                "wall_clock",
                _validate_positive_seconds(self.wall_clock, "wall_clock"),
            )
        if self.output is not UNBUDGETED and not isinstance(self.output, OutputBudget):
            raise ValueError("output budget must be an OutputBudget or UNBUDGETED")
        if self.input is not UNBUDGETED:
            object.__setattr__(
                self, "input", _validate_positive_bytes(self.input, "input")
            )


@unique
class GrantKind(StrEnum):
    """Which environment grant shape a declaration used.

    Values are persisted-format strings. Never build a payload by iterating
    this enum.
    """

    NONE = "none"
    NAMED = "named"
    FIXED = "fixed"
    OVERLAY = "overlay"


def _validate_name(name: str) -> str:
    if not isinstance(name, str) or not name or "=" in name or "\0" in name:
        raise ValueError(
            "environment variable names must be nonempty, without '=' or NUL"
        )
    return name


def _validate_value(value: str, name: str) -> str:
    if not isinstance(value, str) or "\0" in value:
        raise ValueError(f"environment value for {name} must be text without NUL")
    return value


@dataclass(frozen=True, slots=True)
class EnvironmentGrant:
    """A frozen snapshot of what environment the child receives.

    ``named`` resolves values from the parent environment at *construction*,
    never at spawn, so an identity derived from a grant is a claim every
    later run honors.

    Values are secrets by assumption: they are never in ``repr`` and never
    persisted. :meth:`contents_digest` gives value-sensitive identity
    without disclosure.
    """

    kind: GrantKind
    resolved: Mapping[str, str]
    exclusions: tuple[str, ...] = ()

    def __repr__(self) -> str:
        return (
            f"EnvironmentGrant(kind={self.kind.value!r}, "
            f"declared_names={self.declared_names!r}, "
            f"exclusions={self.exclusions!r})"
        )

    @property
    def declared_names(self) -> tuple[str, ...]:
        """The sorted names this grant delivers, beyond any overlay base."""
        return tuple(sorted(self.resolved))

    @classmethod
    def none(cls) -> Self:
        """Grant nothing: the child's environment is empty."""
        return cls(kind=GrantKind.NONE, resolved={})

    @classmethod
    def named(cls, names: Iterable[str]) -> Self:
        """Grant the listed parent variables, resolved now and frozen.

        Names absent from the parent environment are absent from the grant;
        the snapshot reflects what the child will actually receive.
        """
        snapshot = dict(os.environ)
        resolved = {}
        for name in names:
            _validate_name(name)
            if name in snapshot:
                resolved[name] = _validate_value(snapshot[name], name)
        return cls(kind=GrantKind.NAMED, resolved=resolved)

    @classmethod
    def fixed(cls, mapping: Mapping[str, str]) -> Self:
        """Grant a literal environment; nothing is read from the parent."""
        resolved = {
            _validate_name(name): _validate_value(value, name)
            for name, value in mapping.items()
        }
        return cls(kind=GrantKind.FIXED, resolved=resolved)

    @classmethod
    def overlay(
        cls,
        extra: Mapping[str, str],
        exclusions: Iterable[str] = (),
    ) -> Self:
        """Grant the whole parent environment plus extras minus exclusions.

        ``resolved`` holds only the extras — the parent base is read at
        spawn. Exclusions are stored here and verified absent pre-spawn.
        """
        resolved = {
            _validate_name(name): _validate_value(value, name)
            for name, value in extra.items()
        }
        excluded = tuple(_validate_name(name) for name in exclusions)
        return cls(kind=GrantKind.OVERLAY, resolved=resolved, exclusions=excluded)

    def contents_digest(self) -> str:
        """SHA-256 hex over the canonicalized grant contents.

        Canonicalization: names sorted, each rendered ``name=value``, joined
        by ``"\\0"``, encoded UTF-8. Value-sensitive identity that never
        persists a value.
        """
        payload = "\0".join(
            f"{name}={self.resolved[name]}" for name in sorted(self.resolved)
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class ContainmentProfile:
    """A named, complete declaration of what a payload may reach.

    ``declared_limits`` is contract text, not a docstring caveat: what the
    profile does *not* contain is part of what it promises.
    """

    name: str
    declared_limits: str


PROCESS_BOUNDARY_ONLY: Final[ContainmentProfile] = ContainmentProfile(
    name="process_boundary_only",
    declared_limits=(
        "Restricts nothing beyond the process boundary. The payload runs as "
        "the invoking user with full filesystem read and write reach, full "
        "network reach, full credential reach, and unrestricted process "
        "spawning. Enforced isolation is limited to a fresh process session, "
        "an explicitly granted environment, a per-run scratch working "
        "directory, and the declared budgets."
    ),
)


@unique
class ExitVerdict(StrEnum):
    """What a caller's exit policy makes of an exit status.

    Values are persisted-format strings. Never build a payload by iterating
    this enum.
    """

    REPORT_ONLY = "report_only"
    SUCCESS = "success"
    FAILURE = "failure"


@dataclass(frozen=True, slots=True)
class ExitPolicy:
    """A caller-declared mapping from exit status to meaning.

    Statuses absent from the mapping take ``default_verdict``. The executor
    reports the verdict; what to do about it stays the caller's.
    """

    name: str
    verdicts: Mapping[int, ExitVerdict] = field(default_factory=dict)
    default_verdict: ExitVerdict = ExitVerdict.REPORT_ONLY

    def verdict_for(self, returncode: int | None) -> ExitVerdict:
        if returncode is None:
            return self.default_verdict
        return self.verdicts.get(returncode, self.default_verdict)


REPORT_ONLY: Final[ExitPolicy] = ExitPolicy(name="report_only")


@unique
class RecordsKind(StrEnum):
    """Whether a run persists a record, and where.

    Values are persisted-format strings. Never build a payload by iterating
    this enum.
    """

    DIRECTORY = "directory"
    NONE = "none"


@dataclass(frozen=True, slots=True)
class Records:
    """Where a run's durable record lands.

    Required at every call site with no ambient default: a process-global
    record directory would be inherited state by another name, and would
    break under concurrent callers in one process.
    """

    kind: RecordsKind
    directory: Path | None = None

    @classmethod
    def directory_at(cls, path: Path | str) -> Self:
        """Write one JSON record file per run into ``path``."""
        return cls(kind=RecordsKind.DIRECTORY, directory=Path(path))

    @classmethod
    def none(cls) -> Self:
        """Write no record — the explicit choice, for hot loops."""
        return cls(kind=RecordsKind.NONE)


@dataclass(frozen=True, slots=True)
class PythonRuntime:
    """The declared interpreter and importable package set for Python source.

    ``interpreter=None`` means the running interpreter, resolved at spawn.
    """

    name: str
    interpreter: str | None = None
    isolated: bool = True
    packages: tuple[str, ...] = ()


HERMETIC: Final[PythonRuntime] = PythonRuntime(name="hermetic")
"""No undeclared inputs: ``interpreter -I -c <source>``, injecting nothing.

The child's environment is solely the caller's grant; ``-I`` drops
``PYTHON*`` environment, user site, and cwd from ``sys.path``.
"""


def contents_digest_of(text: str) -> str:
    """SHA-256 hex over UTF-8 text — the pinned digest of source and input."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


type BudgetWireValue = float | int | Literal["unbudgeted"] | dict[str, object]
