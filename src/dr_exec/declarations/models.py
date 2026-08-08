from __future__ import annotations

import os
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import Annotated, Final, Literal, Self

from dr_serialize import Sha256Digest, canonical_sorted_values
from pydantic import (
    Field,
    NonNegativeInt,
    PositiveInt,
    field_validator,
    model_validator,
)

from dr_exec.core.kinds import (
    BudgetAxis,
    ContainmentProfile,
    EnvGrantKind,
    ExecutionTargetKind,
    LimitKind,
    OutputOverflowPolicy,
)
from dr_exec.core.model import (
    Base64UrlBytes,
    ContractModel,
    IdentityDocumentField,
)
from dr_exec.core.names import JobId


class UnbudgetedLimit(ContractModel):
    kind: Literal[LimitKind.UNBUDGETED] = LimitKind.UNBUDGETED


class FiniteByteLimit(ContractModel):
    kind: Literal[LimitKind.FINITE] = LimitKind.FINITE
    max_bytes: PositiveInt


class FiniteDurationLimit(ContractModel):
    kind: Literal[LimitKind.FINITE] = LimitKind.FINITE
    max_ns: PositiveInt


class FiniteCountLimit(ContractModel):
    kind: Literal[LimitKind.FINITE] = LimitKind.FINITE
    max_count: PositiveInt


type ByteBudget = Annotated[
    UnbudgetedLimit | FiniteByteLimit,
    Field(discriminator="kind"),
]
type DurationBudget = Annotated[
    UnbudgetedLimit | FiniteDurationLimit,
    Field(discriminator="kind"),
]
type CountBudget = Annotated[
    UnbudgetedLimit | FiniteCountLimit,
    Field(discriminator="kind"),
]


class UnbudgetedOutput(ContractModel):
    kind: Literal[LimitKind.UNBUDGETED] = LimitKind.UNBUDGETED


class StreamRetentionBudget(ContractModel):
    head_bytes: NonNegativeInt
    tail_bytes: NonNegativeInt


class PayloadRetentionBudget(ContractModel):
    stdout: StreamRetentionBudget
    stderr: StreamRetentionBudget


class FiniteOutput(ContractModel):
    kind: Literal[LimitKind.FINITE] = LimitKind.FINITE
    max_bytes: PositiveInt
    overflow_policy: OutputOverflowPolicy
    retention: PayloadRetentionBudget

    @model_validator(mode="after")
    def retention_must_match_max_bytes(self) -> Self:
        retained_bytes = (
            self.retention.stdout.head_bytes
            + self.retention.stdout.tail_bytes
            + self.retention.stderr.head_bytes
            + self.retention.stderr.tail_bytes
        )
        if retained_bytes != self.max_bytes:
            raise ValueError("retention bytes must sum to max_bytes")
        return self


type OutputBudget = Annotated[
    UnbudgetedOutput | FiniteOutput,
    Field(discriminator="kind"),
]


_UNSUPPORTED_FINITE_WORKLOAD_AXES: Final = (
    BudgetAxis.MEMORY_BYTES,
    BudgetAxis.CPU_TIME,
    BudgetAxis.PROCESS_COUNT,
    BudgetAxis.FILE_SIZE_BYTES,
    BudgetAxis.OPEN_FILE_COUNT,
    BudgetAxis.DISK_BYTES,
)


class Budgets(ContractModel):
    wall_time: DurationBudget = Field(default_factory=UnbudgetedLimit)
    input_bytes: ByteBudget = Field(default_factory=UnbudgetedLimit)
    payload_output: OutputBudget = Field(default_factory=UnbudgetedOutput)
    memory_bytes: ByteBudget = Field(default_factory=UnbudgetedLimit)
    cpu_time: DurationBudget = Field(default_factory=UnbudgetedLimit)
    process_count: CountBudget = Field(default_factory=UnbudgetedLimit)
    file_size_bytes: ByteBudget = Field(default_factory=UnbudgetedLimit)
    open_file_count: CountBudget = Field(default_factory=UnbudgetedLimit)
    disk_bytes: ByteBudget = Field(default_factory=UnbudgetedLimit)

    @model_validator(mode="after")
    def unsupported_axes_must_be_unbudgeted(self) -> Budgets:
        """Reject finite limits for axes v1 cannot enforce."""
        declared_finite = [
            axis
            for axis in _UNSUPPORTED_FINITE_WORKLOAD_AXES
            if getattr(self, axis).kind is LimitKind.FINITE
        ]
        if declared_finite:
            raise ValueError(
                f"{', '.join(declared_finite)} accept no finite limit in "
                "v1 and must be unbudgeted"
            )
        return self

    @classmethod
    def unbudgeted(cls) -> Budgets:
        return cls()


class ExecutorSelfBudgets(ContractModel):
    protocol_frame_bytes: ByteBudget = Field(default_factory=UnbudgetedLimit)
    protocol_total_bytes: ByteBudget = Field(default_factory=UnbudgetedLimit)
    protocol_output_count: CountBudget = Field(default_factory=UnbudgetedLimit)
    json_depth: CountBudget = Field(default_factory=UnbudgetedLimit)
    manifest_bytes: ByteBudget = Field(default_factory=UnbudgetedLimit)
    narration_bytes: ByteBudget = Field(default_factory=UnbudgetedLimit)
    recording_failure_count: CountBudget = Field(
        default_factory=UnbudgetedLimit
    )
    failure_detail_bytes: ByteBudget = Field(default_factory=UnbudgetedLimit)
    startup_time: DurationBudget = Field(default_factory=UnbudgetedLimit)
    termination_time: DurationBudget = Field(default_factory=UnbudgetedLimit)
    join_time: DurationBudget = Field(default_factory=UnbudgetedLimit)

    @classmethod
    def unbudgeted(cls) -> ExecutorSelfBudgets:
        return cls()


def _validate_env_var_name(name: str) -> None:
    if not name or "=" in name or "\0" in name:
        raise ValueError(
            "environment variable names must be nonempty and "
            "contain neither '=' nor NUL"
        )


def _validate_env_var_value(value: str) -> None:
    if "\0" in value:
        raise ValueError("environment variable values must not contain NUL")


@dataclass(frozen=True, slots=True)
class EnvVar:
    name: str
    value: str = dataclass_field(repr=False)

    def __post_init__(self) -> None:
        _validate_env_var_name(self.name)
        _validate_env_var_value(self.value)


@dataclass(frozen=True, slots=True)
class EnvGrant:
    kind: EnvGrantKind
    variables: tuple[EnvVar, ...]
    excluded_var_names: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        names = tuple(variable.name for variable in self.variables)
        if self.kind == EnvGrantKind.NONE and names:
            raise ValueError(
                "none environment grants must not contain variables"
            )
        if self.kind != EnvGrantKind.OVERLAY and self.excluded_var_names:
            raise ValueError(
                "only overlay environment grants may exclude variables"
            )
        if len(set(names)) != len(names):
            raise ValueError("environment variable names must be unique")
        for name in self.excluded_var_names:
            _validate_env_var_name(name)
        if len(set(self.excluded_var_names)) != len(self.excluded_var_names):
            raise ValueError("excluded variable names must be unique")
        if set(names) & set(self.excluded_var_names):
            raise ValueError("granted and excluded variable names must differ")

    @classmethod
    def none(cls) -> EnvGrant:
        return cls(kind=EnvGrantKind.NONE, variables=())

    @classmethod
    def named(cls, var_names: Iterable[str]) -> EnvGrant:
        names = tuple(sorted(var_names))
        variables = tuple(EnvVar(name, os.environ[name]) for name in names)
        return cls(kind=EnvGrantKind.NAMED, variables=variables)

    @classmethod
    def fixed(cls, variables: Mapping[str, str]) -> EnvGrant:
        resolved = tuple(
            EnvVar(name, value) for name, value in sorted(variables.items())
        )
        return cls(kind=EnvGrantKind.FIXED, variables=resolved)

    @classmethod
    def overlay(
        cls,
        extra_variables: Mapping[str, str],
        *,
        exclusions: Iterable[str] = (),
    ) -> EnvGrant:
        excluded = tuple(sorted(exclusions))
        values = {
            name: value
            for name, value in os.environ.items()
            if name not in excluded
        }
        values.update(extra_variables)
        resolved = tuple(
            EnvVar(name, value) for name, value in sorted(values.items())
        )
        return cls(
            kind=EnvGrantKind.OVERLAY,
            variables=resolved,
            excluded_var_names=excluded,
        )


class EnvGrantRecord(ContractModel):
    kind: EnvGrantKind
    var_names: tuple[str, ...]
    excluded_var_names: tuple[str, ...]
    canonical_values_sha256: Sha256Digest

    @model_validator(mode="after")
    def names_must_be_canonical_and_disjoint(self) -> EnvGrantRecord:
        if self.kind == EnvGrantKind.NONE and self.var_names:
            raise ValueError(
                "none environment grants must not contain variables"
            )
        if self.kind != EnvGrantKind.OVERLAY and self.excluded_var_names:
            raise ValueError(
                "only overlay environment grants may exclude variables"
            )
        for name in (*self.var_names, *self.excluded_var_names):
            _validate_env_var_name(name)
        # Persist unordered names in canonical JSON text order.
        if self.var_names != tuple(
            canonical_sorted_values(set(self.var_names))
        ):
            raise ValueError("var_names must be sorted and unique")
        if self.excluded_var_names != tuple(
            canonical_sorted_values(set(self.excluded_var_names))
        ):
            raise ValueError("excluded_var_names must be sorted and unique")
        if set(self.var_names) & set(self.excluded_var_names):
            raise ValueError("granted and excluded variable names must differ")
        return self


def _validate_argv(argv: tuple[str, ...]) -> tuple[str, ...]:
    if not argv:
        raise ValueError("argv must not be empty")
    if any(not argument or "\0" in argument for argument in argv):
        raise ValueError("argv elements must be nonempty and NUL-free")
    return argv


class TrustedCommandTarget(ContractModel):
    kind: Literal[ExecutionTargetKind.TRUSTED_COMMAND] = (
        ExecutionTargetKind.TRUSTED_COMMAND
    )
    argv: tuple[str, ...]
    stdin: Base64UrlBytes = b""

    _validated_argv = field_validator("argv")(_validate_argv)


class UntrustedCommandTarget(ContractModel):
    kind: Literal[ExecutionTargetKind.UNTRUSTED_COMMAND] = (
        ExecutionTargetKind.UNTRUSTED_COMMAND
    )
    argv: tuple[str, ...]
    stdin: Base64UrlBytes = b""
    containment_profile: ContainmentProfile

    _validated_argv = field_validator("argv")(_validate_argv)


def _validate_driver_source(source: str) -> str:
    if "\0" in source:
        raise ValueError("driver_source must not contain NUL")
    return source


class TrustedPythonTarget(ContractModel):
    kind: Literal[ExecutionTargetKind.TRUSTED_PYTHON] = (
        ExecutionTargetKind.TRUSTED_PYTHON
    )
    driver_source: str = Field(
        description=(
            "Python source defining dr_exec_main(request, emit); the "
            "library-owned bootstrap opens fd 3 before loading it"
        )
    )
    request: IdentityDocumentField

    _validated_driver_source = field_validator("driver_source")(
        _validate_driver_source
    )


class UntrustedPythonTarget(ContractModel):
    kind: Literal[ExecutionTargetKind.UNTRUSTED_PYTHON] = (
        ExecutionTargetKind.UNTRUSTED_PYTHON
    )
    driver_source: str = Field(
        description=(
            "Python source defining dr_exec_main(request, emit); the "
            "library-owned bootstrap opens fd 3 before loading it"
        )
    )
    request: IdentityDocumentField
    containment_profile: ContainmentProfile

    _validated_driver_source = field_validator("driver_source")(
        _validate_driver_source
    )


type ExecutionTarget = Annotated[
    TrustedCommandTarget
    | TrustedPythonTarget
    | UntrustedCommandTarget
    | UntrustedPythonTarget,
    Field(discriminator="kind"),
]


@dataclass(frozen=True, slots=True)
class ExecutionJob:
    job_id: JobId
    target: ExecutionTarget
    env: EnvGrant
    budgets: Budgets = dataclass_field(default_factory=Budgets.unbudgeted)


__all__ = [
    "Budgets",
    "ByteBudget",
    "CountBudget",
    "DurationBudget",
    "EnvGrant",
    "EnvGrantRecord",
    "EnvVar",
    "ExecutionJob",
    "ExecutionTarget",
    "ExecutorSelfBudgets",
    "FiniteByteLimit",
    "FiniteCountLimit",
    "FiniteDurationLimit",
    "FiniteOutput",
    "OutputBudget",
    "PayloadRetentionBudget",
    "StreamRetentionBudget",
    "TrustedCommandTarget",
    "TrustedPythonTarget",
    "UnbudgetedLimit",
    "UnbudgetedOutput",
    "UntrustedCommandTarget",
    "UntrustedPythonTarget",
]
