from __future__ import annotations

import os
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import Annotated, Literal, Self

from dr_serialize import IdentityDocument
from pydantic import (
    Field,
    NonNegativeInt,
    PositiveInt,
    field_validator,
    model_validator,
)

from dr_exec._model import ContractModel, _validate_sha256_digest
from dr_exec.kinds import (
    ContainmentProfile,
    EnvGrantKind,
    ExecutionTargetKind,
    LimitKind,
    OutputOverflowPolicy,
)
from dr_exec.names import JobId


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
    canonical_values_sha256: str

    _validated_canonical_values_sha256 = field_validator(
        "canonical_values_sha256"
    )(_validate_sha256_digest)


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
    stdin: bytes = b""

    _validated_argv = field_validator("argv")(_validate_argv)


class UntrustedCommandTarget(ContractModel):
    kind: Literal[ExecutionTargetKind.UNTRUSTED_COMMAND] = (
        ExecutionTargetKind.UNTRUSTED_COMMAND
    )
    argv: tuple[str, ...]
    stdin: bytes = b""
    containment_profile: ContainmentProfile

    _validated_argv = field_validator("argv")(_validate_argv)


class UntrustedPythonTarget(ContractModel):
    kind: Literal[ExecutionTargetKind.UNTRUSTED_PYTHON] = (
        ExecutionTargetKind.UNTRUSTED_PYTHON
    )
    driver_source: str
    request: IdentityDocument
    containment_profile: ContainmentProfile

    @field_validator("driver_source")
    @classmethod
    def driver_source_must_be_nul_free(cls, source: str) -> str:
        if "\0" in source:
            raise ValueError("driver_source must not contain NUL")
        return source


type ExecutionTarget = Annotated[
    TrustedCommandTarget | UntrustedCommandTarget | UntrustedPythonTarget,
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
    "UnbudgetedLimit",
    "UnbudgetedOutput",
    "UntrustedCommandTarget",
    "UntrustedPythonTarget",
]
