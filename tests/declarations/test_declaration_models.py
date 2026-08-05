"""Declaration construction and validation boundaries."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

import pytest
from dr_serialize import IdentityDocument, Sha256Digest
from pydantic import ValidationError

from dr_exec import (
    Budgets,
    ContainmentProfile,
    EnvGrant,
    EnvGrantKind,
    EnvGrantRecord,
    EnvVar,
    FiniteByteLimit,
    FiniteCountLimit,
    FiniteDurationLimit,
    FiniteOutput,
    OutputOverflowPolicy,
    PayloadRetentionBudget,
    StreamRetentionBudget,
    TrustedCommandTarget,
    UnbudgetedLimit,
    UntrustedCommandTarget,
    UntrustedPythonTarget,
)

if TYPE_CHECKING:
    from collections.abc import Callable

VALID_DIGEST = Sha256Digest("a" * 64)


def retention(total_bytes: int) -> PayloadRetentionBudget:
    return PayloadRetentionBudget(
        stdout=StreamRetentionBudget(head_bytes=total_bytes, tail_bytes=0),
        stderr=StreamRetentionBudget(head_bytes=0, tail_bytes=0),
    )


def test_finite_output_accepts_retention_equal_to_its_limit() -> None:
    output = FiniteOutput(
        max_bytes=4,
        overflow_policy=OutputOverflowPolicy.MARKED_TRUNCATION,
        retention=retention(4),
    )

    assert output.retention == retention(4)


@pytest.mark.parametrize("retained_bytes", [3, 5])
def test_finite_output_rejects_retention_unequal_to_its_limit(
    retained_bytes: int,
) -> None:
    with pytest.raises(ValidationError, match="must sum to max_bytes"):
        FiniteOutput(
            max_bytes=4,
            overflow_policy=OutputOverflowPolicy.MARKED_TRUNCATION,
            retention=retention(retained_bytes),
        )


@pytest.mark.parametrize(
    "name",
    [
        pytest.param("", id="empty"),
        pytest.param("A=B", id="equals"),
        pytest.param("A\0B", id="nul"),
    ],
)
def test_environment_variable_names_reject_os_invalid_spellings(
    name: str,
) -> None:
    with pytest.raises(ValueError, match="variable names must be nonempty"):
        EnvVar(name, "value")


def test_environment_variable_values_reject_nul() -> None:
    with pytest.raises(ValueError, match="values must not contain NUL"):
        EnvVar("NAME", "before\0after")


@pytest.mark.parametrize(
    ("grant", "message"),
    [
        pytest.param(
            lambda: EnvGrant(
                kind=EnvGrantKind.NONE,
                variables=(EnvVar("A", "value"),),
            ),
            "none environment grants",
            id="none-with-variable",
        ),
        pytest.param(
            lambda: EnvGrant(
                kind=EnvGrantKind.FIXED,
                variables=(),
                excluded_var_names=("A",),
            ),
            "only overlay",
            id="non-overlay-exclusion",
        ),
        pytest.param(
            lambda: EnvGrant(
                kind=EnvGrantKind.FIXED,
                variables=(EnvVar("A", "one"), EnvVar("A", "two")),
            ),
            "variable names must be unique",
            id="duplicate-variables",
        ),
        pytest.param(
            lambda: EnvGrant(
                kind=EnvGrantKind.OVERLAY,
                variables=(),
                excluded_var_names=("A", "A"),
            ),
            "excluded variable names must be unique",
            id="duplicate-exclusions",
        ),
        pytest.param(
            lambda: EnvGrant(
                kind=EnvGrantKind.OVERLAY,
                variables=(),
                excluded_var_names=("BAD=NAME",),
            ),
            "variable names must be nonempty",
            id="invalid-exclusion-name",
        ),
        pytest.param(
            lambda: EnvGrant(
                kind=EnvGrantKind.OVERLAY,
                variables=(EnvVar("A", "value"),),
                excluded_var_names=("A",),
            ),
            "granted and excluded variable names must differ",
            id="overlapping-grant-and-exclusion",
        ),
    ],
)
def test_environment_grants_reject_incoherent_shapes(
    grant: Callable[[], EnvGrant], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        grant()


def test_overlay_snapshots_exclusions_and_explicit_overrides(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ambient = {
        "DROP": "secret",
        "KEEP": "ambient",
        "OVERRIDE": "ambient",
    }
    explicit = {"OVERRIDE": "explicit", "NEW": "added"}
    monkeypatch.setattr(os, "environ", ambient)

    grant = EnvGrant.overlay(explicit, exclusions=("DROP",))
    ambient["KEEP"] = "changed"
    explicit["NEW"] = "changed"

    assert grant == EnvGrant(
        kind=EnvGrantKind.OVERLAY,
        variables=(
            EnvVar("KEEP", "ambient"),
            EnvVar("NEW", "added"),
            EnvVar("OVERRIDE", "explicit"),
        ),
        excluded_var_names=("DROP",),
    )


@pytest.mark.parametrize(
    ("record", "message"),
    [
        pytest.param(
            lambda: EnvGrantRecord(
                kind=EnvGrantKind.NONE,
                var_names=("A",),
                excluded_var_names=(),
                canonical_values_sha256=VALID_DIGEST,
            ),
            "none environment grants",
            id="none-with-variable",
        ),
        pytest.param(
            lambda: EnvGrantRecord(
                kind=EnvGrantKind.FIXED,
                var_names=("A",),
                excluded_var_names=("B",),
                canonical_values_sha256=VALID_DIGEST,
            ),
            "only overlay",
            id="non-overlay-exclusion",
        ),
        pytest.param(
            lambda: EnvGrantRecord(
                kind=EnvGrantKind.FIXED,
                var_names=("B", "A"),
                excluded_var_names=(),
                canonical_values_sha256=VALID_DIGEST,
            ),
            "var_names must be sorted and unique",
            id="unsorted-variables",
        ),
        pytest.param(
            lambda: EnvGrantRecord(
                kind=EnvGrantKind.FIXED,
                var_names=("A", "A"),
                excluded_var_names=(),
                canonical_values_sha256=VALID_DIGEST,
            ),
            "var_names must be sorted and unique",
            id="duplicate-variables",
        ),
        pytest.param(
            lambda: EnvGrantRecord(
                kind=EnvGrantKind.FIXED,
                var_names=("BAD=NAME",),
                excluded_var_names=(),
                canonical_values_sha256=VALID_DIGEST,
            ),
            "variable names must be nonempty",
            id="invalid-variable-name",
        ),
        pytest.param(
            lambda: EnvGrantRecord(
                kind=EnvGrantKind.OVERLAY,
                var_names=(),
                excluded_var_names=("A", "A"),
                canonical_values_sha256=VALID_DIGEST,
            ),
            "excluded_var_names must be sorted and unique",
            id="duplicate-exclusions",
        ),
        pytest.param(
            lambda: EnvGrantRecord(
                kind=EnvGrantKind.OVERLAY,
                var_names=("A",),
                excluded_var_names=("A",),
                canonical_values_sha256=VALID_DIGEST,
            ),
            "granted and excluded variable names must differ",
            id="overlapping-grant-and-exclusion",
        ),
    ],
)
def test_environment_records_reject_incoherent_shapes(
    record: Callable[[], EnvGrantRecord], message: str
) -> None:
    with pytest.raises(ValidationError, match=message):
        record()


@pytest.mark.parametrize(
    ("declare", "message"),
    [
        pytest.param(
            lambda: TrustedCommandTarget(argv=()),
            "argv must not be empty",
            id="trusted-empty-argv",
        ),
        pytest.param(
            lambda: TrustedCommandTarget(argv=("/bin/echo", "")),
            "argv elements must be nonempty and NUL-free",
            id="trusted-empty-element",
        ),
        pytest.param(
            lambda: UntrustedCommandTarget(
                argv=("/bin/echo", "bad\0argument"),
                containment_profile=ContainmentProfile.PROCESS_BOUNDARY_ONLY,
            ),
            "argv elements must be nonempty and NUL-free",
            id="untrusted-nul-element",
        ),
    ],
)
def test_command_targets_reject_invalid_argv(
    declare: Callable[[], object], message: str
) -> None:
    with pytest.raises(ValidationError, match=message):
        declare()


def test_python_targets_reject_nul_in_driver_source(
    request_document: IdentityDocument,
) -> None:
    with pytest.raises(ValidationError, match="must not contain NUL"):
        UntrustedPythonTarget(
            driver_source="before\0after",
            request=request_document,
            containment_profile=ContainmentProfile.PROCESS_BOUNDARY_ONLY,
        )


# Each case uses a real keyword rather than a mapping splat, so the
# declaration stays representative and type-checked.
UNSUPPORTED_FINITE_WORKLOAD_AXES = (
    pytest.param(
        lambda: Budgets(memory_bytes=FiniteByteLimit(max_bytes=1)),
        id="memory-bytes",
    ),
    pytest.param(
        lambda: Budgets(cpu_time=FiniteDurationLimit(max_ns=1)),
        id="cpu-time",
    ),
    pytest.param(
        lambda: Budgets(process_count=FiniteCountLimit(max_count=1)),
        id="process-count",
    ),
    pytest.param(
        lambda: Budgets(file_size_bytes=FiniteByteLimit(max_bytes=1)),
        id="file-size-bytes",
    ),
    pytest.param(
        lambda: Budgets(open_file_count=FiniteCountLimit(max_count=1)),
        id="open-file-count",
    ),
    pytest.param(
        lambda: Budgets(disk_bytes=FiniteByteLimit(max_bytes=1)),
        id="disk-bytes",
    ),
)


@pytest.mark.parametrize("declare", UNSUPPORTED_FINITE_WORKLOAD_AXES)
def test_budgets_reject_a_finite_limit_v1_never_enforces(
    declare: Callable[[], Budgets],
) -> None:
    with pytest.raises(ValidationError, match="accept no finite limit"):
        declare()


def test_unsupported_axes_accept_their_unbudgeted_spelling() -> None:
    budgets = Budgets(
        memory_bytes=UnbudgetedLimit(),
        cpu_time=UnbudgetedLimit(),
        process_count=UnbudgetedLimit(),
        file_size_bytes=UnbudgetedLimit(),
        open_file_count=UnbudgetedLimit(),
        disk_bytes=UnbudgetedLimit(),
    )

    assert budgets == Budgets.unbudgeted()


def test_budgets_accept_the_finite_limits_v1_enforces() -> None:
    wall_time = FiniteDurationLimit(max_ns=1)
    input_bytes = FiniteByteLimit(max_bytes=1)

    budgets = Budgets(wall_time=wall_time, input_bytes=input_bytes)

    assert budgets.wall_time == wall_time
    assert budgets.input_bytes == input_bytes
