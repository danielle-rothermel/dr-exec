"""Public API import surface and boundary-model validation."""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from dr_serialize import Sha256Digest, build_identity_document
from pydantic import ValidationError

import dr_exec
from dr_exec import (
    Budgets,
    EnvGrantKind,
    EnvGrantRecord,
    ExecutorSelfBudgets,
    FiniteByteLimit,
    FiniteCountLimit,
    FiniteDurationLimit,
    OutputArtifactRecord,
    RuntimeRecord,
    UnbudgetedLimit,
)
from dr_exec.capabilities import protocols
from dr_exec.recording.identity import (
    _build_executor_config_identity,
    _validate_executor_config_identity,
    _validate_executor_identity,
)
from dr_exec.runtime.identity import _validate_isolated_host_runtime_identity
from dr_exec.runtime.wire import ProtocolPrelude

if TYPE_CHECKING:
    from collections.abc import Callable

VALID_DIGEST = "a" * 64


def test_every_exported_name_is_importable() -> None:
    missing = [name for name in dr_exec.__all__ if not hasattr(dr_exec, name)]
    assert missing == []


def test_the_export_list_is_sorted_and_unique() -> None:
    assert list(dr_exec.__all__) == sorted(set(dr_exec.__all__))


def test_only_the_three_named_protocols_are_capability_boundaries() -> None:
    assert set(protocols.__all__) == {
        "Executor",
        "RunStore",
        "Runtime",
    }


@pytest.mark.parametrize(
    "digest",
    [
        pytest.param("a" * 63, id="too-short"),
        pytest.param("a" * 65, id="too-long"),
        pytest.param("A" * 64, id="uppercase"),
        pytest.param("g" * 64, id="non-hexadecimal"),
        pytest.param("sha256:" + "a" * 57, id="prefixed"),
    ],
)
def test_digest_boundaries_reject_non_canonical_spellings(
    digest: str,
) -> None:
    with pytest.raises(ValidationError):
        # The bare `str` is the point: this pins that the model, not
        # only the nominal constructor, rejects a non-canonical digest.
        ProtocolPrelude(request_id_sha256=digest)  # ty: ignore[invalid-argument-type]


def test_digest_boundaries_yield_nominal_digest_values() -> None:
    frame = ProtocolPrelude(request_id_sha256=Sha256Digest(VALID_DIGEST))
    assert isinstance(frame.request_id_sha256, Sha256Digest)
    record = OutputArtifactRecord(
        relative_path=Path("stdout.bin"),
        size_bytes=0,
        sha256=Sha256Digest(VALID_DIGEST),
    )
    assert isinstance(record.sha256, Sha256Digest)


@pytest.mark.parametrize(
    "relative_path",
    [
        pytest.param("/absolute.bin", id="absolute"),
        pytest.param("./stdout.bin", id="dot-component"),
        pytest.param("../stdout.bin", id="parent-component"),
        pytest.param("nested//stdout.bin", id="empty-component"),
        pytest.param("", id="empty"),
        pytest.param(".", id="bare-dot"),
    ],
)
def test_artifact_paths_reject_unsafe_wire_spellings(
    relative_path: str,
) -> None:
    """The wire spelling is rejected before Path construction normalizes it."""
    payload = json.dumps(
        {
            "relative_path": relative_path,
            "size_bytes": 0,
            "sha256": VALID_DIGEST,
        }
    ).encode("utf-8")
    with pytest.raises(ValidationError):
        OutputArtifactRecord.model_validate_json(payload, strict=True)


def test_artifact_paths_accept_a_normalized_relative_spelling() -> None:
    record = OutputArtifactRecord(
        relative_path=Path("stdout.bin"),
        size_bytes=0,
        sha256=Sha256Digest(VALID_DIGEST),
    )
    assert record.relative_path == Path("stdout.bin")


def test_environment_records_reject_unsorted_names() -> None:
    with pytest.raises(ValidationError, match="sorted and unique"):
        EnvGrantRecord(
            kind=EnvGrantKind.FIXED,
            var_names=("B", "A"),
            excluded_var_names=(),
            canonical_values_sha256=Sha256Digest(VALID_DIGEST),
        )


def test_environment_records_reject_exclusions_outside_overlay() -> None:
    with pytest.raises(ValidationError, match="only overlay"):
        EnvGrantRecord(
            kind=EnvGrantKind.FIXED,
            var_names=("A",),
            excluded_var_names=("B",),
            canonical_values_sha256=Sha256Digest(VALID_DIGEST),
        )


def test_identity_validators_reject_an_incomplete_payload() -> None:
    """The role's payload model is the sole key authority."""
    foreign = build_identity_document(
        schema="dr_exec.executor",
        schema_version=1,
        payload={"kind": "process_executor"},
    )
    with pytest.raises(ValidationError, match="Field required"):
        _validate_executor_identity(foreign)


def test_identity_validators_reject_an_unknown_payload_key() -> None:
    document = build_identity_document(
        schema="dr_exec.executor",
        schema_version=1,
        payload={
            "kind": "process_executor",
            "package_version": "0.1.0",
            "source_commit": "0" * 40,
            "source_state": "clean",
            "session_id": None,
            "extra": 1,
        },
    )
    with pytest.raises(ValidationError, match="Extra inputs"):
        _validate_executor_identity(document)


def test_identity_validators_reject_a_payload_missing_its_kind() -> None:
    """``kind`` is a persisted key, never supplied by a model default."""
    document = build_identity_document(
        schema="dr_exec.executor",
        schema_version=1,
        payload={
            "package_version": "0.1.0",
            "source_commit": "0" * 40,
            "source_state": "clean",
            "session_id": None,
        },
    )
    with pytest.raises(ValidationError, match="Field required"):
        _validate_executor_identity(document)


def test_identity_validators_reject_a_foreign_schema_version() -> None:
    document = build_identity_document(
        schema="dr_exec.executor_config",
        schema_version=2,
        payload=ExecutorSelfBudgets.unbudgeted().model_dump(mode="json"),
    )
    with pytest.raises(ValueError, match="schema version 1"):
        _validate_executor_config_identity(document)


# Each case builds `Budgets` through a real keyword rather than a mapping
# splat, so the declaration each case makes is exactly the one a caller
# could write and stays type-checked.
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
    """A declared limit no component applies is refused, not recorded.

    These axes exist so that every axis is explicitly unbudgeted rather
    than absent. Accepting a finite value on one would put a protection
    into the record and the config identity that the engine never
    applies.
    """
    with pytest.raises(ValidationError, match="accept no finite limit"):
        declare()


def test_unsupported_axes_still_accept_their_unbudgeted_spelling() -> None:
    """Only the finite spelling is refused, never the axis itself."""
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


def test_executor_config_identity_accepts_effective_self_budgets() -> None:
    document = _build_executor_config_identity(
        ExecutorSelfBudgets.unbudgeted()
    )
    assert _validate_executor_config_identity(document) is document


def test_runtime_records_reject_identity_mismatch() -> None:
    id_doc = build_identity_document(
        schema="dr_exec.isolated_host_python_runtime",
        schema_version=1,
        payload={
            "kind": "isolated_host_python",
            "resolved_executable": "/opt/py/bin/python3.13",
            "implementation": "cpython",
            "python_version": "3.13.2",
            "cache_tag": "cpython-313",
            "platform": "darwin",
        },
    )
    assert _validate_isolated_host_runtime_identity(id_doc) is id_doc
    with pytest.raises(ValidationError, match="does not match identity"):
        RuntimeRecord(
            kind=dr_exec.RuntimeKind.ISOLATED_HOST_PYTHON,
            resolved_executable=Path("/opt/py/bin/python3.12"),
            id_doc=id_doc,
        )
