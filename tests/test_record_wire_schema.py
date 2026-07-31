"""Golden test over the serialized run record.

Consumers derive persisted cache keys from this shape, so both the key set
and representative values are pinned. Identity is injected, never
monkeypatched: a fully-populated record is constructed from fixed inputs.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime

from dr_exec.declare import (
    HERMETIC,
    PROCESS_BOUNDARY_ONLY,
    Budgets,
    EnvironmentGrant,
    OutputBudget,
    OverflowPolicy,
    contents_digest_of,
)
from dr_exec.record import (
    EXECUTOR_IDENTITY,
    FAKE_EXECUTOR_IDENTITY,
    RECORD_SCHEMA_VERSION,
    Attribution,
    BudgetAxis,
    Measurements,
    Outcome,
    OutputsLocation,
    RecordStatus,
    RunRecord,
    RunResult,
    TruncationMark,
    TrustCategory,
    format_record_timestamp,
    record_filename,
    serialize_budgets,
    serialize_grant,
)

FIXED_RUN_ID = "0123456789abcdef0123456789abcdef"
FIXED_STARTED_AT = datetime(2026, 7, 31, 14, 5, 6, 789012, tzinfo=UTC)
FIXED_FINISHED_AT = datetime(2026, 7, 31, 14, 5, 8, 123456, tzinfo=UTC)


def _fully_populated_record() -> RunRecord:
    grant = EnvironmentGrant.fixed({"OPENBLAS_NUM_THREADS": "1"})
    budgets = Budgets(
        wall_clock=30.0,
        output=OutputBudget(
            limit_bytes=1048576, overflow_policy=OverflowPolicy.MARKED_TRUNCATION
        ),
        input=4194304,
    )
    serialized_budgets = serialize_budgets(budgets)
    serialized_grant = serialize_grant(grant)

    return RunRecord(
        schema_version=RECORD_SCHEMA_VERSION,
        executor_identity=EXECUTOR_IDENTITY,
        trust_category=TrustCategory.UNTRUSTED_PYTHON,
        run_id=FIXED_RUN_ID,
        argv=None,
        source_digest=contents_digest_of("print('hi')"),
        input_digest=contents_digest_of("stdin payload"),
        grant_kind=serialized_grant["grant_kind"],
        grant_names=serialized_grant["grant_names"],
        grant_exclusions=serialized_grant["grant_exclusions"],
        grant_contents_digest=serialized_grant["grant_contents_digest"],
        profile_name=PROCESS_BOUNDARY_ONLY.name,
        budget_wall_clock_seconds=serialized_budgets.wall_clock_seconds,
        budget_output_bytes=serialized_budgets.output_bytes,
        budget_output_overflow_policy=serialized_budgets.output_overflow_policy,
        budget_input_bytes=serialized_budgets.input_bytes,
        unbudgeted_axes=serialized_budgets.unbudgeted_axes,
        runtime_name=HERMETIC.name,
        runtime_interpreter="/usr/bin/python3.12",
        started_at=format_record_timestamp(FIXED_STARTED_AT),
        outputs_location=OutputsLocation.CAPTURED,
        scratch_path="/tmp/dr-exec-scratch/run",
        record_status=RecordStatus.SPAWNED,
    )


def _finalized_record() -> RunRecord:
    result = RunResult(
        returncode=-9,
        stdout="partial stdout",
        stderr="partial stderr",
        truncation=TruncationMark(stdout_bytes_dropped=512, stderr_bytes_dropped=64),
        measurements=Measurements(
            duration_seconds=1.334444,
            teardown_seconds=0.002,
            stdout_bytes_produced=1048576 + 512,
            stderr_bytes_produced=64,
            input_bytes=13,
        ),
        outcome=Outcome(
            attribution=Attribution.BUDGET,
            violated_axis=BudgetAxis.OUTPUT,
            exit_verdict="report_only",
        ),
    )
    return _fully_populated_record().finalized_with(
        result=result, finished_at=FIXED_FINISHED_AT
    )


def test_wire_key_set_is_exact() -> None:
    wire = _finalized_record().to_wire()
    assert set(wire) == {
        "schema_version",
        "executor_identity",
        "trust_category",
        "run_id",
        "argv",
        "source_digest",
        "input_digest",
        "grant_kind",
        "grant_names",
        "grant_exclusions",
        "grant_contents_digest",
        "profile_name",
        "budget_wall_clock_seconds",
        "budget_output_bytes",
        "budget_output_overflow_policy",
        "budget_input_bytes",
        "unbudgeted_axes",
        "runtime_name",
        "runtime_interpreter",
        "started_at",
        "finished_at",
        "attribution",
        "violated_axis",
        "spawn_errno",
        "exit_verdict",
        "returncode",
        "duration_seconds",
        "teardown_seconds",
        "stdout_bytes_produced",
        "stderr_bytes_produced",
        "input_bytes",
        "stdout_bytes_dropped",
        "stderr_bytes_dropped",
        "outputs_location",
        "scratch_path",
        "record_status",
    }


def test_wire_values_are_pinned() -> None:
    wire = _finalized_record().to_wire()

    assert wire["schema_version"] == 1
    assert wire["executor_identity"] == EXECUTOR_IDENTITY
    assert wire["trust_category"] == "untrusted_python"
    assert wire["run_id"] == "0123456789abcdef0123456789abcdef"
    assert wire["argv"] is None
    assert wire["source_digest"] == hashlib.sha256(b"print('hi')").hexdigest()
    assert wire["input_digest"] == contents_digest_of("stdin payload")
    assert wire["grant_kind"] == "fixed"
    assert wire["grant_names"] == ["OPENBLAS_NUM_THREADS"]
    assert wire["grant_exclusions"] == []
    assert wire["profile_name"] == "process_boundary_only"
    assert wire["budget_wall_clock_seconds"] == 30.0
    assert wire["budget_output_bytes"] == 1048576
    assert wire["budget_output_overflow_policy"] == "marked_truncation"
    assert wire["budget_input_bytes"] == 4194304
    assert wire["unbudgeted_axes"] == []
    assert wire["runtime_name"] == "hermetic"
    assert wire["runtime_interpreter"] == "/usr/bin/python3.12"
    assert wire["started_at"] == "2026-07-31T14:05:06.789012+00:00"
    assert wire["finished_at"] == "2026-07-31T14:05:08.123456+00:00"
    assert wire["attribution"] == "budget"
    assert wire["violated_axis"] == "output"
    assert wire["spawn_errno"] is None
    assert wire["exit_verdict"] == "report_only"
    assert wire["returncode"] == -9
    assert wire["duration_seconds"] == 1.334444
    assert wire["teardown_seconds"] == 0.002
    assert wire["stdout_bytes_produced"] == 1049088
    assert wire["stderr_bytes_produced"] == 64
    assert wire["input_bytes"] == 13
    assert wire["stdout_bytes_dropped"] == 512
    assert wire["stderr_bytes_dropped"] == 64
    assert wire["outputs_location"] == "captured"
    assert wire["scratch_path"] == "/tmp/dr-exec-scratch/run"
    assert wire["record_status"] == "finalized"


def test_digests_are_sha256_hex_over_utf8() -> None:
    assert contents_digest_of("naïve") == hashlib.sha256("naïve".encode()).hexdigest()
    assert contents_digest_of("") == (
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    )


def test_spawn_time_record_carries_no_outcome_yet() -> None:
    wire = _fully_populated_record().to_wire()
    assert wire["record_status"] == "spawned"
    assert wire["finished_at"] is None
    assert wire["attribution"] is None
    assert wire["returncode"] is None
    assert wire["duration_seconds"] is None


def test_unbudgeted_axes_serialize_as_the_pinned_literal() -> None:
    serialized = serialize_budgets(Budgets())
    assert serialized.wall_clock_seconds == "unbudgeted"
    assert serialized.output_bytes == "unbudgeted"
    assert serialized.output_overflow_policy == "unbudgeted"
    assert serialized.input_bytes == "unbudgeted"
    assert serialized.unbudgeted_axes == ("wall_clock", "output", "input")


def test_record_is_json_serializable() -> None:
    payload = json.dumps(_finalized_record().to_wire())
    assert json.loads(payload)["attribution"] == "budget"


def test_executor_identity_shape() -> None:
    from importlib.metadata import version

    installed = version("dr-exec")
    expected_real = f"dr-exec@{installed}"
    expected_fake = f"dr-exec-fake@{installed}"

    assert EXECUTOR_IDENTITY == expected_real
    assert FAKE_EXECUTOR_IDENTITY == expected_fake
    assert FAKE_EXECUTOR_IDENTITY != EXECUTOR_IDENTITY


def test_record_filename_scheme() -> None:
    assert (
        record_filename(started_at=FIXED_STARTED_AT, run_id=FIXED_RUN_ID)
        == "run-20260731T140506789012-0123456789abcdef0123456789abcdef.json"
    )


def test_record_filename_uses_utc_regardless_of_input_offset() -> None:
    from datetime import timedelta, timezone

    local = FIXED_STARTED_AT.astimezone(timezone(timedelta(hours=-4)))
    assert record_filename(started_at=local, run_id=FIXED_RUN_ID) == record_filename(
        started_at=FIXED_STARTED_AT, run_id=FIXED_RUN_ID
    )
