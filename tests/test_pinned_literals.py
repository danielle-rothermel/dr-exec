"""Golden tests over persisted-format literals.

These assertions are the contract: a literal changes only by contract
revision, never by a local edit that happens to pass other tests. Each set
assertion is written out member by member so a silent addition, removal, or
rename is a failure here first.
"""

from __future__ import annotations

from dr_exec.declare import ExitVerdict, GrantKind, OverflowPolicy, RecordsKind
from dr_exec.record import (
    UNBUDGETED_WIRE_VALUE,
    Attribution,
    BudgetAxis,
    OutputsLocation,
    RecordKey,
    RecordStatus,
    TrustCategory,
)


def test_attribution_literals() -> None:
    assert Attribution.PAYLOAD.value == "payload"
    assert Attribution.EXECUTOR.value == "executor"
    assert Attribution.CHANNEL.value == "channel"
    assert Attribution.BUDGET.value == "budget"
    assert Attribution.MACHINE.value == "machine"
    assert Attribution.ABSENCE.value == "absence"


def test_attribution_member_set_is_exactly_the_six_parties() -> None:
    assert {member.value for member in Attribution} == {
        "payload",
        "executor",
        "channel",
        "budget",
        "machine",
        "absence",
    }


def test_budget_axis_literals() -> None:
    assert BudgetAxis.WALL_CLOCK.value == "wall_clock"
    assert BudgetAxis.OUTPUT.value == "output"
    assert BudgetAxis.INPUT.value == "input"
    assert {member.value for member in BudgetAxis} == {
        "wall_clock",
        "output",
        "input",
    }


def test_trust_category_literals() -> None:
    assert TrustCategory.TRUSTED_TOOL.value == "trusted_tool"
    assert TrustCategory.UNTRUSTED_PYTHON.value == "untrusted_python"
    assert TrustCategory.UNTRUSTED_COMMAND.value == "untrusted_command"
    assert {member.value for member in TrustCategory} == {
        "trusted_tool",
        "untrusted_python",
        "untrusted_command",
    }


def test_overflow_policy_literals() -> None:
    assert OverflowPolicy.FAIL.value == "fail"
    assert OverflowPolicy.MARKED_TRUNCATION.value == "marked_truncation"
    assert {member.value for member in OverflowPolicy} == {
        "fail",
        "marked_truncation",
    }


def test_unbudgeted_wire_literal() -> None:
    assert UNBUDGETED_WIRE_VALUE == "unbudgeted"


def test_grant_kind_literals() -> None:
    assert GrantKind.NONE.value == "none"
    assert GrantKind.NAMED.value == "named"
    assert GrantKind.FIXED.value == "fixed"
    assert GrantKind.OVERLAY.value == "overlay"
    assert {member.value for member in GrantKind} == {
        "none",
        "named",
        "fixed",
        "overlay",
    }


def test_exit_verdict_literals() -> None:
    assert ExitVerdict.REPORT_ONLY.value == "report_only"
    assert ExitVerdict.SUCCESS.value == "success"
    assert ExitVerdict.FAILURE.value == "failure"
    assert {member.value for member in ExitVerdict} == {
        "report_only",
        "success",
        "failure",
    }


def test_records_kind_literals() -> None:
    assert RecordsKind.DIRECTORY.value == "directory"
    assert RecordsKind.NONE.value == "none"
    assert {member.value for member in RecordsKind} == {"directory", "none"}


def test_record_status_literals() -> None:
    assert RecordStatus.SPAWNED.value == "spawned"
    assert RecordStatus.FINALIZED.value == "finalized"
    assert RecordStatus.WRITE_FAILED.value == "write_failed"
    assert {member.value for member in RecordStatus} == {
        "spawned",
        "finalized",
        "write_failed",
    }


def test_outputs_location_literals() -> None:
    assert OutputsLocation.CAPTURED.value == "captured"
    assert {member.value for member in OutputsLocation} == {"captured"}


def test_record_key_literals() -> None:
    assert RecordKey.SCHEMA_VERSION.value == "schema_version"
    assert RecordKey.EXECUTOR_IDENTITY.value == "executor_identity"
    assert RecordKey.TRUST_CATEGORY.value == "trust_category"
    assert RecordKey.RUN_ID.value == "run_id"
    assert RecordKey.ARGV.value == "argv"
    assert RecordKey.SOURCE_DIGEST.value == "source_digest"
    assert RecordKey.INPUT_DIGEST.value == "input_digest"
    assert RecordKey.GRANT_KIND.value == "grant_kind"
    assert RecordKey.GRANT_NAMES.value == "grant_names"
    assert RecordKey.GRANT_EXCLUSIONS.value == "grant_exclusions"
    assert RecordKey.GRANT_CONTENTS_DIGEST.value == "grant_contents_digest"
    assert RecordKey.PROFILE_NAME.value == "profile_name"
    assert RecordKey.BUDGET_WALL_CLOCK_SECONDS.value == "budget_wall_clock_seconds"
    assert RecordKey.BUDGET_OUTPUT_BYTES.value == "budget_output_bytes"
    assert (
        RecordKey.BUDGET_OUTPUT_OVERFLOW_POLICY.value == "budget_output_overflow_policy"
    )
    assert RecordKey.BUDGET_INPUT_BYTES.value == "budget_input_bytes"
    assert RecordKey.UNBUDGETED_AXES.value == "unbudgeted_axes"
    assert RecordKey.RUNTIME_NAME.value == "runtime_name"
    assert RecordKey.RUNTIME_INTERPRETER.value == "runtime_interpreter"
    assert RecordKey.STARTED_AT.value == "started_at"
    assert RecordKey.FINISHED_AT.value == "finished_at"
    assert RecordKey.ATTRIBUTION.value == "attribution"
    assert RecordKey.VIOLATED_AXIS.value == "violated_axis"
    assert RecordKey.SPAWN_ERRNO.value == "spawn_errno"
    assert RecordKey.EXIT_VERDICT.value == "exit_verdict"
    assert RecordKey.RETURNCODE.value == "returncode"
    assert RecordKey.DURATION_SECONDS.value == "duration_seconds"
    assert RecordKey.TEARDOWN_SECONDS.value == "teardown_seconds"
    assert RecordKey.STDOUT_BYTES_PRODUCED.value == "stdout_bytes_produced"
    assert RecordKey.STDERR_BYTES_PRODUCED.value == "stderr_bytes_produced"
    assert RecordKey.INPUT_BYTES.value == "input_bytes"
    assert RecordKey.STDOUT_BYTES_DROPPED.value == "stdout_bytes_dropped"
    assert RecordKey.STDERR_BYTES_DROPPED.value == "stderr_bytes_dropped"
    assert RecordKey.OUTPUTS_LOCATION.value == "outputs_location"
    assert RecordKey.SCRATCH_PATH.value == "scratch_path"
    assert RecordKey.RECORD_STATUS.value == "record_status"


def test_record_key_member_set() -> None:
    assert {member.value for member in RecordKey} == {
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


def test_containment_profile_and_runtime_names() -> None:
    from dr_exec.declare import HERMETIC, PROCESS_BOUNDARY_ONLY

    assert PROCESS_BOUNDARY_ONLY.name == "process_boundary_only"
    assert HERMETIC.name == "hermetic"
