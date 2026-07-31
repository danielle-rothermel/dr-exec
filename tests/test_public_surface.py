"""The package's public surface, pinned name by name.

Consumers import from ``dr_exec`` directly, so what the package exports is
part of what it promises. Writing the set out member by member makes a silent
addition or removal a failure here first.
"""

from __future__ import annotations

import importlib

import pytest

import dr_exec

_PUBLIC_SURFACE = {
    "BODY_HOOK_NAME",
    "CLIP_MARKER",
    "EXECUTOR_IDENTITY",
    "FAKE_EXECUTOR_IDENTITY",
    "HERMETIC",
    "INVOCATION_AGGREGATE_BOUND_BYTES",
    "IPC_JOIN_SELF_BUDGET_SECONDS",
    "PROCESS_BOUNDARY_ONLY",
    "PROTOCOL_VERSION",
    "RECORD_SCHEMA_VERSION",
    "REPORT_ONLY",
    "SOURCE_BOUND_BYTES",
    "TERMINATION_SELF_BUDGET_SECONDS",
    "UNBUDGETED",
    "UNBUDGETED_WIRE_VALUE",
    "Attribution",
    "BatchItem",
    "BatchRequest",
    "BatchResult",
    "BudgetAxis",
    "Budgets",
    "ContainmentProfile",
    "DeclarationError",
    "DrExecError",
    "EntryPoint",
    "EnvironmentGrant",
    "ExecutorFailure",
    "ExitPolicy",
    "ExitVerdict",
    "FakeExecutor",
    "GrantKind",
    "ItemResult",
    "Measurements",
    "Outcome",
    "OutputBudget",
    "OutputsLocation",
    "OverflowPolicy",
    "ProtocolChannelBudget",
    "ProtocolFailure",
    "PythonRuntime",
    "RecordKey",
    "RecordStatus",
    "RecordedBatchCall",
    "RecordedCall",
    "Records",
    "RecordsKind",
    "RunRecord",
    "RunResult",
    "ScriptError",
    "ScriptedBatch",
    "TruncationMark",
    "TrustCategory",
    "UnscriptedCall",
    "WireKey",
    "WireKind",
    "config_digest_of",
    "contents_digest_of",
    "format_record_timestamp",
    "new_run_id",
    "record_filename",
    "run_batch",
    "run_tool",
    "run_untrusted_command",
    "run_untrusted_python",
    "serialize_budgets",
    "serialize_grant",
}


def test_the_public_surface_is_exactly_these_names() -> None:
    assert set(dr_exec.__all__) == _PUBLIC_SURFACE


def test_every_exported_name_resolves() -> None:
    assert [name for name in dr_exec.__all__ if not hasattr(dr_exec, name)] == []


def test_the_export_list_carries_no_duplicates() -> None:
    assert len(dr_exec.__all__) == len(set(dr_exec.__all__))


@pytest.mark.parametrize(
    "module",
    ["batch", "declare", "engine", "errors", "fake", "record", "run"],
)
def test_every_module_imports(module: str) -> None:
    assert importlib.import_module(f"dr_exec.{module}") is not None
