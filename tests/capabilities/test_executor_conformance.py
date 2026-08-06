from __future__ import annotations

import sys
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from dr_store import MemoryBackend, ObjectStore, RecordCache
from support.executor import (
    cache_scope_identity_document,
    completion_for,
    fake_completion,
    job_for,
    python_target,
    trusted_target,
    untrusted_command_target,
)

from dr_exec import (
    Budgets,
    CompleteRecordReceipt,
    DeclarationError,
    DirectoryRunStore,
    EnvGrant,
    ExecutionJob,
    FakeExecutor,
    FakeRecordReceipt,
    FiniteByteLimit,
    IsolatedHostPythonRuntime,
    ProcessExecutor,
    RecordReceiptKind,
    TrustedCommandTarget,
)
from dr_exec.capabilities import CachedRecordReceipt, CachingExecutor

if TYPE_CHECKING:
    from dr_exec.capabilities.protocols import Executor

requires_macos = pytest.mark.skipif(
    sys.platform != "darwin",
    reason="real macOS process semantics",
)


def build_process_executor(
    root: Path,
    runtime: IsolatedHostPythonRuntime,
    /,
) -> ProcessExecutor:
    records = root / "records"
    records.mkdir(exist_ok=True)
    return ProcessExecutor(
        runtime=runtime,
        run_store=DirectoryRunStore(root=records),
    )


def build_fake_executor(_root: Path, /) -> FakeExecutor:
    return FakeExecutor(
        responder=lambda _job, _cancellation: fake_completion()
    )


def build_caching_fake_executor(root: Path, /) -> CachingExecutor:
    return CachingExecutor(
        build_fake_executor(root),
        cache=RecordCache(ObjectStore(MemoryBackend())),
        cache_scope_identity=cache_scope_identity_document(),
    )


EXECUTOR_IMPLEMENTATIONS = [
    pytest.param(
        "process",
        marks=(
            requires_macos,
            pytest.mark.integration,
            pytest.mark.subprocess,
            pytest.mark.platform_macos,
        ),
        id="process",
    ),
    pytest.param("fake", id="fake"),
    pytest.param("caching-fake", id="caching-fake"),
]


@pytest.fixture(params=EXECUTOR_IMPLEMENTATIONS)
def executor(request: pytest.FixtureRequest, tmp_path: Path) -> Executor:
    if request.param == "process":
        runtime = request.getfixturevalue("host_runtime")
        assert isinstance(runtime, IsolatedHostPythonRuntime)
        return build_process_executor(tmp_path, runtime)
    if request.param == "caching-fake":
        return build_caching_fake_executor(tmp_path)
    return build_fake_executor(tmp_path)


def clean_exit_command() -> tuple[str, ...]:
    return (sys.executable, "-I", "-c", "pass")


def relative_executable_without_granted_path() -> ExecutionJob:
    return job_for(trusted_target(("dr-exec-test-relative",)))


def untrusted_relative_executable_without_granted_path() -> ExecutionJob:
    return job_for(untrusted_command_target(("dr-exec-test-relative",)))


def relative_executable_with_separator() -> ExecutionJob:
    return job_for(
        trusted_target(("bin/dr-exec-test-relative",)),
        env=EnvGrant.fixed({"PATH": "/usr/bin:/bin"}),
    )


def untrusted_relative_executable_with_separator() -> ExecutionJob:
    return job_for(
        untrusted_command_target(("bin/dr-exec-test-relative",)),
        env=EnvGrant.fixed({"PATH": "/usr/bin:/bin"}),
    )


def relative_granted_path_entry() -> ExecutionJob:
    return job_for(
        trusted_target(("dr-exec-test-relative",)),
        env=EnvGrant.fixed({"PATH": "bin"}),
    )


def empty_granted_path_entry() -> ExecutionJob:
    return job_for(
        trusted_target(("dr-exec-test-relative",)),
        env=EnvGrant.fixed({"PATH": "/usr/bin:"}),
    )


def input_exceeding_its_declared_budget() -> ExecutionJob:
    return job_for(
        TrustedCommandTarget(
            argv=clean_exit_command(), stdin=b"more than four bytes"
        ),
        budgets=Budgets(input_bytes=FiniteByteLimit(max_bytes=4)),
    )


def python_request_exceeding_its_declared_budget() -> ExecutionJob:
    return job_for(
        python_target("a-request-longer-than-four-bytes"),
        budgets=Budgets(input_bytes=FiniteByteLimit(max_bytes=4)),
    )


INVALID_DECLARATIONS = [
    pytest.param(
        relative_executable_without_granted_path, id="relative-no-path"
    ),
    pytest.param(
        untrusted_relative_executable_without_granted_path,
        id="untrusted-relative-no-path",
    ),
    pytest.param(
        relative_executable_with_separator,
        id="relative-with-separator",
    ),
    pytest.param(
        untrusted_relative_executable_with_separator,
        id="untrusted-relative-with-separator",
    ),
    pytest.param(
        relative_granted_path_entry, id="relative-granted-path-entry"
    ),
    pytest.param(empty_granted_path_entry, id="empty-granted-path-entry"),
    pytest.param(input_exceeding_its_declared_budget, id="input-over-budget"),
    pytest.param(
        python_request_exceeding_its_declared_budget,
        id="python-request-over-budget",
    ),
]


@pytest.mark.parametrize("declaration", INVALID_DECLARATIONS)
def test_every_executor_rejects_the_same_invalid_declarations(
    executor: Executor, declaration: Callable[[], ExecutionJob]
) -> None:
    with pytest.raises(DeclarationError):
        executor.run(declaration())


def valid_absolute_command() -> ExecutionJob:
    return job_for(trusted_target(("/usr/bin/true",)))


def valid_relative_command_with_granted_path() -> ExecutionJob:
    return job_for(
        trusted_target(("true",)),
        env=EnvGrant.fixed({"PATH": "/usr/bin:/bin"}),
    )


def valid_input_within_its_budget() -> ExecutionJob:
    return job_for(
        trusted_target(("/usr/bin/true",)),
        budgets=Budgets(input_bytes=FiniteByteLimit(max_bytes=4096)),
    )


def valid_untrusted_command() -> ExecutionJob:
    return job_for(untrusted_command_target(clean_exit_command()))


def valid_python_target() -> ExecutionJob:
    return job_for(python_target("conformance"))


VALID_DECLARATIONS = [
    pytest.param(valid_absolute_command, id="absolute-command"),
    pytest.param(
        valid_relative_command_with_granted_path, id="relative-granted-path"
    ),
    pytest.param(valid_input_within_its_budget, id="input-within-budget"),
    pytest.param(valid_untrusted_command, id="untrusted-command"),
    pytest.param(valid_python_target, id="untrusted-python"),
]


@pytest.mark.parametrize("declaration", VALID_DECLARATIONS)
def test_every_executor_accepts_the_same_supported_declarations(
    executor: Executor, declaration: Callable[[], ExecutionJob]
) -> None:
    executor.run(declaration())


@pytest.mark.parametrize("declaration", VALID_DECLARATIONS)
def test_caching_executor_preserves_supported_declarations_on_a_warm_hit(
    declaration: Callable[[], ExecutionJob],
) -> None:
    inner = FakeExecutor(
        responder=lambda job, _cancellation: completion_for(job.job_id)
    )
    executor = CachingExecutor(
        inner,
        cache=RecordCache(ObjectStore(MemoryBackend())),
        cache_scope_identity=cache_scope_identity_document(),
    )
    source_job = declaration()
    requested_job = declaration()

    source = executor.run(source_job)
    replayed = executor.run(requested_job)

    assert inner.calls == (source_job,)
    assert replayed.result == source.result
    assert isinstance(replayed.record_receipt, CachedRecordReceipt)
    assert replayed.record_receipt.requested_job_id == requested_job.job_id
    assert replayed.record_receipt.source_execution_id == (
        source.result.execution_id
    )


def test_each_executor_enforces_its_own_receipt_kind(
    executor: Executor,
) -> None:
    receipt = executor.run(valid_absolute_command()).record_receipt

    if isinstance(executor, CachingExecutor):
        # A fresh cache misses, so the wrapper passes the inner fake
        # receipt through unchanged.
        assert isinstance(receipt, FakeRecordReceipt)
        assert receipt.kind is RecordReceiptKind.NOT_APPLICABLE
    elif isinstance(executor, FakeExecutor):
        assert isinstance(receipt, FakeRecordReceipt)
        assert receipt.kind is RecordReceiptKind.NOT_APPLICABLE
    else:
        assert isinstance(receipt, CompleteRecordReceipt)
        assert receipt.kind is RecordReceiptKind.COMPLETE
        assert receipt.record_dir.is_dir()
