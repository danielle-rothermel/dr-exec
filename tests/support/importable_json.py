"""Shared harness for the two trusted importable-JSON executors.

The in-process executor serves any entry point; a worker pool serves exactly
the entry point it was opened with. Both are driven here through one
``ExecutorHarness`` so the semantic suite can be parameterized over them
instead of copied.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
from typing import Protocol

from dr_exec import (
    CompletedExecution,
    ExecutionJob,
    ExecutionPool,
    ExecutionPoolConfig,
    FiniteDurationLimit,
    ImportableEntryPoint,
    ImportableJsonExecutor,
    InProcessRecordReceipt,
    RecordReceipt,
    WorkerPoolImportableJsonExecutor,
    WorkerPoolRecordReceipt,
)
from dr_exec.capabilities.protocols import Executor

ENTRY_POINT_MODULE = "support.in_process_entry_points"
IMPORT_FAIL_MODULE = "support.in_process_import_fail"
IMPORT_BLOCKS_MODULE = "support.in_process_import_blocks"

ECHO = ImportableEntryPoint(
    module_name=ENTRY_POINT_MODULE,
    attribute_name="echo",
)
MISSING_MODULE = ImportableEntryPoint(
    module_name="support.missing_in_process_entry_points",
    attribute_name="echo",
)
NOT_CALLABLE = ImportableEntryPoint(
    module_name=ENTRY_POINT_MODULE,
    attribute_name="NOT_CALLABLE",
)
RAISES = ImportableEntryPoint(
    module_name=ENTRY_POINT_MODULE,
    attribute_name="raise_error",
)
RAISES_SENTINEL = ImportableEntryPoint(
    module_name=ENTRY_POINT_MODULE,
    attribute_name="raise_sentinel_value_error",
)
RAISES_HUGE_MESSAGE = ImportableEntryPoint(
    module_name=ENTRY_POINT_MODULE,
    attribute_name="raise_huge_message",
)
ECHO_OR_RAISE = ImportableEntryPoint(
    module_name=ENTRY_POINT_MODULE,
    attribute_name="echo_unless_asked_to_raise",
)
RETURN_NULL = ImportableEntryPoint(
    module_name=ENTRY_POINT_MODULE,
    attribute_name="return_null",
)
RETURN_NON_JSON = ImportableEntryPoint(
    module_name=ENTRY_POINT_MODULE,
    attribute_name="return_non_json",
)
SLEEP_LONG = ImportableEntryPoint(
    module_name=ENTRY_POINT_MODULE,
    attribute_name="sleep_long",
)
IMPORT_FAIL = ImportableEntryPoint(
    module_name=IMPORT_FAIL_MODULE,
    attribute_name="echo",
)
IMPORT_BLOCKS = ImportableEntryPoint(
    module_name=IMPORT_BLOCKS_MODULE,
    attribute_name="echo",
)
BLOCK_ON_GATE = ImportableEntryPoint(
    module_name=ENTRY_POINT_MODULE,
    attribute_name="block_on_gate",
)
BLOCK_ON_BARRIER = ImportableEntryPoint(
    module_name=ENTRY_POINT_MODULE,
    attribute_name="block_on_barrier",
)
BURN_UNTIL_GATE = ImportableEntryPoint(
    module_name=ENTRY_POINT_MODULE,
    attribute_name="burn_until_gate",
)
FORK_CHILD = ImportableEntryPoint(
    module_name=ENTRY_POINT_MODULE,
    attribute_name="fork_child",
)
FORK_THEN_SYSTEM_EXIT = ImportableEntryPoint(
    module_name=ENTRY_POINT_MODULE,
    attribute_name="fork_then_system_exit",
)
ECHO_OR_BLOCK = ImportableEntryPoint(
    module_name=ENTRY_POINT_MODULE,
    attribute_name="echo_or_block_on_gate",
)
COUNT_IMPORTS = ImportableEntryPoint(
    module_name=ENTRY_POINT_MODULE,
    attribute_name="import_count",
)
EXIT_ABRUPTLY = ImportableEntryPoint(
    module_name=ENTRY_POINT_MODULE,
    attribute_name="exit_abruptly",
)
RAISE_SYSTEM_EXIT = ImportableEntryPoint(
    module_name=ENTRY_POINT_MODULE,
    attribute_name="raise_system_exit",
)


class PooledExecutor(Executor, Protocol):
    """An executor that also offers the concrete batch and pool entries.

    ``run_many`` and ``open_pool`` are conveniences every concrete executor
    provides over ``ExecutionPool``; they are not members of the substitutable
    ``Executor`` capability, so the suite names the wider shape here.
    """

    def run_many(
        self,
        jobs: Iterable[ExecutionJob],
        /,
        *,
        config: ExecutionPoolConfig | None = None,
        wall_time: FiniteDurationLimit | None = None,
    ) -> Iterator[CompletedExecution]:
        raise NotImplementedError

    def open_pool(
        self, *, config: ExecutionPoolConfig | None = None
    ) -> ExecutionPool:
        raise NotImplementedError


class ExecutorHarness(Protocol):
    """Open an executor for one entry point and describe its evidence."""

    name: str

    def open(
        self, entry_point: ImportableEntryPoint, /, *, workers: int
    ) -> AbstractContextManager[PooledExecutor]:
        raise NotImplementedError

    def receipt_is_own(self, receipt: RecordReceipt, /) -> bool:
        raise NotImplementedError


@dataclass(frozen=True, slots=True)
class InProcessHarness:
    name: str = "in_process"

    @contextmanager
    def open(
        self,
        entry_point: ImportableEntryPoint,
        /,
        *,
        workers: int = 1,
    ) -> Iterator[PooledExecutor]:
        yield ImportableJsonExecutor()

    def receipt_is_own(self, receipt: RecordReceipt, /) -> bool:
        return isinstance(receipt, InProcessRecordReceipt)


@dataclass(frozen=True, slots=True)
class WorkerPoolHarness:
    name: str = "worker_pool"

    @contextmanager
    def open(
        self,
        entry_point: ImportableEntryPoint,
        /,
        *,
        workers: int = 1,
    ) -> Iterator[PooledExecutor]:
        with WorkerPoolImportableJsonExecutor(
            entry_point=entry_point,
            worker_count=workers,
        ) as executor:
            yield executor

    def receipt_is_own(self, receipt: RecordReceipt, /) -> bool:
        return isinstance(receipt, WorkerPoolRecordReceipt)


HARNESSES = (InProcessHarness(), WorkerPoolHarness())
