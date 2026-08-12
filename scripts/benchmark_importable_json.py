from __future__ import annotations

import argparse
import asyncio
import hashlib
import importlib
import json
import os
import platform
import subprocess
import sys
import threading
from collections.abc import AsyncIterator, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from time import perf_counter
from typing import cast
from unittest.mock import patch
from uuid import uuid4

from dr_serialize import Jsonable, canonical_json_bytes
from dr_store.document_directory import sidecar as sidecar_module
from dr_store.document_file import canonical_json as document_file_module

from dr_exec import (
    Budgets,
    CompletedExecution,
    CompleteRecordReceipt,
    DirectoryRunStore,
    EnvGrant,
    ExecutionPoolConfig,
    ExecutionSubmission,
    ExecutorSelfBudgets,
    FinalizedRecord,
    FiniteByteLimit,
    FiniteCountLimit,
    FiniteOutput,
    FixedPoolCapacity,
    ImportableEntryPoint,
    ImportableJsonResultError,
    IsolatedHostPythonRuntime,
    JobId,
    OutputOverflowPolicy,
    PayloadRetentionBudget,
    ProcessExecutor,
    StreamRetentionBudget,
    TrustedPythonTarget,
    build_trusted_importable_json_job,
    parse_importable_json_result,
)
from dr_exec.declarations.models import ExecutionJob
from dr_exec.execution import engine as execution_engine

FIXTURE_MODULE = "dr_exec_importable_json_fixture"
SUCCESS_ATTRIBUTE = "echo"
FAILURE_ATTRIBUTE = "raise_error"

INPUT_LIMIT_BYTES = 64 * 1024
PAYLOAD_OUTPUT_LIMIT_BYTES = 64 * 1024
PROTOCOL_FRAME_LIMIT_BYTES = 64 * 1024
PROTOCOL_TOTAL_LIMIT_BYTES = 128 * 1024
PROTOCOL_JSON_DEPTH = 32

DEFAULT_CAPACITIES = (1, 2, 4)
DEFAULT_JOBS_PER_CAPACITY = 24
DEFAULT_POLL_INTERVAL_SECONDS = 0.002

# This is an investigation report shape, not a stable persisted API.
REPORT_FORMAT = "dr_exec.importable_json.performance_investigation"
REPORT_FORMAT_VERSION = 1

CALLER_BATCH = cast(
    "list[Jsonable]",
    [
        {"case_id": "edge-empty", "arguments": [[], 0]},
        {"case_id": "edge-null", "arguments": [[None], 1]},
        {"case_id": "ordinary", "arguments": [[2, 3, 5], 8]},
    ],
)
CALLER_REQUEST = cast(
    "Jsonable",
    {
        "candidate": {
            "id": "candidate-017",
            "language": "python",
            "source_sha256": "a" * 64,
        },
        "caller_owned_batch": CALLER_BATCH,
        "options": {"capture_details": False, "seed": 1729},
    },
)


@dataclass(frozen=True, slots=True)
class SyncCounts:
    manifest: int
    sidecar: int

    @property
    def total(self) -> int:
        return self.manifest + self.sidecar

    def since(self, earlier: SyncCounts, /) -> SyncCounts:
        return SyncCounts(
            manifest=self.manifest - earlier.manifest,
            sidecar=self.sidecar - earlier.sidecar,
        )

    def to_json(self) -> dict[str, int]:
        return {
            "total": self.total,
            "manifest": self.manifest,
            "sidecar": self.sidecar,
        }


class SynchronizationCounter:
    """Count investigation-time calls to dr-store's existing flush helper."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._manifest = 0
        self._sidecar = 0

    def snapshot(self) -> SyncCounts:
        with self._lock:
            return SyncCounts(
                manifest=self._manifest,
                sidecar=self._sidecar,
            )

    @contextmanager
    def instrument(self) -> Iterator[None]:
        original = document_file_module.flush_descriptor

        def manifest_flush(descriptor: int, /) -> None:
            with self._lock:
                self._manifest += 1
            original(descriptor)

        def sidecar_flush(descriptor: int, /) -> None:
            with self._lock:
                self._sidecar += 1
            original(descriptor)

        with (
            patch.object(
                document_file_module,
                "flush_descriptor",
                manifest_flush,
            ),
            patch.object(
                sidecar_module,
                "flush_descriptor",
                sidecar_flush,
            ),
        ):
            yield


class ChildProcessTracker:
    """Observe direct children from launch until executor-owned reaping."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._processes: dict[int, subprocess.Popen[bytes]] = {}
        self._peak_live = 0

    def reset_peak(self) -> None:
        with self._lock:
            self._peak_live = self._live_count_locked()

    def peak_live(self) -> int:
        with self._lock:
            return self._peak_live

    def live_count(self) -> int:
        with self._lock:
            return self._live_count_locked()

    def _live_count_locked(self) -> int:
        return sum(
            process.returncode is None for process in self._processes.values()
        )

    @contextmanager
    def instrument(self) -> Iterator[None]:
        original_launch = execution_engine.launch_bootstrap
        original_teardown = execution_engine._tear_down

        def launch_bootstrap(
            *,
            executable: str,
            argv: tuple[str, ...],
            environment: dict[str, str],
            scratch_directory: str,
            descriptor_map: tuple[tuple[int, int], ...],
            status_write: int,
        ) -> subprocess.Popen[bytes]:
            process = original_launch(
                executable=executable,
                argv=argv,
                environment=environment,
                scratch_directory=scratch_directory,
                descriptor_map=descriptor_map,
                status_write=status_write,
            )
            with self._lock:
                self._processes[process.pid] = process
                self._peak_live = max(
                    self._peak_live,
                    self._live_count_locked(),
                )
            return process

        def tear_down(
            process: subprocess.Popen[bytes],
            self_budgets: ExecutorSelfBudgets,
            /,
            *,
            leads_group: bool = True,
        ) -> int:
            try:
                return original_teardown(
                    process,
                    self_budgets,
                    leads_group=leads_group,
                )
            finally:
                with self._lock:
                    self._processes.pop(process.pid, None)

        with (
            patch.object(
                execution_engine,
                "launch_bootstrap",
                launch_bootstrap,
            ),
            patch.object(execution_engine, "_tear_down", tear_down),
        ):
            yield


@dataclass(frozen=True, slots=True)
class ResourcePeaks:
    parent_threads: int
    parent_file_descriptors: int | None


class ResourceMonitor:
    """Poll parent-only resources while one capacity sample is active."""

    def __init__(self, *, interval_seconds: float) -> None:
        self._interval_seconds = interval_seconds
        self._fd_root = _file_descriptor_root()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._peak_threads = 0
        self._peak_file_descriptors: int | None = None

    def start(self) -> None:
        self._sample()
        self._thread = threading.Thread(
            target=self._poll,
            name="dr-exec-benchmark-resource-monitor",
            daemon=True,
        )
        self._thread.start()

    def finish(self) -> ResourcePeaks:
        self._sample()
        self._stop.set()
        thread = self._thread
        if thread is not None:
            thread.join()
        self._sample()
        return ResourcePeaks(
            parent_threads=self._peak_threads,
            parent_file_descriptors=self._peak_file_descriptors,
        )

    def _poll(self) -> None:
        while not self._stop.wait(self._interval_seconds):
            self._sample()

    def _sample(self) -> None:
        self._peak_threads = max(
            self._peak_threads,
            threading.active_count(),
        )
        count = _file_descriptor_count(self._fd_root)
        if count is not None:
            self._peak_file_descriptors = max(
                count,
                self._peak_file_descriptors or 0,
            )


@dataclass(frozen=True, slots=True)
class DirectoryStats:
    record_objects: int
    files: int
    logical_bytes: int

    def since(self, earlier: DirectoryStats, /) -> DirectoryStats:
        return DirectoryStats(
            record_objects=self.record_objects - earlier.record_objects,
            files=self.files - earlier.files,
            logical_bytes=self.logical_bytes - earlier.logical_bytes,
        )

    def to_json(self) -> dict[str, int]:
        return {
            "record_objects": self.record_objects,
            "files": self.files,
            "logical_bytes": self.logical_bytes,
        }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Measure representative importable JSON jobs without setting "
            "performance pass/fail thresholds."
        )
    )
    parser.add_argument(
        "--capacities",
        default=",".join(str(value) for value in DEFAULT_CAPACITIES),
        help="comma-separated positive pool capacities",
    )
    parser.add_argument(
        "--jobs-per-capacity",
        type=int,
        default=DEFAULT_JOBS_PER_CAPACITY,
    )
    parser.add_argument(
        "--poll-interval-seconds",
        type=float,
        default=DEFAULT_POLL_INTERVAL_SECONDS,
    )
    args = parser.parse_args()
    capacities = tuple(int(item) for item in args.capacities.split(","))
    if not capacities or any(value < 1 for value in capacities):
        parser.error("--capacities must contain positive integers")
    if args.jobs_per_capacity < 1:
        parser.error("--jobs-per-capacity must be positive")
    if args.poll_interval_seconds <= 0:
        parser.error("--poll-interval-seconds must be positive")
    args.capacities = capacities
    return args


def _workload_budgets() -> Budgets:
    stream = StreamRetentionBudget(
        head_bytes=PAYLOAD_OUTPUT_LIMIT_BYTES // 4,
        tail_bytes=PAYLOAD_OUTPUT_LIMIT_BYTES // 4,
    )
    return Budgets(
        input_bytes=FiniteByteLimit(max_bytes=INPUT_LIMIT_BYTES),
        payload_output=FiniteOutput(
            max_bytes=PAYLOAD_OUTPUT_LIMIT_BYTES,
            overflow_policy=OutputOverflowPolicy.FAIL,
            retention=PayloadRetentionBudget(stdout=stream, stderr=stream),
        ),
    )


def _self_budgets() -> ExecutorSelfBudgets:
    return ExecutorSelfBudgets(
        protocol_frame_bytes=FiniteByteLimit(
            max_bytes=PROTOCOL_FRAME_LIMIT_BYTES
        ),
        protocol_total_bytes=FiniteByteLimit(
            max_bytes=PROTOCOL_TOTAL_LIMIT_BYTES
        ),
        protocol_output_count=FiniteCountLimit(max_count=1),
        json_depth=FiniteCountLimit(max_count=PROTOCOL_JSON_DEPTH),
    )


def _build_job(
    entry_point: ImportableEntryPoint,
    request: Jsonable,
    /,
    *,
    budgets: Budgets,
) -> ExecutionJob:
    return build_trusted_importable_json_job(
        JobId(uuid4()),
        entry_point,
        request,
        env=EnvGrant.none(),
        budgets=budgets,
    )


async def _execute_capacity(
    executor: ProcessExecutor,
    jobs: tuple[ExecutionJob, ...],
    /,
    *,
    capacity: int,
) -> tuple[list[CompletedExecution], list[Jsonable]]:
    async def submissions() -> AsyncIterator[ExecutionSubmission[int]]:
        for index, job in enumerate(jobs):
            yield ExecutionSubmission(job=job, context=index)

    completions: list[CompletedExecution] = []
    parsed: list[Jsonable] = []
    pool = executor.open_pool(
        config=ExecutionPoolConfig(
            capacity=FixedPoolCapacity(max_active_jobs=capacity)
        )
    )
    async with pool:
        async for delivered in pool.run_stream(submissions()):
            completions.append(delivered.completed_execution)
            parsed.append(
                parse_importable_json_result(delivered.completed_execution)
            )
    return completions, parsed


def _run_capacity(
    *,
    executor: ProcessExecutor,
    store: DirectoryRunStore,
    entry_point: ImportableEntryPoint,
    budgets: Budgets,
    capacity: int,
    job_count: int,
    synchronization_counter: SynchronizationCounter,
    child_tracker: ChildProcessTracker,
    poll_interval_seconds: float,
) -> tuple[dict[str, Jsonable], CompletedExecution, Jsonable]:
    jobs = tuple(
        _build_job(entry_point, CALLER_REQUEST, budgets=budgets)
        for _ in range(job_count)
    )
    before_directory = _directory_stats(store.root)
    before_sync = synchronization_counter.snapshot()
    baseline_threads = threading.active_count()
    baseline_file_descriptors = _file_descriptor_count(_file_descriptor_root())
    child_tracker.reset_peak()
    monitor = ResourceMonitor(interval_seconds=poll_interval_seconds)

    monitor.start()
    started = perf_counter()
    completions, parsed = asyncio.run(
        _execute_capacity(executor, jobs, capacity=capacity)
    )
    elapsed_seconds = perf_counter() - started
    peaks = monitor.finish()

    after_directory = _directory_stats(store.root)
    directory_delta = after_directory.since(before_directory)
    sync_delta = synchronization_counter.snapshot().since(before_sync)
    records = [
        _load_finalized(store, completion) for completion in completions
    ]
    round_trip_preserved = all(
        isinstance(result, dict) and result.get("value") == CALLER_REQUEST
        for result in parsed
    )
    if not round_trip_preserved:
        raise ValueError("the installed fixture did not preserve the request")

    report = cast(
        "dict[str, Jsonable]",
        {
            "capacity": capacity,
            "jobs": job_count,
            "elapsed_seconds": elapsed_seconds,
            "seconds_per_job": elapsed_seconds / job_count,
            "sustained_jobs_per_second": job_count / elapsed_seconds,
            "resource_baseline": {
                "parent_threads": baseline_threads,
                "parent_file_descriptors": baseline_file_descriptors,
            },
            "resource_peaks": {
                "live_children": child_tracker.peak_live(),
                "parent_threads": peaks.parent_threads,
                "parent_file_descriptors": peaks.parent_file_descriptors,
            },
            "live_children_after_sample": child_tracker.live_count(),
            "recording": {
                "finalized_record_objects": sum(
                    isinstance(record, FinalizedRecord) for record in records
                ),
                **directory_delta.to_json(),
                "synchronization_calls": sync_delta.to_json(),
            },
            "caller_owned_batch": {
                "member_count": len(CALLER_BATCH),
                "dr_exec_interpretation": "one opaque JSON value per job",
                "round_trip_preserved": round_trip_preserved,
            },
        },
    )
    return report, completions[0], parsed[0]


def _load_finalized(
    store: DirectoryRunStore,
    completed: CompletedExecution,
    /,
) -> FinalizedRecord:
    receipt = completed.record_receipt
    if not isinstance(receipt, CompleteRecordReceipt):
        raise TypeError("the benchmark requires a complete record receipt")
    record = store.load(receipt.reference)
    if not isinstance(record, FinalizedRecord):
        raise TypeError("the benchmark requires a finalized run record")
    return record


def _reference_json(
    completed: CompletedExecution,
    /,
) -> dict[str, Jsonable]:
    receipt = completed.record_receipt
    if not isinstance(receipt, CompleteRecordReceipt):
        raise TypeError("the evidence probe requires a complete receipt")
    return cast(
        "dict[str, Jsonable]",
        receipt.reference.model_dump(mode="json"),
    )


def _evidence_report(
    *,
    executor: ProcessExecutor,
    store: DirectoryRunStore,
    success: CompletedExecution,
    budgets: Budgets,
) -> dict[str, Jsonable]:
    failed = executor.run_blocking(
        _build_job(
            ImportableEntryPoint(
                module_name=FIXTURE_MODULE,
                attribute_name=FAILURE_ATTRIBUTE,
            ),
            cast("Jsonable", {"deliberate_failure_probe": True}),
            budgets=budgets,
        )
    )
    try:
        parse_importable_json_result(failed)
    except ImportableJsonResultError:
        parser_rejected_failure = True
    else:
        parser_rejected_failure = False
        raise ValueError("the deliberate adapter failure parsed as success")

    success_record = _load_finalized(store, success)
    failed_record = _load_finalized(store, failed)
    artifacts = (
        failed_record.outputs.stderr,
        failed_record.outputs.stdout,
        success_record.outputs.stderr,
        success_record.outputs.stdout,
    )
    artifact = next((one for one in artifacts if one.size_bytes > 0), None)
    if artifact is None:
        artifact_read: dict[str, Jsonable] = {"artifact_exists": False}
    else:
        receipt = failed.record_receipt
        if artifact not in (
            failed_record.outputs.stderr,
            failed_record.outputs.stdout,
        ):
            receipt = success.record_receipt
        if not isinstance(receipt, CompleteRecordReceipt):
            raise ValueError("the artifact probe requires a complete receipt")
        content = store.read_artifact(
            receipt.reference,
            artifact,
            max_bytes=artifact.size_bytes,
        )
        artifact_read = {
            "artifact_exists": True,
            "relative_name": artifact.relative_path.as_posix(),
            "declared_bytes": artifact.size_bytes,
            "finite_read_bound_bytes": artifact.size_bytes,
            "returned_bytes": len(content),
            "sha256": hashlib.sha256(content).hexdigest(),
            "digest_matches_record": (
                hashlib.sha256(content).hexdigest() == artifact.sha256
            ),
        }

    return cast(
        "dict[str, Jsonable]",
        {
            "successful_reference": {
                "reference": _reference_json(success),
                "loadable": True,
                "state": success_record.state,
            },
            "deliberately_failed_reference": {
                "reference": _reference_json(failed),
                "loadable": True,
                "state": failed_record.state,
                "outcome": failed.result.outcome.model_dump(mode="json"),
                "adapter_parser_rejected": parser_rejected_failure,
            },
            "bounded_artifact_read": artifact_read,
        },
    )


def _representative_sizes(
    *,
    entry_point: ImportableEntryPoint,
    representative_job: ExecutionJob,
    parsed_result: Jsonable,
    fixture_file: Path,
) -> dict[str, Jsonable]:
    target = representative_job.target
    if not isinstance(target, TrustedPythonTarget):
        raise TypeError("the representative job must target trusted Python")
    if not isinstance(parsed_result, dict):
        raise TypeError("the representative result must be an object")
    child_module_location = parsed_result.get("module_file")
    if not isinstance(child_module_location, str):
        raise TypeError("the fixture result must identify its module file")
    child_module_file = Path(child_module_location).resolve(strict=True)
    if child_module_file != fixture_file:
        raise ValueError(
            "parent and isolated child resolved different fixtures"
        )
    request = target.request
    driver_source = target.driver_source
    return cast(
        "dict[str, Jsonable]",
        {
            "import": {
                "module_name": entry_point.module_name,
                "attribute_name": entry_point.attribute_name,
                "installed_module_file": fixture_file.as_posix(),
                "installed_module_file_bytes": fixture_file.stat().st_size,
                "child_reported_module_file": child_module_file.as_posix(),
                "child_import_matches_parent_install": True,
                "driver_source_bytes": len(driver_source.encode("utf-8")),
            },
            "canonical_payload_bytes": {
                "caller_request": len(canonical_json_bytes(CALLER_REQUEST)),
                "enveloped_request": len(
                    canonical_json_bytes(request.to_json_dict())
                ),
                "parsed_result": len(canonical_json_bytes(parsed_result)),
            },
        },
    )


def _directory_stats(root: Path, /) -> DirectoryStats:
    objects = [path for path in root.iterdir() if path.is_dir()]
    files = [path for path in root.rglob("*") if path.is_file()]
    return DirectoryStats(
        record_objects=len(objects),
        files=len(files),
        logical_bytes=sum(path.stat().st_size for path in files),
    )


def _file_descriptor_root() -> Path | None:
    for candidate in (Path("/dev/fd"), Path("/proc/self/fd")):
        if candidate.is_dir():
            return candidate
    return None


def _file_descriptor_count(root: Path | None, /) -> int | None:
    if root is None:
        return None
    try:
        # The listing descriptor appears in its own directory and is removed
        # so the sampler does not inflate the observed parent count.
        return max(0, len(os.listdir(root)) - 1)
    except OSError:
        return None


def main() -> None:
    args = _parse_args()
    capacities = cast("tuple[int, ...]", args.capacities)
    jobs_per_capacity = cast("int", args.jobs_per_capacity)
    poll_interval_seconds = cast("float", args.poll_interval_seconds)

    fixture = importlib.import_module(FIXTURE_MODULE)
    fixture_location = fixture.__file__
    if not isinstance(fixture_location, str):
        raise TypeError("the installed fixture has no module file")
    fixture_file = Path(fixture_location).resolve(strict=True)
    repository_root = Path(__file__).resolve().parents[1]
    if fixture_file.is_relative_to(repository_root):
        raise ValueError("the fixture resolved to repository source")

    entry_point = ImportableEntryPoint(
        module_name=FIXTURE_MODULE,
        attribute_name=SUCCESS_ATTRIBUTE,
    )
    budgets = _workload_budgets()
    self_budgets = _self_budgets()
    synchronization_counter = SynchronizationCounter()
    child_tracker = ChildProcessTracker()

    with TemporaryDirectory(
        prefix="dr-exec-importable-json-benchmark-"
    ) as tmp:
        records_root = Path(tmp) / "records"
        records_root.mkdir()
        runtime = IsolatedHostPythonRuntime(Path(sys.executable))
        store = DirectoryRunStore(root=records_root)
        executor = ProcessExecutor(
            runtime=runtime,
            run_store=store,
            self_budgets=self_budgets,
        )
        capacity_reports: list[dict[str, Jsonable]] = []
        representative_completion: CompletedExecution | None = None
        representative_result: Jsonable | None = None

        with (
            synchronization_counter.instrument(),
            child_tracker.instrument(),
        ):
            for capacity in capacities:
                report, completion, parsed = _run_capacity(
                    executor=executor,
                    store=store,
                    entry_point=entry_point,
                    budgets=budgets,
                    capacity=capacity,
                    job_count=jobs_per_capacity,
                    synchronization_counter=synchronization_counter,
                    child_tracker=child_tracker,
                    poll_interval_seconds=poll_interval_seconds,
                )
                capacity_reports.append(report)
                if representative_completion is None:
                    representative_completion = completion
                    representative_result = parsed

            if (
                representative_completion is None
                or representative_result is None
            ):
                raise ValueError("the benchmark produced no representative")
            evidence = _evidence_report(
                executor=executor,
                store=store,
                success=representative_completion,
                budgets=budgets,
            )
            total_directory = _directory_stats(records_root)
            total_sync = synchronization_counter.snapshot()

        representative_job = _build_job(
            entry_point,
            CALLER_REQUEST,
            budgets=budgets,
        )
        report = cast(
            "dict[str, Jsonable]",
            {
                "report_format": REPORT_FORMAT,
                "report_format_version": REPORT_FORMAT_VERSION,
                "performance_thresholds": None,
                "host": {
                    "platform": platform.platform(),
                    "python": platform.python_version(),
                    "logical_cpus": os.cpu_count(),
                },
                "measurement": {
                    "capacities": list(capacities),
                    "jobs_per_capacity": jobs_per_capacity,
                    "resource_poll_interval_seconds": poll_interval_seconds,
                    "timed_region": (
                        "pool enter, execution, result parsing, durable record "
                        "finalization, and pool drain"
                    ),
                    "instance_reuse": {
                        "runtime": "one instance across all capacity samples",
                        "process_executor": (
                            "one instance across all capacity samples"
                        ),
                        "directory_run_store": (
                            "one instance across all capacity samples"
                        ),
                        "execution_pool": (
                            "one instance opened once for all jobs in each "
                            "capacity sample"
                        ),
                    },
                    "live_child_definition": (
                        "launched direct child with no observed return code, "
                        "until executor-owned reaping"
                    ),
                    "parent_thread_peak_includes_monitor_thread": True,
                    "file_descriptor_count_supported": (
                        _file_descriptor_root() is not None
                    ),
                    "synchronization_call_definition": (
                        "calls to dr-store's existing descriptor flush helper, "
                        "classified as manifest or sidecar calls"
                    ),
                },
                "representative_sizes": _representative_sizes(
                    entry_point=entry_point,
                    representative_job=representative_job,
                    parsed_result=representative_result,
                    fixture_file=fixture_file,
                ),
                "budget_composition": {
                    "workload": budgets.model_dump(mode="json"),
                    "executor_self": self_budgets.model_dump(mode="json"),
                    "high_volume_finite_axes": [
                        "input_bytes",
                        "payload_output",
                        "protocol_frame_bytes",
                        "protocol_total_bytes",
                        "protocol_output_count",
                        "json_depth",
                    ],
                },
                "capacity_samples": capacity_reports,
                "evidence": evidence,
                "directory_store_total": {
                    **total_directory.to_json(),
                    "synchronization_calls": total_sync.to_json(),
                },
            },
        )
        print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
