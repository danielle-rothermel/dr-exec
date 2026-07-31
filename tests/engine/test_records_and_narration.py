"""Record lifecycle and narration: neither can fail a run."""

from __future__ import annotations

import errno
import json
import logging
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path

import pytest

from dr_exec.declare import Budgets, Records
from dr_exec.errors import ExecutorFailure
from dr_exec.record import (
    EXECUTOR_IDENTITY,
    Attribution,
    RecordKey,
    RecordStatus,
    RunResult,
    TrustCategory,
)
from dr_exec.run import run_tool


def _sole_record(directory: Path) -> dict[str, object]:
    files = sorted(directory.glob("run-*.json"))
    assert len(files) == 1, files
    return json.loads(files[0].read_text())


class TestRecordLifecycle:
    def test_a_record_lands_in_the_declared_directory(
        self, tmp_path: Path, run_python: Callable[..., RunResult]
    ) -> None:
        run_python("print('recorded')", records=Records.directory(tmp_path))

        wire = _sole_record(tmp_path)
        assert wire[RecordKey.EXECUTOR_IDENTITY.value] == EXECUTOR_IDENTITY
        assert wire[RecordKey.TRUST_CATEGORY.value] == TrustCategory.TRUSTED_TOOL.value

    def test_the_record_is_finalized_with_the_outcome(
        self, tmp_path: Path, run_python: Callable[..., RunResult]
    ) -> None:
        run_python("raise SystemExit(7)", records=Records.directory(tmp_path))

        wire = _sole_record(tmp_path)
        assert wire[RecordKey.RECORD_STATUS.value] == RecordStatus.FINALIZED.value
        assert wire[RecordKey.RETURNCODE.value] == 7
        assert wire[RecordKey.ATTRIBUTION.value] == Attribution.PAYLOAD.value
        assert wire[RecordKey.FINISHED_AT.value] is not None

    def test_a_record_exists_at_spawn_time_before_the_child_exits(
        self, tmp_path: Path, run_python: Callable[..., RunResult]
    ) -> None:
        # The child observes the record directory itself: the spawn-time
        # write must already be on disk when the payload starts.
        result = run_python(
            "import json, pathlib\n"
            f"directory = pathlib.Path({str(tmp_path)!r})\n"
            "files = sorted(directory.glob('run-*.json'))\n"
            "contents = [json.loads(path.read_text()) for path in files]\n"
            "print(json.dumps([entry['record_status'] for entry in contents]))\n",
            records=Records.directory(tmp_path),
        )

        assert json.loads(result.stdout) == [RecordStatus.SPAWNED.value]
        assert (
            _sole_record(tmp_path)[RecordKey.RECORD_STATUS.value]
            == RecordStatus.FINALIZED.value
        )

    def test_the_record_names_the_scratch_workspace(
        self, tmp_path: Path, run_python: Callable[..., RunResult]
    ) -> None:
        result = run_python(
            "import os; print(os.getcwd())", records=Records.directory(tmp_path)
        )

        wire = _sole_record(tmp_path)
        assert wire[RecordKey.SCRATCH_PATH.value] == result.stdout.strip()

    def test_records_none_writes_nothing(
        self, tmp_path: Path, run_python: Callable[..., RunResult]
    ) -> None:
        run_python("print('quiet')", records=Records.none())

        assert list(tmp_path.iterdir()) == []

    def test_a_record_write_failure_never_fails_the_run(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        run_python: Callable[..., RunResult],
    ) -> None:
        # Monkeypatched rather than chmod-driven: a directory mode is no
        # obstacle to root, so the permission form of this test passes
        # vacuously in a CI container.
        def refuse(*_: object, **__: object) -> None:
            raise OSError(errno.EACCES, "Permission denied")

        monkeypatch.setattr(Path, "write_text", refuse)

        result = run_python("print('completed')", records=Records.directory(tmp_path))

        assert result.stdout == "completed\n"
        assert result.outcome.attribution is Attribution.PAYLOAD
        assert list(tmp_path.iterdir()) == []

    def test_a_finalize_write_failure_lands_as_write_failed_on_disk(
        self, tmp_path: Path, run_python: Callable[..., RunResult]
    ) -> None:
        # The spawn write succeeds and the finalize write fails, so a durable
        # record survives the failure: it has to say the failure happened
        # rather than sit at the mid-flight status forever.
        real_write_text = Path.write_text
        writes = {"count": 0}

        def fail_after_the_spawn_write(
            self: Path, data: str, **keywords: object
        ) -> int:
            writes["count"] += 1
            if writes["count"] == 2:
                raise OSError(errno.EIO, "Input/output error")
            return real_write_text(self, data, **keywords)  # type: ignore[arg-type]

        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(Path, "write_text", fail_after_the_spawn_write)
            result = run_python(
                "print('completed')", records=Records.directory(tmp_path)
            )

        assert result.outcome.attribution is Attribution.PAYLOAD
        assert (
            _sole_record(tmp_path)[RecordKey.RECORD_STATUS.value]
            == RecordStatus.WRITE_FAILED.value
        )

    def test_a_write_failure_is_narrated_on_the_record_logger(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
        run_python: Callable[..., RunResult],
    ) -> None:
        def refuse(*_: object, **__: object) -> None:
            raise OSError(errno.EACCES, "Permission denied")

        monkeypatch.setattr(Path, "write_text", refuse)

        with caplog.at_level(logging.WARNING, logger="dr_exec.record"):
            run_python("pass", records=Records.directory(tmp_path))

        assert any(
            "could not be written" in record.getMessage() for record in caplog.records
        )

    def test_concurrent_runs_write_distinct_record_files(
        self, tmp_path: Path, run_python: Callable[..., RunResult]
    ) -> None:
        run_python("pass", records=Records.directory(tmp_path))
        run_python("pass", records=Records.directory(tmp_path))

        assert len(sorted(tmp_path.glob("run-*.json"))) == 2


class TestExecutorFailureRecords:
    def test_an_executor_failure_leaves_a_terminal_record_status(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # A record still reading `spawned` cannot be told apart from one
        # whose parent process died mid-run, so the executor's own abort
        # writes its own terminal status before the exception propagates.
        def never_reaps(self: object, timeout: float | None = None) -> int:
            raise subprocess.TimeoutExpired(cmd="stub", timeout=timeout or 0.0)

        monkeypatch.setattr(subprocess.Popen, "wait", never_reaps)

        with pytest.raises(ExecutorFailure, match="termination self-budget"):
            run_tool(
                [sys.executable, "-I", "-c", "pass"],
                budgets=Budgets(wall_clock=5.0),
                records=Records.directory(tmp_path),
            )

        wire = _sole_record(tmp_path)
        assert wire[RecordKey.RECORD_STATUS.value] == RecordStatus.EXECUTOR_FAILED.value
        assert wire[RecordKey.ATTRIBUTION.value] == Attribution.EXECUTOR.value
        assert wire[RecordKey.FINISHED_AT.value] is not None


class TestNarration:
    def test_lifecycle_narration_lands_on_the_dr_exec_loggers(
        self, caplog: pytest.LogCaptureFixture, run_python: Callable[..., RunResult]
    ) -> None:
        with caplog.at_level(logging.DEBUG, logger="dr_exec"):
            run_python("print('narrated')")

        messages = [record.getMessage() for record in caplog.records]
        assert any(message.startswith("spawning ") for message in messages)
        assert any("waiting on pid" in message for message in messages)
        assert any("killing process group" in message for message in messages)
        assert any("reaped pid" in message for message in messages)

    def test_the_record_location_is_narrated(
        self,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
        run_python: Callable[..., RunResult],
    ) -> None:
        with caplog.at_level(logging.INFO, logger="dr_exec.record"):
            run_python("pass", records=Records.directory(tmp_path))

        assert any("run record at" in record.getMessage() for record in caplog.records)

    def test_narration_never_enters_the_child_streams(
        self, caplog: pytest.LogCaptureFixture, run_python: Callable[..., RunResult]
    ) -> None:
        with caplog.at_level(logging.DEBUG, logger="dr_exec"):
            result = run_python(
                "import sys\n"
                "sys.stderr.write('exactly this')\n"
                "sys.stdout.write('and this')\n"
            )

        assert result.stdout == "and this"
        assert result.stderr == "exactly this"
        assert caplog.records

    def test_a_broken_narration_handler_never_fails_the_run(
        self, run_python: Callable[..., RunResult]
    ) -> None:
        class ExplodingHandler(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                raise RuntimeError("handler exploded")

        logger = logging.getLogger("dr_exec")
        handler = ExplodingHandler()
        logger.addHandler(handler)
        try:
            result = run_python("print('survived')")
        finally:
            logger.removeHandler(handler)

        assert result.stdout == "survived\n"


class TestMeasurements:
    def test_duration_is_spawn_to_reap_and_teardown_is_its_own_field(
        self, run_python: Callable[..., RunResult]
    ) -> None:
        result = run_python("import time; time.sleep(0.2)")

        # Bounded above as well as below: a lower bound alone passes for any
        # inflated number, and the interval this pins is a narrow one.
        assert 0.2 <= result.measurements.duration_seconds < 2.0
        assert 0.0 <= result.measurements.teardown_seconds < 1.0
        assert (
            result.measurements.teardown_seconds <= result.measurements.duration_seconds
        )

    def test_teardown_is_measured_not_reported_as_zero(
        self, tmp_path: Path, run_python: Callable[..., RunResult]
    ) -> None:
        # A grandchild in the run's group makes teardown do real work, so
        # this is the case where a hardcoded zero is observable.
        pid_path = tmp_path / "descendant.pid"
        result = run_python(
            "import pathlib, subprocess, sys\n"
            "child = subprocess.Popen([sys.executable, '-c', "
            "'import time; time.sleep(60)'])\n"
            f"pathlib.Path({str(pid_path)!r}).write_text(str(child.pid))\n"
            "raise SystemExit(0)\n"
        )

        assert result.measurements.teardown_seconds > 0.0

    def test_parent_side_setup_is_excluded_from_duration(
        self, run_python: Callable[..., RunResult]
    ) -> None:
        # A large declared input is encoded and validated before the spawn
        # clock starts, so it must not inflate the measured duration.
        result = run_python(
            "import sys; sys.stdin.buffer.read()",
            input_text="x" * (4 * 1024 * 1024),
        )

        assert result.measurements.duration_seconds < 5.0

    def test_bytes_produced_are_counted_per_stream(
        self, run_python: Callable[..., RunResult]
    ) -> None:
        result = run_python("import os; os.write(1, b'x' * 10); os.write(2, b'y' * 20)")

        assert result.measurements.stdout_bytes_produced == 10
        assert result.measurements.stderr_bytes_produced == 20


class TestSpawnFailureRecords:
    def test_an_absence_outcome_is_still_recorded(self, tmp_path: Path) -> None:
        run_tool(
            ["/nonexistent/dr-exec-probe"],
            budgets=Budgets(wall_clock=5.0),
            records=Records.directory(tmp_path),
        )

        wire = _sole_record(tmp_path)
        assert wire[RecordKey.ATTRIBUTION.value] == Attribution.ABSENCE.value
        assert wire[RecordKey.RECORD_STATUS.value] == RecordStatus.FINALIZED.value
        assert wire[RecordKey.RETURNCODE.value] is None
