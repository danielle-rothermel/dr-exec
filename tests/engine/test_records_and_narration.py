"""Record lifecycle and narration: neither can fail a run."""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from pathlib import Path

import pytest

from dr_exec.declare import Budgets, Records
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
        run_python("print('recorded')", records=Records.directory_at(tmp_path))

        wire = _sole_record(tmp_path)
        assert wire[RecordKey.EXECUTOR_IDENTITY.value] == EXECUTOR_IDENTITY
        assert wire[RecordKey.TRUST_CATEGORY.value] == TrustCategory.TRUSTED_TOOL.value

    def test_the_record_is_finalized_with_the_outcome(
        self, tmp_path: Path, run_python: Callable[..., RunResult]
    ) -> None:
        run_python("raise SystemExit(7)", records=Records.directory_at(tmp_path))

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
            records=Records.directory_at(tmp_path),
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
            "import os; print(os.getcwd())", records=Records.directory_at(tmp_path)
        )

        wire = _sole_record(tmp_path)
        assert wire[RecordKey.SCRATCH_PATH.value] == result.stdout.strip()

    def test_records_none_writes_nothing(
        self, tmp_path: Path, run_python: Callable[..., RunResult]
    ) -> None:
        run_python("print('quiet')", records=Records.none())

        assert list(tmp_path.iterdir()) == []

    def test_a_record_write_failure_never_fails_the_run(
        self, tmp_path: Path, run_python: Callable[..., RunResult]
    ) -> None:
        unwritable = tmp_path / "unwritable"
        unwritable.mkdir(mode=0o500)

        try:
            result = run_python(
                "print('completed')", records=Records.directory_at(unwritable)
            )
        finally:
            unwritable.chmod(0o700)

        assert result.stdout == "completed\n"
        assert result.outcome.attribution is Attribution.PAYLOAD
        assert list(unwritable.iterdir()) == []

    def test_concurrent_runs_write_distinct_record_files(
        self, tmp_path: Path, run_python: Callable[..., RunResult]
    ) -> None:
        run_python("pass", records=Records.directory_at(tmp_path))
        run_python("pass", records=Records.directory_at(tmp_path))

        assert len(sorted(tmp_path.glob("run-*.json"))) == 2


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
            run_python("pass", records=Records.directory_at(tmp_path))

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

        assert result.measurements.duration_seconds >= 0.2
        assert result.measurements.teardown_seconds >= 0.0
        assert (
            result.measurements.teardown_seconds <= result.measurements.duration_seconds
        )

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
            records=Records.directory_at(tmp_path),
        )

        wire = _sole_record(tmp_path)
        assert wire[RecordKey.ATTRIBUTION.value] == Attribution.ABSENCE.value
        assert wire[RecordKey.RECORD_STATUS.value] == RecordStatus.FINALIZED.value
        assert wire[RecordKey.RETURNCODE.value] is None
