"""What a written record claims the run was, against independent literals.

The record is the durable artifact consumers derive persisted cache keys
from, so every identity field is asserted against a hash computed here
rather than against the executor's own helper: a constant or a stale digest
in the writing path collapses every cache key onto one, and nothing at
runtime would say so.

The invocation half is a disjunction — argv *or* source digest — so these
also pin which branch each entry point writes.
"""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

from dr_exec.batch import BatchItem, BatchRequest, run_batch
from dr_exec.declare import (
    PROCESS_BOUNDARY_ONLY,
    Budgets,
    EnvironmentGrant,
    OutputBudget,
    OverflowPolicy,
    Records,
)
from dr_exec.record import RecordKey
from dr_exec.run import run_tool, run_untrusted_python

_SOURCE = "SECRET_TOKEN = 'sk-live-abcdef0123456789'\nprint('ok')\n"
_INPUT = "stdin payload the record digests"


def _sole_record(directory: Path) -> dict[str, object]:
    files = sorted(directory.glob("run-*.json"))
    assert len(files) == 1, files
    return json.loads(files[0].read_text())


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


class TestSourceRunsRecordTheDigestNotTheSource:
    def test_an_untrusted_python_record_carries_no_argv(self, tmp_path: Path) -> None:
        run_untrusted_python(
            _SOURCE,
            profile=PROCESS_BOUNDARY_ONLY,
            budgets=Budgets(wall_clock=10.0),
            records=Records.directory(tmp_path),
        )

        wire = _sole_record(tmp_path)
        assert wire[RecordKey.ARGV.value] is None
        assert wire[RecordKey.SOURCE_DIGEST.value] == _sha256(_SOURCE)

    def test_the_source_text_is_nowhere_in_the_written_record(
        self, tmp_path: Path
    ) -> None:
        run_untrusted_python(
            _SOURCE,
            profile=PROCESS_BOUNDARY_ONLY,
            budgets=Budgets(wall_clock=10.0),
            records=Records.directory(tmp_path),
        )

        contents = sorted(tmp_path.glob("run-*.json"))[0].read_text()
        assert "SECRET_TOKEN" not in contents

    def test_a_batch_record_carries_no_item_payloads(self, tmp_path: Path) -> None:
        request = BatchRequest(
            items=(BatchItem(item_id="one", payload="CONFIDENTIAL-PROMPT-7"),),
            body_source="def run_item(item_id, payload):\n    return {'ok': item_id}\n",
            item_schema="opaque",
            config={"model": "probe"},
        )

        run_batch(
            request,
            profile=PROCESS_BOUNDARY_ONLY,
            budgets=Budgets(wall_clock=20.0),
            records=Records.directory(tmp_path),
        )

        contents = sorted(tmp_path.glob("run-*.json"))[0].read_text()
        assert "CONFIDENTIAL-PROMPT-7" not in contents
        assert json.loads(contents)[RecordKey.ARGV.value] is None

    def test_a_command_run_records_argv_and_no_source_digest(
        self, tmp_path: Path
    ) -> None:
        # Argv *is* the invocation for a command form, so this branch of the
        # disjunction records it in full.
        command = [sys.executable, "-I", "-c", "pass"]

        run_tool(
            command,
            budgets=Budgets(wall_clock=10.0),
            records=Records.directory(tmp_path),
        )

        wire = _sole_record(tmp_path)
        assert wire[RecordKey.ARGV.value] == command
        assert wire[RecordKey.SOURCE_DIGEST.value] is None


class TestIdentityFieldsComeFromTheActualInvocation:
    def test_every_identity_field_matches_an_independently_computed_literal(
        self, tmp_path: Path
    ) -> None:
        grant = EnvironmentGrant.fixed({"BETA": "two", "ALPHA": "one"})

        run_untrusted_python(
            _SOURCE,
            profile=PROCESS_BOUNDARY_ONLY,
            budgets=Budgets(
                wall_clock=12.5,
                output=OutputBudget(
                    limit_bytes=4096, overflow_policy=OverflowPolicy.MARKED_TRUNCATION
                ),
                input=8192,
            ),
            records=Records.directory(tmp_path),
            input_text=_INPUT,
            environment=grant,
        )

        wire = _sole_record(tmp_path)
        assert wire[RecordKey.SOURCE_DIGEST.value] == _sha256(_SOURCE)
        assert wire[RecordKey.INPUT_DIGEST.value] == _sha256(_INPUT)
        # Canonicalization: names sorted, name=value, NUL-joined, UTF-8.
        assert wire[RecordKey.GRANT_CONTENTS_DIGEST.value] == _sha256(
            "ALPHA=one\0BETA=two"
        )
        assert wire[RecordKey.GRANT_NAMES.value] == ["ALPHA", "BETA"]
        assert wire[RecordKey.GRANT_KIND.value] == "fixed"
        assert wire[RecordKey.BUDGET_WALL_CLOCK_SECONDS.value] == 12.5
        assert wire[RecordKey.BUDGET_OUTPUT_BYTES.value] == 4096
        assert (
            wire[RecordKey.BUDGET_OUTPUT_OVERFLOW_POLICY.value] == "marked_truncation"
        )
        assert wire[RecordKey.BUDGET_INPUT_BYTES.value] == 8192
        assert wire[RecordKey.UNBUDGETED_AXES.value] == []
        assert wire[RecordKey.INPUT_BYTES.value] == len(_INPUT.encode("utf-8"))

    def test_a_different_source_writes_a_different_digest(self, tmp_path: Path) -> None:
        # A constant digest is invisible in any single record; two runs that
        # differ only in source are what catch it.
        for index, source in enumerate(("print(1)\n", "print(2)\n")):
            directory = tmp_path / str(index)
            directory.mkdir()
            run_untrusted_python(
                source,
                profile=PROCESS_BOUNDARY_ONLY,
                budgets=Budgets(wall_clock=10.0),
                records=Records.directory(directory),
            )

        first = _sole_record(tmp_path / "0")
        second = _sole_record(tmp_path / "1")
        assert first[RecordKey.SOURCE_DIGEST.value] == _sha256("print(1)\n")
        assert second[RecordKey.SOURCE_DIGEST.value] == _sha256("print(2)\n")

    def test_an_unbudgeted_axis_is_named_in_the_record(self, tmp_path: Path) -> None:
        run_untrusted_python(
            "pass",
            profile=PROCESS_BOUNDARY_ONLY,
            budgets=Budgets(wall_clock=10.0),
            records=Records.directory(tmp_path),
        )

        wire = _sole_record(tmp_path)
        assert sorted(wire[RecordKey.UNBUDGETED_AXES.value]) == ["input", "output"]
        assert wire[RecordKey.BUDGET_OUTPUT_BYTES.value] == "unbudgeted"
