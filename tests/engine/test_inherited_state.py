"""What the child inherits: descriptors, environment, cwd, and nothing else."""

from __future__ import annotations

import errno
import json
import os
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path

import pytest

from dr_exec.declare import (
    Budgets,
    EnvironmentGrant,
    Records,
)
from dr_exec.errors import DeclarationError
from dr_exec.record import Attribution, RunResult
from dr_exec.run import run_tool

_PLATFORM_EXEC_VARIABLES = frozenset({"LC_CTYPE", "__CF_USER_TEXT_ENCODING"})
"""What the platform's own exec machinery adds beneath the grant on macOS.

The grant governs what the executor delivers; the C library's locale
handshake is outside it, so an exactness assertion allows only these.
"""


_OPEN_DESCRIPTOR_PROBE = (
    "import json, os\n"
    "open_fds = []\n"
    "for fd in range(64):\n"
    "    try:\n"
    "        os.fstat(fd)\n"
    "    except OSError:\n"
    "        continue\n"
    "    open_fds.append(fd)\n"
    "print(json.dumps(open_fds))\n"
)
"""The child's whole descriptor table, asserted without a filter.

``fstat`` over a fixed range answers for every number a leak could occupy.
A ``/dev/fd`` listing would need its own handle excluded, and excluding it
by numeric threshold discards exactly the region a leak lands in — which is
how a descriptor-probe test comes to prove nothing.
"""


class TestDescriptorTable:
    def test_the_child_starts_with_exactly_descriptors_zero_one_and_two(
        self, run_python: Callable[..., RunResult]
    ) -> None:
        result = run_python(
            "import json, os, sys\n"
            "duplicate = os.dup(1)\n"
            "print(json.dumps({'duplicate': duplicate}))\n"
        )

        assert result.returncode == 0, result.stderr
        assert json.loads(result.stdout) == {"duplicate": 3}

    def test_the_child_sees_no_extra_open_descriptors(
        self, run_python: Callable[..., RunResult]
    ) -> None:
        result = run_python(_OPEN_DESCRIPTOR_PROBE)

        assert result.returncode == 0, result.stderr
        assert json.loads(result.stdout) == [0, 1, 2]

    def test_a_parent_side_inheritable_descriptor_never_reaches_the_child(
        self, tmp_path: Path, run_python: Callable[..., RunResult]
    ) -> None:
        # The realistic future edit: an executor descriptor opened without
        # O_CLOEXEC before the spawn. `close_fds=True` is what keeps it out
        # of the child, and this is the test that observes the child rather
        # than the Popen keywords.
        leaked = os.open(tmp_path / "leak", os.O_CREAT | os.O_RDWR)
        # A high number too, so a leak that lands above the descriptors the
        # child opens for itself is caught as readily as a contiguous one.
        high = os.dup2(leaked, 31)
        os.set_inheritable(leaked, True)
        os.set_inheritable(high, True)
        try:
            result = run_python(_OPEN_DESCRIPTOR_PROBE)
        finally:
            os.close(high)
            os.close(leaked)

        assert result.returncode == 0, result.stderr
        assert json.loads(result.stdout) == [0, 1, 2]


class TestEnvironmentGrants:
    def test_none_grants_an_empty_environment(
        self, monkeypatch: pytest.MonkeyPatch, run_python: Callable[..., RunResult]
    ) -> None:
        monkeypatch.setenv("DR_EXEC_MUST_NOT_LEAK", "secret")

        result = run_python(
            "import json, os; print(json.dumps(dict(os.environ)))",
            environment=EnvironmentGrant.none(),
        )

        assert "DR_EXEC_MUST_NOT_LEAK" not in json.loads(result.stdout)

    def test_named_delivers_the_values_snapshotted_at_construction(
        self, monkeypatch: pytest.MonkeyPatch, run_python: Callable[..., RunResult]
    ) -> None:
        monkeypatch.setenv("DR_EXEC_SNAPSHOT", "at-construction")
        grant = EnvironmentGrant.named(["DR_EXEC_SNAPSHOT"])
        monkeypatch.setenv("DR_EXEC_SNAPSHOT", "changed-after")

        result = run_python(
            "import json, os; print(json.dumps(os.environ.get('DR_EXEC_SNAPSHOT')))",
            environment=grant,
        )

        assert json.loads(result.stdout) == "at-construction"

    def test_fixed_delivers_exactly_the_literal_environment(
        self, monkeypatch: pytest.MonkeyPatch, run_python: Callable[..., RunResult]
    ) -> None:
        monkeypatch.setenv("DR_EXEC_MUST_NOT_LEAK", "secret")

        result = run_python(
            "import json, os; print(json.dumps(dict(os.environ)))",
            environment=EnvironmentGrant.fixed({"OPENBLAS_NUM_THREADS": "1"}),
        )

        delivered = json.loads(result.stdout)
        assert delivered["OPENBLAS_NUM_THREADS"] == "1"
        assert set(delivered) <= {"OPENBLAS_NUM_THREADS"} | _PLATFORM_EXEC_VARIABLES

    def test_overlay_delivers_the_parent_environment_plus_extras(
        self, monkeypatch: pytest.MonkeyPatch, run_python: Callable[..., RunResult]
    ) -> None:
        monkeypatch.setenv("DR_EXEC_INHERITED", "inherited")

        result = run_python(
            "import json, os\n"
            "print(json.dumps({\n"
            "    'inherited': os.environ.get('DR_EXEC_INHERITED'),\n"
            "    'extra': os.environ.get('DR_EXEC_EXTRA'),\n"
            "}))\n",
            environment=EnvironmentGrant.overlay({"DR_EXEC_EXTRA": "extra"}),
        )

        assert json.loads(result.stdout) == {
            "inherited": "inherited",
            "extra": "extra",
        }

    def test_an_overlay_exclusion_present_in_the_parent_is_a_caller_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("DR_EXEC_FORBIDDEN", "present")

        with pytest.raises(DeclarationError, match="DR_EXEC_FORBIDDEN"):
            run_tool(
                [sys.executable, "-I", "-c", "pass"],
                budgets=Budgets(wall_clock=5.0),
                records=Records.none(),
                environment=EnvironmentGrant.overlay(
                    {}, exclusions=["DR_EXEC_FORBIDDEN"]
                ),
            )

    def test_an_absent_overlay_exclusion_spawns_normally(
        self, monkeypatch: pytest.MonkeyPatch, run_python: Callable[..., RunResult]
    ) -> None:
        monkeypatch.delenv("DR_EXEC_FORBIDDEN", raising=False)

        result = run_python(
            "import json, os; print(json.dumps(os.environ.get('DR_EXEC_FORBIDDEN')))",
            environment=EnvironmentGrant.overlay({}, exclusions=["DR_EXEC_FORBIDDEN"]),
        )

        assert json.loads(result.stdout) is None


class TestScratchWorkspace:
    def test_the_child_runs_in_a_fresh_workspace_that_is_removed_after(
        self, run_python: Callable[..., RunResult]
    ) -> None:
        result = run_python("import os; print(os.getcwd())")

        scratch = Path(result.stdout.strip())
        assert scratch.name.startswith("dr-exec-")
        assert not scratch.exists()

    def test_concurrent_runs_get_distinct_workspaces(
        self, run_python: Callable[..., RunResult]
    ) -> None:
        first = run_python("import os; print(os.getcwd())")
        second = run_python("import os; print(os.getcwd())")

        assert first.stdout != second.stdout

    def test_a_workspace_the_payload_filled_is_still_removed(
        self, run_python: Callable[..., RunResult]
    ) -> None:
        result = run_python(
            "import os, pathlib\n"
            "pathlib.Path('nested/deeper').mkdir(parents=True)\n"
            "pathlib.Path('nested/deeper/artifact').write_text('x')\n"
            "print(os.getcwd())\n"
        )

        assert not Path(result.stdout.strip()).exists()

    def test_a_cleanup_failure_never_converts_a_completed_run_into_a_raise(
        self,
        monkeypatch: pytest.MonkeyPatch,
        run_python: Callable[..., RunResult],
    ) -> None:
        from dr_exec import engine

        def refuse_removal(path: object) -> None:
            raise OSError(errno.EBUSY, "Device or resource busy")

        monkeypatch.setattr(engine.shutil, "rmtree", refuse_removal)

        result = run_python("print('completed')")

        assert result.stdout == "completed\n"
        assert result.outcome.attribution is Attribution.PAYLOAD


class TestStdin:
    def test_a_child_reading_stdin_with_no_declared_input_sees_eof(
        self, run_python: Callable[..., RunResult]
    ) -> None:
        result = run_python("import sys; print(repr(sys.stdin.read()))")

        assert result.stdout == "''\n"

    def test_a_child_that_ignores_stdin_still_completes(
        self, run_python: Callable[..., RunResult]
    ) -> None:
        result = run_python("print('ignored')", input_text="x" * 100000)

        assert result.returncode == 0
        assert result.stdout == "ignored\n"


class TestSpawnErrno:
    def test_an_unresolvable_program_is_an_absence_outcome(self) -> None:
        result = run_tool(
            ["/nonexistent/dr-exec-probe"],
            budgets=Budgets(wall_clock=5.0),
            records=Records.none(),
        )

        assert result.outcome.attribution is Attribution.ABSENCE
        assert result.outcome.spawn_errno == errno.ENOENT
        assert result.returncode is None

    def test_a_non_executable_program_is_machine_attributed_with_its_errno(
        self, tmp_path: Path
    ) -> None:
        unexecutable = tmp_path / "not-executable"
        unexecutable.write_text("#!/bin/sh\necho hi\n")
        unexecutable.chmod(0o600)

        result = run_tool(
            [str(unexecutable)],
            budgets=Budgets(wall_clock=5.0),
            records=Records.none(),
        )

        assert result.outcome.attribution is Attribution.MACHINE
        assert result.outcome.spawn_errno == errno.EACCES

    def test_a_relative_program_with_no_granted_path_is_a_caller_error(self) -> None:
        with pytest.raises(DeclarationError, match="declares no PATH"):
            run_tool(
                ["python3"],
                budgets=Budgets(wall_clock=5.0),
                records=Records.none(),
                environment=EnvironmentGrant.none(),
            )

    def test_a_relative_program_resolves_against_the_granted_path(self) -> None:
        result = run_tool(
            ["sh", "-c", "printf resolved"],
            budgets=Budgets(wall_clock=5.0),
            records=Records.none(),
            environment=EnvironmentGrant.fixed({"PATH": "/bin:/usr/bin"}),
        )

        assert result.stdout == "resolved"


class TestArgvValidation:
    @pytest.mark.parametrize(
        "command",
        [
            (),
            [],
            "python",
            b"python",
            ("",),
            (sys.executable, 1),
            (sys.executable, "a\0b"),
        ],
    )
    def test_malformed_argv_is_rejected_before_any_spawn(self, command: object) -> None:
        with pytest.raises(DeclarationError):
            run_tool(
                command,  # type: ignore[arg-type]
                budgets=Budgets(wall_clock=5.0),
                records=Records.none(),
            )

    def test_empty_arguments_are_preserved(self) -> None:
        result = run_tool(
            [
                sys.executable,
                "-I",
                "-c",
                "import json, sys; print(json.dumps(sys.argv[1:]))",
                "",
            ],
            budgets=Budgets(wall_clock=5.0),
            records=Records.none(),
        )

        assert json.loads(result.stdout) == [""]

    def test_nothing_is_ever_interpreted_by_a_shell(self) -> None:
        result = run_tool(
            [
                sys.executable,
                "-I",
                "-c",
                "import sys; print(sys.argv[1])",
                "$(echo hi)",
            ],
            budgets=Budgets(wall_clock=5.0),
            records=Records.none(),
        )

        assert result.stdout == "$(echo hi)\n"


class TestByteExactCapture:
    def test_captured_output_carries_no_framing_or_newline_normalization(
        self, run_python: Callable[..., RunResult]
    ) -> None:
        result = run_python(
            "import sys\n"
            "sys.stdout.buffer.write(b'a\\r\\nb')\n"
            "sys.stdout.buffer.flush()\n"
        )

        assert result.stdout == "a\r\nb"

    def test_hostile_bytes_decode_to_replacement_instead_of_raising(
        self, run_python: Callable[..., RunResult]
    ) -> None:
        result = run_python(
            "import sys\n"
            "sys.stdout.buffer.write(b'\\xff\\xfe')\n"
            "sys.stdout.buffer.flush()\n"
        )

        assert result.stdout == "��"
        assert result.outcome.attribution is Attribution.PAYLOAD


class TestSpawnShape:
    def test_the_child_runs_in_a_fresh_session(
        self, run_python: Callable[..., RunResult]
    ) -> None:
        result = run_python(
            "import json, os\n"
            "print(json.dumps({'sid': os.getsid(0), 'pid': os.getpid()}))\n"
        )

        reported = json.loads(result.stdout)
        assert reported["sid"] == reported["pid"]
        assert reported["sid"] != os.getsid(0)

    def test_close_fds_is_set_and_no_descriptors_are_passed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        seen: dict[str, object] = {}
        real_popen = subprocess.Popen

        def record_popen(argv: object, **kwargs: object) -> object:
            seen.update(kwargs)
            return real_popen(argv, **kwargs)  # type: ignore[arg-type]

        monkeypatch.setattr(subprocess, "Popen", record_popen)

        run_tool(
            [sys.executable, "-I", "-c", "pass"],
            budgets=Budgets(wall_clock=5.0),
            records=Records.none(),
        )

        assert seen["start_new_session"] is True
        assert seen["close_fds"] is True
        assert "pass_fds" not in seen
