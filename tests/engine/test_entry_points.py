"""The three entry points: signatures, trust categorization, and the
untrusted-Python invocation shape."""

from __future__ import annotations

import inspect
import json
import sys
from pathlib import Path

import pytest

from dr_exec.declare import (
    HERMETIC,
    PROCESS_BOUNDARY_ONLY,
    REPORT_ONLY,
    SOURCE_BOUND_BYTES,
    Budgets,
    EnvironmentGrant,
    ExitPolicy,
    ExitVerdict,
    PythonRuntime,
    Records,
)
from dr_exec.errors import DeclarationError
from dr_exec.record import Attribution, RecordKey, TrustCategory
from dr_exec.run import run_tool, run_untrusted_command, run_untrusted_python

_QUICK = Budgets(wall_clock=10.0)


def _sole_record(directory: Path) -> dict[str, object]:
    files = sorted(directory.glob("run-*.json"))
    assert len(files) == 1, files
    return json.loads(files[0].read_text())


class TestSignatures:
    @pytest.mark.parametrize(
        "entry_point", [run_tool, run_untrusted_python, run_untrusted_command]
    )
    def test_records_is_a_required_keyword(self, entry_point: object) -> None:
        parameter = inspect.signature(entry_point).parameters["records"]  # type: ignore[arg-type]

        assert parameter.kind is inspect.Parameter.KEYWORD_ONLY
        assert parameter.default is inspect.Parameter.empty

    @pytest.mark.parametrize(
        "entry_point", [run_tool, run_untrusted_python, run_untrusted_command]
    )
    def test_budgets_is_a_required_keyword(self, entry_point: object) -> None:
        parameter = inspect.signature(entry_point).parameters["budgets"]  # type: ignore[arg-type]

        assert parameter.kind is inspect.Parameter.KEYWORD_ONLY
        assert parameter.default is inspect.Parameter.empty

    @pytest.mark.parametrize(
        "entry_point", [run_untrusted_python, run_untrusted_command]
    )
    def test_profile_is_required_and_undefaultable_on_untrusted_forms(
        self, entry_point: object
    ) -> None:
        parameter = inspect.signature(entry_point).parameters["profile"]  # type: ignore[arg-type]

        assert parameter.kind is inspect.Parameter.KEYWORD_ONLY
        assert parameter.default is inspect.Parameter.empty

    def test_run_tool_declares_no_profile(self) -> None:
        assert "profile" not in inspect.signature(run_tool).parameters

    @pytest.mark.parametrize(
        "entry_point", [run_tool, run_untrusted_python, run_untrusted_command]
    )
    def test_every_entry_point_shares_the_declaration_surface(
        self, entry_point: object
    ) -> None:
        parameters = inspect.signature(entry_point).parameters  # type: ignore[arg-type]

        assert parameters["input_text"].default == ""
        assert parameters["environment"].default == EnvironmentGrant.none()
        assert parameters["exit_policy"].default is REPORT_ONLY

    @pytest.mark.parametrize(
        ("entry_point", "expected"),
        [
            (
                run_tool,
                (
                    "command",
                    "budgets",
                    "records",
                    "input_text",
                    "environment",
                    "exit_policy",
                ),
            ),
            (
                run_untrusted_python,
                (
                    "source",
                    "profile",
                    "budgets",
                    "records",
                    "runtime",
                    "input_text",
                    "environment",
                    "exit_policy",
                ),
            ),
            (
                run_untrusted_command,
                (
                    "command",
                    "profile",
                    "budgets",
                    "records",
                    "input_text",
                    "environment",
                    "exit_policy",
                ),
            ),
        ],
        ids=["run_tool", "run_untrusted_python", "run_untrusted_command"],
    )
    def test_the_parameter_set_is_exactly_the_declaration_surface(
        self, entry_point: object, expected: tuple[str, ...]
    ) -> None:
        # Asymmetries between the three are limited to the trust parameters,
        # so a capture-shaping knob on one of them and not the others is a
        # divergence this pins rather than lets pass.
        assert tuple(inspect.signature(entry_point).parameters) == expected  # type: ignore[arg-type]

    def test_the_python_runtime_defaults_to_hermetic(self) -> None:
        parameter = inspect.signature(run_untrusted_python).parameters["runtime"]

        assert parameter.default is HERMETIC


class TestTrustCategoryIsRecorded:
    def test_run_tool_records_the_trusted_tool_category(self, tmp_path: Path) -> None:
        run_tool(
            [sys.executable, "-I", "-c", "pass"],
            budgets=_QUICK,
            records=Records.directory(tmp_path),
        )

        assert (
            _sole_record(tmp_path)[RecordKey.TRUST_CATEGORY.value]
            == TrustCategory.TRUSTED_TOOL.value
        )

    def test_run_untrusted_python_records_its_category_and_runtime(
        self, tmp_path: Path
    ) -> None:
        run_untrusted_python(
            "pass",
            profile=PROCESS_BOUNDARY_ONLY,
            budgets=_QUICK,
            records=Records.directory(tmp_path),
        )

        wire = _sole_record(tmp_path)
        assert (
            wire[RecordKey.TRUST_CATEGORY.value] == TrustCategory.UNTRUSTED_PYTHON.value
        )
        assert wire[RecordKey.RUNTIME_NAME.value] == "hermetic"
        assert wire[RecordKey.PROFILE_NAME.value] == "process_boundary_only"
        assert wire[RecordKey.SOURCE_DIGEST.value] is not None

    def test_run_untrusted_command_records_its_category(self, tmp_path: Path) -> None:
        run_untrusted_command(
            [sys.executable, "-I", "-c", "pass"],
            profile=PROCESS_BOUNDARY_ONLY,
            budgets=_QUICK,
            records=Records.directory(tmp_path),
        )

        wire = _sole_record(tmp_path)
        assert (
            wire[RecordKey.TRUST_CATEGORY.value]
            == TrustCategory.UNTRUSTED_COMMAND.value
        )
        assert wire[RecordKey.SOURCE_DIGEST.value] is None


class TestHermeticInvocationShape:
    def test_hermetic_runs_the_running_interpreter_isolated_with_source_as_argv(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from dr_exec import engine

        seen: list[tuple[str, ...]] = []
        real_execute = engine.execute

        def record_argv(declaration: engine.Declaration) -> object:
            seen.append(declaration.invocation.argv)
            return real_execute(declaration)

        monkeypatch.setattr("dr_exec.run.execute", record_argv)

        run_untrusted_python(
            "pass",
            profile=PROCESS_BOUNDARY_ONLY,
            budgets=_QUICK,
            records=Records.none(),
        )

        assert seen == [(sys.executable, "-I", "-c", "pass")]

    def test_the_source_has_no_file_and_reports_module_main(self) -> None:
        result = run_untrusted_python(
            "import json, sys\n"
            "print(json.dumps({\n"
            "    'name': __name__,\n"
            "    'has_file': '__file__' in globals(),\n"
            "    'cwd_on_path': '' in sys.path,\n"
            "}))\n",
            profile=PROCESS_BOUNDARY_ONLY,
            budgets=_QUICK,
            records=Records.none(),
        )

        assert json.loads(result.stdout) == {
            "name": "__main__",
            "has_file": False,
            "cwd_on_path": False,
        }

    def test_a_traceback_names_the_string_source(self) -> None:
        result = run_untrusted_python(
            "raise ValueError('boom')",
            profile=PROCESS_BOUNDARY_ONLY,
            budgets=_QUICK,
            records=Records.none(),
        )

        assert '"<string>"' in result.stderr
        assert result.returncode == 1

    def test_the_child_environment_is_solely_the_grant(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("DR_EXEC_MUST_NOT_LEAK", "secret")

        result = run_untrusted_python(
            "import json, os; print(json.dumps(dict(os.environ)))",
            profile=PROCESS_BOUNDARY_ONLY,
            budgets=_QUICK,
            records=Records.none(),
            environment=EnvironmentGrant.fixed({"OPENBLAS_NUM_THREADS": "1"}),
        )

        delivered = json.loads(result.stdout)
        assert delivered["OPENBLAS_NUM_THREADS"] == "1"
        assert "DR_EXEC_MUST_NOT_LEAK" not in delivered

    def test_a_declared_alternative_runtime_names_its_interpreter(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from dr_exec import engine

        seen: list[tuple[str, ...]] = []
        real_execute = engine.execute

        def record_argv(declaration: engine.Declaration) -> object:
            seen.append(declaration.invocation.argv)
            return real_execute(declaration)

        monkeypatch.setattr("dr_exec.run.execute", record_argv)

        run_untrusted_python(
            "pass",
            profile=PROCESS_BOUNDARY_ONLY,
            budgets=_QUICK,
            records=Records.none(),
            runtime=PythonRuntime(name="declared", interpreter=sys.executable),
        )

        assert seen == [(sys.executable, "-I", "-c", "pass")]


class TestPreSpawnBounds:
    def test_source_at_the_bound_is_accepted(self) -> None:
        padding = "#" * (SOURCE_BOUND_BYTES - len("print('at-bound')\n") - 1)
        source = f"print('at-bound')\n{padding}\n"
        assert len(source.encode("utf-8")) == SOURCE_BOUND_BYTES

        result = run_untrusted_python(
            source,
            profile=PROCESS_BOUNDARY_ONLY,
            budgets=_QUICK,
            records=Records.none(),
        )

        assert result.stdout == "at-bound\n"

    def test_oversized_source_is_rejected_before_any_spawn(self) -> None:
        with pytest.raises(DeclarationError, match="source bound"):
            run_untrusted_python(
                "#" * (SOURCE_BOUND_BYTES + 1),
                profile=PROCESS_BOUNDARY_ONLY,
                budgets=_QUICK,
                records=Records.none(),
            )

    def test_an_oversized_grant_is_rejected_even_with_a_valid_source(self) -> None:
        with pytest.raises(DeclarationError, match="invocation bound"):
            run_untrusted_python(
                "pass",
                profile=PROCESS_BOUNDARY_ONLY,
                budgets=_QUICK,
                records=Records.none(),
                environment=EnvironmentGrant.fixed(
                    {"DR_EXEC_BULK": "x" * (1024 * 1024 + 1)}
                ),
            )

    def test_a_non_text_source_is_a_caller_error(self) -> None:
        with pytest.raises(DeclarationError, match="source must be text"):
            run_untrusted_python(
                b"pass",  # type: ignore[arg-type]
                profile=PROCESS_BOUNDARY_ONLY,
                budgets=_QUICK,
                records=Records.none(),
            )


class TestExitPolicy:
    def test_a_declared_verdict_lands_on_the_outcome(self) -> None:
        policy = ExitPolicy(name="zero_is_success", verdicts={0: ExitVerdict.SUCCESS})

        result = run_tool(
            [sys.executable, "-I", "-c", "pass"],
            budgets=_QUICK,
            records=Records.none(),
            exit_policy=policy,
        )

        assert result.outcome.exit_verdict is ExitVerdict.SUCCESS
        assert result.outcome.attribution is Attribution.PAYLOAD

    def test_an_undeclared_status_takes_the_default_verdict(self) -> None:
        policy = ExitPolicy(name="zero_is_success", verdicts={0: ExitVerdict.SUCCESS})

        result = run_tool(
            [sys.executable, "-I", "-c", "raise SystemExit(3)"],
            budgets=_QUICK,
            records=Records.none(),
            exit_policy=policy,
        )

        assert result.outcome.exit_verdict is ExitVerdict.REPORT_ONLY

    def test_a_budget_outcome_carries_no_exit_verdict(self) -> None:
        result = run_tool(
            [sys.executable, "-I", "-c", "import time; time.sleep(60)"],
            budgets=Budgets(wall_clock=0.2),
            records=Records.none(),
        )

        assert result.outcome.attribution is Attribution.BUDGET
        assert result.outcome.exit_verdict is None


class TestInputRouting:
    def test_every_entry_point_feeds_declared_input(self) -> None:
        echo = "import sys; sys.stdout.write(sys.stdin.read())"

        assert (
            run_tool(
                [sys.executable, "-I", "-c", echo],
                budgets=_QUICK,
                records=Records.none(),
                input_text="tool",
            ).stdout
            == "tool"
        )
        assert (
            run_untrusted_python(
                echo,
                profile=PROCESS_BOUNDARY_ONLY,
                budgets=_QUICK,
                records=Records.none(),
                input_text="python",
            ).stdout
            == "python"
        )
        assert (
            run_untrusted_command(
                [sys.executable, "-I", "-c", echo],
                profile=PROCESS_BOUNDARY_ONLY,
                budgets=_QUICK,
                records=Records.none(),
                input_text="command",
            ).stdout
            == "command"
        )
