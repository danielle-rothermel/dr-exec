"""Scripting: a behavioral callable, a FIFO convenience, and neither."""

from __future__ import annotations

import pytest

from dr_exec.declare import (
    HERMETIC,
    PROCESS_BOUNDARY_ONLY,
    Budgets,
    EnvironmentGrant,
    ExitPolicy,
    ExitVerdict,
    OutputBudget,
    OverflowPolicy,
    PythonRuntime,
    Records,
)
from dr_exec.fake import (
    EntryPoint,
    FakeExecutor,
    RecordedCall,
    ScriptError,
    UnscriptedCall,
)
from dr_exec.record import RunResult, TrustCategory

from .conftest import QUICK, payload_result


class TestBehavioralScript:
    def test_the_script_sees_the_whole_declaration(self) -> None:
        seen: list[RecordedCall] = []
        exit_policy = ExitPolicy(name="candidate", verdicts={0: ExitVerdict.SUCCESS})
        grant = EnvironmentGrant.fixed({"OPENBLAS_NUM_THREADS": "1"})
        runtime = PythonRuntime(name="declared", interpreter="/usr/bin/python3")
        budgets = Budgets(
            wall_clock=2.0,
            output=OutputBudget(
                limit_bytes=1024, overflow_policy=OverflowPolicy.MARKED_TRUNCATION
            ),
            input=64,
        )
        fake = FakeExecutor()

        def script(call: RecordedCall) -> RunResult:
            seen.append(call)
            return payload_result(
                stdout=f"saw {call.source}",
                input_bytes=len(call.input_text.encode("utf-8")),
                exit_verdict=ExitVerdict.SUCCESS.value,
            )

        fake.script_with(script)
        result = fake.run_untrusted_python(
            "print(1)",
            profile=PROCESS_BOUNDARY_ONLY,
            budgets=budgets,
            records=Records.none(),
            runtime=runtime,
            input_text="stdin",
            environment=grant,
            exit_policy=exit_policy,
        )

        (call,) = seen
        assert call.entry_point is EntryPoint.RUN_UNTRUSTED_PYTHON
        assert call.trust_category is TrustCategory.UNTRUSTED_PYTHON
        assert call.source == "print(1)"
        assert call.command == ("/usr/bin/python3", "-I", "-c", "print(1)")
        assert call.input_text == "stdin"
        assert call.runtime is runtime
        assert call.profile is PROCESS_BOUNDARY_ONLY
        assert call.budgets is budgets
        assert call.environment is grant
        assert call.exit_policy is exit_policy
        assert call.records.kind is Records.none().kind
        assert result.stdout == "saw print(1)"

    def test_a_script_can_answer_by_inspecting_the_payload(self) -> None:
        fake = FakeExecutor()
        fake.script_with(lambda call: payload_result(stdout=call.command[-1].upper()))

        first = fake.run_tool(
            ["/bin/echo", "one"], budgets=QUICK, records=Records.none()
        )
        second = fake.run_tool(
            ["/bin/echo", "two"], budgets=QUICK, records=Records.none()
        )

        assert (first.stdout, second.stdout) == ("ONE", "TWO")

    def test_the_script_wins_over_the_queue(self) -> None:
        fake = FakeExecutor()
        fake.enqueue(payload_result(stdout="queued"))
        fake.script_with(lambda call: payload_result(stdout="scripted"))

        result = fake.run_tool(["/bin/echo"], budgets=QUICK, records=Records.none())

        assert result.stdout == "scripted"

    def test_a_script_returning_something_else_is_refused(self) -> None:
        fake = FakeExecutor()
        fake.script_with(lambda call: "not a result")  # type: ignore[return-value,arg-type]

        with pytest.raises(ScriptError, match="not a RunResult"):
            fake.run_tool(["/bin/echo"], budgets=QUICK, records=Records.none())


class TestQueue:
    def test_enqueued_results_are_returned_in_order(self) -> None:
        fake = FakeExecutor()
        for index in range(3):
            fake.enqueue(payload_result(stdout=str(index)))

        stdouts = [
            fake.run_tool(["/bin/echo"], budgets=QUICK, records=Records.none()).stdout
            for _ in range(3)
        ]

        assert stdouts == ["0", "1", "2"]

    def test_the_queue_spans_entry_points(self) -> None:
        fake = FakeExecutor()
        fake.enqueue(payload_result(stdout="first"))
        fake.enqueue(payload_result(stdout="second"))

        tool = fake.run_tool(["/bin/echo"], budgets=QUICK, records=Records.none())
        untrusted = fake.run_untrusted_python(
            "print(1)",
            profile=PROCESS_BOUNDARY_ONLY,
            budgets=QUICK,
            records=Records.none(),
            runtime=HERMETIC,
        )

        assert (tool.stdout, untrusted.stdout) == ("first", "second")


class TestUnscripted:
    def test_an_unscripted_call_names_the_entry_point_and_the_remedy(self) -> None:
        fake = FakeExecutor()

        with pytest.raises(UnscriptedCall) as raised:
            fake.run_untrusted_command(
                ["/bin/echo"],
                profile=PROCESS_BOUNDARY_ONLY,
                budgets=QUICK,
                records=Records.none(),
            )

        assert "run_untrusted_command" in str(raised.value)
        assert "script_with" in str(raised.value)
        assert "enqueue" in str(raised.value)

    def test_a_drained_queue_is_an_unscripted_call(self) -> None:
        fake = FakeExecutor()
        fake.enqueue(payload_result())
        fake.run_tool(["/bin/echo"], budgets=QUICK, records=Records.none())

        with pytest.raises(UnscriptedCall):
            fake.run_tool(["/bin/echo"], budgets=QUICK, records=Records.none())

    def test_an_unscripted_call_is_still_recorded(self) -> None:
        fake = FakeExecutor()

        with pytest.raises(UnscriptedCall):
            fake.run_tool(["/bin/echo"], budgets=QUICK, records=Records.none())

        assert fake.last_call.command == ("/bin/echo",)
