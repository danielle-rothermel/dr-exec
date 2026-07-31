"""Recording: every declaration is assertable, and adding one breaks nothing.

The no-fake-breakage rule lives here: a production call site that grows a
budget, a grant, or an exit policy changes what a recorded call *reports*,
never whether existing tests run. The pattern is demonstrated directly — the
same script and the same assertions survive a declaration that gained a
budget, and the new budget is visible immediately.
"""

from __future__ import annotations

import pytest

from dr_exec.declare import (
    HERMETIC,
    PROCESS_BOUNDARY_ONLY,
    UNBUDGETED,
    Budgets,
    EnvironmentGrant,
    ExitPolicy,
    ExitVerdict,
    OutputBudget,
    OverflowPolicy,
    Records,
)
from dr_exec.fake import EntryPoint, FakeExecutor
from dr_exec.record import TrustCategory

from .conftest import QUICK, payload_result


def _fake_answering_everything() -> FakeExecutor:
    fake = FakeExecutor()
    fake.script_with(lambda call: payload_result())
    return fake


class TestRecording:
    def test_every_call_is_appended_in_order(self) -> None:
        fake = _fake_answering_everything()

        fake.run_tool(["/bin/echo"], budgets=QUICK, records=Records.none())
        fake.run_untrusted_command(
            ["/bin/false"],
            profile=PROCESS_BOUNDARY_ONLY,
            budgets=QUICK,
            records=Records.none(),
        )

        assert [call.entry_point for call in fake.calls] == [
            EntryPoint.RUN_TOOL,
            EntryPoint.RUN_UNTRUSTED_COMMAND,
        ]

    def test_last_call_is_the_most_recent(self) -> None:
        fake = _fake_answering_everything()

        fake.run_tool(["/bin/echo", "first"], budgets=QUICK, records=Records.none())
        fake.run_tool(["/bin/echo", "second"], budgets=QUICK, records=Records.none())

        assert fake.last_call.command == ("/bin/echo", "second")

    def test_last_call_on_a_fake_that_never_ran_is_a_test_bug(self) -> None:
        with pytest.raises(AssertionError, match="no call has been recorded"):
            _ = FakeExecutor().last_call

    def test_calls_for_selects_by_entry_point(self) -> None:
        fake = _fake_answering_everything()

        fake.run_tool(["/bin/echo", "a"], budgets=QUICK, records=Records.none())
        fake.run_untrusted_python(
            "print(1)",
            profile=PROCESS_BOUNDARY_ONLY,
            budgets=QUICK,
            records=Records.none(),
        )
        fake.run_tool(["/bin/echo", "b"], budgets=QUICK, records=Records.none())

        tools = fake.calls_for(EntryPoint.RUN_TOOL)
        assert [call.command[-1] for call in tools] == ["a", "b"]
        assert len(fake.calls_for(EntryPoint.RUN_UNTRUSTED_PYTHON)) == 1

    def test_the_trust_category_is_the_entry_point_that_ran(self) -> None:
        fake = _fake_answering_everything()

        fake.run_tool(["/bin/echo"], budgets=QUICK, records=Records.none())
        fake.run_untrusted_python(
            "print(1)",
            profile=PROCESS_BOUNDARY_ONLY,
            budgets=QUICK,
            records=Records.none(),
        )
        fake.run_untrusted_command(
            ["/bin/false"],
            profile=PROCESS_BOUNDARY_ONLY,
            budgets=QUICK,
            records=Records.none(),
        )

        assert [call.trust_category for call in fake.calls] == [
            TrustCategory.TRUSTED_TOOL,
            TrustCategory.UNTRUSTED_PYTHON,
            TrustCategory.UNTRUSTED_COMMAND,
        ]

    def test_a_recorded_call_is_frozen(self) -> None:
        fake = _fake_answering_everything()
        fake.run_tool(["/bin/echo"], budgets=QUICK, records=Records.none())

        with pytest.raises(AttributeError):
            fake.last_call.declaration = None  # type: ignore[misc]

    def test_the_records_declaration_is_recorded(self, tmp_path: object) -> None:
        fake = _fake_answering_everything()

        fake.run_tool(
            ["/bin/echo"],
            budgets=QUICK,
            records=Records.directory(str(tmp_path)),
        )

        assert fake.last_call.records.path is not None

    def test_the_untrusted_python_call_records_source_and_argv(self) -> None:
        fake = _fake_answering_everything()

        fake.run_untrusted_python(
            "print(1)",
            profile=PROCESS_BOUNDARY_ONLY,
            budgets=QUICK,
            records=Records.none(),
        )

        call = fake.last_call
        assert call.source == "print(1)"
        assert call.command[1:3] == ("-I", "-c")
        assert call.runtime is HERMETIC

    def test_a_tool_call_declares_no_profile(self) -> None:
        fake = _fake_answering_everything()

        fake.run_tool(["/bin/echo"], budgets=QUICK, records=Records.none())

        assert fake.last_call.profile is None


class TestAddingADeclarationIsVisibleNotBreaking:
    """The same test body, before and after a call site grows a budget."""

    @staticmethod
    def _production_call(fake: FakeExecutor, budgets: Budgets) -> None:
        fake.run_untrusted_command(
            ["/bin/candidate"],
            profile=PROCESS_BOUNDARY_ONLY,
            budgets=budgets,
            records=Records.none(),
        )

    def test_the_unbudgeted_call_site_reports_unbudgeted_axes(self) -> None:
        fake = _fake_answering_everything()

        self._production_call(fake, Budgets())

        assert fake.last_call.budgets.output is UNBUDGETED
        assert fake.last_call.profile is PROCESS_BOUNDARY_ONLY

    def test_the_same_assertions_survive_a_newly_declared_budget(self) -> None:
        fake = _fake_answering_everything()
        budgets = Budgets(
            wall_clock=5.0,
            output=OutputBudget(limit_bytes=2048, overflow_policy=OverflowPolicy.FAIL),
        )

        self._production_call(fake, budgets)

        assert fake.last_call.profile is PROCESS_BOUNDARY_ONLY
        assert fake.last_call.budgets.output.limit_bytes == 2048
        assert fake.last_call.budgets.output.overflow_policy is OverflowPolicy.FAIL

    def test_a_newly_declared_grant_and_policy_are_visible(self) -> None:
        grant = EnvironmentGrant.fixed({"OPENBLAS_NUM_THREADS": "1"})
        policy = ExitPolicy(name="candidate", verdicts={0: ExitVerdict.SUCCESS})
        fake = FakeExecutor()
        # The scripted verdict follows the newly declared policy: the fake
        # refuses a result the declared policy could not have produced.
        fake.script_with(lambda call: payload_result(exit_verdict=ExitVerdict.SUCCESS))

        fake.run_untrusted_command(
            ["/bin/candidate"],
            profile=PROCESS_BOUNDARY_ONLY,
            budgets=QUICK,
            records=Records.none(),
            environment=grant,
            exit_policy=policy,
        )

        assert fake.last_call.environment.declared_names == ("OPENBLAS_NUM_THREADS",)
        assert fake.last_call.exit_policy is policy
