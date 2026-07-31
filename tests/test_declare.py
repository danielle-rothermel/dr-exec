"""Behavior tests for the declaration layer."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path

import pytest

from dr_exec.declare import (
    HERMETIC,
    PROCESS_BOUNDARY_ONLY,
    REPORT_ONLY,
    UNBUDGETED,
    Budgets,
    EnvironmentGrant,
    ExitPolicy,
    ExitVerdict,
    GrantKind,
    OutputBudget,
    OverflowPolicy,
    Records,
    RecordsKind,
)


class TestUnbudgeted:
    def test_is_a_singleton(self) -> None:
        from dr_exec.declare import _Unbudgeted

        assert _Unbudgeted() is UNBUDGETED

    def test_repr_names_itself(self) -> None:
        assert repr(UNBUDGETED) == "UNBUDGETED"

    def test_is_every_axis_default(self) -> None:
        budgets = Budgets()
        assert budgets.wall_clock is UNBUDGETED
        assert budgets.output is UNBUDGETED
        assert budgets.input is UNBUDGETED


class TestBudgetValidation:
    @pytest.mark.parametrize("bad", [0, -1, -0.5, float("inf"), float("nan")])
    def test_wall_clock_must_be_finite_and_positive(self, bad: float) -> None:
        with pytest.raises(ValueError, match="wall_clock"):
            Budgets(wall_clock=bad)

    def test_wall_clock_rejects_bool(self) -> None:
        with pytest.raises(ValueError, match="wall_clock"):
            Budgets(wall_clock=True)

    def test_wall_clock_accepts_int_seconds_as_float(self) -> None:
        assert Budgets(wall_clock=30).wall_clock == 30.0

    @pytest.mark.parametrize("bad", [0, -1, True, 2.5])
    def test_input_must_be_a_positive_int_of_bytes(self, bad: object) -> None:
        with pytest.raises(ValueError, match="input"):
            Budgets(input=bad)

    @pytest.mark.parametrize("bad", [0, -1, True, 2.5])
    def test_output_limit_must_be_a_positive_int_of_bytes(self, bad: object) -> None:
        with pytest.raises(ValueError, match="output"):
            OutputBudget(limit_bytes=bad, overflow_policy=OverflowPolicy.FAIL)

    def test_output_requires_a_policy_object(self) -> None:
        with pytest.raises(ValueError, match="OverflowPolicy"):
            OutputBudget(limit_bytes=1024, overflow_policy="fail")

    def test_output_axis_rejects_a_bare_int(self) -> None:
        with pytest.raises(ValueError, match="OutputBudget"):
            Budgets(output=1024)

    def test_declared_budgets_round_trip(self) -> None:
        budgets = Budgets(
            wall_clock=1.5,
            output=OutputBudget(
                limit_bytes=2048, overflow_policy=OverflowPolicy.MARKED_TRUNCATION
            ),
            input=4096,
        )
        assert budgets.wall_clock == 1.5
        assert budgets.output.limit_bytes == 2048
        assert budgets.output.overflow_policy is OverflowPolicy.MARKED_TRUNCATION
        assert budgets.input == 4096

    def test_budgets_are_frozen(self) -> None:
        with pytest.raises(AttributeError):
            Budgets().wall_clock = 1.0


class TestEnvironmentGrantShapes:
    def test_none_grants_nothing(self) -> None:
        grant = EnvironmentGrant.none()
        assert grant.kind is GrantKind.NONE
        assert grant.declared_names == ()
        assert dict(grant.resolved) == {}

    def test_fixed_reads_nothing_from_the_parent(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("DR_EXEC_TEST_PARENT", "parent-value")
        grant = EnvironmentGrant.fixed({"OPENBLAS_NUM_THREADS": "1"})
        assert grant.kind is GrantKind.FIXED
        assert dict(grant.resolved) == {"OPENBLAS_NUM_THREADS": "1"}
        assert "DR_EXEC_TEST_PARENT" not in grant.resolved

    def test_named_resolves_from_the_parent(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("DR_EXEC_TEST_A", "alpha")
        grant = EnvironmentGrant.named(["DR_EXEC_TEST_A"])
        assert grant.kind is GrantKind.NAMED
        assert dict(grant.resolved) == {"DR_EXEC_TEST_A": "alpha"}

    def test_named_omits_variables_absent_from_the_parent(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("DR_EXEC_TEST_MISSING", raising=False)
        grant = EnvironmentGrant.named(["DR_EXEC_TEST_MISSING"])
        assert grant.declared_names == ()

    def test_declared_names_are_sorted(self) -> None:
        grant = EnvironmentGrant.fixed({"ZED": "1", "ALPHA": "2", "MID": "3"})
        assert grant.declared_names == ("ALPHA", "MID", "ZED")

    def test_overlay_stores_extras_and_exclusions(self) -> None:
        grant = EnvironmentGrant.overlay(
            {"COLUMNS": "80", "NO_COLOR": "1"}, exclusions=["AWS_SECRET_ACCESS_KEY"]
        )
        assert grant.kind is GrantKind.OVERLAY
        assert grant.declared_names == ("COLUMNS", "NO_COLOR")
        assert grant.exclusions == ("AWS_SECRET_ACCESS_KEY",)

    @pytest.mark.parametrize("bad", ["", "HAS=EQUALS", "HAS\0NUL"])
    def test_invalid_names_are_rejected(self, bad: str) -> None:
        with pytest.raises(ValueError, match="names"):
            EnvironmentGrant.fixed({bad: "value"})

    def test_values_with_nul_are_rejected(self) -> None:
        with pytest.raises(ValueError, match="NUL"):
            EnvironmentGrant.fixed({"NAME": "has\0nul"})


class TestGrantSnapshotIsFrozenAtConstruction:
    def test_named_ignores_later_parent_mutation(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("DR_EXEC_TEST_SNAPSHOT", "at-construction")
        grant = EnvironmentGrant.named(["DR_EXEC_TEST_SNAPSHOT"])

        os.environ["DR_EXEC_TEST_SNAPSHOT"] = "mutated-after"
        assert grant.resolved["DR_EXEC_TEST_SNAPSHOT"] == "at-construction"

        del os.environ["DR_EXEC_TEST_SNAPSHOT"]
        assert grant.resolved["DR_EXEC_TEST_SNAPSHOT"] == "at-construction"
        assert grant.declared_names == ("DR_EXEC_TEST_SNAPSHOT",)

    def test_named_digest_is_stable_across_parent_mutation(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("DR_EXEC_TEST_DIGEST", "original")
        grant = EnvironmentGrant.named(["DR_EXEC_TEST_DIGEST"])
        before = grant.contents_digest()

        monkeypatch.setenv("DR_EXEC_TEST_DIGEST", "changed")
        assert grant.contents_digest() == before


class TestGrantValuePrivacy:
    def test_repr_never_includes_values(self) -> None:
        grant = EnvironmentGrant.fixed({"API_TOKEN": "super-secret-value"})
        rendered = repr(grant)
        assert "super-secret-value" not in rendered
        assert "API_TOKEN" in rendered
        assert "fixed" in rendered

    def test_repr_of_a_grant_nested_in_a_declaration_leaks_nothing(self) -> None:
        grant = EnvironmentGrant.named([])
        object.__setattr__(grant, "resolved", {"API_TOKEN": "super-secret-value"})
        assert "super-secret-value" not in repr((grant, [grant], {"g": grant}))

    def test_str_never_includes_values(self) -> None:
        grant = EnvironmentGrant.fixed({"API_TOKEN": "super-secret-value"})
        assert "super-secret-value" not in str(grant)


class TestGrantContentsDigest:
    def test_canonicalization_is_nul_joined_name_equals_value(self) -> None:
        grant = EnvironmentGrant.fixed({"B": "two", "A": "one"})
        expected = hashlib.sha256(b"A=one\0B=two").hexdigest()
        assert grant.contents_digest() == expected

    def test_empty_grant_digest_is_the_empty_payload_digest(self) -> None:
        assert (
            EnvironmentGrant.none().contents_digest() == hashlib.sha256(b"").hexdigest()
        )

    def test_digest_is_stable_across_construction(self) -> None:
        first = EnvironmentGrant.fixed({"A": "1", "B": "2"})
        second = EnvironmentGrant.fixed({"B": "2", "A": "1"})
        assert first.contents_digest() == second.contents_digest()

    def test_digest_is_value_sensitive(self) -> None:
        first = EnvironmentGrant.fixed({"A": "1"})
        second = EnvironmentGrant.fixed({"A": "2"})
        assert first.contents_digest() != second.contents_digest()

    def test_digest_is_name_sensitive(self) -> None:
        first = EnvironmentGrant.fixed({"A": "1"})
        second = EnvironmentGrant.fixed({"B": "1"})
        assert first.contents_digest() != second.contents_digest()

    def test_nul_separator_is_unforgeable_from_a_value(self) -> None:
        # The canonicalization is unambiguous precisely because NUL cannot
        # appear in a name or value, so no single entry can forge a pair
        # boundary and collide with a two-entry grant.
        with pytest.raises(ValueError, match="NUL"):
            EnvironmentGrant.fixed({"A": "1\0B=2"})

    def test_equals_in_a_value_cannot_forge_a_name_boundary(self) -> None:
        first = EnvironmentGrant.fixed({"A": "1", "B": "2"})
        second = EnvironmentGrant.fixed({"A": "1=B=2"})
        assert first.contents_digest() != second.contents_digest()


class TestContainmentProfile:
    def test_process_boundary_only_declares_what_it_does_not_contain(self) -> None:
        limits = PROCESS_BOUNDARY_ONLY.declared_limits
        assert "Restricts nothing beyond the process boundary." in limits
        assert "filesystem" in limits
        assert "network" in limits
        assert "credential" in limits

    def test_profile_is_frozen(self) -> None:
        with pytest.raises(AttributeError):
            PROCESS_BOUNDARY_ONLY.name = "other"


class TestExitPolicy:
    def test_default_policy_is_report_only(self) -> None:
        assert REPORT_ONLY.default_verdict is ExitVerdict.REPORT_ONLY
        assert REPORT_ONLY.verdict_for(0) is ExitVerdict.REPORT_ONLY
        assert REPORT_ONLY.verdict_for(1) is ExitVerdict.REPORT_ONLY
        assert REPORT_ONLY.verdict_for(-9) is ExitVerdict.REPORT_ONLY

    def test_declared_statuses_take_their_verdict(self) -> None:
        policy = ExitPolicy(
            name="zero_is_success",
            verdicts={0: ExitVerdict.SUCCESS},
            default_verdict=ExitVerdict.FAILURE,
        )
        assert policy.verdict_for(0) is ExitVerdict.SUCCESS
        assert policy.verdict_for(3) is ExitVerdict.FAILURE

    def test_absent_returncode_takes_the_default(self) -> None:
        policy = ExitPolicy(name="strict", default_verdict=ExitVerdict.FAILURE)
        assert policy.verdict_for(None) is ExitVerdict.FAILURE


class TestRecords:
    def test_directory_declaration(self, tmp_path: Path) -> None:
        records = Records.directory_at(tmp_path)
        assert records.kind is RecordsKind.DIRECTORY
        assert records.directory == tmp_path

    def test_directory_accepts_a_string_path(self) -> None:
        assert Records.directory_at("/tmp/records").directory == Path("/tmp/records")

    def test_none_declaration_has_no_directory(self) -> None:
        records = Records.none()
        assert records.kind is RecordsKind.NONE
        assert records.directory is None


class TestPythonRuntime:
    def test_hermetic_defers_interpreter_resolution(self) -> None:
        assert HERMETIC.interpreter is None
        assert HERMETIC.isolated is True

    def test_hermetic_injects_no_packages(self) -> None:
        assert HERMETIC.packages == ()
