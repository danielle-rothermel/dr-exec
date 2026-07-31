"""A call the real executor rejects, the fake rejects identically.

Every row here is a declaration that fails before any child exists, so both
sides are exercised for real: the real entry point raises on the pre-spawn
path and never spawns, and the fake raises from the same validator. The
assertion is on the exception *type and message*, because parity that holds
only for the type would let the two drift into different reasons.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from typing import Any

import pytest

from dr_exec.declare import (
    PROCESS_BOUNDARY_ONLY,
    SOURCE_BOUND_BYTES,
    Budgets,
    EnvironmentGrant,
    OutputBudget,
    OverflowPolicy,
    Records,
    StreamBounds,
)
from dr_exec.errors import DeclarationError
from dr_exec.fake import FakeExecutor
from dr_exec.run import run_tool, run_untrusted_command, run_untrusted_python

from .conftest import QUICK, payload_result

_ABSENT_PROGRAM = "/nonexistent/dr-exec-parity-probe"

# Each case names the entry point and the keyword arguments that make it
# unrunnable. The real function and the fake's method of the same name both
# receive them.
_INVALID_CALLS: list[tuple[str, dict[str, Any]]] = [
    (
        "run_tool",
        {"command": "echo hello", "budgets": QUICK, "records": Records.none()},
    ),
    ("run_tool", {"command": [], "budgets": QUICK, "records": Records.none()}),
    ("run_tool", {"command": [""], "budgets": QUICK, "records": Records.none()}),
    (
        "run_tool",
        {"command": ["/bin/echo", 7], "budgets": QUICK, "records": Records.none()},
    ),
    (
        "run_tool",
        {
            "command": ["/bin/echo", "a\0b"],
            "budgets": QUICK,
            "records": Records.none(),
        },
    ),
    (
        "run_tool",
        {
            "command": ["echo"],
            "budgets": QUICK,
            "records": Records.none(),
            "environment": EnvironmentGrant.none(),
        },
    ),
    (
        "run_tool",
        {
            "command": [_ABSENT_PROGRAM],
            "budgets": Budgets(wall_clock=10.0, input=4),
            "records": Records.none(),
            "input_text": "far too much input",
        },
    ),
    (
        "run_tool",
        {
            "command": [_ABSENT_PROGRAM],
            "budgets": QUICK,
            "records": Records.none(),
            "input_text": 7,
        },
    ),
    (
        "run_tool",
        {
            "command": [_ABSENT_PROGRAM, "x" * (2 * 1024 * 1024)],
            "budgets": QUICK,
            "records": Records.none(),
        },
    ),
    (
        "run_untrusted_python",
        {
            "source": "x" * (SOURCE_BOUND_BYTES + 1),
            "profile": PROCESS_BOUNDARY_ONLY,
            "budgets": QUICK,
            "records": Records.none(),
        },
    ),
    (
        "run_untrusted_python",
        {
            "source": b"print(1)",
            "profile": PROCESS_BOUNDARY_ONLY,
            "budgets": QUICK,
            "records": Records.none(),
        },
    ),
    (
        "run_untrusted_command",
        {
            "command": "sh -c whoami",
            "profile": PROCESS_BOUNDARY_ONLY,
            "budgets": QUICK,
            "records": Records.none(),
        },
    ),
    (
        "run_untrusted_command",
        {
            "command": ["candidate"],
            "profile": PROCESS_BOUNDARY_ONLY,
            "budgets": QUICK,
            "records": Records.none(),
        },
    ),
]


class TestTheDeclarationTypesRaiseTheSameError:
    """A malformed declaration *value* is the same error as a malformed call.

    A budget that is not a positive number is as unrunnable as an oversized
    source, so a caller catching the executor's own pre-spawn error type
    catches both. These constructors are reached from entry-point call sites
    directly, so a different exception type there would escape a consumer's
    handler entirely.
    """

    @pytest.mark.parametrize(
        "build",
        [
            lambda: Budgets(wall_clock=-1),
            lambda: Budgets(wall_clock=float("inf")),
            lambda: Budgets(input=0),
            lambda: Budgets(output=4096),
            lambda: OutputBudget(limit_bytes=0, overflow_policy=OverflowPolicy.FAIL),
            lambda: OutputBudget(limit_bytes=64, overflow_policy="fail"),
            lambda: StreamBounds(stdout_bytes=0),
            lambda: EnvironmentGrant.fixed({"HAS=EQUALS": "x"}),
            lambda: EnvironmentGrant.fixed({"NAME": "has\0nul"}),
        ],
        ids=[
            "negative-wall-clock",
            "infinite-wall-clock",
            "zero-input",
            "bare-int-output",
            "zero-output-limit",
            "policy-that-is-a-string",
            "zero-stream-bound",
            "name-with-equals",
            "value-with-nul",
        ],
    )
    def test_a_malformed_declaration_value_is_a_declaration_error(
        self, build: Callable[[], object]
    ) -> None:
        with pytest.raises(DeclarationError):
            build()

    def test_a_stream_bound_on_an_entry_point_raises_inside_the_call(self) -> None:
        # StreamBounds is an entry-point keyword, so its validation happens
        # within the call a consumer wrapped in its own error handling.
        with pytest.raises(DeclarationError, match="positive integer of bytes"):
            run_untrusted_python(
                "pass",
                profile=PROCESS_BOUNDARY_ONLY,
                budgets=QUICK,
                records=Records.none(),
                stream_bounds=StreamBounds(stdout_bytes=0),
            )


def _real(entry_point: str) -> Callable[..., Any]:
    return {
        "run_tool": run_tool,
        "run_untrusted_python": run_untrusted_python,
        "run_untrusted_command": run_untrusted_command,
    }[entry_point]


def _raised(call: Callable[..., Any], arguments: dict[str, Any]) -> DeclarationError:
    """Invoke with the payload positional and everything else by keyword."""
    keywords = dict(arguments)
    payload = keywords.pop("source", None)
    if payload is None:
        payload = keywords.pop("command")
    with pytest.raises(DeclarationError) as raised:
        call(payload, **keywords)
    return raised.value


@pytest.mark.parametrize(
    ("entry_point", "arguments"),
    _INVALID_CALLS,
    ids=[f"{name}-{index}" for index, (name, _) in enumerate(_INVALID_CALLS)],
)
class TestParity:
    def test_the_fake_raises_the_same_error_as_the_real_entry_point(
        self, entry_point: str, arguments: dict[str, Any]
    ) -> None:
        real_error = _raised(_real(entry_point), arguments)
        fake_error = _raised(getattr(FakeExecutor(), entry_point), arguments)

        assert type(fake_error) is type(real_error)
        assert str(fake_error) == str(real_error)

    def test_the_fake_records_nothing_it_rejected(
        self, entry_point: str, arguments: dict[str, Any]
    ) -> None:
        fake = FakeExecutor()
        fake.enqueue(payload_result())

        _raised(getattr(fake, entry_point), arguments)

        assert fake.calls == []


class TestOverlayExclusionParity:
    """The one case whose validity depends on the parent's own environment."""

    def test_a_present_exclusion_is_rejected_identically(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("DR_EXEC_PARITY_PROBE", "present")
        grant = EnvironmentGrant.overlay({}, exclusions=("DR_EXEC_PARITY_PROBE",))
        arguments = {
            "budgets": QUICK,
            "records": Records.none(),
            "environment": grant,
        }

        with pytest.raises(DeclarationError) as real_raised:
            run_tool([_ABSENT_PROGRAM], **arguments)
        with pytest.raises(DeclarationError) as fake_raised:
            FakeExecutor().run_tool([_ABSENT_PROGRAM], **arguments)

        assert str(fake_raised.value) == str(real_raised.value)
        assert "DR_EXEC_PARITY_PROBE" in str(fake_raised.value)

    def test_an_absent_exclusion_is_accepted_by_both(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("DR_EXEC_PARITY_PROBE", raising=False)
        grant = EnvironmentGrant.overlay({}, exclusions=("DR_EXEC_PARITY_PROBE",))
        fake = FakeExecutor()
        fake.enqueue(payload_result())

        result = fake.run_tool(
            [_ABSENT_PROGRAM],
            budgets=QUICK,
            records=Records.none(),
            environment=grant,
        )

        assert result.returncode == 0
        assert fake.last_call.environment is grant


class TestRelativeProgramResolution:
    def test_a_relative_program_resolves_when_the_grant_declares_a_path(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("PATH", os.defpath)
        fake = FakeExecutor()
        fake.enqueue(payload_result())

        result = fake.run_tool(
            ["echo", "hi"],
            budgets=QUICK,
            records=Records.none(),
            environment=EnvironmentGrant.named(["PATH"]),
        )

        assert result.returncode == 0

    def test_an_absolute_program_resolves_with_no_grant_at_all(self) -> None:
        fake = FakeExecutor()
        fake.enqueue(payload_result())

        result = fake.run_tool([_ABSENT_PROGRAM], budgets=QUICK, records=Records.none())

        assert result.returncode == 0
