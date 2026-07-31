"""Public call-scoped entry points.

Trust categorization is declared by which function you call: the call-site
acknowledgment is the function name, ungreppable-around, and the category
lands in the run record so it is auditable after the fact. All three share
one declaration surface; the asymmetries are the trust parameters
themselves.
"""

from __future__ import annotations

import sys
from collections.abc import Sequence
from typing import Final

from dr_exec.declare import (
    HERMETIC,
    REPORT_ONLY,
    SOURCE_BOUND_BYTES,
    Budgets,
    ContainmentProfile,
    EnvironmentGrant,
    ExitPolicy,
    PythonRuntime,
    Records,
)
from dr_exec.engine import Declaration, Invocation, execute
from dr_exec.errors import DeclarationError
from dr_exec.record import RunResult, TrustCategory

_ISOLATED_FLAG: Final[str] = "-I"
_SOURCE_FLAG: Final[str] = "-c"

_NO_ENVIRONMENT: Final[EnvironmentGrant] = EnvironmentGrant.none()
"""The default grant, as one frozen value shared by every entry point."""

__all__ = ["run_tool", "run_untrusted_command", "run_untrusted_python"]


def run_tool(
    command: Sequence[str],
    *,
    budgets: Budgets,
    records: Records,
    input_text: str = "",
    environment: EnvironmentGrant = _NO_ENVIRONMENT,
    exit_policy: ExitPolicy = REPORT_ONLY,
) -> RunResult:
    """Run a trusted payload: a known program with first-party arguments.

    Absence — an unresolvable program — is a distinct outcome in the
    result, not a start failure.
    """
    return execute(
        Declaration(
            invocation=Invocation(
                argv=tuple(_as_argv(command)),
                trust_category=TrustCategory.TRUSTED_TOOL,
                input_text=input_text,
            ),
            budgets=budgets,
            records=records,
            environment=environment,
            exit_policy=exit_policy,
        )
    )


def run_untrusted_python(
    source: str,
    *,
    profile: ContainmentProfile,
    budgets: Budgets,
    records: Records,
    runtime: PythonRuntime = HERMETIC,
    input_text: str = "",
    environment: EnvironmentGrant = _NO_ENVIRONMENT,
    exit_policy: ExitPolicy = REPORT_ONLY,
) -> RunResult:
    """Run untrusted Python source in a declared runtime.

    ``HERMETIC`` runs ``interpreter -I -c <source>``: source is delivered
    as argv so child-observable state is run-invariant, and the child's
    environment is solely the caller's grant — the runtime injects nothing.
    """
    if not isinstance(source, str):
        raise DeclarationError("source must be text")
    source_bytes = len(source.encode("utf-8"))
    if source_bytes > SOURCE_BOUND_BYTES:
        raise DeclarationError(
            f"source of {source_bytes} bytes exceeds the "
            f"{SOURCE_BOUND_BYTES}-byte source bound"
        )

    return execute(
        Declaration(
            invocation=Invocation(
                argv=_python_argv(source, runtime),
                trust_category=TrustCategory.UNTRUSTED_PYTHON,
                input_text=input_text,
                source=source,
                runtime=runtime,
                profile=profile,
            ),
            budgets=budgets,
            records=records,
            environment=environment,
            exit_policy=exit_policy,
        )
    )


def run_untrusted_command(
    command: Sequence[str],
    *,
    profile: ContainmentProfile,
    budgets: Budgets,
    records: Records,
    input_text: str = "",
    environment: EnvironmentGrant = _NO_ENVIRONMENT,
    exit_policy: ExitPolicy = REPORT_ONLY,
) -> RunResult:
    """Run an untrusted argument vector: compiled generated code, agent CLIs.

    Same engine and same invariants as :func:`run_tool`, plus the
    undefaultable ``profile`` and absence as a distinct outcome.
    """
    return execute(
        Declaration(
            invocation=Invocation(
                argv=tuple(_as_argv(command)),
                trust_category=TrustCategory.UNTRUSTED_COMMAND,
                input_text=input_text,
                profile=profile,
            ),
            budgets=budgets,
            records=records,
            environment=environment,
            exit_policy=exit_policy,
        )
    )


def _as_argv(command: Sequence[str]) -> Sequence[str]:
    """A string is a shell-ish mistake, not a one-element vector."""
    if isinstance(command, str | bytes):
        raise DeclarationError("command must be a nonempty sequence of strings")
    return command


def _python_argv(source: str, runtime: PythonRuntime) -> tuple[str, ...]:
    interpreter = runtime.interpreter if runtime.interpreter else sys.executable
    if runtime.isolated:
        return (interpreter, _ISOLATED_FLAG, _SOURCE_FLAG, source)
    return (interpreter, _SOURCE_FLAG, source)
