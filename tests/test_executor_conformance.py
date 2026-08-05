"""The shared behavioral conformance suite for the `Executor` Protocol.

Structural typing is not qualification. Every case here runs against both
supported implementations -- `ProcessExecutor` over the real engine and a
real directory store, and `FakeExecutor` -- and pins a behavior the
`Executor` boundary promises rather than a behavior either implementation
happens to have.

The direction of conformance is fixed: the fake conforms to production, not
the reverse. Where the two could plausibly differ, the production behavior
is the specification, and a case that the fake cannot honestly satisfy is
not weakened here -- it is either narrowed to production or dropped from
the shared promise. Two such narrowings exist and are stated explicitly:

- host support. `ProcessExecutor` refuses an unsupported platform because
  its containment claim only holds on macOS. The fake makes no containment
  claim and spawns nothing, so refusing there would buy no fidelity and
  would make consumer logic tests unrunnable off macOS. Host refusal is
  therefore a production behavior, qualified in the engine suites, and not
  part of the shared promise.
- outcome production. Only production decides an outcome; the fake returns
  what a consumer scripted. What both must agree on is the *shape* of what
  comes back and which receipt kind it carries -- so the shared cases pin
  those, and the fake's script is set to the outcome production genuinely
  produces for the same job.

Cases that need a real child are darwin-marked, because real macOS process
semantics are what they rest on. Declaration-parity cases are not: a
declaration is well-formed or not independently of the host, and running
them everywhere is exactly what keeps the two validation paths from
drifting on CI.

Synchronization is on terminal outcomes and explicit events; every case
carries a watchdog.
"""

from __future__ import annotations

import os
import signal
import sys
import threading
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from executor_support import (
    completion_for,
    execution_result,
    job_for,
    python_target,
    trusted_target,
    untrusted_command_target,
)

from dr_exec import (
    Budgets,
    CancelledOutcome,
    CancelToken,
    CompletedExecution,
    CompleteRecordReceipt,
    DeclarationError,
    DirectoryRunStore,
    EnvGrant,
    ExecutionJob,
    ExecutionTarget,
    ExitedOutcome,
    FakeExecutor,
    FakeRecordReceipt,
    FiniteByteLimit,
    IsolatedHostPythonRuntime,
    ProcessExecutor,
    RecordReceiptKind,
    TrustedCommandTarget,
)

if TYPE_CHECKING:
    from dr_exec.protocols import Executor

WATCHDOG_SECONDS = 60.0

requires_macos = pytest.mark.skipif(
    sys.platform != "darwin",
    reason="real macOS process semantics",
)


@pytest.fixture(autouse=True)
def watchdog() -> object:
    """Fail a hung case instead of letting it hang the whole suite."""
    timer = threading.Timer(
        WATCHDOG_SECONDS,
        lambda: os.kill(os.getpid(), signal.SIGALRM),
    )
    previous = signal.signal(
        signal.SIGALRM,
        lambda *_: pytest.fail("watchdog fired: the case did not finish"),
    )
    timer.start()
    yield timer
    timer.cancel()
    signal.signal(signal.SIGALRM, previous)


# --- Both implementations, built the same way for every case -------------


type ExecutorFactory = Callable[[Path], "Executor"]


def build_process_executor(root: Path, /) -> ProcessExecutor:
    records = root / "records"
    records.mkdir(exist_ok=True)
    return ProcessExecutor(
        runtime=IsolatedHostPythonRuntime(executable=Path(sys.executable)),
        run_store=DirectoryRunStore(root=records),
    )


def build_fake_executor(_root: Path, /) -> FakeExecutor:
    """A fake whose responder mirrors what production returns.

    Scripting per case would let the fake answer questions production was
    never asked. Deriving the response from the declaration instead keeps
    the shared cases asking one thing of both: given this job, is the
    result the right shape with the right receipt?
    """
    return FakeExecutor(responder=_mirror_of_production)


def _mirror_of_production(
    job: ExecutionJob, cancellation: CancelToken | None, /
) -> CompletedExecution:
    completed = completion_for(job.job_id)
    if cancellation is None or not cancellation.cancelled:
        return completed
    result = execution_result(completed.result.execution_id)
    return CompletedExecution(
        result=result.model_copy(update={"outcome": CancelledOutcome()}),
        record_receipt=completed.record_receipt,
    )


EXECUTOR_FACTORIES = [
    pytest.param(build_process_executor, id="process"),
    pytest.param(build_fake_executor, id="fake"),
]


@pytest.fixture(params=EXECUTOR_FACTORIES)
def executor(
    request: pytest.FixtureRequest, tmp_path: Path
) -> Iterator[Executor]:
    factory: ExecutorFactory = request.param
    yield factory(tmp_path)


def clean_exit_command() -> tuple[str, ...]:
    """An absolute command that exits 0 without producing output."""
    return (sys.executable, "-I", "-c", "pass")


# --- Declaration validation parity ---------------------------------------
#
# These run on every host. A declaration is well-formed or not
# independently of where the process would have run, so this is the part of
# the boundary CI can keep honest.


def relative_executable_without_granted_path() -> ExecutionJob:
    """argv[0] has no defensible meaning: no absolute path, no PATH."""
    return job_for(trusted_target(("dr-exec-test-relative",)))


def untrusted_relative_executable_without_granted_path() -> ExecutionJob:
    """The same rule does not soften for an untrusted command."""
    return job_for(untrusted_command_target(("dr-exec-test-relative",)))


def input_exceeding_its_declared_budget() -> ExecutionJob:
    """Declared stdin is larger than the declared input budget."""
    return job_for(
        TrustedCommandTarget(
            argv=clean_exit_command(), stdin=b"more than four bytes"
        ),
        budgets=Budgets(input_bytes=FiniteByteLimit(max_bytes=4)),
    )


def python_request_exceeding_its_declared_budget() -> ExecutionJob:
    """The Python target's transport bytes are its canonical request."""
    return job_for(
        python_target("a-request-longer-than-four-bytes"),
        budgets=Budgets(input_bytes=FiniteByteLimit(max_bytes=4)),
    )


INVALID_DECLARATIONS = [
    pytest.param(
        relative_executable_without_granted_path, id="relative-no-path"
    ),
    pytest.param(
        untrusted_relative_executable_without_granted_path,
        id="untrusted-relative-no-path",
    ),
    pytest.param(input_exceeding_its_declared_budget, id="input-over-budget"),
    pytest.param(
        python_request_exceeding_its_declared_budget,
        id="python-request-over-budget",
    ),
]


@pytest.mark.parametrize("declaration", INVALID_DECLARATIONS)
def test_every_executor_rejects_the_same_invalid_declarations(
    executor: Executor, declaration: Callable[[], ExecutionJob]
) -> None:
    """Parity is the point: a fake must not accept what production won't.

    A fake that accepted a job production rejects would let a consumer
    build a workflow on a declaration that can never actually run.
    """
    with pytest.raises(DeclarationError):
        executor.run(declaration())


def valid_absolute_command() -> ExecutionJob:
    return job_for(trusted_target(("/usr/bin/true",)))


def valid_relative_command_with_granted_path() -> ExecutionJob:
    """A granted PATH gives a relative executable a defensible meaning."""
    return job_for(
        trusted_target(("true",)),
        env=EnvGrant.fixed({"PATH": "/usr/bin:/bin"}),
    )


def valid_input_within_its_budget() -> ExecutionJob:
    return job_for(
        trusted_target(("/usr/bin/true",)),
        budgets=Budgets(input_bytes=FiniteByteLimit(max_bytes=4096)),
    )


VALID_DECLARATIONS = [
    pytest.param(valid_absolute_command, id="absolute-command"),
    pytest.param(
        valid_relative_command_with_granted_path, id="relative-granted-path"
    ),
    pytest.param(valid_input_within_its_budget, id="input-within-budget"),
]


@requires_macos
@pytest.mark.parametrize("declaration", VALID_DECLARATIONS)
def test_every_executor_accepts_the_same_valid_declarations(
    executor: Executor, declaration: Callable[[], ExecutionJob]
) -> None:
    """The other half of parity: neither may reject what the other runs.

    Acceptance is only observable where production can actually execute,
    so this half is darwin-marked while the rejection half is not.
    """
    completed = executor.run(declaration())

    assert completed.result.execution_id.job_id is not None


# --- Outcome shape -------------------------------------------------------


@requires_macos
@pytest.mark.parametrize(
    "target_factory",
    [
        pytest.param(
            lambda: trusted_target(clean_exit_command()), id="trusted-command"
        ),
        pytest.param(
            lambda: untrusted_command_target(clean_exit_command()),
            id="untrusted-command",
        ),
        pytest.param(python_target, id="untrusted-python"),
    ],
)
def test_one_job_yields_one_completion_bound_to_its_own_job(
    executor: Executor, target_factory: Callable[[], ExecutionTarget]
) -> None:
    """One job, one completion, and its identity carries that job."""
    job = job_for(target_factory())

    completed = executor.run(job)

    assert completed.result.execution_id.job_id == job.job_id
    assert (
        completed.record_receipt.execution_id == completed.result.execution_id
    )


@requires_macos
def test_a_clean_run_reports_a_clean_exit_outcome(executor: Executor) -> None:
    """Both implementations describe success the same way."""
    completed = executor.run(job_for(trusted_target(clean_exit_command())))

    assert completed.result.outcome == ExitedOutcome(exit_code=0)


# --- Receipt kind --------------------------------------------------------


@requires_macos
def test_each_executor_returns_only_its_own_receipt_kind(
    executor: Executor,
) -> None:
    """The receipt says whether a durable record exists. Nothing else.

    Production returns a real receipt naming a record on disk; the fake
    returns the fake receipt, which means no record was ever attempted.
    Neither kind is reachable from the other implementation, which is what
    keeps `NOT_APPLICABLE` from becoming a production no-record option.
    """
    completed = executor.run(job_for(trusted_target(clean_exit_command())))
    receipt = completed.record_receipt

    if isinstance(executor, FakeExecutor):
        assert isinstance(receipt, FakeRecordReceipt)
        assert receipt.kind is RecordReceiptKind.NOT_APPLICABLE
    else:
        assert isinstance(receipt, CompleteRecordReceipt)
        assert receipt.kind is RecordReceiptKind.COMPLETE
        assert receipt.record_dir.is_dir()


# --- Cancellation semantics ----------------------------------------------


@requires_macos
def test_a_token_cancelled_before_the_call_yields_a_cancelled_outcome(
    executor: Executor,
) -> None:
    """The one deterministic cancellation both implementations share.

    A token already cancelled when the call begins cannot race anything,
    so it is the cancellation behavior the shared boundary can promise.
    Production observes it before spawn and finalizes without a child; the
    fake hands its responder the same token.
    """
    token = CancelToken()
    token.cancel()

    completed = executor.run(
        job_for(trusted_target(clean_exit_command())), cancellation=token
    )

    assert completed.result.outcome == CancelledOutcome()


@requires_macos
def test_an_uncancelled_token_does_not_cancel_a_call(
    executor: Executor,
) -> None:
    """Passing a token is not itself a cancellation."""
    completed = executor.run(
        job_for(trusted_target(clean_exit_command())),
        cancellation=CancelToken(),
    )

    assert completed.result.outcome == ExitedOutcome(exit_code=0)


@requires_macos
def test_omitting_cancellation_is_supported_by_every_executor(
    executor: Executor,
) -> None:
    """`cancellation` is optional at the boundary, for both."""
    completed = executor.run(job_for(trusted_target(clean_exit_command())))

    assert completed.result.outcome == ExitedOutcome(exit_code=0)


# --- Concurrent call isolation -------------------------------------------


@requires_macos
def test_concurrent_calls_on_one_executor_stay_separate(
    executor: Executor,
) -> None:
    """The protocol promises thread safety, so both must deliver it.

    Every caller is held at one barrier so the calls genuinely overlap,
    and the evidence is the exact set of completions: one per job, each
    bound to its own job's identity, with no identity delivered twice.
    """
    jobs = [job_for(trusted_target(clean_exit_command())) for _ in range(4)]
    barrier = threading.Barrier(len(jobs))
    completions: list[CompletedExecution] = []
    guard = threading.Lock()

    def call(one: ExecutionJob) -> None:
        barrier.wait(WATCHDOG_SECONDS)
        completed = executor.run(one)
        with guard:
            completions.append(completed)

    threads = [threading.Thread(target=call, args=(one,)) for one in jobs]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(WATCHDOG_SECONDS)

    assert not any(thread.is_alive() for thread in threads)
    delivered = {
        completed.result.execution_id.job_id for completed in completions
    }
    assert delivered == {one.job_id for one in jobs}


@requires_macos
def test_concurrent_calls_use_distinct_attempt_identities(
    executor: Executor,
) -> None:
    """One job run twice is two attempts, never one shared identity."""
    job = job_for(trusted_target(clean_exit_command()))

    first = executor.run(job)
    second = executor.run(job)

    assert (
        first.result.execution_id.job_id == second.result.execution_id.job_id
    )
    assert (
        first.result.execution_id.attempt_id
        != second.result.execution_id.attempt_id
    )


# --- The Python target is declared identically for both ------------------


@requires_macos
def test_a_python_target_is_accepted_by_every_executor(
    executor: Executor,
) -> None:
    """The fake needs no runtime, but must still accept the target.

    A consumer testing Python-target logic against the fake and running it
    against production declares exactly one thing, so the declaration has
    to be acceptable to both.
    """
    job = job_for(python_target("conformance"))

    completed = executor.run(job)

    assert completed.result.execution_id.job_id == job.job_id
