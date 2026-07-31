"""Group teardown: the completion race, escalation, and the reap budget.

These are white-box tests over ``_terminate_group`` with ``os.killpg``
fault-injected, because the races they cover cannot be provoked reliably by
running real children.
"""

from __future__ import annotations

import errno
import os
import signal
import subprocess

import pytest

from dr_exec import engine
from dr_exec.errors import ExecutorFailure

from .conftest import requires_posix_groups

pytestmark = requires_posix_groups


class _ProcessStub:
    """A Popen stand-in whose completion state the test drives."""

    def __init__(self, *, returncode: int | None) -> None:
        self.pid = 12345
        self.returncode = returncode
        self.kill_called = False
        self.wait_called = False

    def poll(self) -> int | None:
        return self.returncode

    def kill(self) -> None:
        self.kill_called = True
        self.returncode = -signal.SIGKILL

    def wait(self, timeout: float) -> int:
        self.wait_called = True
        assert timeout > 0
        assert self.returncode is not None
        return self.returncode


def _deny_group(*_: object) -> None:
    raise PermissionError(errno.EPERM, "Operation not permitted")


class TestTerminateGroup:
    def test_a_stale_group_signal_error_after_exit_is_not_a_failure(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        process = _ProcessStub(returncode=0)
        monkeypatch.setattr(os, "killpg", _deny_group)

        engine._terminate_group(process)  # type: ignore[arg-type]

        assert process.kill_called is False
        assert process.wait_called is True

    def test_a_live_group_that_cannot_be_signaled_is_an_executor_failure(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        process = _ProcessStub(returncode=None)
        monkeypatch.setattr(os, "killpg", _deny_group)

        with pytest.raises(
            ExecutorFailure,
            match=r"could not be signaled: errno=1 \(Operation not permitted\)",
        ):
            engine._terminate_group(process)  # type: ignore[arg-type]

        assert process.kill_called is True
        assert process.wait_called is True

    def test_completion_winning_the_kill_race_clears_the_stale_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        process = _ProcessStub(returncode=None)

        def completion_wins_kill_race() -> None:
            process.kill_called = True
            process.returncode = 0

        monkeypatch.setattr(os, "killpg", _deny_group)
        monkeypatch.setattr(process, "kill", completion_wins_kill_race)

        engine._terminate_group(process)  # type: ignore[arg-type]

        assert process.kill_called is True
        assert process.returncode == 0

    def test_the_group_is_retried_after_the_leader_is_reaped(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        process = _ProcessStub(returncode=None)
        attempts = 0

        def transient_denial(*_: object) -> None:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise PermissionError(errno.EPERM, "Operation not permitted")

        monkeypatch.setattr(os, "killpg", transient_denial)

        engine._terminate_group(process)  # type: ignore[arg-type]

        assert attempts == 2
        assert process.kill_called is True
        assert process.returncode == -signal.SIGKILL

    def test_the_group_is_signaled_with_sigkill(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        process = _ProcessStub(returncode=0)
        signals: list[tuple[int, int]] = []

        def record_signal(group_id: int, sent: int) -> None:
            signals.append((group_id, sent))

        monkeypatch.setattr(os, "killpg", record_signal)

        engine._terminate_group(process)  # type: ignore[arg-type]

        assert signals == [(12345, signal.SIGKILL)]

    def test_a_group_outliving_the_reap_budget_is_an_executor_failure(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        process = _ProcessStub(returncode=None)

        def never_reaps(timeout: float) -> int:
            raise subprocess.TimeoutExpired(cmd="stub", timeout=timeout)

        monkeypatch.setattr(os, "killpg", lambda *_: None)
        monkeypatch.setattr(process, "wait", never_reaps)

        with pytest.raises(
            ExecutorFailure, match="outlived the termination self-budget"
        ):
            engine._terminate_group(process)  # type: ignore[arg-type]
