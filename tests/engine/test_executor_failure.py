"""The executor's own failure path, and concurrency safety.

:class:`ExecutorFailure` is the only exception a spawned run may raise, and
it means the machinery broke — never that the payload misbehaved.
"""

from __future__ import annotations

import threading
from collections.abc import Callable

import pytest

from dr_exec import engine
from dr_exec.declare import PROCESS_BOUNDARY_ONLY, Budgets, Records
from dr_exec.errors import ExecutorFailure
from dr_exec.record import Attribution, RunResult
from dr_exec.run import run_untrusted_python


class TestIpcThreadFailure:
    def test_ipc_threads_that_will_not_join_are_an_executor_failure(
        self, monkeypatch: pytest.MonkeyPatch, run_python: Callable[..., RunResult]
    ) -> None:
        class UnjoinableThread(threading.Thread):
            def join(self, timeout: float | None = None) -> None:
                return

            def is_alive(self) -> bool:
                return True

        monkeypatch.setattr(engine.threading, "Thread", UnjoinableThread)

        with pytest.raises(ExecutorFailure, match="join self-budget"):
            run_python("print('irrelevant')")

    def test_a_misbehaving_payload_is_never_an_executor_failure(
        self, run_python: Callable[..., RunResult]
    ) -> None:
        result = run_python(
            "import os, sys\n"
            "os.close(1)\n"
            "sys.stderr.write('closed my own stdout')\n"
            "raise SystemExit(3)\n"
        )

        assert result.returncode == 3
        assert result.outcome.attribution is Attribution.PAYLOAD
        assert result.stderr == "closed my own stdout"


class TestConcurrency:
    def test_concurrent_calls_from_one_process_stay_independent(self) -> None:
        results: dict[int, RunResult] = {}
        errors: list[BaseException] = []

        def run_one(index: int) -> None:
            try:
                results[index] = run_untrusted_python(
                    f"import os, sys; sys.stdout.write('{index}:' + os.getcwd())",
                    profile=PROCESS_BOUNDARY_ONLY,
                    budgets=Budgets(wall_clock=20.0),
                    records=Records.none(),
                )
            except BaseException as failure:
                errors.append(failure)

        threads = [
            threading.Thread(target=run_one, args=(index,)) for index in range(8)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30.0)

        assert errors == []
        assert len(results) == 8
        for index, result in results.items():
            assert result.stdout.startswith(f"{index}:")
        scratch_paths = {result.stdout.split(":", 1)[1] for result in results.values()}
        assert len(scratch_paths) == 8
