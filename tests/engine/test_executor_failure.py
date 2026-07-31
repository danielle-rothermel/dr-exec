"""The executor's own failure path, and concurrency safety.

:class:`ExecutorFailure` is the only exception a spawned run may raise, and
it means the machinery broke — never that the payload misbehaved. A payload
that merely leaves a pipe open is not broken machinery: the child was
reaped, so a result exists and the fault rides on the result.
"""

from __future__ import annotations

import errno
import io
import threading
from collections.abc import Callable

import pytest

from dr_exec import engine
from dr_exec.declare import PROCESS_BOUNDARY_ONLY, Budgets, Records
from dr_exec.record import Attribution, RunResult
from dr_exec.run import run_untrusted_python


class TestIpcThreadFailure:
    def test_a_drain_that_never_reaches_eof_is_a_channel_outcome_not_an_exception(
        self, monkeypatch: pytest.MonkeyPatch, run_python: Callable[..., RunResult]
    ) -> None:
        # A drain thread that will not join is what an escaped descendant
        # holding an inherited write end looks like from the parent. The
        # child was reaped, so a result exists: returning it with the
        # channel fault named beats discarding it as an executor failure.
        class StalledDrainThread(engine._IpcThread):
            """Only the drains stall: stdin is fed and closed as usual."""

            def join(self, timeout: float | None = None) -> None:
                if self.side is engine._IpcSide.STDIN:
                    super().join(timeout)

            def is_alive(self) -> bool:
                if self.side is engine._IpcSide.STDIN:
                    return super().is_alive()
                return True

        monkeypatch.setattr(engine, "_IpcThread", StalledDrainThread)

        result = run_python("print('produced before the stall')")

        assert result.outcome.attribution is Attribution.CHANNEL
        assert result.outcome.exit_verdict is None
        assert result.returncode == 0
        assert result.stdout == "produced before the stall\n"

    def test_a_drain_read_fault_is_never_payload_attributed(
        self, monkeypatch: pytest.MonkeyPatch, run_python: Callable[..., RunResult]
    ) -> None:
        # An unknown cause is never assigned to the payload by elimination,
        # and a channel claim needs evidence: a failing read on the run's
        # own pipe is exactly that evidence.
        real_drain = engine._drain

        def failing_drain(stream, is_stdout, capture, ipc_errors):  # type: ignore[no-untyped-def]
            if is_stdout and stream is not None:
                stream = _FailingRead(stream)
            return real_drain(stream, is_stdout, capture, ipc_errors)

        monkeypatch.setattr(engine, "_drain", failing_drain)

        result = run_python("print('x' * 100)\n")

        assert result.outcome.attribution is Attribution.CHANNEL
        assert result.outcome.exit_verdict is None

    def test_a_feed_fault_is_executor_attributed(
        self, monkeypatch: pytest.MonkeyPatch, run_python: Callable[..., RunResult]
    ) -> None:
        real_feed = engine._feed_input

        def failing_feed(stream, payload, ipc_errors):  # type: ignore[no-untyped-def]
            return real_feed(_FailingWrite(stream), payload, ipc_errors)

        monkeypatch.setattr(engine, "_feed_input", failing_feed)

        # The child never sees its declared input, and exits cleanly anyway:
        # a clean exit over input the executor failed to deliver is not a
        # payload outcome.
        result = run_python("print('never read stdin')", input_text="declared")

        assert result.outcome.attribution is Attribution.EXECUTOR
        assert result.outcome.exit_verdict is None
        assert result.returncode == 0

    def test_an_ipc_fault_is_narrated_on_the_engine_logger(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
        run_python: Callable[..., RunResult],
    ) -> None:
        import logging

        real_drain = engine._drain

        def failing_drain(stream, is_stdout, capture, ipc_errors):  # type: ignore[no-untyped-def]
            if is_stdout and stream is not None:
                stream = _FailingRead(stream)
            return real_drain(stream, is_stdout, capture, ipc_errors)

        monkeypatch.setattr(engine, "_drain", failing_drain)

        with caplog.at_level(logging.WARNING, logger="dr_exec.engine"):
            run_python("print('x' * 100)\n")

        assert any(
            "fault on the run's stdout pipe" in record.getMessage()
            for record in caplog.records
        )

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


class _FailingRead(io.RawIOBase):
    """A pipe whose second read raises EIO, as a severed pipe does."""

    def __init__(self, stream: object) -> None:
        self._stream = stream
        self._reads = 0

    def read1(self, size: int = -1) -> bytes:
        self._reads += 1
        if self._reads > 1:
            raise OSError(errno.EIO, "Input/output error")
        return self._stream.read1(size)  # type: ignore[attr-defined]


class _FailingWrite:
    """A stdin pipe whose write fails for a reason that is not a broken pipe."""

    def __init__(self, stream: object) -> None:
        self._stream = stream

    def write(self, payload: bytes) -> int:
        raise OSError(errno.EIO, "Input/output error")

    def close(self) -> None:
        self._stream.close()  # type: ignore[attr-defined]


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
