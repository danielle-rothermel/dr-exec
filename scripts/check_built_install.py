from __future__ import annotations

import os
import sys
import threading
from importlib import import_module
from pathlib import Path
from tempfile import TemporaryDirectory
from time import monotonic
from uuid import uuid4

from dr_serialize import Jsonable, validate_strict_json

import dr_exec
from dr_exec.capabilities import CachedRecordReceipt, CachingExecutor

EXPECTED_ROOT_EXPORT_COUNT = 116
WATCHDOG_SECONDS = 30.0


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: check_built_install.py REPOSITORY_ROOT")

    repository_root = Path(sys.argv[1]).resolve(strict=True)
    package_file = Path(dr_exec.__file__).resolve(strict=True)
    if package_file.is_relative_to(repository_root):
        raise ValueError(
            f"import resolved to repository source: {package_file}"
        )
    fixture_module = import_module("dr_exec_importable_json_fixture")
    fixture_location = getattr(fixture_module, "__file__", None)
    if not isinstance(fixture_location, str):
        raise TypeError("installed fixture has no module file")
    fixture_file = Path(fixture_location).resolve(strict=True)
    if fixture_file.is_relative_to(repository_root):
        raise ValueError(
            f"fixture import resolved to repository source: {fixture_file}"
        )

    exports = dr_exec.__all__
    if len(exports) != EXPECTED_ROOT_EXPORT_COUNT:
        raise ValueError(
            f"expected {EXPECTED_ROOT_EXPORT_COUNT} root exports, "
            f"found {len(exports)}"
        )
    missing_exports = [name for name in exports if not hasattr(dr_exec, name)]
    if missing_exports:
        raise ValueError(f"missing root exports: {missing_exports!r}")

    capability_exports = (CachedRecordReceipt, CachingExecutor)
    if sys.platform == "darwin":
        _check_importable_json_process_jobs(repository_root)
    print(
        f"Validated {len(exports)} root exports from installed wheel at "
        f"{package_file}, plus {len(capability_exports)} capability exports "
        f"and the separately installed fixture at {fixture_file}."
    )


def _check_importable_json_process_jobs(repository_root: Path, /) -> None:
    with TemporaryDirectory(prefix="dr-exec-installed-check-") as temporary:
        root = Path(temporary)
        records = root / "records"
        records.mkdir()
        store = dr_exec.DirectoryRunStore(records)
        executor = dr_exec.ProcessExecutor(
            runtime=dr_exec.IsolatedHostPythonRuntime(Path(sys.executable)),
            run_store=store,
        )
        fixture_module = "dr_exec_importable_json_fixture"
        echo = dr_exec.ImportableEntryPoint(
            module_name=fixture_module, attribute_name="echo"
        )
        trusted = dr_exec.build_trusted_importable_json_job(
            dr_exec.JobId(uuid4()),
            echo,
            {"installed": True},
            env=dr_exec.EnvGrant.none(),
        )
        untrusted = dr_exec.build_untrusted_importable_json_job(
            dr_exec.JobId(uuid4()),
            echo,
            ["opaque", {"batch": 2}],
            env=dr_exec.EnvGrant.none(),
        )

        trusted_completion = executor.run(trusted)
        untrusted_completion = executor.run(untrusted)
        trusted_result = dr_exec.parse_importable_json_result(
            trusted_completion
        )
        untrusted_result = dr_exec.parse_importable_json_result(
            untrusted_completion
        )
        assert isinstance(trusted_result, dict)
        assert isinstance(untrusted_result, dict)
        assert trusted_result["value"] == {"installed": True}
        assert untrusted_result["value"] == ["opaque", {"batch": 2}]
        module_file = trusted_result["module_file"]
        if not isinstance(module_file, str):
            raise TypeError("fixture result did not carry its module file")
        child_fixture_file = Path(module_file).resolve(strict=True)
        assert not child_fixture_file.is_relative_to(repository_root)
        assert isinstance(
            trusted_completion.record_receipt, dr_exec.CompleteRecordReceipt
        )
        assert isinstance(
            untrusted_completion.record_receipt, dr_exec.CompleteRecordReceipt
        )
        trusted_record = store.load(
            trusted_completion.record_receipt.reference
        )
        untrusted_record = store.load(
            untrusted_completion.record_receipt.reference
        )
        assert isinstance(trusted_record, dr_exec.FinalizedRecord)
        assert isinstance(untrusted_record, dr_exec.FinalizedRecord)
        assert isinstance(
            trusted_record.declaration.target,
            dr_exec.TrustedPythonTargetRecord,
        )
        assert isinstance(
            untrusted_record.declaration.target,
            dr_exec.UntrustedPythonTargetRecord,
        )
        assert untrusted_record.declaration.target.containment_profile is (
            dr_exec.ContainmentProfile.PROCESS_BOUNDARY_ONLY
        )

        null_completion = executor.run(
            _trusted_job(fixture_module, "return_null", None)
        )
        assert dr_exec.parse_importable_json_result(null_completion) is None

        for module_name, attribute_name in (
            ("missing_fixture_module", "echo"),
            (fixture_module, "missing_attribute"),
            (fixture_module, "NOT_CALLABLE"),
            (fixture_module, "raise_error"),
            (fixture_module, "return_object"),
            (fixture_module, "return_coroutine"),
            (fixture_module, "return_generator"),
        ):
            failed = executor.run(
                _trusted_job(module_name, attribute_name, {"value": 1})
            )
            _assert_parse_fails(failed)

        nonzero = executor.run(
            _trusted_job(fixture_module, "register_nonzero_exit", "result")
        )
        assert nonzero.result.outcome == dr_exec.ExitedOutcome(exit_code=7)
        _assert_parse_fails(nonzero)

        output_limited = executor.run(
            _trusted_job(
                fixture_module,
                "emit_payload_output",
                {"bytes": 1024, "result": "done"},
                budgets=dr_exec.Budgets(
                    payload_output=dr_exec.FiniteOutput(
                        max_bytes=16,
                        overflow_policy=dr_exec.OutputOverflowPolicy.FAIL,
                        retention=dr_exec.PayloadRetentionBudget(
                            stdout=dr_exec.StreamRetentionBudget(
                                head_bytes=8, tail_bytes=8
                            ),
                            stderr=dr_exec.StreamRetentionBudget(
                                head_bytes=0, tail_bytes=0
                            ),
                        ),
                    )
                ),
            )
        )
        assert output_limited.result.outcome == dr_exec.BudgetExceededOutcome(
            axis=dr_exec.BudgetAxis.PAYLOAD_OUTPUT
        )
        _assert_parse_fails(output_limited)

        finite_job = _trusted_job(
            fixture_module,
            "echo",
            {"small": True},
            budgets=dr_exec.Budgets(
                input_bytes=dr_exec.FiniteByteLimit(max_bytes=4096)
            ),
        )
        assert _object_result(executor.run(finite_job))["value"] == {
            "small": True
        }

        _check_protocol_budgets(store, fixture_module)
        _check_timeout_and_cancellation(executor, root, fixture_module)


def _trusted_job(
    module_name: str,
    attribute_name: str,
    request: Jsonable,
    *,
    budgets: dr_exec.Budgets | None = None,
) -> dr_exec.ExecutionJob:
    return dr_exec.build_trusted_importable_json_job(
        dr_exec.JobId(uuid4()),
        dr_exec.ImportableEntryPoint(
            module_name=module_name, attribute_name=attribute_name
        ),
        request,
        env=dr_exec.EnvGrant.none(),
        budgets=budgets,
    )


def _assert_parse_fails(completed: dr_exec.CompletedExecution, /) -> None:
    try:
        dr_exec.parse_importable_json_result(completed)
    except dr_exec.ImportableJsonResultError:
        return
    raise AssertionError("completion unexpectedly parsed as importable JSON")


def _object_result(
    completed: dr_exec.CompletedExecution, /
) -> dict[str, Jsonable]:
    result = dr_exec.parse_importable_json_result(completed)
    if not isinstance(result, dict):
        raise TypeError("fixture did not return a JSON object")
    return result


def _check_protocol_budgets(
    store: dr_exec.DirectoryRunStore,
    fixture_module: str,
    /,
) -> None:
    cases = (
        dr_exec.ExecutorSelfBudgets(
            protocol_frame_bytes=dr_exec.FiniteByteLimit(max_bytes=80)
        ),
        dr_exec.ExecutorSelfBudgets(
            protocol_total_bytes=dr_exec.FiniteByteLimit(max_bytes=100)
        ),
        dr_exec.ExecutorSelfBudgets(
            protocol_output_count=dr_exec.FiniteCountLimit(max_count=1)
        ),
        dr_exec.ExecutorSelfBudgets(
            json_depth=dr_exec.FiniteCountLimit(max_count=4)
        ),
    )
    requests = (
        ("echo", {"text": "x" * 512}),
        ("echo", {"text": "x" * 512}),
        ("echo", {"ok": True}),
        ("nested", {"depth": 8, "leaf": "x"}),
    )
    for index, (self_budgets, invocation) in enumerate(
        zip(cases, requests, strict=True)
    ):
        executor = dr_exec.ProcessExecutor(
            runtime=dr_exec.IsolatedHostPythonRuntime(Path(sys.executable)),
            run_store=store,
            self_budgets=self_budgets,
        )
        completed = executor.run(
            _trusted_job(
                fixture_module,
                invocation[0],
                validate_strict_json(invocation[1]),
            )
        )
        if index == 2:
            assert _object_result(completed)["value"] == {"ok": True}
        else:
            assert isinstance(
                completed.result.outcome, dr_exec.ProtocolFailedOutcome
            )
            _assert_parse_fails(completed)


def _check_timeout_and_cancellation(
    executor: dr_exec.ProcessExecutor,
    root: Path,
    fixture_module: str,
) -> None:
    timeout_gate = root / "timeout.fifo"
    timeout_ready = root / "timeout.ready"
    os.mkfifo(timeout_gate)
    timed_out = executor.run(
        _trusted_job(
            fixture_module,
            "block_after_ready",
            {
                "gate_path": timeout_gate.as_posix(),
                "ready_path": timeout_ready.as_posix(),
            },
            budgets=dr_exec.Budgets(
                wall_time=dr_exec.FiniteDurationLimit(max_ns=250_000_000)
            ),
        )
    )
    assert timed_out.result.outcome == dr_exec.BudgetExceededOutcome(
        axis=dr_exec.BudgetAxis.WALL_TIME
    )
    _assert_parse_fails(timed_out)

    cancel_gate = root / "cancel.fifo"
    cancel_ready = root / "cancel.ready"
    os.mkfifo(cancel_gate)
    token = dr_exec.CancelToken()
    completed: list[dr_exec.CompletedExecution] = []
    failures: list[BaseException] = []

    def run() -> None:
        try:
            completed.append(
                executor.run(
                    _trusted_job(
                        fixture_module,
                        "block_after_ready",
                        {
                            "gate_path": cancel_gate.as_posix(),
                            "ready_path": cancel_ready.as_posix(),
                        },
                    ),
                    cancellation=token,
                )
            )
        except Exception as error:  # noqa: BLE001 - preserve worker evidence
            failures.append(error)

    caller = threading.Thread(target=run, name="installed-fixture-caller")
    caller.start()
    deadline = monotonic() + WATCHDOG_SECONDS
    while not cancel_ready.exists():
        if monotonic() >= deadline:
            raise AssertionError(
                "watchdog fired waiting for fixture readiness"
            )
    token.cancel()
    caller.join(WATCHDOG_SECONDS)
    assert not caller.is_alive()
    assert failures == []
    assert len(completed) == 1
    assert completed[0].result.outcome == dr_exec.CancelledOutcome()
    _assert_parse_fails(completed[0])


if __name__ == "__main__":
    main()
