from __future__ import annotations

import asyncio
import hashlib
from datetime import UTC, datetime
from typing import cast
from uuid import UUID

import pytest
from dr_serialize import (
    IdentityDocument,
    Jsonable,
    StrictJsonError,
    build_identity_document,
)
from pydantic import ValidationError

from dr_exec import (
    AttemptId,
    Budgets,
    CompletedExecution,
    ContainmentProfile,
    DeclarationError,
    EnvGrant,
    ExecutionAttribution,
    ExecutionId,
    ExecutionJob,
    ExecutionMeasurements,
    ExecutionPool,
    ExecutionPoolConfig,
    ExecutionResult,
    ExecutionSubmission,
    ExitedOutcome,
    FailureOwner,
    FakeExecutor,
    FakeRecordReceipt,
    FiniteByteLimit,
    FixedPoolCapacity,
    ImportableEntryPoint,
    ImportableJsonResultError,
    JobId,
    PayloadOutputs,
    ProtocolFailedOutcome,
    ProtocolFailureCode,
    RetainedPayloadStream,
    TrustedPythonTarget,
    UntrustedPythonTarget,
    build_trusted_importable_json_job,
    build_untrusted_importable_json_job,
    parse_importable_json_result,
)
from dr_exec.importable_json import _driver_source
from dr_exec.recording.identity import _canonical_declaration_digest

JOB_ID = JobId(UUID("0189d3f4-1c2b-7e3a-9f10-2b3c4d5e6f70"))
ATTEMPT_ID = AttemptId(UUID("0189d3f4-1c2b-7e3a-9f10-2b3c4d5e6f71"))
ENTRY_POINT = ImportableEntryPoint(
    module_name="example.workers", attribute_name="run"
)
ENVELOPE_SCHEMA = "dr_exec.importable_json"
ENVELOPE_VERSION = 1


def _envelope(payload: object) -> IdentityDocument:
    return build_identity_document(
        schema=ENVELOPE_SCHEMA,
        schema_version=ENVELOPE_VERSION,
        payload=payload,
    )


@pytest.mark.parametrize(
    "module_name",
    [
        "",
        ".workers",
        "workers.",
        "workers..jobs",
        "workers/jobs",
        "x:y",
        "for.jobs",
    ],
)
def test_module_name_requires_absolute_python_module_syntax(
    module_name: str,
) -> None:
    with pytest.raises(ValidationError):
        ImportableEntryPoint(module_name=module_name, attribute_name="run")


@pytest.mark.parametrize(
    "attribute_name",
    ["", "worker.run", "worker()", "worker/run", "for"],
)
def test_attribute_name_is_one_exact_identifier(attribute_name: str) -> None:
    with pytest.raises(ValidationError):
        ImportableEntryPoint(
            module_name="example.workers", attribute_name=attribute_name
        )


def test_entrypoint_is_strict_and_frozen() -> None:
    with pytest.raises(ValidationError):
        ImportableEntryPoint.model_validate(
            {"module_name": 1, "attribute_name": "run"}
        )
    with pytest.raises(ValidationError):
        ENTRY_POINT.__setattr__("module_name", "changed")


def test_an_allowlist_can_reject_before_building() -> None:
    allowed = frozenset(
        {
            ImportableEntryPoint(
                module_name="approved.jobs", attribute_name="run"
            )
        }
    )
    proposed = ImportableEntryPoint(
        module_name="unapproved.jobs", attribute_name="run"
    )

    assert proposed not in allowed


@pytest.mark.parametrize(
    "value", [{1: "bad"}, {"bad": object()}, float("nan")]
)
def test_builders_reject_non_json_before_returning_a_job(
    value: object,
) -> None:
    with pytest.raises(StrictJsonError):
        build_trusted_importable_json_job(
            JOB_ID,
            ENTRY_POINT,
            cast("Jsonable", value),
            env=EnvGrant.none(),
        )


def test_builders_share_the_driver_request_and_unbudgeted_default() -> None:
    request = cast("Jsonable", {"items": [1, None, {"enabled": True}]})

    trusted = build_trusted_importable_json_job(
        JOB_ID, ENTRY_POINT, request, env=EnvGrant.none()
    )
    untrusted = build_untrusted_importable_json_job(
        JOB_ID, ENTRY_POINT, request, env=EnvGrant.none()
    )

    assert isinstance(trusted.target, TrustedPythonTarget)
    assert isinstance(untrusted.target, UntrustedPythonTarget)
    assert untrusted.target.containment_profile is (
        ContainmentProfile.PROCESS_BOUNDARY_ONLY
    )
    assert trusted.target.driver_source == untrusted.target.driver_source
    assert trusted.target.request == untrusted.target.request
    assert trusted.target.request.to_json_dict() == {
        "schema": ENVELOPE_SCHEMA,
        "schema_version": ENVELOPE_VERSION,
        "payload": request,
    }
    assert trusted.budgets == Budgets.unbudgeted()
    assert untrusted.budgets == Budgets.unbudgeted()


def test_builder_preserves_explicit_environment_and_budgets() -> None:
    env = EnvGrant.fixed({"VISIBLE": "yes"})
    budgets = Budgets(input_bytes=FiniteByteLimit(max_bytes=4096))

    job = build_trusted_importable_json_job(
        JOB_ID, ENTRY_POINT, None, env=env, budgets=budgets
    )

    assert job.env is env
    assert job.budgets is budgets


def test_finite_input_budget_is_enforced_before_fake_execution() -> None:
    job = build_trusted_importable_json_job(
        JOB_ID,
        ENTRY_POINT,
        {"too": "large"},
        env=EnvGrant.none(),
        budgets=Budgets(input_bytes=FiniteByteLimit(max_bytes=1)),
    )
    executor = FakeExecutor([_completion()])

    with pytest.raises(DeclarationError):
        executor.run(job)

    assert executor.calls == ()


def test_driver_material_is_cached_and_pinned() -> None:
    source = _driver_source(ENTRY_POINT)

    assert _driver_source(ENTRY_POINT) is source
    assert "_DR_EXEC_MODULE_NAME = 'example.workers'" in source
    assert "_DR_EXEC_ATTRIBUTE_NAME = 'run'" in source
    assert "_DR_EXEC_ENVELOPE_SCHEMA = 'dr_exec.importable_json'" in source
    assert "_DR_EXEC_ENVELOPE_SCHEMA_VERSION = 1" in source
    assert hashlib.sha256(source.encode()).hexdigest() == (
        "9947fa109e4058a7bf5a4c17d0f161a3bcfe15827ec80519cde64ea9c06f4a76"
    )


def test_target_identity_is_deterministic_and_pinned() -> None:
    first = build_trusted_importable_json_job(
        JOB_ID, ENTRY_POINT, {"b": 2, "a": 1}, env=EnvGrant.none()
    )
    second = build_trusted_importable_json_job(
        JOB_ID, ENTRY_POINT, {"a": 1, "b": 2}, env=EnvGrant.none()
    )

    assert first.target == second.target
    assert str(_canonical_declaration_digest(first.target)) == (
        "3c4edb9f76476bde39a3a110b218e9ecf04a35330f36e968f208189200bc6941"
    )


def test_fake_captures_the_exact_job_and_parser_returns_its_json() -> None:
    job = build_untrusted_importable_json_job(
        JOB_ID, ENTRY_POINT, {"question": 42}, env=EnvGrant.none()
    )
    scripted = _completion(outputs=(_envelope({"answer": [42, None]}),))
    executor = FakeExecutor([scripted])

    completed = executor.run(job)

    assert executor.calls == (job,)
    assert parse_importable_json_result(completed) == {"answer": [42, None]}


@pytest.mark.parametrize("outputs", [(), (_envelope(1), _envelope(2))])
def test_parser_rejects_zero_or_multiple_outputs(
    outputs: tuple[IdentityDocument, ...],
) -> None:
    with pytest.raises(ImportableJsonResultError):
        parse_importable_json_result(_completion(outputs=outputs))


@pytest.mark.parametrize(
    "output",
    [
        build_identity_document(
            schema="other.schema", schema_version=1, payload={"ok": True}
        ),
        build_identity_document(
            schema=ENVELOPE_SCHEMA, schema_version=2, payload={"ok": True}
        ),
    ],
)
def test_parser_rejects_the_wrong_envelope(output: IdentityDocument) -> None:
    with pytest.raises(ImportableJsonResultError):
        parse_importable_json_result(_completion(outputs=(output,)))


def test_parser_accepts_json_null() -> None:
    assert (
        parse_importable_json_result(_completion(outputs=(_envelope(None),)))
        is None
    )


def test_parser_rejects_nonzero_exit_even_with_one_valid_output() -> None:
    with pytest.raises(ImportableJsonResultError):
        parse_importable_json_result(
            _completion(
                outcome=ExitedOutcome(exit_code=7), outputs=(_envelope(1),)
            )
        )


def test_parser_rejects_protocol_failure_even_with_one_accepted_output() -> (
    None
):
    outcome = ProtocolFailedOutcome(
        failure_code=ProtocolFailureCode.INCOMPLETE_STREAM,
        failure_detail="stream ended",
        accepted_output_count=1,
    )
    with pytest.raises(ImportableJsonResultError):
        parse_importable_json_result(
            _completion(outcome=outcome, outputs=(_envelope(1),))
        )


def test_a_caller_owned_batch_remains_one_opaque_request() -> None:
    batch = cast(
        "Jsonable",
        [
            {"candidate_id": "a", "tests": [1, 2]},
            {"candidate_id": "b", "tests": [3]},
        ],
    )
    job = build_trusted_importable_json_job(
        JOB_ID, ENTRY_POINT, batch, env=EnvGrant.none()
    )

    assert isinstance(job.target, TrustedPythonTarget)
    assert job.target.request.payload == batch


def test_pool_reuses_existing_context_without_adapter_scheduler_state() -> (
    None
):
    jobs = tuple(
        build_trusted_importable_json_job(
            JobId(UUID(int=index + 1)),
            ENTRY_POINT,
            {"index": index},
            env=EnvGrant.none(),
        )
        for index in range(2)
    )

    def respond(
        job: ExecutionJob, _cancellation: object
    ) -> CompletedExecution:
        assert isinstance(job.target, TrustedPythonTarget)
        return _completion(
            job_id=job.job_id,
            outputs=(_envelope(job.target.request.payload),),
        )

    executor = FakeExecutor(responder=respond)

    async def run() -> list[tuple[str, object]]:
        async def submissions():
            for index, job in enumerate(jobs):
                yield ExecutionSubmission(job=job, context=f"context-{index}")

        async with ExecutionPool(
            executor=executor,
            config=ExecutionPoolConfig(
                capacity=FixedPoolCapacity(max_active_jobs=2)
            ),
        ) as pool:
            return [
                (
                    item.context,
                    parse_importable_json_result(item.completed_execution),
                )
                async for item in pool.run_stream(submissions())
            ]

    returned = asyncio.run(run())

    assert sorted(returned) == [
        ("context-0", {"index": 0}),
        ("context-1", {"index": 1}),
    ]
    assert sorted(call.job_id for call in executor.calls) == sorted(
        job.job_id for job in jobs
    )


def _completion(
    *,
    job_id: JobId = JOB_ID,
    outcome: ExitedOutcome | ProtocolFailedOutcome | None = None,
    outputs: tuple[IdentityDocument, ...] = (),
) -> CompletedExecution:
    execution_id = ExecutionId(job_id=job_id, attempt_id=ATTEMPT_ID)
    moment = datetime.now(UTC)
    empty = RetainedPayloadStream(
        head=b"", tail=b"", produced_bytes=0, dropped_bytes=0
    )
    selected_outcome = (
        ExitedOutcome(exit_code=0) if outcome is None else outcome
    )
    return CompletedExecution(
        result=ExecutionResult(
            execution_id=execution_id,
            outcome=selected_outcome,
            attribution=ExecutionAttribution(
                owner=(
                    FailureOwner.NONE
                    if isinstance(selected_outcome, ExitedOutcome)
                    and selected_outcome.exit_code == 0
                    else FailureOwner.PAYLOAD
                )
            ),
            protocol_outputs=outputs,
            payload_outputs=PayloadOutputs(stdout=empty, stderr=empty),
            measurements=ExecutionMeasurements(
                started_at=moment,
                finished_at=moment,
                duration_ns=0,
                teardown_duration_ns=0,
                input_bytes=0,
                protocol_bytes_received=0,
            ),
        ),
        record_receipt=FakeRecordReceipt(execution_id=execution_id),
    )
