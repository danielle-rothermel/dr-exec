"""Caller-scoped durable replay of execution outcomes."""

from __future__ import annotations

from typing import Literal, cast

from dr_serialize import (
    IdentityDocument,
    Jsonable,
    SerializationError,
    Sha256Digest,
    canonical_json_bytes,
    json_hash,
)
from dr_store import CacheHit, RecordCache, StoreError, derive_cache_key
from pydantic import ValidationError

from dr_exec.capabilities.protocols import Executor
from dr_exec.core.cancel import CancelToken
from dr_exec.core.kinds import FailureOwner
from dr_exec.core.model import ContractModel
from dr_exec.core.names import JobId
from dr_exec.declarations.models import Budgets, EnvGrantRecord, ExecutionJob
from dr_exec.declarations.validation import validate_declaration
from dr_exec.recording.identity import (
    _build_env_grant_record,
    _canonical_declaration_digest,
)
from dr_exec.recording.models import (
    BudgetExceededOutcome,
    CachedRecordReceipt,
    CompletedExecution,
    ExecutionResult,
    ExitedOutcome,
)

# Changing what the key covers is a namespace and payload-version bump:
# old entries stop being addressable instead of being reinterpreted.
CACHE_KEY_NAMESPACE = "dr_exec.caching_executor.key.v1"
CACHE_VALUE_SCHEMA = "dr_exec.caching_executor.execution_result.v1"

# Attributions dr-exec treats as the retriable class: caching either
# would make a transient or unexplained fault permanent.
_RETRIABLE_OWNERS = frozenset({FailureOwner.EXECUTOR, FailureOwner.MACHINE})


class _CacheKeyPayload(ContractModel):
    key_version: Literal[1] = 1
    target_declaration_sha256: Sha256Digest
    env: EnvGrantRecord
    budgets: Budgets
    cache_scope_identity_sha256: Sha256Digest
    cache_budget_exceeded: bool


class CachingExecutor:
    """Executor wrapper replaying stored completions for repeat runs.

    The key covers the target declaration, environment grant, workload
    budgets, cache policy, and a caller-owned cache-scope identity. The
    caller is responsible for changing that opaque scope when relevant
    executor, runtime, command, or ambient inputs change, and for using
    this wrapper only when results are deterministic within that scope.
    The scope does not prove what runtime or executor the inner uses.

    Only exited outcomes are stored. Budget-exceeded outcomes are stored
    only behind ``cache_budget_exceeded``, and retriable-class
    attributions are never stored or replayed. Missing, mismatched,
    corrupt, and otherwise unverifiable records are misses. Operational
    backend failures may propagate. A binding conflict keeps the first
    stored entry. There is no TTL, eviction, or delete: invalidation is
    by key-namespace and value-schema versioning only.
    """

    def __init__(
        self,
        inner: Executor,
        /,
        *,
        cache: RecordCache,
        cache_scope_identity: IdentityDocument,
        cache_budget_exceeded: bool = False,
    ) -> None:
        self._inner = inner
        self._cache = cache
        self._cache_scope_identity = cache_scope_identity
        self._cache_budget_exceeded = cache_budget_exceeded

    def run(
        self,
        job: ExecutionJob,
        /,
        *,
        cancellation: CancelToken | None = None,
    ) -> CompletedExecution:
        validate_declaration(job)
        key = _cache_key(
            job,
            cache_scope_identity=self._cache_scope_identity,
            cache_budget_exceeded=self._cache_budget_exceeded,
        )
        replayed = _replayed_result(
            self._cache.get(key, schema=CACHE_VALUE_SCHEMA)
        )
        if replayed is not None and _is_cacheable(
            replayed,
            cache_budget_exceeded=self._cache_budget_exceeded,
        ):
            return _cached_completion(
                replayed,
                key,
                requested_job_id=job.job_id,
            )
        completed = self._inner.run(job, cancellation=cancellation)
        if _is_cacheable(
            completed.result,
            cache_budget_exceeded=self._cache_budget_exceeded,
        ):
            self._store(key, completed.result)
        return completed

    def _store(self, key: str, result: ExecutionResult, /) -> None:
        record = cast("Jsonable", result.model_dump(mode="json"))
        try:
            self._cache.put(key, CACHE_VALUE_SCHEMA, record)
        except StoreError:
            # Recognized store failures are non-fatal. Backend-specific
            # operational exceptions may propagate.
            pass


def _cache_key(
    job: ExecutionJob,
    /,
    *,
    cache_scope_identity: IdentityDocument,
    cache_budget_exceeded: bool = False,
) -> str:
    payload = _CacheKeyPayload(
        target_declaration_sha256=_canonical_declaration_digest(job.target),
        env=_build_env_grant_record(job.env),
        budgets=job.budgets,
        cache_scope_identity_sha256=json_hash(
            cache_scope_identity.to_json_dict()
        ),
        cache_budget_exceeded=cache_budget_exceeded,
    )
    projection = cast("Jsonable", payload.model_dump(mode="json"))
    return derive_cache_key(CACHE_KEY_NAMESPACE, projection)


def _replayed_result(
    hit: CacheHit | None,
    /,
) -> ExecutionResult | None:
    """Parse a stored result, or miss on an unverifiable record."""
    if hit is None:
        return None
    try:
        return ExecutionResult.model_validate_json(
            canonical_json_bytes(hit.record), strict=True
        )
    except (SerializationError, ValidationError):
        return None


def _cached_completion(
    result: ExecutionResult,
    key: str,
    /,
    *,
    requested_job_id: JobId,
) -> CompletedExecution:
    return CompletedExecution(
        result=result,
        record_receipt=CachedRecordReceipt(
            requested_job_id=requested_job_id,
            source_execution_id=result.execution_id,
            cache_key=key,
        ),
    )


def _is_cacheable(
    result: ExecutionResult,
    /,
    *,
    cache_budget_exceeded: bool,
) -> bool:
    if result.attribution.owner in _RETRIABLE_OWNERS:
        return False
    match result.outcome:
        case ExitedOutcome():
            return True
        case BudgetExceededOutcome():
            # Budget overflow is load-dependent: replaying one asserts
            # something about the host, not about the payload.
            return cache_budget_exceeded
        case _:
            return False


__all__ = ["CACHE_KEY_NAMESPACE", "CACHE_VALUE_SCHEMA", "CachingExecutor"]
