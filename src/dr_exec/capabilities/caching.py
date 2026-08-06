"""Durable memoization of execution outcomes over an inner executor.

A cache hit certifies the *same declared runtime*: the resolved
interpreter path, version, and platform recorded in the isolated-host
runtime identity document. It does not certify that interpreter,
standard-library, or installed package bytes match what was on the host
when the entry was written; a host that swaps what lives at a resolved
path produces a stale hit this design cannot detect.
"""

from __future__ import annotations

from typing import cast

from dr_serialize import (
    IdentityDocument,
    Jsonable,
    SerializationError,
    canonical_json_bytes,
    json_hash,
)
from dr_store import RecordCache, StoreError, derive_cache_key
from pydantic import ValidationError

from dr_exec.capabilities.protocols import Executor
from dr_exec.core.cancel import CancelToken
from dr_exec.core.kinds import FailureOwner
from dr_exec.declarations.models import ExecutionJob
from dr_exec.declarations.validation import validate_declaration
from dr_exec.recording.identity import _canonical_declaration_digest
from dr_exec.recording.models import (
    BudgetExceededOutcome,
    CachedRecordReceipt,
    CompletedExecution,
    ExecutionResult,
    ExitedOutcome,
)

# Changing what the key covers or what the value means is a version
# bump: old entries stop being addressable instead of being reread.
CACHE_KEY_NAMESPACE = "dr_exec.caching_executor.key.v1"
CACHE_VALUE_SCHEMA = "dr_exec.caching_executor.execution_result.v1"

# Attributions dr-exec treats as the retriable class: caching either
# would make a transient or unexplained fault permanent.
_RETRIABLE_OWNERS = frozenset({FailureOwner.EXECUTOR, FailureOwner.MACHINE})


class CachingExecutor:
    """Executor wrapper replaying stored completions for repeat runs.

    The key pairs the canonical declaration digest with the declared
    runtime identity hash; the environment grant and workload budgets
    are not part of the v1 key. Only exited outcomes are stored —
    budget-exceeded outcomes only behind ``cache_budget_exceeded``, and
    retriable-class attributions never. A hit replays the stored result
    under a cached receipt and certifies the same declared runtime, not
    verified interpreter or package bytes. Reads are best-effort: any
    storage fault or invalid entry is a miss that falls through to the
    inner executor. A binding conflict on a deterministic key is a
    nondeterminism signal; the first stored entry wins. There is no
    TTL, eviction, or delete: invalidation is by key-namespace and
    value-schema versioning only.
    """

    def __init__(
        self,
        inner: Executor,
        /,
        *,
        cache: RecordCache,
        runtime_identity: IdentityDocument,
        cache_budget_exceeded: bool = False,
    ) -> None:
        self._inner = inner
        self._cache = cache
        self._runtime_identity = runtime_identity
        self._cache_budget_exceeded = cache_budget_exceeded

    def run(
        self,
        job: ExecutionJob,
        /,
        *,
        cancellation: CancelToken | None = None,
    ) -> CompletedExecution:
        validate_declaration(job)
        key = _cache_key(job, runtime_identity=self._runtime_identity)
        replayed = _replayed_completion(
            self._cache.get(key, schema=CACHE_VALUE_SCHEMA), key
        )
        if replayed is not None:
            return replayed
        completed = self._inner.run(job, cancellation=cancellation)
        if _is_stored(
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
            # A cache that cannot store degrades cost, not correctness.
            pass


def _cache_key(
    job: ExecutionJob,
    /,
    *,
    runtime_identity: IdentityDocument,
) -> str:
    payload: Jsonable = {
        "key_version": 1,
        "declaration_sha256": str(_canonical_declaration_digest(job.target)),
        "runtime_identity_sha256": str(
            json_hash(runtime_identity.to_json_dict())
        ),
    }
    return derive_cache_key(CACHE_KEY_NAMESPACE, payload)


def _replayed_completion(
    record: Jsonable | None,
    key: str,
    /,
) -> CompletedExecution | None:
    """Rebuild a completion from a stored record, or miss on any fault."""
    if record is None:
        return None
    try:
        result = ExecutionResult.model_validate_json(
            canonical_json_bytes(record), strict=True
        )
    except (SerializationError, ValidationError):
        # A mismatched or corrupt entry reads as a miss, never raises.
        return None
    return CompletedExecution(
        result=result,
        record_receipt=CachedRecordReceipt(
            execution_id=result.execution_id,
            cache_key=key,
        ),
    )


def _is_stored(
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
