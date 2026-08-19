from enum import UNIQUE, StrEnum, verify


@verify(UNIQUE)
class RuntimeKind(StrEnum):
    ISOLATED_HOST_PYTHON = "isolated_host_python"


@verify(UNIQUE)
class RecordState(StrEnum):
    PREPARED = "prepared"
    RUNNING = "running"
    FINALIZED = "finalized"


@verify(UNIQUE)
class EnvGrantKind(StrEnum):
    NONE = "none"
    NAMED = "named"
    FIXED = "fixed"
    OVERLAY = "overlay"


@verify(UNIQUE)
class WorkingDirectoryGrantKind(StrEnum):
    SCRATCH = "scratch"
    CALLER = "caller"


@verify(UNIQUE)
class LimitKind(StrEnum):
    UNBUDGETED = "unbudgeted"
    FINITE = "finite"


@verify(UNIQUE)
class OutputOverflowPolicy(StrEnum):
    FAIL = "fail"
    MARKED_TRUNCATION = "marked_truncation"


@verify(UNIQUE)
class ExecutionTargetKind(StrEnum):
    TRUSTED_COMMAND = "trusted_command"
    TRUSTED_PYTHON = "trusted_python"
    UNTRUSTED_COMMAND = "untrusted_command"
    UNTRUSTED_PYTHON = "untrusted_python"
    IN_PROCESS_IMPORTABLE_JSON = "in_process_importable_json"


@verify(UNIQUE)
class ContainmentProfile(StrEnum):
    PROCESS_BOUNDARY_ONLY = "process_boundary_only"


@verify(UNIQUE)
class OutcomeKind(StrEnum):
    EXITED = "exited"
    SIGNALED = "signaled"
    SPAWN_ABSENT = "spawn_absent"
    SPAWN_FAILED = "spawn_failed"
    BUDGET_EXCEEDED = "budget_exceeded"
    PROTOCOL_FAILED = "protocol_failed"
    CANCELLED = "cancelled"


@verify(UNIQUE)
class BudgetAxis(StrEnum):
    WALL_TIME = "wall_time"
    INPUT_BYTES = "input_bytes"
    PAYLOAD_OUTPUT = "payload_output"
    MEMORY_BYTES = "memory_bytes"
    CPU_TIME = "cpu_time"
    PROCESS_COUNT = "process_count"
    FILE_SIZE_BYTES = "file_size_bytes"
    OPEN_FILE_COUNT = "open_file_count"
    DISK_BYTES = "disk_bytes"


@verify(UNIQUE)
class ProtocolFailureCode(StrEnum):
    MALFORMED_FRAME = "malformed_frame"
    OVERSIZED_FRAME = "oversized_frame"
    UNEXPECTED_FRAME = "unexpected_frame"
    ID_MISMATCH = "id_mismatch"
    DUPLICATE_OUTPUT = "duplicate_output"
    INCOMPLETE_STREAM = "incomplete_stream"


@verify(UNIQUE)
class ExecutorFailureCode(StrEnum):
    TARGET_NOT_SUPPORTED = "target_not_supported"
    BOOTSTRAP_TIMEOUT = "bootstrap_timeout"
    TRANSPORT_WORKER_FAILED = "transport_worker_failed"
    STDIN_TRANSPORT_TAKEN = "stdin_transport_taken"
    PROTOCOL_TRANSPORT_TAKEN = "protocol_transport_taken"
    TRANSPORT_JOIN_TIMEOUT = "transport_join_timeout"
    BOOTSTRAP_START_FAILED = "bootstrap_start_failed"
    RECORDING_OPERATION_FAILED = "recording_operation_failed"
    POOL_CAPACITY_UNRESOLVED = "pool_capacity_unresolved"
    POOL_INVALID_STATE = "pool_invalid_state"
    POOL_WRONG_EVENT_LOOP = "pool_wrong_event_loop"
    POOL_NO_SCHEDULER = "pool_no_scheduler"
    SCHEDULER_BROKEN = "scheduler_broken"
    WORKER_POOL_TARGET_MISMATCH = "worker_pool_target_mismatch"
    WORKER_POOL_ENTRY_POINT_MISMATCH = "worker_pool_entry_point_mismatch"
    IMPORTABLE_JSON_TARGET_MISMATCH = "importable_json_target_mismatch"
    FAKE_NO_RESPONSE = "fake_no_response"
    FAKE_RECEIPT_MISMATCH = "fake_receipt_mismatch"


@verify(UNIQUE)
class FailureOwner(StrEnum):
    NONE = "none"
    PAYLOAD = "payload"
    EXECUTOR = "executor"
    MACHINE = "machine"


@verify(UNIQUE)
class RecordReceiptKind(StrEnum):
    COMPLETE = "complete"
    DEGRADED = "degraded"
    NOT_APPLICABLE = "not_applicable"
    IN_PROCESS = "in_process"
    WORKER_POOL = "worker_pool"


@verify(UNIQUE)
class CapacitySource(StrEnum):
    AUTO = "auto"
    FIXED = "fixed"


@verify(UNIQUE)
class ExecutionPoolState(StrEnum):
    CREATED = "created"
    RUNNING = "running"
    DRAINING = "draining"
    CLOSED = "closed"
    BROKEN = "broken"


@verify(UNIQUE)
class ProtocolFrameKind(StrEnum):
    PRELUDE = "prelude"
    OUTPUT = "output"
    COMPLETE = "complete"


__all__ = [
    "BudgetAxis",
    "CapacitySource",
    "ContainmentProfile",
    "EnvGrantKind",
    "ExecutionPoolState",
    "ExecutionTargetKind",
    "ExecutorFailureCode",
    "FailureOwner",
    "LimitKind",
    "OutcomeKind",
    "OutputOverflowPolicy",
    "ProtocolFailureCode",
    "ProtocolFrameKind",
    "RecordReceiptKind",
    "RecordState",
    "RuntimeKind",
    "WorkingDirectoryGrantKind",
]
