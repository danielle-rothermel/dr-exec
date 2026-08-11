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
    "FailureOwner",
    "LimitKind",
    "OutcomeKind",
    "OutputOverflowPolicy",
    "ProtocolFailureCode",
    "RecordReceiptKind",
    "RecordState",
    "RuntimeKind",
]
