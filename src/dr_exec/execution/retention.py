from __future__ import annotations

from dataclasses import dataclass, field

from dr_exec.declarations.models import (
    FiniteOutput,
    OutputBudget,
    StreamRetentionBudget,
)
from dr_exec.recording.models import PayloadOutputs, RetainedPayloadStream


@dataclass(slots=True)
class StreamRetention:
    head_bytes: int | None
    tail_bytes: int | None
    _head: bytearray = field(default_factory=bytearray)
    _tail: bytearray = field(default_factory=bytearray)
    produced_bytes: int = 0
    dropped_bytes: int = 0

    def offer(self, chunk: bytes, /) -> None:
        self.produced_bytes += len(chunk)
        if self.head_bytes is None:
            self._head.extend(chunk)
            return
        remaining_head = self.head_bytes - len(self._head)
        if remaining_head > 0:
            self._head.extend(chunk[:remaining_head])
            chunk = chunk[remaining_head:]
        if not chunk:
            return
        if self.tail_bytes is None or self.tail_bytes == 0:
            self.dropped_bytes += len(chunk)
            return
        self._tail.extend(chunk)
        overflow = len(self._tail) - self.tail_bytes
        if overflow > 0:
            del self._tail[:overflow]
            self.dropped_bytes += overflow

    def snapshot(self) -> RetainedPayloadStream:
        return RetainedPayloadStream(
            head=bytes(self._head),
            tail=bytes(self._tail),
            produced_bytes=self.produced_bytes,
            dropped_bytes=self.dropped_bytes,
        )


def _stream_retention(
    budget: StreamRetentionBudget | None,
    /,
) -> StreamRetention:
    if budget is None:
        return StreamRetention(head_bytes=None, tail_bytes=None)
    return StreamRetention(
        head_bytes=budget.head_bytes,
        tail_bytes=budget.tail_bytes,
    )


@dataclass(slots=True)
class PayloadRetention:
    stdout: StreamRetention
    stderr: StreamRetention
    max_total_bytes: int | None

    @classmethod
    def for_budget(cls, budget: OutputBudget, /) -> PayloadRetention:
        if isinstance(budget, FiniteOutput):
            return cls(
                stdout=_stream_retention(budget.retention.stdout),
                stderr=_stream_retention(budget.retention.stderr),
                max_total_bytes=budget.max_bytes,
            )
        return cls(
            stdout=_stream_retention(None),
            stderr=_stream_retention(None),
            max_total_bytes=None,
        )

    @property
    def produced_bytes(self) -> int:
        return self.stdout.produced_bytes + self.stderr.produced_bytes

    @property
    def overflowed(self) -> bool:
        return (
            self.max_total_bytes is not None
            and self.produced_bytes > self.max_total_bytes
        )

    def snapshot(self) -> PayloadOutputs:
        return PayloadOutputs(
            stdout=self.stdout.snapshot(),
            stderr=self.stderr.snapshot(),
        )


__all__ = ["PayloadRetention", "StreamRetention"]
