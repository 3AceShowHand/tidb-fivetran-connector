from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, Callable, Protocol

from observability import OperationStats, elapsed_ms, timer_start


class OperationSink(Protocol):
    def upsert(self, *, table: str, data: dict[str, Any]) -> None: ...

    def update(self, *, table: str, modified: dict[str, Any]) -> None: ...

    def delete(self, *, table: str, keys: dict[str, Any]) -> None: ...

    def truncate(self, *, table: str) -> None: ...

    def checkpoint(self, *, state: dict[str, Any]) -> None: ...


class FivetranOperationSink:
    def __init__(self, op_module: Any) -> None:
        self.op = op_module

    def upsert(self, *, table: str, data: dict[str, Any]) -> None:
        self.op.upsert(table=table, data=data)

    def update(self, *, table: str, modified: dict[str, Any]) -> None:
        self.op.update(table=table, modified=modified)

    def delete(self, *, table: str, keys: dict[str, Any]) -> None:
        self.op.delete(table=table, keys=keys)

    def truncate(self, *, table: str) -> None:
        self.op.truncate(table=table)

    def checkpoint(self, *, state: dict[str, Any]) -> None:
        self.op.checkpoint(state=state)


class TimedOperationSink:
    def __init__(self, sink: OperationSink, stats: OperationStats | None = None) -> None:
        self.sink = sink
        self.stats = stats or OperationStats()

    def _timed(self, operation: str, call: Callable[[], None]) -> None:
        start = timer_start()
        try:
            call()
        finally:
            self.stats.record(operation, elapsed_ms(start))

    def upsert(self, *, table: str, data: dict[str, Any]) -> None:
        self._timed("upsert", lambda: self.sink.upsert(table=table, data=data))

    def update(self, *, table: str, modified: dict[str, Any]) -> None:
        self._timed("update", lambda: self.sink.update(table=table, modified=modified))

    def delete(self, *, table: str, keys: dict[str, Any]) -> None:
        self._timed("delete", lambda: self.sink.delete(table=table, keys=keys))

    def truncate(self, *, table: str) -> None:
        self._timed("truncate", lambda: self.sink.truncate(table=table))

    def checkpoint(self, *, state: dict[str, Any]) -> None:
        self._timed("checkpoint", lambda: self.sink.checkpoint(state=state))


@dataclass
class RecordingOperationSink:
    calls: list[tuple[str, dict[str, Any]]] = field(default_factory=list)

    def upsert(self, *, table: str, data: dict[str, Any]) -> None:
        self.calls.append(("upsert", {"table": table, "data": data.copy()}))

    def update(self, *, table: str, modified: dict[str, Any]) -> None:
        self.calls.append(("update", {"table": table, "modified": modified.copy()}))

    def delete(self, *, table: str, keys: dict[str, Any]) -> None:
        self.calls.append(("delete", {"table": table, "keys": keys.copy()}))

    def truncate(self, *, table: str) -> None:
        self.calls.append(("truncate", {"table": table}))

    def checkpoint(self, *, state: dict[str, Any]) -> None:
        self.calls.append(("checkpoint", {"state": deepcopy(state)}))


__all__ = [
    "FivetranOperationSink",
    "OperationSink",
    "RecordingOperationSink",
    "TimedOperationSink",
]
