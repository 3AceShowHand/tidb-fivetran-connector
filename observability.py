from __future__ import annotations

from dataclasses import dataclass, field
import json
import logging
import time
from typing import Any


LOGGER = logging.getLogger("ticdc_fivetran")
try:  # pragma: no cover - exercised by fivetran debug, not pure unit tests.
    from fivetran_connector_sdk import Logging as fivetran_log
except Exception:  # noqa: BLE001 - fallback keeps unit tests independent of SDK import timing.
    fivetran_log = None


def structured_log(level: int, message: str, **fields: Any) -> None:
    payload = {"message": message, **fields}
    encoded = json.dumps(payload, sort_keys=True, default=str)
    if fivetran_log is not None and getattr(fivetran_log, "LOG_LEVEL", None) is not None:
        if level >= logging.ERROR:
            fivetran_log.error(encoded)
        elif level >= logging.WARNING:
            fivetran_log.warning(encoded)
        elif level <= logging.DEBUG:
            fivetran_log.debug(encoded)
        else:
            fivetran_log.info(encoded)
        return
    LOGGER.log(level, encoded)


def info(message: str, **fields: Any) -> None:
    structured_log(logging.INFO, message, **fields)


def warning(message: str, **fields: Any) -> None:
    structured_log(logging.WARNING, message, **fields)


def timer_start() -> float:
    return time.perf_counter()


def elapsed_ms(start: float) -> float:
    return round((time.perf_counter() - start) * 1000, 3)


@dataclass
class OperationStats:
    counts: dict[str, int] = field(default_factory=dict)
    durations_ms: dict[str, float] = field(default_factory=dict)

    def record(self, operation: str, duration_ms: float) -> None:
        self.counts[operation] = self.counts.get(operation, 0) + 1
        self.durations_ms[operation] = self.durations_ms.get(operation, 0.0) + duration_ms

    def merge(self, other: "OperationStats") -> None:
        for operation, count in other.counts.items():
            self.counts[operation] = self.counts.get(operation, 0) + count
        for operation, duration in other.durations_ms.items():
            self.durations_ms[operation] = self.durations_ms.get(operation, 0.0) + duration

    @property
    def total_count(self) -> int:
        return sum(self.counts.values())

    @property
    def total_duration_ms(self) -> float:
        return round(sum(self.durations_ms.values()), 3)

    def as_log_fields(self, prefix: str = "operation_") -> dict[str, Any]:
        fields: dict[str, Any] = {
            f"{prefix}total_count": self.total_count,
            f"{prefix}total_duration_ms": self.total_duration_ms,
        }
        for operation in sorted(set(self.counts) | set(self.durations_ms)):
            fields[f"{prefix}{operation}_count"] = self.counts.get(operation, 0)
            fields[f"{prefix}{operation}_duration_ms"] = round(
                self.durations_ms.get(operation, 0.0), 3
            )
        return fields


__all__ = [
    "OperationStats",
    "elapsed_ms",
    "info",
    "structured_log",
    "timer_start",
    "warning",
]
