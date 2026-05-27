from __future__ import annotations

import csv
from dataclasses import dataclass
from typing import Any, Iterable

from config import ConnectorConfig
from errors import IncrementalError
from metadata.catalog import CatalogTable
from type_mapping import coerce_value
from .schema import TableDefinition


@dataclass(frozen=True)
class CDCEvent:
    operation: str
    source_schema: str
    source_table: str
    commit_ts: int
    data: dict[str, Any]
    row_offset: int


def _looks_like_header(row: list[str]) -> bool:
    if not row:
        return False
    return row[0].lower() == "operation"


def _business_columns(table_def: TableDefinition, snapshot_schema: CatalogTable) -> list[tuple[str, str]]:
    if table_def.columns:
        return [(column.name, column.tidb_type) for column in table_def.columns]
    return [(column.name, column.tidb_type) for column in snapshot_schema.columns]


def iter_cdc_events(
    handle: Iterable[str],
    *,
    table_def: TableDefinition,
    snapshot_schema: CatalogTable,
    config: ConnectorConfig,
    start_row: int = 0,
) -> Iterable[CDCEvent]:
    reader = csv.reader(handle)
    columns = _business_columns(table_def, snapshot_schema)
    expected_values = len(columns)

    for row_offset, row in enumerate(reader):
        if row_offset < start_row:
            continue
        if _looks_like_header(row):
            continue
        if not row:
            continue

        meta_count = 5 if config.ticdc_output_old_value else 4
        if len(row) == expected_values + 5 and row[4].lower() in {"true", "false", "0", "1"}:
            meta_count = 5
        if len(row) != expected_values + meta_count:
            raise IncrementalError(
                f"CDC row has {len(row)} fields, expected {expected_values + meta_count} "
                f"for {table_def.source_key}"
            )

        operation = row[0]
        if operation not in {"I", "U", "D"}:
            raise IncrementalError(f"unsupported CDC row operation: {operation!r}")
        source_table = row[1]
        source_schema = row[2]
        if source_schema != table_def.source_schema or source_table != table_def.source_table:
            raise IncrementalError(
                f"CDC row table identity {source_schema}.{source_table} does not match "
                f"{table_def.source_key}"
            )
        try:
            commit_ts = int(row[3])
        except ValueError as exc:
            raise IncrementalError(f"CDC row commit-ts must be an integer: {row[3]!r}") from exc

        data_values = row[meta_count:]
        data: dict[str, Any] = {}
        for (name, tidb_type), raw_value in zip(columns, data_values, strict=True):
            data[name] = coerce_value(
                raw_value,
                tidb_type,
                null_marker=config.csv_null_value,
                binary_encoding=config.binary_encoding,
            )

        yield CDCEvent(
            operation=operation,
            source_schema=source_schema,
            source_table=source_table,
            commit_ts=commit_ts,
            data=data,
            row_offset=row_offset,
        )


def add_metadata_columns(event: CDCEvent, data: dict[str, Any]) -> dict[str, Any]:
    enriched = dict(data)
    enriched["_ticdc_commit_ts"] = event.commit_ts
    enriched["_ticdc_source_schema"] = event.source_schema
    enriched["_ticdc_source_table"] = event.source_table
    return enriched


__all__ = ["CDCEvent", "add_metadata_columns", "iter_cdc_events"]
