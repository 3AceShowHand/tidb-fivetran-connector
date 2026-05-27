from __future__ import annotations

import json
import zlib
from dataclasses import dataclass
from typing import Any

from errors import IncrementalError, SchemaChecksumError
from .paths import SchemaPath


@dataclass(frozen=True)
class TiCDCColumn:
    name: str
    tidb_type: str
    raw: dict[str, Any]


@dataclass(frozen=True)
class TableDefinition:
    source_schema: str
    source_table: str
    table_version: int
    ddl_type: int
    query: str
    columns: tuple[TiCDCColumn, ...]
    raw: dict[str, Any]

    @property
    def source_key(self) -> str:
        return f"{self.source_schema}.{self.source_table}"

    @property
    def quoted_key(self) -> str:
        return f"`{self.source_schema}`.`{self.source_table}`"

    @property
    def is_truncate(self) -> bool:
        query = self.query.strip().lower()
        return self.ddl_type in {11, 23} or query.startswith("truncate table")


def _column_name(raw: dict[str, Any]) -> str:
    for name in ("Name", "ColumnName", "ColumnNameO", "name"):
        value = raw.get(name)
        if value is not None and str(value):
            return str(value)
    raise IncrementalError(f"TiCDC column is missing a name: {raw!r}")


def _column_type(raw: dict[str, Any]) -> str:
    for name in ("Type", "ColumnType", "tidb_type", "type"):
        value = raw.get(name)
        if value is not None and str(value):
            return str(value)
    # TiDB model.ColumnInfo may carry only numeric MySQL type code in some encodings.
    return "STRING"


def _table_columns(raw: dict[str, Any]) -> list[dict[str, Any]]:
    columns = raw.get("TableColumns", raw.get("Columns", []))
    if not isinstance(columns, list):
        raise IncrementalError("TiCDC TableColumns must be a list")
    if not all(isinstance(column, dict) for column in columns):
        raise IncrementalError("TiCDC TableColumns entries must be objects")
    return columns


def _canonical_checksum_bytes(raw: dict[str, Any]) -> bytes:
    columns = sorted(_table_columns(raw), key=_column_name)
    total_columns = raw.get("TotalColumns", raw.get("TableColumnsTotal", len(columns)))
    checksum_obj = {
        "Table": raw.get("Table", ""),
        "Schema": raw.get("Schema", ""),
        # TiCDC's tableDefWithoutQuery leaves Version at the Go zero value
        # while excluding Query, Type, and TableVersion from the checksum.
        "Version": 0,
        "TableColumns": columns,
        "TableColumnsTotal": total_columns,
    }
    encoded = json.dumps(checksum_obj, ensure_ascii=False, indent=1, separators=(",", ": "))
    return encoded.encode("utf-8")


def compute_table_definition_checksum(raw: dict[str, Any]) -> int:
    return zlib.crc32(_canonical_checksum_bytes(raw)) & 0xFFFFFFFF


def load_table_definition(raw: dict[str, Any], path: SchemaPath) -> TableDefinition:
    if not isinstance(raw, dict):
        raise IncrementalError(f"TiCDC schema file must contain a JSON object: {path.key}")
    try:
        table_version = int(raw["TableVersion"])
    except (KeyError, TypeError, ValueError) as exc:
        raise IncrementalError(f"TiCDC schema file is missing numeric TableVersion: {path.key}") from exc
    if table_version != path.table_version:
        raise IncrementalError(
            f"TableVersion mismatch for {path.key}: path={path.table_version} json={table_version}"
        )
    actual_checksum = compute_table_definition_checksum(raw)
    if actual_checksum != path.checksum:
        raise SchemaChecksumError(
            f"schema checksum mismatch for {path.key}: path={path.checksum} computed={actual_checksum}"
        )
    if str(raw.get("Schema")) != path.source_schema or str(raw.get("Table")) != path.source_table:
        raise IncrementalError(
            f"schema file path and JSON table identity differ for {path.key}"
        )

    columns = tuple(
        TiCDCColumn(name=_column_name(column), tidb_type=_column_type(column), raw=column)
        for column in _table_columns(raw)
    )
    return TableDefinition(
        source_schema=path.source_schema,
        source_table=path.source_table,
        table_version=table_version,
        ddl_type=int(raw.get("Type") or 0),
        query=str(raw.get("Query") or ""),
        columns=columns,
        raw=raw,
    )


__all__ = [
    "TableDefinition",
    "TiCDCColumn",
    "compute_table_definition_checksum",
    "load_table_definition",
]
