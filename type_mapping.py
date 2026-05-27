from __future__ import annotations

import base64
import json
import re
from decimal import Decimal, InvalidOperation
from typing import Any

from errors import ConnectorError


def _base_tidb_type(tidb_type: str) -> str:
    return re.split(r"[\s(]", tidb_type.strip().upper(), maxsplit=1)[0]


def map_tidb_type_to_fivetran(tidb_type: str) -> str:
    base = _base_tidb_type(tidb_type)
    if base in {"BOOL", "BOOLEAN"}:
        return "BOOLEAN"
    if base == "TINYINT":
        return "SHORT"
    if base in {"SMALLINT", "MEDIUMINT", "INT", "INTEGER"}:
        return "INT"
    if base == "BIGINT":
        return "LONG"
    if base in {"DECIMAL", "NUMERIC", "NEWDECIMAL"}:
        return "DECIMAL"
    if base == "FLOAT":
        return "FLOAT"
    if base in {"DOUBLE", "REAL"}:
        return "DOUBLE"
    if base == "DATE":
        return "NAIVE_DATE"
    if base in {"DATETIME", "TIME", "YEAR"}:
        return "NAIVE_DATETIME" if base == "DATETIME" else "STRING"
    if base == "TIMESTAMP":
        return "UTC_DATETIME"
    if base in {"BIT", "BINARY", "VARBINARY", "TINYBLOB", "BLOB", "MEDIUMBLOB", "LONGBLOB"}:
        return "BINARY"
    if base == "JSON":
        return "JSON"
    if base == "XML":
        return "XML"
    return "STRING"


def coerce_value(raw: str | None, tidb_type: str, *, null_marker: str, binary_encoding: str) -> Any:
    if raw is None or raw == null_marker:
        return None
    base = _base_tidb_type(tidb_type)
    if raw == "" and base not in {"CHAR", "VARCHAR", "TEXT", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT"}:
        return None
    try:
        if base in {"BOOL", "BOOLEAN"}:
            return raw.lower() in {"1", "true", "t", "yes", "y"}
        if base in {"TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT"}:
            return int(raw)
        if base in {"FLOAT", "DOUBLE", "REAL"}:
            return float(raw)
        if base in {"DECIMAL", "NUMERIC", "NEWDECIMAL"}:
            try:
                return Decimal(raw)
            except (InvalidOperation, ValueError) as exc:
                raise ConnectorError(
                    f"failed to coerce {raw!r} as {tidb_type}: {type(exc).__name__}: {exc}"
                ) from exc
        if base == "JSON":
            return json.loads(raw)
        if base in {
            "BIT",
            "BINARY",
            "VARBINARY",
            "TINYBLOB",
            "BLOB",
            "MEDIUMBLOB",
            "LONGBLOB",
        }:
            if binary_encoding == "hex":
                return bytes.fromhex(raw)
            if binary_encoding == "base64":
                return base64.b64decode(raw)
            return raw.encode("utf-8")
    except (ValueError, TypeError, json.JSONDecodeError) as exc:
        raise ConnectorError(
            f"failed to coerce {raw!r} as {tidb_type}: {type(exc).__name__}: {exc}"
        ) from exc
    return raw


__all__ = ["coerce_value", "map_tidb_type_to_fivetran"]
