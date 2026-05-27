from __future__ import annotations

import json
import time
from copy import deepcopy
from typing import Any

from errors import ConnectorError
from metadata.catalog import Catalog, CatalogTable


def make_stream_key(
    source_schema: str,
    source_table: str,
    table_version: int,
    partition_id: int,
    dispatcher_id: str,
) -> str:
    return f"{source_schema}/{source_table}/{table_version}/{partition_id}/{dispatcher_id}"


def normalize_state(raw_state: dict[str, Any] | str | None, catalog: Catalog) -> dict[str, Any]:
    if raw_state is None or raw_state == "":
        state: dict[str, Any] = {}
    elif isinstance(raw_state, str):
        parsed = json.loads(raw_state)
        if not isinstance(parsed, dict):
            raise ConnectorError("Fivetran state must decode to a JSON object")
        state = parsed
    elif isinstance(raw_state, dict):
        state = deepcopy(raw_state)
    else:
        raise ConnectorError("Fivetran state must be a dict or JSON string")

    version = state.get("version", 1)
    if version != 1:
        raise ConnectorError(f"unsupported state version: {version!r}")
    state["version"] = 1

    snapshot = state.setdefault("snapshot", {})
    if not isinstance(snapshot, dict):
        raise ConnectorError("state.snapshot must be an object")
    snapshot["required"] = True
    snapshot.setdefault("snapshot-tso", 0)
    tables = snapshot.setdefault("tables", {})
    if not isinstance(tables, dict):
        raise ConnectorError("state.snapshot.tables must be an object")
    for table in catalog.tables:
        entry = tables.setdefault(table.source_key, {})
        if not isinstance(entry, dict):
            raise ConnectorError(f"state snapshot entry for {table.source_key} must be an object")
        entry.setdefault("done", False)
        entry.setdefault("file", "")
        entry.setdefault("next_row", 0)

    incremental = state.setdefault("incremental", {})
    if not isinstance(incremental, dict):
        raise ConnectorError("state.incremental must be an object")
    incremental.setdefault("last_seen_checkpoint_ts", 0)
    incremental.setdefault("last_processed_commit_ts", 0)
    incremental.setdefault("commit_ts_by_table", {})
    incremental.setdefault("ddl_watermark", {})
    incremental.setdefault("streams", {})
    changed = False
    if "dml" in incremental:
        changed |= _migrate_legacy_dml_cursors(incremental)
    else:
        incremental["dml"] = {}
    if not isinstance(incremental["commit_ts_by_table"], dict):
        raise ConnectorError("state.incremental.commit_ts_by_table must be an object")
    if not isinstance(incremental["ddl_watermark"], dict):
        raise ConnectorError("state.incremental.ddl_watermark must be an object")
    if not isinstance(incremental["streams"], dict):
        raise ConnectorError("state.incremental.streams must be an object")
    if not isinstance(incremental["dml"], dict):
        raise ConnectorError("state.incremental.dml must be an object")
    changed |= compact_incremental_state(state, catalog)
    if changed:
        state["_needs_checkpoint"] = True
    return state


def _migrate_legacy_dml_cursors(incremental: dict[str, Any]) -> bool:
    legacy = incremental.get("dml")
    if not isinstance(legacy, dict):
        return False
    streams = incremental.setdefault("streams", {})
    if not isinstance(streams, dict):
        return False

    changed = False
    for index_path, cursor in legacy.items():
        if not isinstance(index_path, str) or not isinstance(cursor, dict):
            continue
        parsed = _parse_legacy_index_cursor(index_path)
        if parsed is None:
            continue
        source_schema, source_table, table_version, partition_id, date, dispatcher_id = parsed
        key = make_stream_key(source_schema, source_table, table_version, partition_id, dispatcher_id)
        existing = streams.get(key)
        migrated = {
            "source_schema": source_schema,
            "source_table": source_table,
            "table_version": table_version,
            "partition_id": partition_id,
            "dispatcher_id": dispatcher_id,
            "last_date": date,
            "last_file_index": int(cursor.get("last_file_index") or 0),
            "current_date": date if int(cursor.get("row") or 0) > 0 else "",
            "current_file_index": int(cursor.get("current_file_index") or 0),
            "row": int(cursor.get("row") or 0),
            "latest_index_file_index": int(cursor.get("last_file_index") or 0),
            "last_commit_ts": int(cursor.get("last_commit_ts") or 0),
        }
        if not isinstance(existing, dict) or _stream_position(migrated) >= _stream_position(existing):
            streams[key] = migrated
            changed = True
    incremental["dml"] = {}
    return changed or bool(legacy)


def _parse_legacy_index_cursor(
    index_path: str,
) -> tuple[str, str, int, int, str, str] | None:
    parts = index_path.strip("/").split("/")
    try:
        increment_pos = parts.index("increment")
        relative = parts[increment_pos + 1 :]
    except ValueError:
        relative = parts
    if len(relative) < 5 or relative[-2] != "meta":
        return None
    try:
        table_version = int(relative[2])
    except ValueError:
        return None
    tail = relative[3:-2]
    date = ""
    partition_id = 0
    if tail:
        if _looks_like_date(tail[-1]):
            date = tail[-1]
            tail = tail[:-1]
        if tail:
            try:
                partition_id = int(tail[-1])
            except ValueError:
                return None
    dispatcher_id = ""
    filename = relative[-1]
    if filename.startswith("CDC_") and filename.endswith(".index"):
        dispatcher_id = filename[len("CDC_") : -len(".index")]
    return relative[0], relative[1], table_version, partition_id, date, dispatcher_id


def _looks_like_date(value: str) -> bool:
    parts = value.split("-")
    return len(parts) in {1, 2, 3} and all(part.isdigit() for part in parts)


def _stream_position(cursor: dict[str, Any]) -> tuple[str, int]:
    return str(cursor.get("last_date") or ""), int(cursor.get("last_file_index") or 0)


def snapshot_entry(state: dict[str, Any], table: CatalogTable) -> dict[str, Any]:
    return state["snapshot"]["tables"][table.source_key]


def snapshot_tso(state: dict[str, Any]) -> int:
    return int(state["snapshot"].get("snapshot-tso") or 0)


def set_snapshot_tso(state: dict[str, Any], value: int) -> bool:
    current = snapshot_tso(state)
    if current and current != value:
        raise ConnectorError(
            f"state snapshot-tso {current} does not match current snapshot metadata {value}"
        )
    if current == value:
        return False
    state["snapshot"]["snapshot-tso"] = value
    return True


def snapshots_complete(state: dict[str, Any], catalog: Catalog) -> bool:
    return all(snapshot_entry(state, table).get("done") is True for table in catalog.tables)


def stream_cursor(
    state: dict[str, Any],
    *,
    source_schema: str,
    source_table: str,
    table_version: int,
    partition_id: int,
    dispatcher_id: str,
) -> dict[str, Any]:
    streams = state["incremental"]["streams"]
    key = make_stream_key(source_schema, source_table, table_version, partition_id, dispatcher_id)
    cursor = streams.setdefault(
        key,
        {
            "source_schema": source_schema,
            "source_table": source_table,
            "table_version": table_version,
            "partition_id": partition_id,
            "dispatcher_id": "",
            "last_date": "",
            "last_file_index": 0,
            "current_date": "",
            "current_file_index": 0,
            "row": 0,
            "latest_index_file_index": 0,
            "last_commit_ts": 0,
        },
    )
    if not isinstance(cursor, dict):
        raise ConnectorError(f"state stream cursor {key} must be an object")
    cursor.setdefault("source_schema", source_schema)
    cursor.setdefault("source_table", source_table)
    cursor.setdefault("table_version", table_version)
    cursor.setdefault("partition_id", partition_id)
    cursor.setdefault("dispatcher_id", dispatcher_id)
    cursor.setdefault("last_date", "")
    cursor.setdefault("last_file_index", 0)
    cursor.setdefault("current_date", "")
    cursor.setdefault("current_file_index", 0)
    cursor.setdefault("row", 0)
    cursor.setdefault("latest_index_file_index", 0)
    cursor.setdefault("last_commit_ts", 0)
    return cursor


def has_pending_stream_work(state: dict[str, Any]) -> bool:
    for cursor in state["incremental"]["streams"].values():
        if not isinstance(cursor, dict):
            continue
        if int(cursor.get("row") or 0) > 0:
            return True
        if int(cursor.get("current_file_index") or 0) > int(cursor.get("last_file_index") or 0):
            return True
        if int(cursor.get("last_file_index") or 0) < int(cursor.get("latest_index_file_index") or 0):
            return True
    return False


def compact_incremental_state(state: dict[str, Any], catalog: Catalog) -> bool:
    incremental = state["incremental"]
    changed = False
    catalog_tables = set(catalog.table_by_source_key)
    catalog_quoted = {table.source_key: table.quoted_key for table in catalog.tables}

    streams = incremental["streams"]
    for key, cursor in list(streams.items()):
        if not isinstance(cursor, dict):
            del streams[key]
            changed = True
            continue
        source_key = f"{cursor.get('source_schema')}.{cursor.get('source_table')}"
        if source_key not in catalog_tables:
            del streams[key]
            changed = True
            continue
        if int(cursor.get("row") or 0) > 0:
            continue
        watermark = ddl_watermark(state, catalog_quoted[source_key])
        if int(cursor.get("table_version") or 0) < watermark:
            del streams[key]
            changed = True

    commit_ts_by_table = incremental["commit_ts_by_table"]
    for source_key in list(commit_ts_by_table):
        if source_key not in catalog_tables:
            del commit_ts_by_table[source_key]
            changed = True

    if incremental.get("dml"):
        incremental["dml"] = {}
        changed = True
    return changed


def ddl_watermark(state: dict[str, Any], quoted_table_key: str) -> int:
    value = state["incremental"]["ddl_watermark"].get(quoted_table_key, 0)
    return int(value or 0)


def set_ddl_watermark(state: dict[str, Any], quoted_table_key: str, table_version: int) -> None:
    current = ddl_watermark(state, quoted_table_key)
    if table_version > current:
        state["incremental"]["ddl_watermark"][quoted_table_key] = table_version


class CheckpointPolicy:
    def __init__(self, rows: int, seconds: int) -> None:
        self.rows = rows
        self.seconds = seconds
        self.rows_since_checkpoint = 0
        self.last_checkpoint_monotonic = time.monotonic()

    def record_row(self) -> bool:
        self.rows_since_checkpoint += 1
        if self.rows_since_checkpoint >= self.rows:
            return True
        return time.monotonic() - self.last_checkpoint_monotonic >= self.seconds

    def mark_checkpointed(self) -> None:
        self.rows_since_checkpoint = 0
        self.last_checkpoint_monotonic = time.monotonic()


__all__ = [
    "CheckpointPolicy",
    "compact_incremental_state",
    "ddl_watermark",
    "has_pending_stream_work",
    "make_stream_key",
    "normalize_state",
    "set_ddl_watermark",
    "set_snapshot_tso",
    "snapshot_entry",
    "snapshot_tso",
    "snapshots_complete",
    "stream_cursor",
]
