from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from config import ConnectorConfig
from errors import IncrementalError
from fivetran.operations import OperationSink, TimedOperationSink
from metadata.catalog import INCREMENT_PREFIX, Catalog
from observability import OperationStats, elapsed_ms, info, timer_start, warning
from state import (
    CheckpointPolicy,
    compact_incremental_state,
    ddl_watermark,
    has_pending_stream_work,
    make_stream_key,
    set_ddl_watermark,
    snapshot_tso,
    snapshots_complete,
    stream_cursor,
)
from storage.object_store import ObjectStore, join_key
from .csv import add_metadata_columns, iter_cdc_events
from .paths import IndexPath, SchemaPath, parse_index_path, parse_schema_path
from .schema import TableDefinition, load_table_definition


@dataclass(frozen=True)
class SchemaMarker:
    path: SchemaPath
    table_def: TableDefinition

    @property
    def sort_key(self) -> tuple[int, int, str, str, str, int]:
        return (
            self.path.table_version,
            0,
            "",
            self.path.source_schema,
            self.path.source_table,
            0,
        )


@dataclass(frozen=True)
class DMLRange:
    index_path: IndexPath

    @property
    def sort_key(self) -> tuple[int, int, str, str, str, int]:
        return (*self.index_path.sort_key, 1)


@dataclass(frozen=True)
class ScanResult:
    schema_paths: list[SchemaPath]
    index_paths: list[IndexPath]
    listed_prefixes: int
    listed_keys: int


def read_increment_metadata(store: ObjectStore, increment_prefix: str) -> int | None:
    metadata_key = join_key(increment_prefix, "metadata")
    if not store.exists(metadata_key):
        return None
    raw = store.read_json(metadata_key)
    if not isinstance(raw, dict):
        raise IncrementalError("increment/metadata must be a JSON object")
    if "checkpoint-ts" not in raw:
        raise IncrementalError("increment/metadata missing checkpoint-ts")
    try:
        return int(raw["checkpoint-ts"])
    except (TypeError, ValueError) as exc:
        raise IncrementalError("increment/metadata checkpoint-ts must be an integer") from exc


def _scan_visible_inputs(
    *,
    config: ConnectorConfig,
    store: ObjectStore,
    catalog: Catalog,
    state: dict[str, Any],
    checkpoint_ts: int,
) -> ScanResult:
    schema_paths: list[SchemaPath] = []
    index_paths: list[IndexPath] = []
    unknown: list[str] = []
    listed_prefixes = 0
    listed_keys = 0
    skipped: dict[str, int] = {
        "future_schema": 0,
        "future_index": 0,
        "unknown": 0,
        "old_version": 0,
    }

    for table in catalog.tables:
        schema_prefix = join_key(INCREMENT_PREFIX, table.source_schema, table.source_table, "meta")
        keys = store.list_keys(schema_prefix)
        listed_prefixes += 1
        listed_keys += len(keys)

        table_schema_paths: list[SchemaPath] = []
        for key in keys:
            schema_path = parse_schema_path(key, INCREMENT_PREFIX)
            if schema_path is None:
                if config.strict_unknown_files:
                    unknown.append(key)
                skipped["unknown"] += 1
                continue
            if schema_path.table_version <= checkpoint_ts:
                table_schema_paths.append(schema_path)
            else:
                skipped["future_schema"] += 1

        schema_paths.extend(table_schema_paths)
        for schema_path in table_schema_paths:
            if schema_path.table_version < ddl_watermark(state, table.quoted_key):
                skipped["old_version"] += 1
                continue
            discovered, prefix_count = _discover_index_paths(
                config=config,
                store=store,
                catalog=catalog,
                state=state,
                schema_path=schema_path,
                checkpoint_ts=checkpoint_ts,
            )
            listed_prefixes += prefix_count
            listed_keys += len(discovered)
            for index_path in discovered:
                if index_path.table_version <= checkpoint_ts:
                    index_paths.append(index_path)
                else:
                    skipped["future_index"] += 1

    if unknown and config.strict_unknown_files:
        raise IncrementalError(f"unknown incremental files under managed prefix: {unknown[:20]}")
    skipped = {name: count for name, count in skipped.items() if count}
    if skipped:
        info(
            "skipped incremental inputs",
            phase="incremental",
            metadata_checkpoint_ts=checkpoint_ts,
            skipped=skipped,
            sample_keys=unknown[:5],
        )
    return ScanResult(
        schema_paths=schema_paths,
        index_paths=index_paths,
        listed_prefixes=listed_prefixes,
        listed_keys=listed_keys,
    )


def _discover_index_paths(
    *,
    config: ConnectorConfig,
    store: ObjectStore,
    catalog: Catalog,
    state: dict[str, Any],
    schema_path: SchemaPath,
    checkpoint_ts: int,
) -> tuple[list[IndexPath], int]:
    base_prefix = join_key(
        INCREMENT_PREFIX,
        schema_path.source_schema,
        schema_path.source_table,
        str(schema_path.table_version),
    )
    prefix_count = 0
    candidate_keys = [join_key(base_prefix, "meta", "CDC.index")]

    version_children = store.list_common_prefixes(base_prefix)
    prefix_count += 1
    for child in version_children:
        child_name = child.strip("/").split("/")[-1]
        if child_name == "meta":
            continue
        if _looks_like_date(child_name):
            if _should_scan_date(
                state,
                schema_path,
                partition_id=0,
                dispatcher_id="",
                date=child_name,
            ):
                candidate_keys.append(join_key(child, "meta", "CDC.index"))
            continue

        try:
            partition_id = int(child_name)
        except ValueError:
            continue

        candidate_keys.append(join_key(child, "meta", "CDC.index"))
        date_children = store.list_common_prefixes(child)
        prefix_count += 1
        for date_child in date_children:
            date = date_child.strip("/").split("/")[-1]
            if not _looks_like_date(date):
                continue
            if _should_scan_date(
                state,
                schema_path,
                partition_id=partition_id,
                dispatcher_id="",
                date=date,
            ):
                candidate_keys.append(join_key(date_child, "meta", "CDC.index"))

    index_paths: list[IndexPath] = []
    for key in sorted(set(candidate_keys)):
        if not store.exists(key):
            continue
        content = store.read_text(key)
        index_path = parse_index_path(key, INCREMENT_PREFIX, content, config)
        if index_path is None or index_path.table_version > checkpoint_ts:
            continue
        if not _should_scan_index_path(state, index_path):
            continue
        index_paths.append(index_path)
    return index_paths, prefix_count


def _looks_like_date(value: str) -> bool:
    parts = value.split("-")
    return len(parts) in {1, 2, 3} and all(part.isdigit() for part in parts)


def _should_scan_date(
    state: dict[str, Any],
    schema_path: SchemaPath,
    *,
    partition_id: int,
    dispatcher_id: str,
    date: str,
) -> bool:
    cursor = _existing_stream_cursor(
        state,
        source_schema=schema_path.source_schema,
        source_table=schema_path.source_table,
        table_version=schema_path.table_version,
        partition_id=partition_id,
        dispatcher_id=dispatcher_id,
    )
    if cursor is None:
        return True
    current_date = str(cursor.get("current_date") or "")
    if current_date and date == current_date:
        return True
    last_date = str(cursor.get("last_date") or "")
    return not last_date or date >= last_date


def _should_scan_index_path(state: dict[str, Any], index_path: IndexPath) -> bool:
    cursor = _existing_stream_cursor(
        state,
        source_schema=index_path.source_schema,
        source_table=index_path.source_table,
        table_version=index_path.table_version,
        partition_id=index_path.partition_id,
        dispatcher_id=index_path.dispatcher_id,
    )
    if cursor is None:
        return True
    if int(cursor.get("row") or 0) > 0 and str(cursor.get("current_date") or "") == index_path.date:
        return True
    last_date = str(cursor.get("last_date") or "")
    if last_date and index_path.date and index_path.date < last_date:
        return False
    if last_date and index_path.date == last_date:
        return index_path.latest_file_index > int(cursor.get("last_file_index") or 0)
    return True


def _existing_stream_cursor(
    state: dict[str, Any],
    *,
    source_schema: str,
    source_table: str,
    table_version: int,
    partition_id: int,
    dispatcher_id: str,
) -> dict[str, Any] | None:
    key = make_stream_key(source_schema, source_table, table_version, partition_id, dispatcher_id)
    cursor = state["incremental"]["streams"].get(key)
    return cursor if isinstance(cursor, dict) else None


def _load_table_definitions(
    store: ObjectStore, schema_paths: list[SchemaPath]
) -> dict[tuple[str, int], TableDefinition]:
    table_defs: dict[tuple[str, int], TableDefinition] = {}
    for path in schema_paths:
        raw = store.read_json(path.key)
        table_def = load_table_definition(raw, path)
        key = (table_def.source_key, table_def.table_version)
        if key in table_defs:
            warning(
                "duplicate schema file for same table version",
                phase="incremental",
                source_table=table_def.source_key,
                tableVersion=table_def.table_version,
                file_path=path.key,
            )
        table_defs[key] = table_def
    return table_defs


def _build_work_items(
    schema_paths: list[SchemaPath],
    table_defs: dict[tuple[str, int], TableDefinition],
    index_paths: list[IndexPath],
) -> list[SchemaMarker | DMLRange]:
    items: list[SchemaMarker | DMLRange] = []
    for path in schema_paths:
        items.append(SchemaMarker(path=path, table_def=table_defs[(path.source_key, path.table_version)]))
    for index_path in index_paths:
        items.append(DMLRange(index_path=index_path))
    return sorted(items, key=lambda item: item.sort_key)


def _emit_event(
    *,
    config: ConnectorConfig,
    operations: OperationSink,
    catalog: Catalog,
    event: Any,
) -> None:
    catalog_table = catalog.table_by_source_key[f"{event.source_schema}.{event.source_table}"]
    if event.operation in {"I", "U"}:
        if config.enable_diagnostic_columns:
            data = add_metadata_columns(event, event.data)
        else:
            data = event.data
        operations.upsert(table=catalog_table.fivetran_table, data=data)
        return

    if event.operation == "D":
        missing = [name for name in catalog_table.primary_key if name not in event.data]
        if missing:
            raise IncrementalError(f"delete event missing primary key columns: {missing}")
        keys = {name: event.data[name] for name in catalog_table.primary_key}
        operations.delete(table=catalog_table.fivetran_table, keys=keys)
        return

    raise IncrementalError(f"unsupported CDC event operation: {event.operation!r}")


def _handle_schema_marker(
    *,
    operations: OperationSink,
    catalog: Catalog,
    state: dict[str, Any],
    table_def: TableDefinition,
    baseline_versions: dict[str, int],
) -> None:
    catalog_table = catalog.table_by_source_key.get(table_def.source_key)
    if catalog_table is None:
        info(
            "skipping schema marker for unmanaged table",
            phase="incremental",
            source_schema=table_def.source_schema,
            source_table=table_def.source_table,
            tableVersion=table_def.table_version,
        )
        return
    current = max(
        ddl_watermark(state, catalog_table.quoted_key),
        baseline_versions.get(catalog_table.source_key, 0),
    )
    if table_def.table_version <= current:
        return
    if table_def.is_truncate:
        operations.truncate(table=catalog_table.fivetran_table)
    set_ddl_watermark(state, catalog_table.quoted_key, table_def.table_version)
    operations.checkpoint(state=state)


def _baseline_versions(
    table_defs: dict[tuple[str, int], TableDefinition],
    lower_bound: int,
) -> dict[str, int]:
    baselines: dict[str, int] = {}
    for source_key, table_version in table_defs:
        if table_version <= lower_bound:
            baselines[source_key] = max(baselines.get(source_key, 0), table_version)
    return baselines


def sync_incremental(
    *,
    config: ConnectorConfig,
    store: ObjectStore,
    catalog: Catalog,
    state: dict[str, Any],
    operations: OperationSink,
) -> None:
    phase_start = timer_start()
    phase_stats = OperationStats()
    phase_operations = TimedOperationSink(operations, phase_stats)
    phase_files = 0
    phase_rows_emitted = 0
    phase_rows_skipped_lower_bound = 0
    needs_checkpoint = bool(state.pop("_needs_checkpoint", False))
    if not snapshots_complete(state, catalog):
        if needs_checkpoint:
            phase_operations.checkpoint(state=state)
        info(
            "incremental skipped because snapshot is incomplete",
            phase="incremental",
            duration_ms=elapsed_ms(phase_start),
            **phase_stats.as_log_fields(),
        )
        return

    lower_bound = snapshot_tso(state)
    if lower_bound <= 0:
        raise IncrementalError("snapshot.snapshot-tso is required before incremental sync")

    metadata_start = timer_start()
    checkpoint_ts = read_increment_metadata(store, INCREMENT_PREFIX)
    metadata_read_duration_ms = elapsed_ms(metadata_start)
    if checkpoint_ts is None:
        if needs_checkpoint:
            phase_operations.checkpoint(state=state)
        info(
            "incremental metadata not found",
            phase="incremental",
            increment_prefix=INCREMENT_PREFIX,
            duration_ms=elapsed_ms(phase_start),
            metadata_read_duration_ms=metadata_read_duration_ms,
            **phase_stats.as_log_fields(),
        )
        return

    previous_checkpoint_ts = int(state["incremental"].get("last_seen_checkpoint_ts") or 0)
    if checkpoint_ts == previous_checkpoint_ts and not has_pending_stream_work(state):
        if needs_checkpoint:
            phase_operations.checkpoint(state=state)
        info(
            "incremental fast path skipped scan",
            phase="incremental",
            metadata_checkpoint_ts=checkpoint_ts,
            duration_ms=elapsed_ms(phase_start),
            metadata_read_duration_ms=metadata_read_duration_ms,
            **phase_stats.as_log_fields(),
        )
        return

    state["incremental"]["last_seen_checkpoint_ts"] = checkpoint_ts
    info(
        "starting incremental phase",
        phase="incremental",
        snapshot_tso=lower_bound,
        metadata_checkpoint_ts=checkpoint_ts,
        state_last_seen_checkpoint_ts=previous_checkpoint_ts,
        metadata_read_duration_ms=metadata_read_duration_ms,
    )
    try:
        scan_start = timer_start()
        scan_result = _scan_visible_inputs(
            config=config,
            store=store,
            catalog=catalog,
            state=state,
            checkpoint_ts=checkpoint_ts,
        )
        scan_duration_ms = elapsed_ms(scan_start)
    except Exception as exc:
        warning(
            "incremental phase failed",
            phase="incremental",
            duration_ms=elapsed_ms(phase_start),
            metadata_checkpoint_ts=checkpoint_ts,
            metadata_read_duration_ms=metadata_read_duration_ms,
            error_type=type(exc).__name__,
            error_message=str(exc),
            **phase_stats.as_log_fields(),
        )
        raise
    info(
        "incremental scan completed",
        phase="incremental",
        metadata_checkpoint_ts=checkpoint_ts,
        scan_duration_ms=scan_duration_ms,
        listed_prefixes=scan_result.listed_prefixes,
        listed_keys=scan_result.listed_keys,
        schema_files=len(scan_result.schema_paths),
        index_files=len(scan_result.index_paths),
    )
    schema_paths = scan_result.schema_paths
    index_paths = scan_result.index_paths
    try:
        schema_load_start = timer_start()
        table_defs = _load_table_definitions(store, schema_paths)
        schema_load_duration_ms = elapsed_ms(schema_load_start)
        info(
            "incremental schema definitions loaded",
            phase="incremental",
            metadata_checkpoint_ts=checkpoint_ts,
            schema_load_duration_ms=schema_load_duration_ms,
            schema_definitions=len(table_defs),
        )
        baseline_versions = _baseline_versions(table_defs, lower_bound)
        items = _build_work_items(schema_paths, table_defs, index_paths)
        policy = CheckpointPolicy(config.checkpoint_interval_rows, config.checkpoint_interval_seconds)

        for item in items:
            if isinstance(item, SchemaMarker):
                _handle_schema_marker(
                    operations=phase_operations,
                    catalog=catalog,
                    state=state,
                    table_def=item.table_def,
                    baseline_versions=baseline_versions,
                )
                policy.mark_checkpointed()
                continue

            index_path = item.index_path
            catalog_table = catalog.table_by_source_key.get(index_path.source_key)
            if catalog_table is None:
                info(
                    "skipping DML range for unmanaged table",
                    phase="incremental",
                    source_schema=index_path.source_schema,
                    source_table=index_path.source_table,
                    tableVersion=index_path.table_version,
                )
                continue
            table_def = table_defs.get((index_path.source_key, index_path.table_version))
            if table_def is None:
                raise IncrementalError(f"missing visible schema file for DML index {index_path.key}")
            effective_watermark = max(
                ddl_watermark(state, catalog_table.quoted_key),
                baseline_versions.get(catalog_table.source_key, 0),
            )
            if index_path.table_version < effective_watermark:
                info(
                    "skipping DML range older than DDL watermark",
                    phase="incremental",
                    source_schema=index_path.source_schema,
                    source_table=index_path.source_table,
                    tableVersion=index_path.table_version,
                    ddl_watermark=effective_watermark,
                )
                continue

            cursor = stream_cursor(
                state,
                source_schema=index_path.source_schema,
                source_table=index_path.source_table,
                table_version=index_path.table_version,
                partition_id=index_path.partition_id,
                dispatcher_id=index_path.dispatcher_id,
            )
            cursor["latest_index_file_index"] = max(
                int(cursor.get("latest_index_file_index") or 0),
                index_path.latest_file_index,
            )
            if int(cursor.get("row") or 0) > 0 and int(cursor.get("current_file_index") or 0) > 0:
                start_file_index = int(cursor["current_file_index"])
            else:
                if str(cursor.get("last_date") or "") == index_path.date:
                    start_file_index = int(cursor.get("last_file_index") or 0) + 1
                else:
                    start_file_index = 1
            if start_file_index > index_path.latest_file_index:
                info(
                    "no new DML files exposed by index",
                    phase="incremental",
                    source_schema=index_path.source_schema,
                    source_table=index_path.source_table,
                    tableVersion=index_path.table_version,
                    index_path=index_path.key,
                    date=index_path.date,
                    state_last_file_index=cursor.get("last_file_index"),
                    index_latest_file_index=index_path.latest_file_index,
                )
                continue

            for file_index in range(start_file_index, index_path.latest_file_index + 1):
                file_start = timer_start()
                file_stats = OperationStats()
                file_operations = TimedOperationSink(operations, file_stats)
                file_rows_emitted = 0
                file_rows_skipped_lower_bound = 0
                start_row = (
                    int(cursor.get("row") or 0)
                    if file_index == int(cursor.get("current_file_index") or 0)
                    else 0
                )
                file_key = index_path.data_file_key(file_index)
                if not store.exists(file_key):
                    raise IncrementalError(
                        f"DML file referenced by index does not exist: "
                        f"index={index_path.key} file={file_key}"
                    )
                cursor["current_date"] = index_path.date
                cursor["current_file_index"] = file_index
                with store.open_text(file_key) as handle:
                    for event in iter_cdc_events(
                        handle,
                        table_def=table_def,
                        snapshot_schema=catalog_table,
                        config=config,
                        start_row=start_row,
                    ):
                        if event.commit_ts <= lower_bound:
                            cursor["row"] = event.row_offset + 1
                            file_rows_skipped_lower_bound += 1
                            continue
                        if event.commit_ts > checkpoint_ts:
                            cursor["current_date"] = index_path.date
                            cursor["current_file_index"] = file_index
                            cursor["row"] = event.row_offset
                            file_operations.checkpoint(state=state)
                            file_duration_ms = elapsed_ms(file_start)
                            phase_stats.merge(file_stats)
                            phase_rows_emitted += file_rows_emitted
                            phase_rows_skipped_lower_bound += file_rows_skipped_lower_bound
                            info(
                                "stopped at DML row beyond metadata checkpoint",
                                phase="incremental",
                                source_schema=index_path.source_schema,
                                source_table=index_path.source_table,
                                tableVersion=index_path.table_version,
                                file_path=file_key,
                                file_index=file_index,
                                row_offset=event.row_offset,
                                commit_ts=event.commit_ts,
                                metadata_checkpoint_ts=checkpoint_ts,
                                duration_ms=file_duration_ms,
                                read_parse_duration_ms=round(
                                    max(file_duration_ms - file_stats.total_duration_ms, 0), 3
                                ),
                                rows_emitted=file_rows_emitted,
                                rows_skipped_lower_bound=file_rows_skipped_lower_bound,
                                **file_stats.as_log_fields(),
                            )
                            return
                        commit_ts_by_table = state["incremental"]["commit_ts_by_table"]
                        table_last_commit_ts = int(
                            commit_ts_by_table.get(catalog_table.source_key) or 0
                        )
                        last_commit_ts = max(
                            int(cursor.get("last_commit_ts") or 0), table_last_commit_ts
                        )
                        if last_commit_ts and event.commit_ts < last_commit_ts:
                            raise IncrementalError(
                                f"commit-ts fallback in {file_key}: {event.commit_ts} < {last_commit_ts}"
                            )
                        _emit_event(
                            config=config,
                            operations=file_operations,
                            catalog=catalog,
                            event=event,
                        )
                        file_rows_emitted += 1
                        cursor["row"] = event.row_offset + 1
                        cursor["last_commit_ts"] = event.commit_ts
                        commit_ts_by_table[catalog_table.source_key] = event.commit_ts
                        state["incremental"]["last_processed_commit_ts"] = event.commit_ts
                        if policy.record_row():
                            file_operations.checkpoint(state=state)
                            policy.mark_checkpointed()

                cursor["last_file_index"] = file_index
                cursor["last_date"] = index_path.date
                cursor["current_file_index"] = file_index
                cursor["current_date"] = ""
                cursor["row"] = 0
                file_operations.checkpoint(state=state)
                policy.mark_checkpointed()
                file_duration_ms = elapsed_ms(file_start)
                phase_stats.merge(file_stats)
                phase_files += 1
                phase_rows_emitted += file_rows_emitted
                phase_rows_skipped_lower_bound += file_rows_skipped_lower_bound
                info(
                    "incremental file completed",
                    phase="incremental",
                    source_schema=index_path.source_schema,
                    source_table=index_path.source_table,
                    tableVersion=index_path.table_version,
                    file_path=file_key,
                    file_index=file_index,
                    row_offset=cursor["row"],
                    duration_ms=file_duration_ms,
                    read_parse_duration_ms=round(
                        max(file_duration_ms - file_stats.total_duration_ms, 0), 3
                    ),
                    rows_emitted=file_rows_emitted,
                    rows_skipped_lower_bound=file_rows_skipped_lower_bound,
                    start_row=start_row,
                    **file_stats.as_log_fields(),
                )

        if compact_incremental_state(state, catalog) or needs_checkpoint:
            phase_operations.checkpoint(state=state)
            info("incremental state compacted", phase="incremental")
    except Exception as exc:
        warning(
            "incremental phase failed",
            phase="incremental",
            duration_ms=elapsed_ms(phase_start),
            metadata_checkpoint_ts=checkpoint_ts,
            metadata_read_duration_ms=metadata_read_duration_ms,
            rows_emitted=phase_rows_emitted,
            rows_skipped_lower_bound=phase_rows_skipped_lower_bound,
            files_processed=phase_files,
            error_type=type(exc).__name__,
            error_message=str(exc),
            **phase_stats.as_log_fields(),
        )
        raise

    phase_duration_ms = elapsed_ms(phase_start)
    info(
        "incremental phase completed",
        phase="incremental",
        snapshot_tso=lower_bound,
        metadata_checkpoint_ts=checkpoint_ts,
        duration_ms=phase_duration_ms,
        metadata_read_duration_ms=metadata_read_duration_ms,
        scan_duration_ms=scan_duration_ms,
        schema_load_duration_ms=schema_load_duration_ms,
        read_parse_duration_ms=round(max(phase_duration_ms - phase_stats.total_duration_ms, 0), 3),
        rows_emitted=phase_rows_emitted,
        rows_skipped_lower_bound=phase_rows_skipped_lower_bound,
        files_processed=phase_files,
        **phase_stats.as_log_fields(),
    )


__all__ = ["read_increment_metadata", "sync_incremental"]
