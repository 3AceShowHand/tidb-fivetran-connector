from __future__ import annotations

import csv
from itertools import chain
import re
from typing import Any

from config import ConnectorConfig
from errors import SnapshotError
from fivetran.operations import OperationSink, TimedOperationSink
from metadata.catalog import SNAPSHOT_PREFIX, Catalog, CatalogTable, snapshot_metadata_pos
from observability import OperationStats, elapsed_ms, info, timer_start, warning
from state import CheckpointPolicy, set_snapshot_tso, snapshot_entry
from storage.object_store import ObjectStore
from type_mapping import coerce_value


def _snapshot_files(store: ObjectStore, catalog: Catalog, table: CatalogTable) -> list[str]:
    prefix = SNAPSHOT_PREFIX
    files = store.list_keys(prefix)
    marker = f"{prefix}/{table.snapshot_file_stem}."
    return sorted(
        (key for key in files if key.startswith(marker) and key.endswith(".csv")),
        key=_natural_sort_key,
    )


def _natural_sort_key(value: str) -> list[int | str]:
    return [int(part) if part.isdigit() else part for part in re.split(r"(\d+)", value)]


def _parse_snapshot_row(
    raw_row: dict[str, str],
    table: CatalogTable,
    config: ConnectorConfig,
) -> dict[str, Any]:
    parsed: dict[str, Any] = {}
    for column in table.columns:
        parsed[column.name] = coerce_value(
            raw_row.get(column.name),
            column.tidb_type,
            null_marker=config.csv_null_value,
            binary_encoding=config.binary_encoding,
        )
    return parsed


def _snapshot_row_header(first_row: list[str], expected_header: list[str]) -> tuple[list[str], bool]:
    if len(first_row) == len(expected_header) and set(first_row) == set(expected_header):
        return first_row, True
    return expected_header, False


def _snapshot_row_dict(row: list[str], header: list[str], file_key: str) -> dict[str, str]:
    if len(row) != len(header):
        raise SnapshotError(
            f"snapshot row width mismatch for {file_key}: "
            f"expected={len(header)} actual={len(row)} row={row}"
        )
    return dict(zip(header, row, strict=True))


def sync_snapshot(
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
    phase_rows = 0
    phase_files = 0
    if not store.prefix_exists(SNAPSHOT_PREFIX):
        raise SnapshotError(f"snapshot prefix does not exist: {SNAPSHOT_PREFIX}")

    snapshot_tso = snapshot_metadata_pos(store, SNAPSHOT_PREFIX)
    if set_snapshot_tso(state, snapshot_tso):
        phase_operations.checkpoint(state=state)
    policy = CheckpointPolicy(config.checkpoint_interval_rows, config.checkpoint_interval_seconds)
    info(
        "starting snapshot phase",
        phase="snapshot",
        snapshot_tso=snapshot_tso,
        snapshot_prefix=SNAPSHOT_PREFIX,
        table_count=len(catalog.tables),
    )

    try:
        for table in catalog.tables:
            table_start = timer_start()
            table_stats = OperationStats()
            table_operations = TimedOperationSink(operations, table_stats)
            table_rows = 0
            table_files = 0
            entry = snapshot_entry(state, table)
            if entry.get("done"):
                continue

            expected_header = table.column_names
            files = _snapshot_files(store, catalog, table)
            if not files:
                entry["done"] = True
                entry["file"] = ""
                entry["next_row"] = 0
                table_operations.checkpoint(state=state)
                phase_stats.merge(table_stats)
                policy.mark_checkpointed()
                info(
                    "snapshot table completed with no files",
                    phase="snapshot",
                    source_schema=table.source_schema,
                    source_table=table.source_table,
                    fivetran_table=table.fivetran_table,
                    duration_ms=elapsed_ms(table_start),
                    rows_processed=0,
                    files_processed=0,
                    **table_stats.as_log_fields(),
                )
                continue

            start_file = str(entry.get("file") or files[0])
            if start_file not in files:
                raise SnapshotError(
                    f"snapshot cursor file {start_file!r} is not present for {table.source_key}"
                )

            for file_key in files[files.index(start_file) :]:
                file_start = timer_start()
                file_stats = OperationStats()
                file_operations = TimedOperationSink(operations, file_stats)
                file_rows = 0
                skipped_resume_rows = 0
                next_row = int(entry.get("next_row") or 0) if file_key == start_file else 0
                with store.open_text(file_key) as handle:
                    reader = csv.reader(handle)
                    first_row = next(reader, None)
                    if first_row is None:
                        rows = iter(())
                    else:
                        header, has_header = _snapshot_row_header(first_row, expected_header)
                        rows = reader if has_header else chain([first_row], reader)
                    for row_offset, raw_values in enumerate(rows):
                        if row_offset < next_row:
                            skipped_resume_rows += 1
                            continue
                        raw_row = _snapshot_row_dict(raw_values, header, file_key)
                        data = _parse_snapshot_row(raw_row, table, config)
                        if config.enable_diagnostic_columns:
                            data["_ticdc_commit_ts"] = 0
                            data["_ticdc_source_schema"] = table.source_schema
                            data["_ticdc_source_table"] = table.source_table
                        file_operations.upsert(table=table.fivetran_table, data=data)
                        file_rows += 1
                        entry["file"] = file_key
                        entry["next_row"] = row_offset + 1
                        if policy.record_row():
                            file_operations.checkpoint(state=state)
                            policy.mark_checkpointed()

                entry["file"] = file_key
                entry["next_row"] = 0
                if file_key != files[-1]:
                    file_operations.checkpoint(state=state)
                    policy.mark_checkpointed()

                file_duration_ms = elapsed_ms(file_start)
                table_stats.merge(file_stats)
                table_rows += file_rows
                table_files += 1
                phase_rows += file_rows
                phase_files += 1
                info(
                    "snapshot file completed",
                    phase="snapshot",
                    source_schema=table.source_schema,
                    source_table=table.source_table,
                    fivetran_table=table.fivetran_table,
                    file_path=file_key,
                    duration_ms=file_duration_ms,
                    read_parse_duration_ms=round(
                        max(file_duration_ms - file_stats.total_duration_ms, 0), 3
                    ),
                    rows_processed=file_rows,
                    skipped_resume_rows=skipped_resume_rows,
                    **file_stats.as_log_fields(),
                )

            entry["done"] = True
            entry["file"] = files[-1]
            entry["next_row"] = 0
            table_operations.checkpoint(state=state)
            phase_stats.merge(table_stats)
            policy.mark_checkpointed()
            table_duration_ms = elapsed_ms(table_start)
            info(
                "snapshot table completed",
                phase="snapshot",
                source_schema=table.source_schema,
                source_table=table.source_table,
                fivetran_table=table.fivetran_table,
                file_path=files[-1] if files else "",
                duration_ms=table_duration_ms,
                read_parse_duration_ms=round(
                    max(table_duration_ms - table_stats.total_duration_ms, 0), 3
                ),
                rows_processed=table_rows,
                files_processed=table_files,
                **table_stats.as_log_fields(),
            )
    except Exception as exc:
        warning(
            "snapshot phase failed",
            phase="snapshot",
            duration_ms=elapsed_ms(phase_start),
            rows_processed=phase_rows,
            files_processed=phase_files,
            error_type=type(exc).__name__,
            error_message=str(exc),
            **phase_stats.as_log_fields(),
        )
        raise

    phase_duration_ms = elapsed_ms(phase_start)
    info(
        "snapshot phase completed",
        phase="snapshot",
        snapshot_tso=snapshot_tso,
        duration_ms=phase_duration_ms,
        read_parse_duration_ms=round(max(phase_duration_ms - phase_stats.total_duration_ms, 0), 3),
        rows_processed=phase_rows,
        files_processed=phase_files,
        table_count=len(catalog.tables),
        **phase_stats.as_log_fields(),
    )


__all__ = ["sync_snapshot"]
