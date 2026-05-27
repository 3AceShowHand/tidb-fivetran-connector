from __future__ import annotations

import pytest

from config import ConnectorConfig
from errors import IncrementalError
from fivetran.operations import RecordingOperationSink
from metadata.catalog import discover_catalog
from state import normalize_state
from storage.object_store import LocalObjectStore
from increment.reader import sync_incremental

from conftest import (
    table_definition,
    write_basic_snapshot,
    write_json,
    write_text,
    write_ticdc_schema,
)


class CountingLocalObjectStore(LocalObjectStore):
    def __init__(self, root: str) -> None:
        super().__init__(root)
        self.list_key_prefixes: list[str] = []
        self.common_prefixes: list[str] = []

    def list_keys(self, prefix: str) -> list[str]:
        self.list_key_prefixes.append(prefix)
        return super().list_keys(prefix)

    def list_common_prefixes(self, prefix: str) -> list[str]:
        self.common_prefixes.append(prefix)
        return super().list_common_prefixes(prefix)


def _load_context(workspace, *, diagnostic: bool = True):
    write_basic_snapshot(workspace, pos=100)
    config = ConnectorConfig.from_dict(
        {"storage_uri": f"file://{workspace}", "enable_diagnostic_columns": diagnostic}
    )
    store = LocalObjectStore(str(workspace))
    catalog = discover_catalog(store)
    state = normalize_state({}, catalog)
    state["snapshot"]["snapshot-tso"] = 100
    state["snapshot"]["tables"]["test.worker"]["done"] = True
    return config, store, catalog, state


def test_incremental_consumes_metadata_index_and_cdc_file(workspace):
    write_json(workspace / "increment/metadata", {"checkpoint-ts": 200})
    write_ticdc_schema(workspace, table_definition(version=120))
    write_text(workspace / "increment/test/worker/120/2026-05-25/meta/CDC.index", "CDC000002.csv\n")
    write_text(workspace / "increment/test/worker/120/2026-05-25/CDC000001.csv", "I,worker,test,121,1,Alice\n")
    write_text(workspace / "increment/test/worker/120/2026-05-25/CDC000002.csv", "U,worker,test,122,1,Alicia\nD,worker,test,123,1,Alicia\n")
    config, store, catalog, state = _load_context(workspace)
    operations = RecordingOperationSink()

    sync_incremental(
        config=config,
        store=store,
        catalog=catalog,
        state=state,
        operations=operations,
    )

    assert [call[0] for call in operations.calls if call[0] != "checkpoint"] == [
        "upsert",
        "upsert",
        "delete",
    ]
    assert next(call for call in operations.calls if call[0] == "upsert") == (
        "upsert",
        {
            "table": "test_worker",
            "data": {
                "id": 1,
                "name": "Alice",
                "_ticdc_commit_ts": 121,
                "_ticdc_source_schema": "test",
                "_ticdc_source_table": "worker",
            },
        },
    )
    cursor = state["incremental"]["streams"]["test/worker/120/0/"]
    assert cursor["last_file_index"] == 2
    assert cursor["last_date"] == "2026-05-25"
    assert cursor["row"] == 0
    assert state["incremental"]["dml"] == {}


def test_incremental_filters_lower_bound_and_stops_at_checkpoint(workspace):
    write_json(workspace / "increment/metadata", {"checkpoint-ts": 122})
    write_ticdc_schema(workspace, table_definition(version=80))
    write_text(workspace / "increment/test/worker/80/2026-05-25/meta/CDC.index", "CDC000001.csv\n")
    write_text(
        workspace / "increment/test/worker/80/2026-05-25/CDC000001.csv",
        "I,worker,test,100,1,Old\nI,worker,test,121,1,Alice\nU,worker,test,123,1,Future\n",
    )
    config, store, catalog, state = _load_context(workspace, diagnostic=False)
    operations = RecordingOperationSink()

    sync_incremental(
        config=config,
        store=store,
        catalog=catalog,
        state=state,
        operations=operations,
    )

    assert [call for call in operations.calls if call[0] == "upsert"] == [
        ("upsert", {"table": "test_worker", "data": {"id": 1, "name": "Alice"}})
    ]
    cursor = state["incremental"]["streams"]["test/worker/80/0/"]
    assert cursor["current_file_index"] == 1
    assert cursor["row"] == 2
    assert cursor["last_file_index"] == 0


def test_incremental_rejects_commit_ts_fallback(workspace):
    write_json(workspace / "increment/metadata", {"checkpoint-ts": 200})
    write_ticdc_schema(workspace, table_definition(version=120))
    write_text(workspace / "increment/test/worker/120/2026-05-25/meta/CDC.index", "CDC000001.csv\n")
    write_text(workspace / "increment/test/worker/120/2026-05-25/CDC000001.csv", "I,worker,test,122,1,Alice\nU,worker,test,121,1,Alicia\n")
    config, store, catalog, state = _load_context(workspace)

    with pytest.raises(IncrementalError, match="commit-ts fallback"):
        sync_incremental(
            config=config,
            store=store,
            catalog=catalog,
            state=state,
            operations=RecordingOperationSink(),
        )


def test_truncate_schema_marker_emits_truncate_after_snapshot_baseline(workspace):
    write_json(workspace / "increment/metadata", {"checkpoint-ts": 200})
    write_ticdc_schema(
        workspace,
        table_definition(version=120, ddl_type=11, query="truncate table `test`.`worker`"),
    )
    config, store, catalog, state = _load_context(workspace)
    operations = RecordingOperationSink()

    sync_incremental(
        config=config,
        store=store,
        catalog=catalog,
        state=state,
        operations=operations,
    )

    assert ("truncate", {"table": "test_worker"}) in operations.calls
    assert state["incremental"]["ddl_watermark"]["`test`.`worker`"] == 120


def test_schema_marker_at_snapshot_baseline_does_not_emit_truncate(workspace):
    write_json(workspace / "increment/metadata", {"checkpoint-ts": 200})
    write_ticdc_schema(
        workspace,
        table_definition(version=80, ddl_type=11, query="truncate table `test`.`worker`"),
    )
    config, store, catalog, state = _load_context(workspace)
    operations = RecordingOperationSink()

    sync_incremental(
        config=config,
        store=store,
        catalog=catalog,
        state=state,
        operations=operations,
    )

    assert ("truncate", {"table": "test_worker"}) not in operations.calls


def test_incremental_wraps_missing_dml_file(workspace):
    write_json(workspace / "increment/metadata", {"checkpoint-ts": 200})
    write_ticdc_schema(workspace, table_definition(version=120))
    write_text(workspace / "increment/test/worker/120/2026-05-25/meta/CDC.index", "CDC000001.csv\n")
    config, store, catalog, state = _load_context(workspace)

    with pytest.raises(IncrementalError, match="DML file referenced by index does not exist"):
        sync_incremental(
            config=config,
            store=store,
            catalog=catalog,
            state=state,
            operations=RecordingOperationSink(),
        )


def test_incremental_scan_is_scoped_to_catalog_table_prefix(workspace):
    write_json(workspace / "increment/metadata", {"checkpoint-ts": 200})
    write_ticdc_schema(workspace, table_definition(version=120))
    write_text(workspace / "increment/test/worker/120/2026-05-25/meta/CDC.index", "CDC000001.csv\n")
    write_text(workspace / "increment/test/worker/120/2026-05-25/CDC000001.csv", "I,worker,test,121,1,Alice\n")
    write_basic_snapshot(workspace)
    config = ConnectorConfig.from_dict({"storage_uri": f"file://{workspace}"})
    store = CountingLocalObjectStore(str(workspace))
    catalog = discover_catalog(store)
    state = normalize_state({}, catalog)
    state["snapshot"]["snapshot-tso"] = 100
    state["snapshot"]["tables"]["test.worker"]["done"] = True

    sync_incremental(
        config=config,
        store=store,
        catalog=catalog,
        state=state,
        operations=RecordingOperationSink(),
    )

    assert "increment/" not in store.list_key_prefixes
    assert "increment/test/worker/meta" in store.list_key_prefixes
    assert "increment/test/worker/120" in store.common_prefixes


def test_incremental_fast_path_skips_listing_when_checkpoint_unchanged(workspace):
    write_json(workspace / "increment/metadata", {"checkpoint-ts": 200})
    write_basic_snapshot(workspace)
    config = ConnectorConfig.from_dict({"storage_uri": f"file://{workspace}"})
    store = CountingLocalObjectStore(str(workspace))
    catalog = discover_catalog(store)
    store.list_key_prefixes.clear()
    store.common_prefixes.clear()
    state = normalize_state(
        {
            "version": 1,
            "snapshot": {
                "required": True,
                "snapshot-tso": 100,
                "tables": {"test.worker": {"done": True, "file": "", "next_row": 0}},
            },
            "incremental": {
                "last_seen_checkpoint_ts": 200,
                "last_processed_commit_ts": 121,
                "commit_ts_by_table": {"test.worker": 121},
                "ddl_watermark": {"`test`.`worker`": 120},
                "streams": {
                    "test/worker/120/0/": {
                        "source_schema": "test",
                        "source_table": "worker",
                        "table_version": 120,
                        "partition_id": 0,
                        "dispatcher_id": "",
                        "last_date": "2026-05-25",
                        "last_file_index": 1,
                        "current_date": "",
                        "current_file_index": 1,
                        "row": 0,
                        "latest_index_file_index": 1,
                        "last_commit_ts": 121,
                    }
                },
                "dml": {},
            },
        },
        catalog,
    )

    sync_incremental(
        config=config,
        store=store,
        catalog=catalog,
        state=state,
        operations=RecordingOperationSink(),
    )

    assert store.list_key_prefixes == []
    assert store.common_prefixes == []
