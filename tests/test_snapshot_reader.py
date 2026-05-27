from __future__ import annotations

from config import ConnectorConfig
from fivetran.operations import RecordingOperationSink
from metadata.catalog import discover_catalog
from snapshot.reader import sync_snapshot
from state import normalize_state
from storage.object_store import LocalObjectStore

from conftest import write_basic_snapshot, write_text


def test_snapshot_upserts_rows_and_checkpoints(workspace):
    write_basic_snapshot(workspace, pos=100)
    write_text(workspace / "snapshot/test.worker.000000001.csv", "id,name\n1,Alice\n2,Bob\n")

    config = ConnectorConfig.from_dict(
        {
            "storage_uri": f"file://{workspace}",
            "checkpoint_interval_rows": "1",
            "enable_diagnostic_columns": True,
        }
    )
    store = LocalObjectStore(str(workspace))
    catalog = discover_catalog(store)
    state = normalize_state({}, catalog)
    operations = RecordingOperationSink()

    sync_snapshot(
        config=config,
        store=store,
        catalog=catalog,
        state=state,
        operations=operations,
    )

    upserts = [call for call in operations.calls if call[0] == "upsert"]
    assert upserts == [
        (
            "upsert",
            {
                "table": "test_worker",
                "data": {
                    "id": 1,
                    "name": "Alice",
                    "_ticdc_commit_ts": 0,
                    "_ticdc_source_schema": "test",
                    "_ticdc_source_table": "worker",
                },
            },
        ),
        (
            "upsert",
            {
                "table": "test_worker",
                "data": {
                    "id": 2,
                    "name": "Bob",
                    "_ticdc_commit_ts": 0,
                    "_ticdc_source_schema": "test",
                    "_ticdc_source_table": "worker",
                },
            },
        ),
    ]
    assert state["snapshot"]["snapshot-tso"] == 100
    assert state["snapshot"]["tables"]["test.worker"]["done"] is True
    assert any(call[0] == "checkpoint" for call in operations.calls)


def test_snapshot_files_use_natural_sort_and_header_name_matching(workspace):
    write_basic_snapshot(workspace)
    write_text(workspace / "snapshot/test.worker.10.csv", "name,id\nLater,10\n")
    write_text(workspace / "snapshot/test.worker.2.csv", "name,id\nEarlier,2\n")

    config = ConnectorConfig.from_dict({"storage_uri": f"file://{workspace}"})
    store = LocalObjectStore(str(workspace))
    catalog = discover_catalog(store)
    state = normalize_state({}, catalog)
    operations = RecordingOperationSink()

    sync_snapshot(
        config=config,
        store=store,
        catalog=catalog,
        state=state,
        operations=operations,
    )

    upserts = [call[1]["data"] for call in operations.calls if call[0] == "upsert"]
    assert upserts == [{"id": 2, "name": "Earlier"}, {"id": 10, "name": "Later"}]


def test_snapshot_file_without_header_uses_schema_column_order(workspace):
    write_basic_snapshot(workspace)
    write_text(workspace / "snapshot/test.worker.000000001.csv", "1,Alice\n2,Bob\n")

    config = ConnectorConfig.from_dict({"storage_uri": f"file://{workspace}"})
    store = LocalObjectStore(str(workspace))
    catalog = discover_catalog(store)
    state = normalize_state({}, catalog)
    operations = RecordingOperationSink()

    sync_snapshot(
        config=config,
        store=store,
        catalog=catalog,
        state=state,
        operations=operations,
    )

    upserts = [call[1]["data"] for call in operations.calls if call[0] == "upsert"]
    assert upserts == [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
