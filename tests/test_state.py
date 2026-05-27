from __future__ import annotations

import state as state_module
from metadata.catalog import discover_catalog
from state import CheckpointPolicy, normalize_state, set_snapshot_tso
from storage.object_store import LocalObjectStore
from conftest import write_basic_snapshot


def _catalog(workspace):
    write_basic_snapshot(workspace)
    store = LocalObjectStore(str(workspace))
    from config import ConnectorConfig

    config = ConnectorConfig.from_dict({"storage_uri": f"file://{workspace}"})
    return discover_catalog(store)


def test_checkpoint_policy_fires_on_elapsed_time(monkeypatch):
    timestamps = iter([100.0, 105.0])
    monkeypatch.setattr(state_module.time, "monotonic", lambda: next(timestamps))

    policy = CheckpointPolicy(rows=100, seconds=5)

    assert policy.record_row() is True


def test_set_snapshot_tso_is_stable(workspace):
    catalog = _catalog(workspace)
    state = normalize_state({}, catalog)

    assert set_snapshot_tso(state, 100) is True
    assert set_snapshot_tso(state, 100) is False
    assert state["snapshot"]["snapshot-tso"] == 100


def test_normalize_state_migrates_legacy_dml_cursor_to_stream(workspace):
    catalog = _catalog(workspace)

    state = normalize_state(
        {
            "version": 1,
            "snapshot": {"required": True, "tables": {}},
            "incremental": {
                "last_seen_checkpoint_ts": 200,
                "last_processed_commit_ts": 121,
                "commit_ts_by_table": {"test.worker": 121},
                "ddl_watermark": {},
                "dml": {
                    "increment/test/worker/120/2026-05-25/meta/CDC.index": {
                        "dispatcher_id": "",
                        "last_file_index": 3,
                        "current_file_index": 3,
                        "row": 0,
                        "last_commit_ts": 121,
                    }
                },
            },
        },
        catalog,
    )

    assert state["incremental"]["dml"] == {}
    assert state["incremental"]["streams"]["test/worker/120/0/"]["last_file_index"] == 3
    assert state["incremental"]["streams"]["test/worker/120/0/"]["last_date"] == "2026-05-25"


def test_normalize_state_compacts_old_streams_and_unmanaged_commit_ts(workspace):
    catalog = _catalog(workspace)

    state = normalize_state(
        {
            "version": 1,
            "snapshot": {"required": True, "tables": {}},
            "incremental": {
                "last_seen_checkpoint_ts": 200,
                "last_processed_commit_ts": 121,
                "commit_ts_by_table": {"test.worker": 121, "other.table": 9},
                "ddl_watermark": {"`test`.`worker`": 120},
                "streams": {
                    "test/worker/100/0/": {
                        "source_schema": "test",
                        "source_table": "worker",
                        "table_version": 100,
                        "partition_id": 0,
                        "dispatcher_id": "",
                        "last_date": "2026-05-24",
                        "last_file_index": 9,
                        "current_date": "",
                        "current_file_index": 9,
                        "row": 0,
                        "latest_index_file_index": 9,
                        "last_commit_ts": 100,
                    },
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
                    },
                },
                "dml": {},
            },
        },
        catalog,
    )

    assert "test/worker/100/0/" not in state["incremental"]["streams"]
    assert "test/worker/120/0/" in state["incremental"]["streams"]
    assert state["incremental"]["commit_ts_by_table"] == {"test.worker": 121}
