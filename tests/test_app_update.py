from __future__ import annotations

from app import _update
from fivetran.operations import RecordingOperationSink

from conftest import table_definition, write_basic_snapshot, write_json, write_text, write_ticdc_schema


def test_update_runs_snapshot_then_incremental_without_manifest(workspace):
    write_basic_snapshot(workspace, pos=100)
    write_text(workspace / "snapshot/test.worker.000000001.csv", "id,name\n1,Alice\n")
    write_json(workspace / "increment/metadata", {"checkpoint-ts": 200})
    write_ticdc_schema(workspace, table_definition(version=80))
    write_text(workspace / "increment/test/worker/80/2026-05-25/meta/CDC.index", "CDC000001.csv\n")
    write_text(workspace / "increment/test/worker/80/2026-05-25/CDC000001.csv", "U,worker,test,121,1,Alicia\n")
    operations = RecordingOperationSink()

    _update({"storage_uri": f"file://{workspace}"}, {}, operations)

    upserts = [call for call in operations.calls if call[0] == "upsert"]
    assert upserts == [
        ("upsert", {"table": "test_worker", "data": {"id": 1, "name": "Alice"}}),
        ("upsert", {"table": "test_worker", "data": {"id": 1, "name": "Alicia"}}),
    ]
    checkpoints = [call for call in operations.calls if call[0] == "checkpoint"]
    assert checkpoints[-1][1]["state"]["snapshot"]["snapshot-tso"] == 100
    assert checkpoints[-1][1]["state"]["incremental"]["last_processed_commit_ts"] == 121
