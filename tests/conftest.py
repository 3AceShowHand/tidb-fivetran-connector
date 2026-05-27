from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from increment.schema import compute_table_definition_checksum


@pytest.fixture
def workspace(tmp_path: Path) -> Path:
    return tmp_path


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def write_text(path: Path, payload: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def write_snapshot_metadata(root: Path, *, pos: int = 100) -> None:
    write_text(
        root / "snapshot/metadata",
        "\n".join(
            [
                "Started dump at: 2026-05-26 06:19:14",
                "SHOW MASTER STATUS:",
                "    Log: tidb-binlog",
                f"    Pos: {pos}",
                "    GTID:",
                "",
                "Finished dump at: 2026-05-26 06:19:36",
                "",
            ]
        ),
    )


def write_snapshot_schema_sql(root: Path, *, table: str = "worker") -> None:
    write_text(
        root / f"snapshot/test.{table}-schema.sql",
        f"""CREATE TABLE `{table}` (
  `id` bigint NOT NULL,
  `name` varchar(64) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
""",
    )


def write_basic_snapshot(root: Path, *, pos: int = 100) -> None:
    write_snapshot_metadata(root, pos=pos)
    write_snapshot_schema_sql(root)


def table_definition(*, version: int = 120, ddl_type: int = 0, query: str = "") -> dict[str, Any]:
    return {
        "Table": "worker",
        "Schema": "test",
        "Version": 0,
        "TableVersion": version,
        "Query": query,
        "Type": ddl_type,
        "TableColumns": [
            {"ColumnName": "id", "ColumnType": "BIGINT"},
            {"ColumnName": "name", "ColumnType": "VARCHAR"},
        ],
        "TableColumnsTotal": 2,
    }


def write_ticdc_schema(root: Path, payload: dict[str, Any]) -> Path:
    checksum = compute_table_definition_checksum(payload)
    path = root / f"increment/test/worker/meta/schema_{payload['TableVersion']}_{checksum}.json"
    write_json(path, payload)
    return path
