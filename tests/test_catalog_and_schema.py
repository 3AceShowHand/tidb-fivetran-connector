from __future__ import annotations

from config import ConnectorConfig
from fivetran.schema import build_fivetran_schema
from metadata.catalog import discover_catalog, parse_create_table_sql
from storage.object_store import LocalObjectStore

from conftest import write_basic_snapshot


def test_discovers_catalog_from_dumpling_schema_sql(workspace):
    write_basic_snapshot(workspace)
    store = LocalObjectStore(str(workspace))
    config = ConnectorConfig.from_dict(
        {"storage_uri": f"file://{workspace}", "enable_diagnostic_columns": True}
    )

    catalog = discover_catalog(store)
    result = build_fivetran_schema(catalog, config)

    assert [table.source_key for table in catalog.tables] == ["test.worker"]
    assert result == [
        {
            "table": "test_worker",
            "primary_key": ["id"],
            "columns": {
                "id": "LONG",
                "name": "STRING",
                "_ticdc_commit_ts": "LONG",
                "_ticdc_source_schema": "STRING",
                "_ticdc_source_table": "STRING",
            },
        }
    ]


def test_schema_parser_handles_tidb_clustered_primary_key_comment():
    table = parse_create_table_sql(
        """/*!40014 SET FOREIGN_KEY_CHECKS=0*/;
/*!40101 SET NAMES binary*/;
CREATE TABLE `bank0` (
  `id` bigint NOT NULL,
  `LAST_UDT_TMS` datetime(3) DEFAULT CURRENT_TIMESTAMP(3),
  `RSRV_GLS_ID` varchar(30) DEFAULT 'Z',
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
""",
        source_schema="test",
        source_table="bank0",
        snapshot_file_stem="test.bank0",
    )

    assert table.primary_key == ("id",)
    assert [(column.name, column.tidb_type, column.nullable) for column in table.columns] == [
        ("id", "BIGINT", False),
        ("LAST_UDT_TMS", "DATETIME(3)", True),
        ("RSRV_GLS_ID", "VARCHAR(30)", True),
    ]


def test_table_filter_applies_during_catalog_discovery(workspace):
    write_basic_snapshot(workspace)
    store = LocalObjectStore(str(workspace))
    config = ConnectorConfig.from_dict(
        {"storage_uri": f"file://{workspace}", "table_filter": ["test.*", "!test.worker"]}
    )

    try:
        discover_catalog(store, ("test.*", "!test.worker"))
    except Exception as exc:
        assert "no supported tables" in str(exc)
    else:  # pragma: no cover - assertion above should always trigger.
        raise AssertionError("catalog discovery should reject an empty filtered catalog")
