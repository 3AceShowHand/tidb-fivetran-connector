from __future__ import annotations

from config import ConnectorConfig
from metadata.catalog import Catalog
from type_mapping import map_tidb_type_to_fivetran


def build_fivetran_schema(catalog: Catalog, config: ConnectorConfig) -> list[dict[str, object]]:
    result: list[dict[str, object]] = []
    for table in catalog.tables:
        columns = {
            column.name: map_tidb_type_to_fivetran(column.tidb_type)
            for column in table.columns
        }
        if config.enable_diagnostic_columns:
            columns.update(
                {
                    "_ticdc_commit_ts": "LONG",
                    "_ticdc_source_schema": "STRING",
                    "_ticdc_source_table": "STRING",
                }
            )
        result.append(
            {
                "table": table.fivetran_table,
                "primary_key": list(table.primary_key),
                "columns": columns,
            }
        )
    return result


__all__ = ["build_fivetran_schema"]
