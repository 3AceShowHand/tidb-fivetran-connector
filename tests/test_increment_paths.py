from __future__ import annotations

import pytest

from config import ConnectorConfig
from errors import IncrementalError
from increment.paths import parse_index_path


def test_rejects_dispatcher_index_when_table_across_nodes_disabled():
    config = ConnectorConfig.from_dict({"storage_uri": "file:///tmp"})
    with pytest.raises(IncrementalError, match="table-across-nodes"):
        parse_index_path(
            "increment/test/worker/120/2026-05-25/meta/CDC_dispatcher01.index",
            "increment/",
            "CDC_dispatcher01_000001.csv\n",
            config,
        )
