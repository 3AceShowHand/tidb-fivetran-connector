from __future__ import annotations

from functools import lru_cache
from typing import Any

from config import ConnectorConfig
from fivetran.operations import FivetranOperationSink, OperationSink
from fivetran.schema import build_fivetran_schema
from metadata.catalog import Catalog, discover_catalog
from observability import info
from snapshot.reader import sync_snapshot
from state import normalize_state
from storage.object_store import ObjectStore
from storage.resolver import StorageURI, create_object_store, redact_storage_uri
from increment.reader import sync_incremental


@lru_cache(maxsize=8)
def _cached_store(storage_uri: str) -> ObjectStore:
    return create_object_store(StorageURI.parse(storage_uri))


def _load_inputs(configuration: dict[str, Any]) -> tuple[ConnectorConfig, ObjectStore, Catalog]:
    config = ConnectorConfig.from_dict(configuration)
    store = _cached_store(config.storage_uri)
    catalog = discover_catalog(store, config.table_filter)
    return config, store, catalog


def schema(configuration: dict[str, Any]) -> list[dict[str, object]]:
    config, _store, catalog = _load_inputs(configuration)
    info(
        "building schema",
        phase="schema",
        storage_uri=redact_storage_uri(config.storage_uri),
        table_count=len(catalog.tables),
    )
    return build_fivetran_schema(catalog, config)


def update(configuration: dict[str, Any], state: dict[str, Any] | str | None) -> None:
    from fivetran_connector_sdk import Operations as op

    _update(configuration, state, FivetranOperationSink(op))


def _update(
    configuration: dict[str, Any],
    state: dict[str, Any] | str | None,
    operations: OperationSink,
) -> None:
    config, store, catalog = _load_inputs(configuration)
    state_obj = normalize_state(state, catalog)
    info(
        "starting update",
        phase="update",
        storage_uri=redact_storage_uri(config.storage_uri),
    )

    sync_snapshot(
        config=config,
        store=store,
        catalog=catalog,
        state=state_obj,
        operations=operations,
    )

    sync_incremental(
        config=config,
        store=store,
        catalog=catalog,
        state=state_obj,
        operations=operations,
    )


__all__ = ["schema", "update"]
