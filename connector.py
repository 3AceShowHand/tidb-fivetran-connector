from __future__ import annotations

from fivetran_connector_sdk import Connector

from app import schema, update


connector = Connector(update=update, schema=schema)
