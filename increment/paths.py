from __future__ import annotations

import re
from dataclasses import dataclass

from config import ConnectorConfig
from errors import IncrementalError


SCHEMA_RE = re.compile(r"^schema_(?P<table_version>\d+)_(?P<checksum>\d+)\.json$")
INDEX_RE = re.compile(r"^CDC(?:_(?P<dispatcher_id>[^.]+))?\.index$")
DATA_RE = re.compile(r"^CDC(?P<file_index>\d+)\.csv$")
DISPATCHER_DATA_RE = re.compile(r"^CDC_(?P<dispatcher_id>.+)_(?P<file_index>\d+)\.csv$")
DATE_RE = re.compile(r"^\d{4}(?:-\d{2})?(?:-\d{2})?$")


@dataclass(frozen=True)
class SchemaPath:
    key: str
    source_schema: str
    source_table: str
    table_version: int
    checksum: int

    @property
    def source_key(self) -> str:
        return f"{self.source_schema}.{self.source_table}"


@dataclass(frozen=True)
class IndexPath:
    key: str
    source_schema: str
    source_table: str
    table_version: int
    partition_id: int
    date: str
    dispatcher_id: str
    data_dir: str
    latest_file_index: int
    file_width: int

    @property
    def source_key(self) -> str:
        return f"{self.source_schema}.{self.source_table}"

    @property
    def quoted_key(self) -> str:
        return f"`{self.source_schema}`.`{self.source_table}`"

    @property
    def cursor_key(self) -> str:
        return self.key

    @property
    def sort_key(self) -> tuple[int, int, str, str, str]:
        return (
            self.table_version,
            self.partition_id,
            self.date,
            self.source_schema,
            self.source_table,
        )

    def data_file_key(self, file_index: int) -> str:
        return f"{self.data_dir}/CDC{file_index:0{self.file_width}d}.csv"


def _strip_increment_prefix(key: str, increment_prefix: str) -> str:
    prefix = increment_prefix.strip("/")
    key = key.strip("/")
    if prefix and key.startswith(prefix + "/"):
        return key[len(prefix) + 1 :]
    return key


def parse_schema_path(key: str, increment_prefix: str) -> SchemaPath | None:
    relative = _strip_increment_prefix(key, increment_prefix)
    parts = relative.split("/")
    if len(parts) != 4 or parts[2] != "meta":
        return None
    match = SCHEMA_RE.match(parts[3])
    if not match:
        return None
    return SchemaPath(
        key=key,
        source_schema=parts[0],
        source_table=parts[1],
        table_version=int(match.group("table_version")),
        checksum=int(match.group("checksum")),
    )


def parse_index_content(content: str) -> tuple[int, int, str]:
    filename = content.strip().splitlines()[0].strip() if content.strip() else ""
    match = DATA_RE.match(filename)
    if match:
        return int(match.group("file_index")), len(match.group("file_index")), ""
    match = DISPATCHER_DATA_RE.match(filename)
    if match:
        return (
            int(match.group("file_index")),
            len(match.group("file_index")),
            match.group("dispatcher_id"),
        )
    raise IncrementalError(f"invalid TiCDC index content: {content!r}")


def _parse_date_and_partition(path_parts: list[str], config: ConnectorConfig) -> tuple[int, str]:
    if not path_parts:
        return 0, ""
    date = ""
    partition_id = 0
    if DATE_RE.match(path_parts[-1]) and (
        config.date_separator != "none" or "-" in path_parts[-1] or len(path_parts[-1]) == 4
    ):
        date = path_parts[-1]
        path_parts = path_parts[:-1]
    if path_parts:
        try:
            partition_id = int(path_parts[-1])
        except ValueError as exc:
            raise IncrementalError(f"invalid partition path segment: {path_parts[-1]!r}") from exc
    return partition_id, date


def parse_index_path(
    key: str, increment_prefix: str, content: str, config: ConnectorConfig
) -> IndexPath | None:
    relative = _strip_increment_prefix(key, increment_prefix)
    parts = relative.split("/")
    if len(parts) < 5 or parts[-2] != "meta":
        return None
    file_match = INDEX_RE.match(parts[-1])
    if not file_match:
        return None
    dispatcher_id = file_match.group("dispatcher_id") or ""
    latest_file_index, file_width, dispatcher_from_data = parse_index_content(content)
    if dispatcher_from_data and dispatcher_id and dispatcher_id != dispatcher_from_data:
        raise IncrementalError(f"dispatcher ID mismatch between index path and content: {key}")
    dispatcher_id = dispatcher_id or dispatcher_from_data
    if dispatcher_id:
        raise IncrementalError("v1 does not support table-across-nodes dispatcher index files")

    try:
        table_version = int(parts[2])
    except ValueError as exc:
        raise IncrementalError(f"invalid tableVersion in index path: {key}") from exc

    partition_id, date = _parse_date_and_partition(parts[3:-2], config)
    data_dir = "/".join(key.strip("/").split("/")[:-2])
    return IndexPath(
        key=key,
        source_schema=parts[0],
        source_table=parts[1],
        table_version=table_version,
        partition_id=partition_id,
        date=date,
        dispatcher_id=dispatcher_id,
        data_dir=data_dir,
        latest_file_index=latest_file_index,
        file_width=file_width,
    )


__all__ = [
    "IndexPath",
    "SchemaPath",
    "parse_index_content",
    "parse_index_path",
    "parse_schema_path",
]
