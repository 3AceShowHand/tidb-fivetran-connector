from __future__ import annotations

import fnmatch
import re
from dataclasses import dataclass
from typing import Iterable
from urllib.parse import unquote

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError

from errors import ManifestError
from observability import warning
from storage.object_store import ObjectStore, join_key


SNAPSHOT_PREFIX = "snapshot"
INCREMENT_PREFIX = "increment"


SNAPSHOT_SCHEMA_SUFFIX = "-schema.sql"
SNAPSHOT_SCHEMA_CREATE_SUFFIX = "-schema-create.sql"


@dataclass(frozen=True)
class ColumnDef:
    name: str
    tidb_type: str
    nullable: bool = True


@dataclass(frozen=True)
class CatalogTable:
    source_schema: str
    source_table: str
    fivetran_table: str
    primary_key: tuple[str, ...]
    columns: tuple[ColumnDef, ...]
    snapshot_file_stem: str

    @property
    def source_key(self) -> str:
        return f"{self.source_schema}.{self.source_table}"

    @property
    def path_key(self) -> str:
        return f"{self.source_schema}/{self.source_table}"

    @property
    def quoted_key(self) -> str:
        return f"`{self.source_schema}`.`{self.source_table}`"

    @property
    def column_names(self) -> list[str]:
        return [column.name for column in self.columns]

    def column(self, name: str) -> ColumnDef:
        for column in self.columns:
            if column.name == name:
                return column
        raise KeyError(name)


@dataclass(frozen=True)
class Catalog:
    tables: tuple[CatalogTable, ...]

    @property
    def table_by_source_key(self) -> dict[str, CatalogTable]:
        return {table.source_key: table for table in self.tables}

    @property
    def table_by_path_key(self) -> dict[str, CatalogTable]:
        return {table.path_key: table for table in self.tables}


def discover_catalog(store: ObjectStore, table_filter: tuple[str, ...] = ()) -> Catalog:
    tables = tuple(_discover_snapshot_tables(store, table_filter))
    if not tables:
        raise ManifestError("no supported tables discovered from snapshot schema files")
    return Catalog(tables=tables)


def table_matches_filter(source_schema: str, source_table: str, rules: Iterable[str]) -> bool:
    rules = [str(rule).strip() for rule in rules if str(rule).strip()]
    if not rules:
        return True

    source_key = f"{source_schema}.{source_table}"
    has_positive = any(not rule.startswith("!") for rule in rules)
    matched_positive = not has_positive
    for rule in rules:
        is_exclude = rule.startswith("!")
        pattern = rule[1:] if is_exclude else rule
        if fnmatch.fnmatchcase(source_key, pattern):
            if is_exclude:
                return False
            matched_positive = True
    return matched_positive


def fivetran_table_name(source_schema: str, source_table: str) -> str:
    return re.sub(r"[^0-9A-Za-z_]+", "_", f"{source_schema}_{source_table}").strip("_")


def _discover_snapshot_tables(store: ObjectStore, table_filter: tuple[str, ...] = ()) -> list[CatalogTable]:
    keys = store.list_keys(SNAPSHOT_PREFIX)
    tables: list[CatalogTable] = []
    seen: set[str] = set()
    for key in sorted(keys):
        stem = _snapshot_schema_stem(key, SNAPSHOT_PREFIX)
        if stem is None:
            continue
        parsed = _parse_snapshot_schema_stem(stem)
        if parsed is None:
            continue
        source_schema, source_table = parsed
        if not table_matches_filter(source_schema, source_table, table_filter):
            continue
        table = parse_create_table_sql(
            store.read_text(key),
            source_schema=source_schema,
            source_table=source_table,
            snapshot_file_stem=stem,
        )
        if not table.primary_key:
            warning(
                "skipping table without primary key",
                phase="schema",
                source_schema=source_schema,
                source_table=source_table,
                file_path=key,
            )
            continue
        if table.source_key in seen:
            raise ManifestError(f"duplicate snapshot schema for table: {table.source_key}")
        seen.add(table.source_key)
        tables.append(table)
    return tables


def _snapshot_schema_stem(key: str, snapshot_prefix: str) -> str | None:
    relative = key.strip("/")
    prefix = snapshot_prefix.strip("/")
    if prefix and relative.startswith(prefix + "/"):
        relative = relative[len(prefix) + 1 :]
    if "/" in relative:
        return None
    if not relative.endswith(SNAPSHOT_SCHEMA_SUFFIX):
        return None
    if relative.endswith(SNAPSHOT_SCHEMA_CREATE_SUFFIX):
        return None
    return relative[: -len(SNAPSHOT_SCHEMA_SUFFIX)]


def _parse_snapshot_schema_stem(stem: str) -> tuple[str, str] | None:
    if "." not in stem:
        return None
    schema, table = stem.split(".", 1)
    return unquote(schema), unquote(table)


def parse_create_table_sql(
    sql: str,
    *,
    source_schema: str,
    source_table: str,
    snapshot_file_stem: str | None = None,
) -> CatalogTable:
    create = _parse_create_table(sql)
    schema = create.this
    if not isinstance(schema, exp.Schema):
        raise ManifestError("CREATE TABLE does not contain a column schema")
    table_name = schema.this.name if schema.this is not None else ""
    if table_name and table_name != source_table:
        raise ManifestError(
            f"CREATE TABLE name {table_name!r} does not match snapshot file table {source_table!r}"
        )

    columns: list[ColumnDef] = []
    primary_key: list[str] = []

    for item in schema.expressions:
        if isinstance(item, exp.ColumnDef):
            column = _column_from_sqlglot(item)
            columns.append(column)
            if _column_has_constraint(item, exp.PrimaryKeyColumnConstraint):
                primary_key.append(column.name)
            continue
        if isinstance(item, exp.PrimaryKey):
            primary_key = [identifier.name for identifier in item.expressions]

    if not columns:
        raise ManifestError(f"CREATE TABLE for {source_schema}.{source_table} has no columns")
    column_names = {column.name for column in columns}
    missing_pk = [name for name in primary_key if name not in column_names]
    if missing_pk:
        raise ManifestError(f"primary key columns missing from schema: {missing_pk}")

    return CatalogTable(
        source_schema=source_schema,
        source_table=source_table,
        fivetran_table=fivetran_table_name(source_schema, source_table),
        primary_key=tuple(primary_key),
        columns=tuple(columns),
        snapshot_file_stem=snapshot_file_stem or f"{source_schema}.{source_table}",
    )


def _parse_create_table(sql: str) -> exp.Create:
    try:
        expressions = sqlglot.parse(sql, read="mysql")
    except ParseError as exc:
        raise ManifestError(f"failed to parse snapshot schema SQL: {exc}") from exc
    for expression in expressions:
        if isinstance(expression, exp.Create) and str(expression.args.get("kind")).upper() == "TABLE":
            return expression
    raise ManifestError("snapshot schema file does not contain CREATE TABLE")


def _column_from_sqlglot(column: exp.ColumnDef) -> ColumnDef:
    name = column.this.name
    kind = column.args.get("kind")
    if not isinstance(kind, exp.DataType):
        raise ManifestError(f"column {name!r} is missing a type")
    return ColumnDef(
        name=name,
        tidb_type=kind.sql(dialect="mysql"),
        nullable=not _column_has_constraint(column, exp.NotNullColumnConstraint),
    )


def _column_has_constraint(
    column: exp.ColumnDef,
    constraint_type: type[exp.Expression],
) -> bool:
    for constraint in column.args.get("constraints") or []:
        if isinstance(constraint, exp.ColumnConstraint) and isinstance(
            constraint.args.get("kind"), constraint_type
        ):
            return True
    return False


def snapshot_metadata_pos(store: ObjectStore, snapshot_prefix: str) -> int:
    key = join_key(snapshot_prefix, "metadata")
    raw = store.read_text(key)
    match = re.search(r"^\s*Pos:\s*(\d+)\s*$", raw, flags=re.MULTILINE)
    if match is None:
        raise ManifestError(f"snapshot metadata is missing Pos: {key}")
    return int(match.group(1))


__all__ = [
    "Catalog",
    "CatalogTable",
    "ColumnDef",
    "discover_catalog",
    "fivetran_table_name",
    "parse_create_table_sql",
    "snapshot_metadata_pos",
    "table_matches_filter",
]
