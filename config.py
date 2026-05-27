from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from errors import ConfigurationError


def _coerce_bool(value: Any, *, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    raise ConfigurationError(f"invalid boolean value: {value!r}")


def _coerce_int(value: Any, *, default: int, minimum: int = 1) -> int:
    if value is None or value == "":
        return default
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise ConfigurationError(f"invalid integer value: {value!r}") from exc
    if result < minimum:
        raise ConfigurationError(f"integer value must be >= {minimum}: {value!r}")
    return result


def _clean_prefix(value: str | None) -> str:
    if not value:
        return ""
    return str(value).strip().strip("/")


def _coerce_string_list(value: Any) -> tuple[str, ...]:
    if value is None or value == "":
        return ()
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return ()
        if text.startswith("["):
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError as exc:
                raise ConfigurationError("table_filter JSON string is invalid") from exc
            return _coerce_string_list(parsed)
        return (text,)
    if isinstance(value, list):
        result: list[str] = []
        for item in value:
            text = str(item).strip()
            if text:
                result.append(text)
        return tuple(result)
    raise ConfigurationError("table_filter must be a list or string")


@dataclass(frozen=True)
class ConnectorConfig:
    storage_uri: str
    enable_diagnostic_columns: bool = False
    checkpoint_interval_rows: int = 50000
    checkpoint_interval_seconds: int = 600
    table_filter: tuple[str, ...] = ()
    csv_null_value: str = r"\N"
    date_separator: str = "day"
    binary_encoding: str = "base64"
    ticdc_output_old_value: bool = False
    strict_unknown_files: bool = False

    @classmethod
    def from_dict(cls, configuration: dict[str, Any]) -> "ConnectorConfig":
        storage_uri = _resolve_storage_uri(configuration)
        if _coerce_bool(configuration.get("snapshot_required"), default=True) is False:
            raise ConfigurationError("snapshot_required=false migration mode is not implemented yet")
        if _coerce_bool(configuration.get("enable_table_across_nodes"), default=False):
            raise ConfigurationError("v1 does not support enable_table_across_nodes=true")

        date_separator = str(configuration.get("date_separator") or "day").lower()
        if date_separator not in {"none", "year", "month", "day"}:
            raise ConfigurationError("date_separator must be one of none/year/month/day")

        binary_encoding = str(configuration.get("binary_encoding") or "base64").lower()
        if binary_encoding not in {"base64", "hex", "raw"}:
            raise ConfigurationError("binary_encoding must be one of base64/hex/raw")

        return cls(
            storage_uri=storage_uri,
            enable_diagnostic_columns=_coerce_bool(
                configuration.get("enable_diagnostic_columns"), default=False
            ),
            checkpoint_interval_rows=_coerce_int(configuration.get("checkpoint_interval_rows"), default=50000),
            checkpoint_interval_seconds=_coerce_int(configuration.get("checkpoint_interval_seconds"), default=600),
            table_filter=_coerce_string_list(configuration.get("table_filter")),
            csv_null_value=str(configuration.get("csv_null_value") or r"\N"),
            date_separator=date_separator,
            binary_encoding=binary_encoding,
            ticdc_output_old_value=_coerce_bool(configuration.get("ticdc_output_old_value"), default=False),
            strict_unknown_files=_coerce_bool(configuration.get("strict_unknown_files"), default=False),
        )


def _resolve_storage_uri(cfg: dict[str, Any]) -> str:
    if cfg.get("storage_uri"):
        return _merge_storage_uri_params(str(cfg["storage_uri"]), cfg)
    if cfg.get("local_root"):
        return f"file://{cfg['local_root']}"
    if cfg.get("s3_bucket"):
        bucket = cfg["s3_bucket"]
        prefix = _clean_prefix(cfg.get("s3_prefix") or cfg.get("workspace_prefix"))
        uri = f"s3://{bucket}/{prefix}" if prefix else f"s3://{bucket}"
        params: list[tuple[str, str]] = []
        if cfg.get("s3_endpoint_url"):
            params.append(("endpoint", str(cfg["s3_endpoint_url"])))
        if cfg.get("aws_region"):
            params.append(("region", str(cfg["aws_region"])))
        if cfg.get("aws_access_key_id"):
            params.append(("access-key", str(cfg["aws_access_key_id"])))
        if cfg.get("aws_secret_access_key"):
            params.append(("secret-access-key", str(cfg["aws_secret_access_key"])))
        if cfg.get("aws_session_token"):
            params.append(("session-token", str(cfg["aws_session_token"])))
        force_path_style = cfg.get("s3_force_path_style") or cfg.get("force_path_style")
        if force_path_style not in (None, ""):
            params.append(("force-path-style", str(force_path_style)))
        if params:
            uri += "?" + urlencode(params)
        return uri
    raise ConfigurationError("storage_uri is required (or legacy local_root / s3_bucket)")


def _merge_storage_uri_params(raw: str, cfg: dict[str, Any]) -> str:
    parsed = urlsplit(raw)
    if parsed.scheme.lower() != "s3":
        return raw

    params = parse_qsl(parsed.query, keep_blank_values=True)
    present = {key for key, _value in params}
    changed = False

    def add_if_missing(uri_key: str, config_key: str) -> None:
        nonlocal changed
        value = cfg.get(config_key)
        if value not in (None, "") and uri_key not in present:
            params.append((uri_key, str(value)))
            present.add(uri_key)
            changed = True

    add_if_missing("endpoint", "s3_endpoint_url")
    add_if_missing("region", "aws_region")
    add_if_missing("access-key", "aws_access_key_id")
    add_if_missing("secret-access-key", "aws_secret_access_key")
    add_if_missing("session-token", "aws_session_token")

    force_path_style = cfg.get("s3_force_path_style") or cfg.get("force_path_style")
    if force_path_style not in (None, "") and "force-path-style" not in present:
        params.append(("force-path-style", str(force_path_style)))
        changed = True

    if not changed:
        return raw

    return urlunsplit(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            urlencode(params),
            parsed.fragment,
        )
    )


__all__ = ["ConnectorConfig"]
