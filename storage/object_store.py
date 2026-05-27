from __future__ import annotations

import io
import json
from pathlib import Path
from typing import Any, Protocol

from errors import ConfigurationError


S3_TIMEOUT_SECONDS = 60


def join_key(*parts: str | None) -> str:
    cleaned: list[str] = []
    for part in parts:
        if part is None or part == "":
            continue
        text = str(part).strip("/")
        if text:
            cleaned.append(text)
    return "/".join(cleaned)


class ObjectStore(Protocol):
    def read_text(self, key: str) -> str: ...

    def open_text(self, key: str) -> io.TextIOBase: ...

    def list_keys(self, prefix: str) -> list[str]: ...

    def list_common_prefixes(self, prefix: str) -> list[str]: ...

    def exists(self, key: str) -> bool: ...

    def prefix_exists(self, prefix: str) -> bool: ...

    def read_json(self, key: str) -> Any:
        return json.loads(self.read_text(key))


class LocalObjectStore(ObjectStore):
    def __init__(self, root: str, workspace_prefix: str = "") -> None:
        self.root = Path(root)
        self.workspace_prefix = ""

    def _path(self, key: str) -> Path:
        return self.root / self.workspace_prefix / key.strip("/")

    def read_text(self, key: str) -> str:
        return self._path(key).read_text(encoding="utf-8")

    def open_text(self, key: str) -> io.TextIOBase:
        return self._path(key).open("r", encoding="utf-8", newline="")

    def list_keys(self, prefix: str) -> list[str]:
        base = self._path(prefix)
        if not base.exists():
            return []
        if base.is_file():
            return [prefix.strip("/")]
        results: list[str] = []
        for path in base.rglob("*"):
            if path.is_file():
                rel = path.relative_to(self.root / self.workspace_prefix)
                results.append(rel.as_posix())
        return sorted(results)

    def list_common_prefixes(self, prefix: str) -> list[str]:
        base = self._path(prefix)
        if not base.exists() or not base.is_dir():
            return []
        prefix_text = prefix.strip("/")
        results: list[str] = []
        for path in base.iterdir():
            if path.is_dir():
                name = path.name
                results.append(f"{prefix_text}/{name}/" if prefix_text else f"{name}/")
        return sorted(results)

    def exists(self, key: str) -> bool:
        return self._path(key).is_file()

    def prefix_exists(self, prefix: str) -> bool:
        return self._path(prefix).exists()


class S3ObjectStore(ObjectStore):
    def __init__(
        self,
        *,
        bucket: str,
        prefix: str = "",
        endpoint_url: str | None = None,
        region: str | None = None,
        access_key_id: str | None = None,
        secret_access_key: str | None = None,
        session_token: str | None = None,
        force_path_style: bool = False,
    ) -> None:
        try:
            import boto3
        except ImportError as exc:  # pragma: no cover - dependency is declared.
            raise ConfigurationError("boto3 is required for S3 access") from exc

        session_kwargs: dict[str, str] = {}
        if access_key_id:
            session_kwargs["aws_access_key_id"] = access_key_id
        if secret_access_key:
            session_kwargs["aws_secret_access_key"] = secret_access_key
        if session_token:
            session_kwargs["aws_session_token"] = session_token
        if region:
            session_kwargs["region_name"] = region
        session = boto3.Session(**session_kwargs)

        client_kwargs: dict[str, Any] = {}
        if endpoint_url:
            client_kwargs["endpoint_url"] = endpoint_url
        from botocore.config import Config

        config_kwargs: dict[str, Any] = {
            "retries": {"max_attempts": 4, "mode": "standard"},
            "connect_timeout": S3_TIMEOUT_SECONDS,
            "read_timeout": S3_TIMEOUT_SECONDS,
        }
        if force_path_style:
            config_kwargs["s3"] = {"addressing_style": "path"}
        client_kwargs["config"] = Config(**config_kwargs)
        self.client = session.client("s3", **client_kwargs)
        self.bucket = bucket
        self.workspace_prefix = prefix.strip("/")

    def _object_key(self, key: str) -> str:
        key = key.strip("/")
        if not self.workspace_prefix:
            return key
        if key == self.workspace_prefix or key.startswith(self.workspace_prefix + "/"):
            return key
        return f"{self.workspace_prefix}/{key}" if key else self.workspace_prefix

    def _strip_workspace(self, key: str) -> str:
        if self.workspace_prefix and key.startswith(self.workspace_prefix + "/"):
            return key[len(self.workspace_prefix) + 1 :]
        if key == self.workspace_prefix:
            return ""
        return key

    def read_text(self, key: str) -> str:
        body = self.client.get_object(Bucket=self.bucket, Key=self._object_key(key))["Body"]
        return body.read().decode("utf-8")

    def open_text(self, key: str) -> io.TextIOBase:
        body = self.client.get_object(Bucket=self.bucket, Key=self._object_key(key))["Body"]
        return io.TextIOWrapper(body, encoding="utf-8", newline="")

    def list_keys(self, prefix: str) -> list[str]:
        prefix_key = self._object_key(prefix)
        paginator = self.client.get_paginator("list_objects_v2")
        results: list[str] = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix_key):
            for item in page.get("Contents", []):
                key = self._strip_workspace(item["Key"])
                if key:
                    results.append(key)
        return sorted(results)

    def list_common_prefixes(self, prefix: str) -> list[str]:
        prefix_key = self._object_key(prefix)
        if prefix_key and not prefix_key.endswith("/"):
            prefix_key += "/"
        paginator = self.client.get_paginator("list_objects_v2")
        results: list[str] = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix_key, Delimiter="/"):
            for item in page.get("CommonPrefixes", []):
                key = self._strip_workspace(item["Prefix"])
                if key:
                    results.append(key)
        return sorted(results)

    def exists(self, key: str) -> bool:
        from botocore.exceptions import ClientError

        try:
            self.client.head_object(Bucket=self.bucket, Key=self._object_key(key))
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code")
            if code in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise
        return True

    def prefix_exists(self, prefix: str) -> bool:
        response = self.client.list_objects_v2(
            Bucket=self.bucket, Prefix=self._object_key(prefix), MaxKeys=1
        )
        return bool(response.get("KeyCount"))


__all__ = ["LocalObjectStore", "ObjectStore", "S3ObjectStore", "join_key"]
