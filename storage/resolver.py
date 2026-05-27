"""Unified storage backend via URI (TiCDC-compatible format).

Examples:
    file:///tmp/workspace
    s3://my-bucket/task1?region=us-east-1
    s3://my-bucket/task1?endpoint=http://minio:9000&access-key=...&secret-access-key=...&region=us-east-1&force-path-style=true
"""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from errors import ConfigurationError
from .object_store import LocalObjectStore, ObjectStore, S3ObjectStore


@dataclass(frozen=True)
class StorageURI:
    scheme: str
    bucket: str
    prefix: str
    endpoint: str | None = None
    region: str | None = None
    access_key: str | None = None
    secret_access_key: str | None = None
    session_token: str | None = None
    force_path_style: bool = False

    @classmethod
    def parse(cls, raw: str) -> "StorageURI":
        if not raw or not raw.strip():
            raise ConfigurationError("storage_uri is required")
        parsed = urlparse(raw.strip())
        scheme = parsed.scheme.lower()
        if scheme not in ("file", "s3"):
            raise ConfigurationError(
                f"unsupported storage_uri scheme {scheme!r}; expected file: or s3:"
            )

        qs: dict[str, str] = {}
        for k, v in parse_qs(parsed.query).items():
            if v:
                qs[k] = v[0]

        if scheme == "file":
            return cls(
                scheme="file",
                bucket="",
                prefix=parsed.path or "",
            )

        bucket = parsed.hostname or ""
        if not bucket:
            raise ConfigurationError("storage_uri must include a bucket name")
        prefix = (parsed.path or "").lstrip("/")

        return cls(
            scheme="s3",
            bucket=bucket,
            prefix=prefix,
            endpoint=qs.get("endpoint"),
            region=qs.get("region"),
            access_key=qs.get("access-key"),
            secret_access_key=qs.get("secret-access-key"),
            session_token=qs.get("session-token"),
            force_path_style=qs.get("force-path-style", "").lower() in ("true", "1"),
        )

    @property
    def workspace_path(self) -> str:
        if self.scheme == "file":
            return self.prefix
        if self.prefix:
            return f"{self.bucket}/{self.prefix}"
        return self.bucket


def create_object_store(uri: StorageURI) -> ObjectStore:
    if uri.scheme == "file":
        return LocalObjectStore(root=uri.prefix)

    return S3ObjectStore(
        bucket=uri.bucket,
        prefix=uri.prefix,
        endpoint_url=uri.endpoint,
        region=uri.region,
        access_key_id=uri.access_key,
        secret_access_key=uri.secret_access_key,
        session_token=uri.session_token,
        force_path_style=uri.force_path_style,
    )


def redact_storage_uri(raw: str) -> str:
    parsed = urlparse(raw)
    if not parsed.query:
        return raw
    sensitive_keys = {"access-key", "secret-access-key", "session-token"}
    params = []
    for key, values in parse_qs(parsed.query, keep_blank_values=True).items():
        value = "***" if key in sensitive_keys else values[0] if values else ""
        params.append((key, value))
    return urlunparse(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            parsed.params,
            urlencode(params),
            parsed.fragment,
        )
    )


__all__ = ["StorageURI", "create_object_store", "redact_storage_uri"]
