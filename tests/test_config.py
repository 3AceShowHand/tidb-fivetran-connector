from __future__ import annotations

import pytest

from config import ConnectorConfig
from errors import ConfigurationError
from storage.object_store import S3ObjectStore
from storage.resolver import StorageURI, redact_storage_uri


def test_storage_uri_file_scheme():
    config = ConnectorConfig.from_dict({"storage_uri": "file:///tmp/workspace"})
    assert config.storage_uri == "file:///tmp/workspace"


def test_storage_uri_s3_scheme_with_params():
    config = ConnectorConfig.from_dict(
        {"storage_uri": "s3://my-bucket/prefix?region=us-east-1&endpoint=http://minio:9000"}
    )
    assert config.storage_uri == "s3://my-bucket/prefix?region=us-east-1&endpoint=http://minio:9000"


def test_s3_credentials_from_configuration_are_encoded_into_storage_uri():
    config = ConnectorConfig.from_dict(
        {
            "s3_bucket": "my-bucket",
            "s3_prefix": "task1",
            "aws_region": "us-east-1",
            "aws_access_key_id": "AKIA_TEST",
            "aws_secret_access_key": "secret+with&reserved=",
            "aws_session_token": "token/with+reserved=",
        }
    )

    uri = StorageURI.parse(config.storage_uri)

    assert uri.bucket == "my-bucket"
    assert uri.prefix == "task1"
    assert uri.region == "us-east-1"
    assert uri.access_key == "AKIA_TEST"
    assert uri.secret_access_key == "secret+with&reserved="
    assert uri.session_token == "token/with+reserved="


def test_storage_uri_merges_separate_s3_credentials():
    config = ConnectorConfig.from_dict(
        {
            "storage_uri": "s3://my-bucket/task1?region=us-east-1",
            "aws_access_key_id": "AKIA_TEST",
            "aws_secret_access_key": "secret",
            "aws_session_token": "token",
        }
    )

    uri = StorageURI.parse(config.storage_uri)

    assert uri.region == "us-east-1"
    assert uri.access_key == "AKIA_TEST"
    assert uri.secret_access_key == "secret"
    assert uri.session_token == "token"


def test_redact_storage_uri_hides_s3_credentials():
    redacted = redact_storage_uri(
        "s3://my-bucket/task1?region=us-east-1&access-key=AKIA_TEST"
        "&secret-access-key=secret&session-token=token"
    )

    assert "AKIA_TEST" not in redacted
    assert "secret-access-key=secret" not in redacted
    assert "session-token=token" not in redacted
    assert "access-key=%2A%2A%2A" in redacted
    assert "region=us-east-1" in redacted


def test_s3_object_store_passes_explicit_credentials(monkeypatch):
    import boto3

    captured: dict[str, object] = {}

    class FakeSession:
        def __init__(self, **kwargs):
            captured["session_kwargs"] = kwargs

        def client(self, service_name, **kwargs):
            captured["service_name"] = service_name
            captured["client_kwargs"] = kwargs
            return object()

    monkeypatch.setattr(boto3, "Session", FakeSession)

    S3ObjectStore(
        bucket="my-bucket",
        region="us-east-1",
        access_key_id="AKIA_TEST",
        secret_access_key="secret",
        session_token="token",
        force_path_style=True,
    )

    assert captured["service_name"] == "s3"
    assert captured["session_kwargs"] == {
        "aws_access_key_id": "AKIA_TEST",
        "aws_secret_access_key": "secret",
        "aws_session_token": "token",
        "region_name": "us-east-1",
    }
    assert captured["client_kwargs"]["config"].s3 == {"addressing_style": "path"}


def test_storage_uri_is_required():
    with pytest.raises(ConfigurationError, match="storage_uri"):
        ConnectorConfig.from_dict({})


def test_snapshot_required_false_is_not_implemented_yet():
    with pytest.raises(ConfigurationError, match="snapshot_required=false"):
        ConnectorConfig.from_dict(
            {"storage_uri": "file:///tmp/workspace", "snapshot_required": False}
        )


def test_enable_diagnostic_columns():
    config = ConnectorConfig.from_dict(
        {"storage_uri": "file:///tmp/workspace", "enable_diagnostic_columns": True}
    )

    assert config.enable_diagnostic_columns is True
