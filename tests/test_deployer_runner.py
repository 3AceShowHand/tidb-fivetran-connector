from __future__ import annotations

import json
from pathlib import Path

import pytest

from deployer.runner import DeployerError, FivetranApiClient, JobConfig, main, run_job


class FakeApiClient:
    def __init__(self, group_id: str = "group_123", connection_id: str = "conn_456"):
        self.group_id = group_id
        self.connection_id = connection_id

    def get_group_id_by_name(self, group_name: str) -> str:
        assert group_name == "tidb_snowflake"
        return self.group_id

    def get_connection_id_by_name(self, group_id: str, connection_name: str) -> str:
        assert group_id == self.group_id
        assert connection_name == "job_e2e"
        return self.connection_id


def test_job_config_reads_env(monkeypatch, tmp_path):
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        """
[project]
name = "ticdc-fivetran-connector"
version = "1.2.3"
""".strip()
    )
    monkeypatch.setenv("FIVETRAN_API_KEY_BASE64", "token")
    monkeypatch.setenv("DESTINATION_NAME", "tidb_snowflake")
    monkeypatch.setenv("CONNECTION_NAME", "job_e2e")
    monkeypatch.setenv("CONNECTOR_CONFIG_JSON", '{"storage_uri":"s3://bucket/prefix"}')
    monkeypatch.setenv("PROJECT_PATH", str(tmp_path))
    monkeypatch.setenv("CONNECTOR_VERSION", "1.2.3")

    config = JobConfig.from_env()

    assert config.destination_name == "tidb_snowflake"
    assert config.connection_name == "job_e2e"
    assert config.connector_version == "1.2.3"
    assert config.connector_config == {"storage_uri": "s3://bucket/prefix"}


def test_job_config_rejects_version_mismatch(monkeypatch, tmp_path):
    (tmp_path / "pyproject.toml").write_text(
        """
[project]
name = "ticdc-fivetran-connector"
version = "9.9.9"
""".strip()
    )
    monkeypatch.setenv("FIVETRAN_API_KEY_BASE64", "token")
    monkeypatch.setenv("DESTINATION_NAME", "tidb_snowflake")
    monkeypatch.setenv("CONNECTION_NAME", "job_e2e")
    monkeypatch.setenv("CONNECTOR_CONFIG_JSON", '{"storage_uri":"s3://bucket/prefix"}')
    monkeypatch.setenv("PROJECT_PATH", str(tmp_path))
    monkeypatch.setenv("CONNECTOR_VERSION", "1.2.3")

    with pytest.raises(DeployerError, match="does not match"):
        JobConfig.from_env()


def test_run_job_resolves_connection_id(monkeypatch, tmp_path):
    termination_log = tmp_path / "termination.log"
    config = JobConfig(
        api_key_b64="token",
        destination_name="tidb_snowflake",
        connection_name="job_e2e",
        connector_config={"storage_uri": "s3://bucket/prefix"},
        request_id="req-1",
        connector_name="tidb-fivetran-connector",
        connector_version="1.2.3",
        project_path=str(tmp_path),
        python_version="3.13",
        termination_log_path=str(termination_log),
        base_url="https://api.fivetran.com/v1",
    )
    monkeypatch.setattr("deployer.runner._run_fivetran_deploy", lambda _config, _config_path: None)
    monkeypatch.setattr("deployer.runner.FivetranApiClient", lambda *_args, **_kwargs: FakeApiClient())

    result = run_job(config)

    assert result.destination_id == "group_123"
    assert result.connection_id == "conn_456"
    assert result.dashboard_url == "https://fivetran.com/dashboard/connectors/conn_456/status"


def test_main_writes_failure_payload(monkeypatch, tmp_path):
    log_path = tmp_path / "termination.log"
    monkeypatch.delenv("FIVETRAN_API_KEY_BASE64", raising=False)
    monkeypatch.setenv("TERMINATION_LOG_PATH", str(log_path))

    assert main() == 1

    payload = json.loads(log_path.read_text())
    assert payload["status"] == "failed"
    assert payload["error_code"] == "missing_env"


def test_fivetran_api_client_matches_group_and_schema(monkeypatch):
    client = FivetranApiClient("token", "https://api.fivetran.com/v1")

    responses = {
        "/groups": {
            "code": "Success",
            "data": {"items": [{"id": "gid", "name": "tidb_snowflake"}]},
        },
        "/connections?group_id=gid": {
            "code": "Success",
            "data": {"items": [{"id": "cid", "schema": "job_e2e"}]},
        },
    }

    monkeypatch.setattr(client, "_get_json", lambda path: responses[path])

    group_id = client.get_group_id_by_name("tidb_snowflake")
    connection_id = client.get_connection_id_by_name(group_id, "job_e2e")

    assert group_id == "gid"
    assert connection_id == "cid"
