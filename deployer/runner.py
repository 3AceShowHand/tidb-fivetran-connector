from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import subprocess
import tempfile
import tomllib
from typing import Any
from urllib import error, parse, request


class DeployerError(Exception):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass(frozen=True)
class JobConfig:
    api_key_b64: str
    destination_name: str
    connection_name: str
    connector_config: dict[str, Any]
    request_id: str | None
    connector_name: str
    connector_version: str
    project_path: str
    python_version: str
    termination_log_path: str
    base_url: str

    @classmethod
    def from_env(cls) -> "JobConfig":
        api_key_b64 = _require_env("FIVETRAN_API_KEY_BASE64")
        destination_name = _require_env("DESTINATION_NAME")
        connection_name = _require_env("CONNECTION_NAME")
        connector_config = _load_connector_config()
        project_path = os.environ.get("PROJECT_PATH", os.getcwd())
        connector_version = os.environ.get("CONNECTOR_VERSION") or _read_project_version(project_path)
        expected_version = _read_project_version(project_path)
        if connector_version != expected_version:
            raise DeployerError(
                "connector_version_mismatch",
                f"CONNECTOR_VERSION={connector_version} does not match project version {expected_version}",
            )
        return cls(
            api_key_b64=api_key_b64,
            destination_name=destination_name,
            connection_name=connection_name,
            connector_config=connector_config,
            request_id=os.environ.get("REQUEST_ID"),
            connector_name=os.environ.get("CONNECTOR_NAME", "tidb-fivetran-connector"),
            connector_version=connector_version,
            project_path=project_path,
            python_version=os.environ.get("PYTHON_VERSION", "3.13"),
            termination_log_path=os.environ.get("TERMINATION_LOG_PATH", "/dev/termination-log"),
            base_url=os.environ.get("FIVETRAN_BASE_URL", "https://api.fivetran.com/v1"),
        )


@dataclass(frozen=True)
class DeploymentResult:
    status: str
    request_id: str | None
    connector_name: str
    connector_version: str
    destination_name: str
    destination_id: str
    connection_name: str
    connection_id: str
    dashboard_url: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "request_id": self.request_id,
            "connector_name": self.connector_name,
            "connector_version": self.connector_version,
            "destination_name": self.destination_name,
            "destination_id": self.destination_id,
            "connection_name": self.connection_name,
            "connection_id": self.connection_id,
            "dashboard_url": self.dashboard_url,
        }


class FivetranApiClient:
    def __init__(self, api_key_b64: str, base_url: str):
        self._api_key_b64 = api_key_b64
        self._base_url = base_url.rstrip("/")

    def get_group_id_by_name(self, group_name: str) -> str:
        payload = self._get_json("/groups")
        items = payload["data"]["items"]
        matches = [item for item in items if item.get("name") == group_name]
        if not matches:
            raise DeployerError("destination_not_found", f"destination {group_name} not found")
        if len(matches) > 1:
            raise DeployerError("destination_ambiguous", f"destination {group_name} resolved to multiple groups")
        return str(matches[0]["id"])

    def get_connection_id_by_name(self, group_id: str, connection_name: str) -> str:
        query = parse.urlencode({"group_id": group_id})
        payload = self._get_json(f"/connections?{query}")
        items = payload["data"]["items"]
        matches = [item for item in items if item.get("schema") == connection_name]
        if not matches:
            raise DeployerError(
                "connection_not_found",
                f"connection {connection_name} not found in destination group {group_id}",
            )
        if len(matches) > 1:
            raise DeployerError(
                "connection_ambiguous",
                f"connection {connection_name} resolved to multiple IDs in destination group {group_id}",
            )
        return str(matches[0]["id"])

    def _get_json(self, path: str) -> dict[str, Any]:
        req = request.Request(
            f"{self._base_url}{path}",
            headers={
                "Authorization": f"Basic {self._api_key_b64}",
                "Accept": "application/json",
            },
            method="GET",
        )
        try:
            with request.urlopen(req) as resp:
                payload = json.load(resp)
        except error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise DeployerError("fivetran_http_error", f"Fivetran API HTTP {exc.code}: {body}") from exc
        except error.URLError as exc:
            raise DeployerError("fivetran_network_error", f"failed to call Fivetran API: {exc.reason}") from exc

        if payload.get("code") != "Success":
            raise DeployerError(
                "fivetran_api_error",
                f"Fivetran API returned {payload.get('code')}: {payload.get('message')}",
            )
        return payload


def main() -> int:
    try:
        config = JobConfig.from_env()
        result = run_job(config)
    except (DeployerError, json.JSONDecodeError, OSError, tomllib.TOMLDecodeError, ValueError) as exc:
        if isinstance(exc, DeployerError):
            payload = {
                "status": "failed",
                "request_id": os.environ.get("REQUEST_ID"),
                "error_code": exc.code,
                "message": exc.message,
            }
        else:
            payload = {
                "status": "failed",
                "request_id": os.environ.get("REQUEST_ID"),
                "error_code": "runtime_error",
                "message": str(exc),
            }
        _emit_result(payload, os.environ.get("TERMINATION_LOG_PATH", "/dev/termination-log"))
        return 1

    _emit_result(result.to_dict(), config.termination_log_path)
    return 0


def run_job(config: JobConfig) -> DeploymentResult:
    print(
        json.dumps(
            {
                "event": "deployer_start",
                "request_id": config.request_id,
                "destination_name": config.destination_name,
                "connection_name": config.connection_name,
                "connector_name": config.connector_name,
                "connector_version": config.connector_version,
                "config_keys": sorted(config.connector_config.keys()),
            }
        )
    )
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as handle:
        json.dump(config.connector_config, handle)
        handle.flush()
        config_path = handle.name

    try:
        _run_fivetran_deploy(config, config_path)
        api = FivetranApiClient(config.api_key_b64, config.base_url)
        destination_id = api.get_group_id_by_name(config.destination_name)
        connection_id = api.get_connection_id_by_name(destination_id, config.connection_name)
        return DeploymentResult(
            status="succeeded",
            request_id=config.request_id,
            connector_name=config.connector_name,
            connector_version=config.connector_version,
            destination_name=config.destination_name,
            destination_id=destination_id,
            connection_name=config.connection_name,
            connection_id=connection_id,
            dashboard_url=f"https://fivetran.com/dashboard/connectors/{connection_id}/status",
        )
    finally:
        Path(config_path).unlink(missing_ok=True)


def _run_fivetran_deploy(config: JobConfig, config_path: str) -> None:
    cmd = [
        "fivetran",
        "deploy",
        config.project_path,
        "--force",
        "--api-key",
        config.api_key_b64,
        "--destination",
        config.destination_name,
        "--connection",
        config.connection_name,
        "--configuration",
        config_path,
        "--python-version",
        config.python_version,
    ]
    proc = subprocess.run(cmd, text=True, capture_output=True)
    if proc.stdout:
        print(proc.stdout, end="" if proc.stdout.endswith("\n") else "\n")
    if proc.stderr:
        print(proc.stderr, end="" if proc.stderr.endswith("\n") else "\n")
    if proc.returncode != 0:
        raise DeployerError("deploy_failed", f"fivetran deploy exited with code {proc.returncode}")


def _load_connector_config() -> dict[str, Any]:
    raw = _require_env("CONNECTOR_CONFIG_JSON")
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise DeployerError("invalid_config", "CONNECTOR_CONFIG_JSON must decode to a JSON object")
    return parsed


def _read_project_version(project_path: str) -> str:
    pyproject_path = Path(project_path) / "pyproject.toml"
    if not pyproject_path.exists():
        raise DeployerError("missing_pyproject", f"pyproject.toml not found under {project_path}")
    with pyproject_path.open("rb") as handle:
        pyproject = tomllib.load(handle)
    project = pyproject.get("project")
    if not isinstance(project, dict):
        raise DeployerError("invalid_pyproject", "pyproject.toml missing [project] section")
    version = project.get("version")
    if not isinstance(version, str) or not version:
        raise DeployerError("invalid_pyproject", "pyproject.toml missing project.version")
    return version


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if value is None or value == "":
        raise DeployerError("missing_env", f"{name} is required")
    return value


def _emit_result(payload: dict[str, Any], termination_log_path: str) -> None:
    rendered = json.dumps(payload, sort_keys=True)
    print(rendered)
    Path(termination_log_path).write_text(rendered)


if __name__ == "__main__":
    raise SystemExit(main())
