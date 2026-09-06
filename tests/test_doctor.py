from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from agentbus.cli import main
from agentbus.doctor import (
    DiagnosticCheck,
    check_database,
    check_isolated_publish_poll,
    check_identity,
    check_mcp_stdio,
    check_process_state,
    check_rbac,
    check_schema_registry,
    check_workspace,
)
from agentbus.rbac import ensure_default_roles
from agentbus.identity import bootstrap_workspace_identity, enroll_identity
from agentbus.schema_registry import register_schema
from agentbus.store import EventStore


def initialized_workspace(tmp_path: Path) -> Path:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    store = EventStore(workspace)
    store.close()
    ensure_default_roles(workspace)
    return workspace


def test_workspace_and_database_checks(tmp_path: Path) -> None:
    workspace = initialized_workspace(tmp_path)
    assert check_workspace(workspace).status == "OK"
    assert check_database(workspace).status == "OK"


def test_database_missing_is_honest_warning(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    assert check_database(workspace).status == "WARN"


def test_identity_diagnostics_are_honest_about_shared_uid(tmp_path: Path) -> None:
    unconfigured = check_identity(tmp_path)
    assert unconfigured.status == "WARN"
    assert unconfigured.details is not None
    assert unconfigured.details["configured"] is False
    assert unconfigured.details["strict_ready"] is False
    # A9: the operator must be able to see the path out of audit mode.
    steps = unconfigured.details["strict_cutover_steps"]
    assert isinstance(steps, list) and len(steps) >= 5
    bootstrap_workspace_identity(tmp_path)
    enroll_identity(tmp_path, "codex")
    configured = check_identity(tmp_path)
    assert configured.status == "WARN"
    assert configured.details["configured"] is True
    assert configured.details["child_credential_scrub"] is True
    assert configured.details["strict_ready"] is False


def test_identity_diagnostics_reject_tampered_registry(tmp_path: Path) -> None:
    bootstrap_workspace_identity(tmp_path)
    registry = tmp_path / ".agentbus" / "identity" / "registry.json"
    document = json.loads(registry.read_text(encoding="utf-8"))
    document["signed"]["registry_version"] = "99"
    registry.write_text(json.dumps(document), encoding="utf-8")
    assert check_identity(tmp_path).status == "FAIL"


def test_rbac_rejects_unknown_role_reference(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    roles = workspace / ".agentbus" / "roles.yaml"
    roles.parent.mkdir(parents=True)
    roles.write_text("roles: {}\nproducers:\n  agy: missing\n", encoding="utf-8")
    result = check_rbac(workspace)
    assert result.status == "FAIL"
    assert "unknown roles" in result.message


def test_schema_registry_validates_json_schema(tmp_path: Path) -> None:
    workspace = initialized_workspace(tmp_path)
    register_schema(workspace, "custom/topic", {"type": "object"})
    assert check_schema_registry(workspace).status == "OK"


def test_process_state_reports_stale_pid(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    state = workspace / ".agentbus" / "swarm.state.json"
    state.parent.mkdir(parents=True)
    state.write_text(json.dumps({"services": {"ghost": {"pid": 999_999_999}}}), encoding="utf-8")
    assert check_process_state(workspace).status == "WARN"


def test_isolated_publish_poll_is_real_round_trip() -> None:
    assert check_isolated_publish_poll().status == "OK"


def test_mcp_stdio_timeout_is_reported_as_failure(monkeypatch) -> None:
    async def timeout() -> tuple[int, str]:
        raise TimeoutError("bounded handshake expired")

    monkeypatch.setattr("agentbus.doctor._mcp_probe", timeout)
    result = check_mcp_stdio()
    assert result.status == "FAIL"
    assert "TimeoutError" in result.message


def test_cli_json_and_strict_exit(tmp_path: Path, monkeypatch) -> None:
    workspace = initialized_workspace(tmp_path)

    monkeypatch.setattr(
        "agentbus.doctor.check_mcp_stdio",
        lambda: DiagnosticCheck("mcp_stdio", "OK", "mocked protocol success"),
    )
    result = CliRunner().invoke(main, ["doctor", "--workspace", str(workspace), "--json"])
    assert result.exit_code == 0
    report = json.loads(result.output)
    assert report["workspace"] == str(workspace.resolve())
    assert {item["name"] for item in report["checks"]} >= {"database", "mcp_stdio", "versions"}

    strict = CliRunner().invoke(main, ["doctor", "--workspace", str(workspace), "--strict"])
    assert strict.exit_code == 1  # packaged Go helpers are optional but warned when absent
