"""Regression tests for Power Ranking v1 failures (events #161 / #159)."""

from __future__ import annotations

import json

import pytest
from click.testing import CliRunner

from agentbus.cli import main
from agentbus.rbac import ForbiddenError, roles_path
from agentbus.schemas import validate_payload
from agentbus.store import EventStore
from agentbus.workspace_config import save_retention_days


def test_init_apply_installs_rbac(tmp_path):
    runner = CliRunner()
    ws = tmp_path / "ws"
    ws.mkdir()
    (ws / ".cursor").mkdir()
    (ws / ".cursor" / "mcp.json").write_text("{}", encoding="utf-8")
    result = runner.invoke(
        main,
        ["init", "--workspace", str(ws), "--producer-id", "grok", "--apply"],
    )
    assert result.exit_code == 0, result.output
    assert roles_path(ws).is_file()


def test_unknown_producer_blocked_after_init_apply(tmp_path):
    runner = CliRunner()
    ws = tmp_path / "ws"
    ws.mkdir()
    (ws / ".cursor").mkdir()
    (ws / ".cursor" / "mcp.json").write_text("{}", encoding="utf-8")
    runner.invoke(
        main,
        ["init", "--workspace", str(ws), "--producer-id", "grok", "--apply"],
    )
    payload = json.dumps(
        {"from": "unknown-prod", "to": "agy", "summary": "should fail"}
    )
    result = runner.invoke(
        main,
        [
            "publish",
            "--workspace",
            str(ws),
            "--topic",
            "okf/handoff",
            "--payload",
            payload,
            "--producer-id",
            "unknown-prod",
        ],
    )
    assert result.exit_code != 0
    assert "403 Forbidden" in result.output


def test_sla_cli_list_command(tmp_path):
    runner = CliRunner()
    ws = str(tmp_path)
    runner.invoke(main, ["token", "ensure", "--workspace", ws, "--quiet"])
    payload = json.dumps({"from": "grok", "to": "hermes", "summary": "sla task"})
    runner.invoke(
        main,
        [
            "publish",
            "--workspace",
            ws,
            "--topic",
            "okf/handoff",
            "--payload",
            payload,
            "--producer-id",
            "grok",
            "--sla-timeout-minutes",
            "5",
        ],
    )
    result = runner.invoke(main, ["sla", "--workspace", ws, "list"])
    assert result.exit_code == 0, result.output
    data = json.loads(result.output)
    assert data["sla_active_count"] == 1
    assert data["active"][0]["event_id"] == 1

    default = runner.invoke(main, ["sla", "--workspace", ws])
    assert default.exit_code == 0, default.output
    assert json.loads(default.output)["sla_active_count"] == 1


def test_retention_days_persisted_for_status(tmp_path):
    runner = CliRunner()
    ws = str(tmp_path)
    save_retention_days(tmp_path, 30)
    result = runner.invoke(main, ["status", "--workspace", ws])
    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["retention_days"] == 30


def test_rbac_blocks_unregistered_producer_in_store(tmp_path):
    from agentbus.rbac import ensure_default_roles

    ensure_default_roles(tmp_path)
    store = EventStore(tmp_path)
    try:
        with pytest.raises(ForbiddenError, match="unknown-prod"):
            store.publish(
                topic="okf/handoff",
                producer_id="unknown-prod",
                schema_version="1.0",
                payload=validate_payload(
                    "okf/handoff",
                    {"from": "x", "to": "agy", "summary": "blocked"},
                ),
            )
    finally:
        store.close()