"""Zero-ceremony behavior for a bare ``agentbus`` invocation."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from agentbus.cli import COMPANION_HITL_MARKER, main
from agentbus.intercepts import load_config
from agentbus.rbac import ForbiddenError
from agentbus.scaffold import MANIFEST_PATH
from agentbus.store import EventStore


@pytest.fixture(autouse=True)
def isolate_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("USERPROFILE", str(home))
    monkeypatch.delenv("AGENTBUS_WORKSPACE", raising=False)
    monkeypatch.delenv("AGENTBUS_PRODUCER_ID", raising=False)


def test_bare_headless_initializes_fresh_workspace_with_safe_defaults(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws = tmp_path / "project"
    ws.mkdir()
    cursor_config = ws / ".cursor" / "mcp.json"
    cursor_config.parent.mkdir()
    cursor_config.write_text("{}\n", encoding="utf-8")
    global_cursor_config = Path.home() / ".cursor" / "mcp.json"
    global_cursor_config.parent.mkdir()
    global_cursor_config.write_text('{"sentinel": "global"}\n', encoding="utf-8")
    monkeypatch.chdir(ws)
    monkeypatch.setenv("AGENTBUS_WORKSPACE", str(ws))
    monkeypatch.setattr("agentbus.cli._is_interactive_terminal", lambda: False)

    result = CliRunner().invoke(main, [])

    assert result.exit_code == 0, result.output
    summary = json.loads(result.output)
    legacy = {
        "action": "initialized",
        "events_db": str(ws / ".agentbus" / "events.db"),
        "hitl_intercepts_enabled": False,
        "mcp_discovery_enabled": True,
        "mcp_scope": "workspace",
        "mcp_updates": [f"cursor-project: updated {cursor_config}"],
        "next": ["agentbus up", "agentbus monitor"],
        "okf_scaffolded": True,
        "producer_id": "local-user",
        "workspace": str(ws),
    }
    for key, value in legacy.items():
        assert summary.pop(key) == value
    # Backups are collision-resistant: name carries a UTC timestamp and PID.
    backups = summary.pop("mcp_backups")
    assert len(backups) == 1
    backup = Path(backups[0])
    assert backup.is_file()
    assert backup.name.startswith("mcp.json.agentbus.bak.")
    discovery = summary.pop("discovery")
    connection = summary.pop("connection")
    assert summary == {}

    # Discovery is real evidence, not a guess: cursor-project exists in the
    # workspace, vscode does not. Connection state must not claim verification.
    clients = {item["installation"]: item for item in discovery["clients"]}
    assert set(clients) == {"cursor-project", "vscode"}
    assert clients["cursor-project"]["status"] == "present"
    assert clients["cursor-project"]["configured"] is True
    assert clients["cursor-project"]["state"] == "awaiting_approval"
    assert clients["vscode"]["status"] == "absent"
    assert connection["state"] == "awaiting_approval"
    assert connection["verification"]["performed"] is False
    assert (ws / ".agentbus" / "events.db").is_file()
    assert not (ws / ".agentbus" / "intercepts.json").exists()
    assert (ws / MANIFEST_PATH).is_file()
    assert (ws / "okf" / "status.md").is_file()
    mcp = json.loads(cursor_config.read_text(encoding="utf-8"))
    assert mcp["mcpServers"]["agentbus"]["env"]["AGENTBUS_PRODUCER_ID"] == (
        "local-user"
    )
    assert json.loads(global_cursor_config.read_text(encoding="utf-8")) == {
        "sentinel": "global"
    }
    roles = yaml.safe_load(
        (ws / ".agentbus" / "roles.yaml").read_text(encoding="utf-8")
    )
    assert roles["producers"]["local-user"] == "engineer"


def test_bare_interactive_applies_prompted_options(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws = tmp_path / "project"
    ws.mkdir()
    cursor_config = ws / ".cursor" / "mcp.json"
    cursor_config.parent.mkdir()
    cursor_config.write_text('{"teamOwned": true}\n', encoding="utf-8")
    monkeypatch.chdir(ws)
    monkeypatch.setenv("AGENTBUS_WORKSPACE", str(ws))
    monkeypatch.setattr("agentbus.cli._is_interactive_terminal", lambda: True)

    result = CliRunner().invoke(main, [], input="y\ny\nn\n")

    assert result.exit_code == 0, result.output
    assert "AgentBus companion setup" in result.output
    assert "AgentBus is ready." in result.output
    assert (ws / ".agentbus" / "events.db").is_file()
    assert (ws / MANIFEST_PATH).is_file()
    assert json.loads(cursor_config.read_text(encoding="utf-8")) == {
        "teamOwned": True
    }
    rules = load_config(ws).rules
    assert len(rules) == 1
    assert rules[0].topic == "okf/handoff"
    assert rules[0].contains == COMPANION_HITL_MARKER
    store = EventStore(ws)
    try:
        event, _duplicate = store.publish(
            topic="okf/handoff",
            producer_id="local-user",
            schema_version="1.0",
            payload={
                "from": "local-user",
                "to": "agy",
                "summary": "Ship after approval",
                "action": {"type": "release", "requires_approval": True},
            },
        )
    finally:
        store.close()
    assert event.status == "PENDING_APPROVAL"
    denied_store = EventStore(ws)
    try:
        with pytest.raises(ForbiddenError, match="unauthorized"):
            denied_store.publish(
                topic="okf/handoff",
                producer_id="unauthorized",
                schema_version="1.0",
                payload={
                    "from": "unauthorized",
                    "to": "agy",
                    "summary": "Ship after approval",
                    "action": {"type": "release", "requires_approval": True},
                },
            )
    finally:
        denied_store.close()


def test_bare_headless_preserves_existing_agents_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws = tmp_path / "project"
    ws.mkdir()
    agents = ws / "AGENTS.md"
    agents.write_text("team-owned instructions\n", encoding="utf-8")
    monkeypatch.chdir(ws)
    monkeypatch.setenv("AGENTBUS_WORKSPACE", str(ws))
    monkeypatch.setattr("agentbus.cli._is_interactive_terminal", lambda: False)

    result = CliRunner().invoke(main, [])

    assert result.exit_code == 0, result.output
    assert agents.read_text(encoding="utf-8") == "team-owned instructions\n"
    assert (ws / MANIFEST_PATH).is_file()


def test_bare_interactive_existing_workspace_launches_monitor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws = tmp_path / "project"
    ws.mkdir()
    EventStore(ws).close()
    monkeypatch.chdir(ws)
    monkeypatch.setenv("AGENTBUS_WORKSPACE", str(ws))
    monkeypatch.setattr("agentbus.cli._is_interactive_terminal", lambda: True)
    called: dict[str, object] = {}

    def fake_monitor(workspace: Path, **kwargs: object) -> None:
        called["workspace"] = workspace
        called.update(kwargs)

    monkeypatch.setattr("agentbus.cli.run_monitor", fake_monitor)

    result = CliRunner().invoke(main, [])

    assert result.exit_code == 0, result.output
    assert called["workspace"] == ws
    assert called["interval"] == 0.5
    assert called["once"] is False
    assert called["plain"] is False
    assert called["retention_days"] == 30


def test_bare_headless_existing_workspace_emits_status_without_tui(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws = tmp_path / "project"
    ws.mkdir()
    cursor_config = ws / ".cursor" / "mcp.json"
    cursor_config.parent.mkdir()
    cursor_config.write_text('{"sentinel": "preserve"}\n', encoding="utf-8")
    EventStore(ws).close()
    monkeypatch.chdir(ws)
    monkeypatch.setenv("AGENTBUS_WORKSPACE", str(ws))
    monkeypatch.setattr("agentbus.cli._is_interactive_terminal", lambda: False)

    def unexpected_monitor(*args: object, **kwargs: object) -> None:
        raise AssertionError("headless companion must not mount the monitor")

    monkeypatch.setattr("agentbus.cli.run_monitor", unexpected_monitor)

    result = CliRunner().invoke(main, [])

    assert result.exit_code == 0, result.output
    status = json.loads(result.output)
    assert status["workspace"] == str(ws)
    assert status["event_count"] == 0
    assert json.loads(cursor_config.read_text(encoding="utf-8")) == {
        "sentinel": "preserve"
    }


def test_init_options_disable_mcp_and_enable_hitl(
    tmp_path: Path,
) -> None:
    ws = tmp_path / "project"
    ws.mkdir()
    cursor_config = ws / ".cursor" / "mcp.json"
    cursor_config.parent.mkdir()
    cursor_config.write_text('{"sentinel": true}\n', encoding="utf-8")

    result = CliRunner().invoke(
        main,
        [
            "init",
            "--workspace",
            str(ws),
            "--producer-id",
            "codex",
            "--apply",
            "--no-mcp",
            "--hitl",
        ],
    )

    assert result.exit_code == 0, result.output
    assert json.loads(cursor_config.read_text(encoding="utf-8")) == {
        "sentinel": True
    }
    assert load_config(ws).rules[0].contains == COMPANION_HITL_MARKER
    assert (ws / ".agentbus" / "events.db").is_file()


def test_bare_headless_reports_scaffold_conflict_without_runtime_db(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws = tmp_path / "project"
    (ws / "okf").mkdir(parents=True)
    (ws / "okf" / "status.md").write_text("team-owned\n", encoding="utf-8")
    monkeypatch.setenv("AGENTBUS_WORKSPACE", str(ws))
    monkeypatch.setattr("agentbus.cli._is_interactive_terminal", lambda: False)

    result = CliRunner().invoke(main, [])

    assert result.exit_code != 0
    assert "scaffold_conflict" in result.output
    assert not (ws / ".agentbus" / "events.db").exists()


@pytest.mark.parametrize(
    "config_text",
    ["{broken\n", "[]\n", '{"mcpServers": []}\n'],
)
def test_bare_headless_reports_invalid_mcp_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, config_text: str
) -> None:
    ws = tmp_path / "project"
    ws.mkdir()
    cursor_config = ws / ".cursor" / "mcp.json"
    cursor_config.parent.mkdir()
    cursor_config.write_text(config_text, encoding="utf-8")
    monkeypatch.setenv("AGENTBUS_WORKSPACE", str(ws))
    monkeypatch.setattr("agentbus.cli._is_interactive_terminal", lambda: False)

    result = CliRunner().invoke(main, [])

    assert result.exit_code != 0
    assert "invalid_mcp_config" in result.output
    assert not isinstance(result.exception, json.JSONDecodeError)
    assert not (ws / ".agentbus" / "events.db").exists()
    assert not (ws / ".agentbus" / "token").exists()
    assert not (ws / ".agentbus" / "roles.yaml").exists()


def test_bare_headless_reports_non_utf8_mcp_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws = tmp_path / "project"
    ws.mkdir()
    cursor_config = ws / ".cursor" / "mcp.json"
    cursor_config.parent.mkdir()
    cursor_config.write_bytes(b"\xff\xfe")
    monkeypatch.setenv("AGENTBUS_WORKSPACE", str(ws))
    monkeypatch.setattr("agentbus.cli._is_interactive_terminal", lambda: False)

    result = CliRunner().invoke(main, [])

    assert result.exit_code != 0
    assert "invalid_mcp_config" in result.output
    assert "expected UTF-8 JSON" in result.output
    assert not (ws / ".agentbus" / "events.db").exists()
    assert not (ws / ".agentbus" / "token").exists()


def test_init_hitl_reports_malformed_intercept_before_mutation(tmp_path: Path) -> None:
    ws = tmp_path / "project"
    intercepts = ws / ".agentbus" / "intercepts.json"
    intercepts.parent.mkdir(parents=True)
    intercepts.write_text("{broken\n", encoding="utf-8")

    result = CliRunner().invoke(
        main,
        [
            "init",
            "--workspace",
            str(ws),
            "--producer-id",
            "codex",
            "--apply",
            "--hitl",
        ],
    )

    assert result.exit_code != 0
    assert "invalid_intercept_config" in result.output
    assert not isinstance(result.exception, json.JSONDecodeError)
    assert intercepts.read_text(encoding="utf-8") == "{broken\n"
    assert not (ws / ".agentbus" / "events.db").exists()
    assert not (ws / ".agentbus" / "token").exists()
    assert not (ws / ".agentbus" / "roles.yaml").exists()


def test_init_hitl_write_failure_does_not_commit_runtime_db(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws = tmp_path / "project"
    ws.mkdir()

    def fail_rule_write(*args: object, **kwargs: object) -> None:
        raise OSError("read-only intercept config")

    monkeypatch.setattr("agentbus.cli.add_rule", fail_rule_write)

    result = CliRunner().invoke(
        main,
        [
            "init",
            "--workspace",
            str(ws),
            "--producer-id",
            "codex",
            "--apply",
            "--hitl",
        ],
    )

    assert result.exit_code != 0
    assert "invalid_intercept_config" in result.output
    assert not (ws / ".agentbus" / "events.db").exists()


def test_bare_headless_recovers_partial_runtime_initialization(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws = tmp_path / "project"
    runtime = ws / ".agentbus"
    runtime.mkdir(parents=True)
    token = runtime / "token"
    token.write_text("existing-token\n", encoding="utf-8")
    monkeypatch.setenv("AGENTBUS_WORKSPACE", str(ws))
    monkeypatch.setattr("agentbus.cli._is_interactive_terminal", lambda: False)

    result = CliRunner().invoke(main, [])

    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["action"] == "initialized"
    assert token.read_text(encoding="utf-8") == "existing-token\n"
    assert (runtime / "events.db").is_file()
    assert (ws / MANIFEST_PATH).is_file()


def test_bare_headless_preserves_existing_rbac_policy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws = tmp_path / "project"
    roles = ws / ".agentbus" / "roles.yaml"
    roles.parent.mkdir(parents=True)
    original = (
        "roles:\n"
        "  reviewer:\n"
        "    can_publish_topics: [okf/handoff]\n"
        "producers:\n"
        "  owner: reviewer\n"
    )
    roles.write_text(original, encoding="utf-8")
    monkeypatch.setenv("AGENTBUS_WORKSPACE", str(ws))
    monkeypatch.setattr("agentbus.cli._is_interactive_terminal", lambda: False)

    result = CliRunner().invoke(main, [])

    assert result.exit_code == 0, result.output
    assert roles.read_text(encoding="utf-8") == original
    policy = yaml.safe_load(original)
    assert "local-user" not in policy["producers"]


def test_bare_headless_uses_producer_override(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws = tmp_path / "project"
    ws.mkdir()
    monkeypatch.setenv("AGENTBUS_WORKSPACE", str(ws))
    monkeypatch.setenv("AGENTBUS_PRODUCER_ID", "release-agent")
    monkeypatch.setattr("agentbus.cli._is_interactive_terminal", lambda: False)

    result = CliRunner().invoke(main, [])

    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["producer_id"] == "release-agent"
    roles = yaml.safe_load(
        (ws / ".agentbus" / "roles.yaml").read_text(encoding="utf-8")
    )
    assert roles["producers"]["release-agent"] == "engineer"


def test_bare_quiet_tty_uses_defaults_and_emits_json(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws = tmp_path / "project"
    ws.mkdir()
    monkeypatch.setenv("AGENTBUS_WORKSPACE", str(ws))
    monkeypatch.setattr("agentbus.cli._is_interactive_terminal", lambda: True)

    result = CliRunner().invoke(main, ["--quiet"])

    assert result.exit_code == 0, result.output
    summary = json.loads(result.output)
    assert summary["action"] == "initialized"
    assert summary["mcp_scope"] == "workspace"
    assert summary["hitl_intercepts_enabled"] is False


def test_init_apply_does_not_prune_existing_events(tmp_path: Path) -> None:
    ws = tmp_path / "project"
    ws.mkdir()
    store = EventStore(ws, auto_prune=False)
    try:
        event, _duplicate = store.publish(
            topic="okf/handoff",
            producer_id="codex",
            schema_version="1.0",
            payload={"from": "codex", "to": "agy", "summary": "Keep me"},
            skip_rbac=True,
        )
    finally:
        store.close()
    with sqlite3.connect(ws / ".agentbus" / "events.db") as conn:
        conn.execute(
            "UPDATE events SET timestamp = ? WHERE event_id = ?",
            ("2000-01-01T00:00:00Z", event.event_id),
        )

    result = CliRunner().invoke(
        main,
        ["init", "--workspace", str(ws), "--producer-id", "codex", "--apply"],
    )

    assert result.exit_code == 0, result.output
    with sqlite3.connect(ws / ".agentbus" / "events.db") as conn:
        assert conn.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 1


def test_bare_interactive_monitor_failure_degrades_to_status(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws = tmp_path / "project"
    ws.mkdir()
    EventStore(ws).close()
    monkeypatch.chdir(ws)
    monkeypatch.setenv("AGENTBUS_WORKSPACE", str(ws))
    monkeypatch.setattr("agentbus.cli._is_interactive_terminal", lambda: True)

    def broken_monitor(*args: object, **kwargs: object) -> None:
        raise RuntimeError("terminal mount unavailable")

    monkeypatch.setattr("agentbus.cli.run_monitor", broken_monitor)

    result = CliRunner().invoke(main, [])

    assert result.exit_code == 0, result.output
    json_lines = [line for line in result.output.splitlines() if line.startswith("{")]
    assert len(json_lines) == 1, result.output
    status = json.loads(json_lines[0])
    assert status["workspace"] == str(ws)
    assert "Mission-control monitor unavailable" in result.output


@pytest.mark.parametrize("flag", ["--help", "--version"])
def test_bare_companion_preserves_eager_click_options(
    flag: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws = tmp_path / "project"
    ws.mkdir()
    monkeypatch.chdir(ws)
    monkeypatch.setenv("AGENTBUS_WORKSPACE", str(ws))
    monkeypatch.setattr("agentbus.cli._is_interactive_terminal", lambda: False)

    result = CliRunner().invoke(main, [flag])

    assert result.exit_code == 0, result.output
    if flag == "--help":
        assert "usage:" in result.output.lower()
    else:
        assert result.output.lower().startswith("agentbus ")
    assert list(ws.iterdir()) == []


def test_bare_headless_survives_discovery_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A broken manifest set must degrade first run, never abort it."""
    ws = tmp_path / "project"
    ws.mkdir()
    monkeypatch.chdir(ws)
    monkeypatch.setenv("AGENTBUS_WORKSPACE", str(ws))
    monkeypatch.setattr("agentbus.cli._is_interactive_terminal", lambda: False)

    def explode(**_kwargs: object) -> None:
        raise RuntimeError("boom")

    monkeypatch.setattr("agentbus.discovery.discover", explode)

    result = CliRunner().invoke(main, [])

    assert result.exit_code == 0, result.output
    summary = json.loads(result.output)
    assert summary["action"] == "initialized"
    assert summary["discovery"] == {"error": "RuntimeError: boom"}
    assert "connection" not in summary
    assert (ws / ".agentbus" / "events.db").is_file()
