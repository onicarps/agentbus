"""DevEx init / monitor / ping tests."""

from __future__ import annotations

import json

from click.testing import CliRunner

from agentbus.cli import main
from agentbus.devex import (
    agentbus_mcp_entry,
    apply_init,
    merge_json_config,
    resolve_workspace,
)


def test_merge_json_config_idempotent():
    entry = {"command": "agentbus", "args": ["mcp-serve"], "env": {}}
    config = {"mcpServers": {"other": {"command": "x"}}}
    merged, changed = merge_json_config(config, entry)
    assert changed is True
    assert merged["mcpServers"]["agentbus"] == entry

    merged2, changed2 = merge_json_config(merged, entry)
    assert changed2 is False


def test_init_writes_workspace_marker(tmp_path):
    mcp_dir = tmp_path / ".cursor"
    mcp_dir.mkdir()
    (mcp_dir / "mcp.json").write_text('{"mcpServers": {}}\n', encoding="utf-8")
    apply_init(tmp_path, producer_id="grok", dry_run=False)
    marker = tmp_path / ".agentbus" / "workspace"
    assert marker.read_text(encoding="utf-8").strip() == str(tmp_path.resolve())


def test_init_bootstraps_a_new_producer_in_a_fresh_default_policy(tmp_path):
    from agentbus.rbac import load_rbac_config

    result = apply_init(tmp_path, producer_id="my-agent", dry_run=False)

    config = load_rbac_config(tmp_path)
    assert config is not None
    assert config.producers["my-agent"] == "engineer"
    assert result.bootstrap_role == "engineer"


def test_init_does_not_widen_an_existing_policy_for_a_new_producer(tmp_path):
    from agentbus.rbac import ensure_default_roles, load_rbac_config

    ensure_default_roles(tmp_path)
    result = apply_init(tmp_path, producer_id="my-agent", dry_run=False)

    config = load_rbac_config(tmp_path)
    assert config is not None
    assert "my-agent" not in config.producers
    assert result.bootstrap_role is None


def test_init_apply_empty_json_config(tmp_path):
    mcp_dir = tmp_path / ".cursor"
    mcp_dir.mkdir()
    cfg_path = mcp_dir / "mcp.json"
    cfg_path.write_text("", encoding="utf-8")

    applied = apply_init(tmp_path, producer_id="grok", dry_run=False)
    assert applied.dry_run is False
    assert any("updated" in u for u in applied.updated)
    data = json.loads(cfg_path.read_text(encoding="utf-8"))
    assert "agentbus" in data["mcpServers"]


def test_init_dry_run_and_apply(tmp_path):
    mcp_dir = tmp_path / ".cursor"
    mcp_dir.mkdir()
    cfg_path = mcp_dir / "mcp.json"
    cfg_path.write_text('{"mcpServers": {}}\n', encoding="utf-8")

    dry = apply_init(tmp_path, producer_id="grok", dry_run=True)
    assert dry.dry_run is True
    assert any("would update" in u for u in dry.updated)
    assert cfg_path.read_text(encoding="utf-8") == '{"mcpServers": {}}\n'

    applied = apply_init(tmp_path, producer_id="grok", dry_run=False)
    assert applied.dry_run is False
    assert any("updated" in u for u in applied.updated)
    data = json.loads(cfg_path.read_text(encoding="utf-8"))
    assert "agentbus" in data["mcpServers"]
    backups = list(cfg_path.parent.glob("mcp.json.agentbus.bak.*"))
    assert len(backups) == 1
    assert backups[0].read_text(encoding="utf-8") == '{"mcpServers": {}}\n'


def test_resolve_workspace_explicit_is_authoritative(tmp_path):
    explicit = tmp_path / "ws"
    explicit.mkdir()
    assert resolve_workspace(explicit) == explicit.resolve()

    git_root = tmp_path / "repo"
    git_root.mkdir()
    (git_root / ".git").mkdir()
    sub = git_root / "pkg"
    sub.mkdir()
    assert resolve_workspace(sub) == sub.resolve()


def test_resolve_workspace_implicit_cwd_walks_to_git_root(
    tmp_path, monkeypatch
):
    git_root = tmp_path / "repo"
    git_root.mkdir()
    (git_root / ".git").mkdir()
    sub = git_root / "pkg"
    sub.mkdir()
    monkeypatch.chdir(sub)
    assert resolve_workspace() == git_root.resolve()


def test_resolve_workspace_explicit_ignores_ancestor_agentbus_marker(tmp_path):
    (tmp_path / ".agentbus").mkdir()
    explicit = tmp_path / "tenant" / "workspace"
    explicit.mkdir(parents=True)
    assert resolve_workspace(explicit) == explicit.resolve()


def test_format_event_row_extracts_from_to():
    from agentbus.devex import format_event_row

    row = format_event_row(
        {
            "event_id": 42,
            "timestamp": "2026-07-16T05:00:51Z",
            "topic": "okf/handoff",
            "producer_id": "agy",
            "payload": {
                "from": "agy",
                "to": "grok",
                "summary": "Feature Request (v0.14): columns",
            },
        }
    )
    assert row["from"] == "agy"
    assert row["to"] == "grok"
    assert "v0.14" in row["summary"]
    assert row["id"] == "42"


def test_format_event_row_fallback_producer():
    from agentbus.devex import format_event_row

    row = format_event_row(
        {
            "event_id": 1,
            "timestamp": "2026-07-16T00:00:00Z",
            "topic": "system/fs",
            "producer_id": "os-watcher",
            "payload": {"event": "modified", "path": "x.md"},
        }
    )
    assert row["from"] == "os-watcher"
    assert row["to"] == "—"


def test_cli_init_monitor_ping(tmp_path, monkeypatch):
    ws = str(tmp_path)
    mcp_dir = tmp_path / ".cursor"
    mcp_dir.mkdir()
    (mcp_dir / "mcp.json").write_text('{"mcpServers": {}}\n', encoding="utf-8")

    runner = CliRunner()
    monkeypatch.setenv("AGENTBUS_PRODUCER_ID", "grok")

    init = runner.invoke(
        main,
        ["init", "--workspace", ws, "--producer-id", "grok", "--apply"],
    )
    assert init.exit_code == 0, init.output
    assert "updated" in init.output

    ping = runner.invoke(
        main,
        ["ping", "--workspace", ws, "--producer-id", "grok"],
    )
    assert ping.exit_code == 0, ping.output
    assert json.loads(ping.output)["event_id"] == 1

    mon = runner.invoke(
        main,
        ["monitor", "--workspace", ws, "--once"],
    )
    assert mon.exit_code == 0, mon.output
    assert "PING" in mon.output
    # v0.14 columns present in rich table or plain output
    assert "from" in mon.output.lower() or "grok" in mon.output
    assert "to" in mon.output.lower() or "all" in mon.output


def test_mcp_serve_entry_shape(tmp_path):
    entry = agentbus_mcp_entry(tmp_path, "cursor")
    assert entry["env"]["AGENTBUS_WORKSPACE"] == str(tmp_path.resolve())
    assert entry["env"]["AGENTBUS_PRODUCER_ID"] == "cursor"
    assert "mcp-serve" in " ".join(entry["args"])


def test_jsonc_vscode_config_preserves_comments_and_strings(tmp_path):
    from agentbus.devex import _load_json_config
    from agentbus.adapters.writer import plan_json_patch
    from agentbus.jsonc import loads

    path = tmp_path / '.vscode' / 'mcp.json'
    path.parent.mkdir()
    original = '{\n // line comment\n "servers": { /* block comment */ "other": {"url": "https://host/a//b"}, },\n}'
    path.write_text(original)
    assert _load_json_config(path)['servers']['other']['url'] == 'https://host/a//b'
    patch = plan_json_patch(installation_id='vscode', path=path, root='servers', entry={'command': 'agentbus'})
    updated = patch.new_bytes.decode()
    assert '// line comment' in updated and '/* block comment */' in updated
    assert loads(updated)['servers']['agentbus']['command'] == 'agentbus'
    assert loads(updated)['servers']['other']['url'] == 'https://host/a//b'


def test_no_mcp_reports_runtime_initialized(tmp_path):
    from agentbus.devex import apply_init

    result = apply_init(tmp_path, producer_id='local-user', dry_run=False, discover_clients=False)
    assert result.runtime_initialized
    runtime = tmp_path / ".agentbus"
    assert (runtime / "token").is_file()
    assert (runtime / "roles.yaml").is_file()
    assert (runtime / "workspace").read_text().strip() == str(tmp_path.resolve())


def test_jsonc_rejects_malformed_empty_member(tmp_path):
    import pytest
    from agentbus.devex import InvalidMcpConfigError, _load_json_config

    path = tmp_path / "mcp.json"
    path.write_text('{"servers": {,}}')
    with pytest.raises(InvalidMcpConfigError):
        _load_json_config(path)


def test_jsonc_duplicate_root_refuses_non_roundtripping_patch(tmp_path):
    import pytest
    from agentbus.adapters.writer import ConfigConflictError, plan_json_patch

    path = tmp_path / "mcp.json"
    original = '{"servers": {}, "servers": {}}'
    path.write_text(original)
    with pytest.raises(ConfigConflictError, match="planned document"):
        plan_json_patch(installation_id="vscode", path=path, root="servers", entry={"command": "agentbus"})
    assert path.read_text() == original
