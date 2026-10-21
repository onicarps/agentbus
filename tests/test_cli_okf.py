"""CLI wiring tests for okf scaffolding and distillation."""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest
from click.testing import CliRunner

from agentbus.cli import main
from agentbus.scaffold import MANIFEST_PATH

requires_git = pytest.mark.skipif(
    shutil.which("git") is None, reason="git is required for this test"
)


@pytest.fixture(autouse=True)
def isolate_user_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("USERPROFILE", str(home))


def _git(*args: str, cwd: Path) -> None:
    subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True)


def _publish_handoff(workspace: Path, *, summary: str = "cli handoff") -> int:
    from agentbus.store import EventStore

    store = EventStore(workspace)
    try:
        event, _duplicate = store.publish(
            topic="okf/handoff",
            producer_id="codex",
            schema_version="1.0",
            payload={
                "from": "codex",
                "to": "factory",
                "summary": summary,
                "initiative": "agentbus",
                "links": ["/initiatives/agentbus/status.md"],
            },
            idempotency_key=f"cli-{summary}",
        )
        return event.event_id
    finally:
        store.close()


def test_init_apply_scaffolds_okf_plane(tmp_path: Path) -> None:
    ws = tmp_path / "ws"
    ws.mkdir()
    runner = CliRunner()

    result = runner.invoke(
        main,
        ["init", "--workspace", str(ws), "--producer-id", "codex", "--apply"],
    )

    assert result.exit_code == 0, result.output
    assert (ws / MANIFEST_PATH).is_file()
    assert (ws / "AGENTS.md").is_file()
    assert (ws / "okf" / "harness" / "runners.toml").is_file()
    assert "Knowledge plane: created" in result.output


def test_init_no_okf_skips_scaffold(tmp_path: Path) -> None:
    ws = tmp_path / "ws"
    ws.mkdir()
    runner = CliRunner()

    result = runner.invoke(
        main,
        [
            "init",
            "--workspace",
            str(ws),
            "--producer-id",
            "codex",
            "--apply",
            "--no-okf",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Knowledge plane: skipped (--no-okf)" in result.output
    assert not (ws / "AGENTS.md").exists()
    assert not (ws / MANIFEST_PATH).exists()


def test_init_bare_skips_tracked_scaffold(tmp_path: Path) -> None:
    ws = tmp_path / "ws"
    ws.mkdir()
    runner = CliRunner()

    result = runner.invoke(
        main,
        [
            "init",
            "--workspace",
            str(ws),
            "--producer-id",
            "codex",
            "--apply",
            "--bare",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "skipped (--bare)" in result.output
    assert not (ws / MANIFEST_PATH).exists()
    assert not (ws / "AGENTS.md").exists()
    assert not (ws / "okf").exists()


@requires_git
def test_init_bare_apply_writes_git_info_exclude(tmp_path: Path) -> None:
    ws = tmp_path / "ws"
    ws.mkdir()
    _git("init", cwd=ws)
    runner = CliRunner()

    result = runner.invoke(
        main,
        [
            "init",
            "--workspace",
            str(ws),
            "--producer-id",
            "codex",
            "--apply",
            "--bare",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "skipped (--bare) -> git-info-exclude" in result.output
    exclude = ws / ".git" / "info" / "exclude"
    assert exclude.is_file()
    assert ".agentbus/" in exclude.read_text(encoding="utf-8")
    # Bare mode must use the local exclude, never a tracked .gitignore or scaffold.
    assert not (ws / ".gitignore").exists()
    assert not (ws / MANIFEST_PATH).exists()
    assert not (ws / "AGENTS.md").exists()


def test_init_bare_dry_run_reports_bare_accurately(tmp_path: Path) -> None:
    ws = tmp_path / "ws"
    ws.mkdir()
    runner = CliRunner()

    result = runner.invoke(
        main,
        ["init", "--workspace", str(ws), "--producer-id", "codex", "--bare"],
    )

    assert result.exit_code == 0, result.output
    assert "skipped (--bare)" in result.output
    assert "--no-okf" not in result.output
    assert "dry-run" in result.output
    assert not (ws / MANIFEST_PATH).exists()
    assert not (ws / ".gitignore").exists()


def test_init_apply_honors_project_name(tmp_path: Path) -> None:
    ws = tmp_path / "ws"
    ws.mkdir()
    runner = CliRunner()

    result = runner.invoke(
        main,
        [
            "init",
            "--workspace",
            str(ws),
            "--producer-id",
            "codex",
            "--apply",
            "--project-name",
            "fleet-core",
        ],
    )

    assert result.exit_code == 0, result.output
    manifest = json.loads((ws / MANIFEST_PATH).read_text(encoding="utf-8"))
    assert manifest["project_name"] == "fleet-core"


def test_init_apply_is_idempotent_after_distillation(tmp_path: Path) -> None:
    ws = tmp_path / "ws"
    ws.mkdir()
    runner = CliRunner()
    first = runner.invoke(
        main,
        ["init", "--workspace", str(ws), "--producer-id", "codex", "--apply"],
    )
    assert first.exit_code == 0, first.output
    _publish_handoff(ws)
    distilled = runner.invoke(main, ["distill", "--workspace", str(ws)])
    assert distilled.exit_code == 0, distilled.output
    log_before = (ws / "okf" / "log.md").read_bytes()

    again = runner.invoke(
        main,
        ["init", "--workspace", str(ws), "--producer-id", "codex", "--apply"],
    )

    assert again.exit_code == 0, again.output
    assert "preserved distilled okf/log.md" in again.output
    assert (ws / "okf" / "log.md").read_bytes() == log_before


def test_init_dry_run_reports_without_scaffolding(tmp_path: Path) -> None:
    ws = tmp_path / "ws"
    ws.mkdir()
    runner = CliRunner()

    result = runner.invoke(
        main,
        ["init", "--workspace", str(ws), "--producer-id", "codex"],
    )

    assert result.exit_code == 0, result.output
    assert "would create" in result.output
    assert not (ws / MANIFEST_PATH).exists()


def test_init_apply_reports_scaffold_conflict(tmp_path: Path) -> None:
    ws = tmp_path / "ws"
    ws.mkdir()
    (ws / "okf").mkdir()
    (ws / "okf" / "status.md").write_text("team-owned\n", encoding="utf-8")
    runner = CliRunner()

    result = runner.invoke(
        main,
        ["init", "--workspace", str(ws), "--producer-id", "codex", "--apply"],
    )

    assert result.exit_code != 0
    assert "scaffold_conflict" in result.output
    assert "Re-run with --no-okf (or --bare)" in result.output
    assert (ws / "okf" / "status.md").read_text(encoding="utf-8") == "team-owned\n"
    assert not (ws / MANIFEST_PATH).exists()


def test_init_apply_preserves_pre_existing_agents_md(tmp_path: Path) -> None:
    ws = tmp_path / "ws"
    ws.mkdir()
    (ws / "AGENTS.md").write_text("team-owned-runbook\n", encoding="utf-8")
    runner = CliRunner()

    result = runner.invoke(
        main,
        ["init", "--workspace", str(ws), "--producer-id", "codex", "--apply"],
    )

    assert result.exit_code == 0
    assert "Knowledge plane: preserved pre-existing AGENTS.md" in result.output
    assert (ws / "AGENTS.md").read_text(encoding="utf-8") == "team-owned-runbook\n"
    assert (ws / "okf" / "index.md").is_file()
    assert (ws / "okf" / "status.md").is_file()
    assert (ws / "okf" / "log.md").is_file()
    assert (ws / MANIFEST_PATH).is_file()


def test_init_conflict_aborts_before_mutating_mcp_config(tmp_path: Path) -> None:
    ws = tmp_path / "ws"
    (ws / ".cursor").mkdir(parents=True)
    config = ws / ".cursor" / "mcp.json"
    config.write_text('{"mcpServers": {}}\n', encoding="utf-8")
    (ws / "okf").mkdir()
    (ws / "okf" / "status.md").write_text("team-owned\n", encoding="utf-8")
    runner = CliRunner()

    result = runner.invoke(
        main,
        ["init", "--workspace", str(ws), "--producer-id", "codex", "--apply"],
    )

    assert result.exit_code != 0
    assert "scaffold_conflict" in result.output
    assert json.loads(config.read_text(encoding="utf-8")) == {"mcpServers": {}}
    assert not (ws / ".cursor" / "mcp.json.agentbus.bak").exists()
    assert not (ws / MANIFEST_PATH).exists()


@requires_git
def test_init_conflict_with_tracked_scaffold_leaves_mcp_config_unchanged(
    tmp_path: Path,
) -> None:
    """A conflict on a Git-tracked scaffold file aborts before MCP config mutation."""
    ws = tmp_path / "ws"
    (ws / ".cursor").mkdir(parents=True)
    config = ws / ".cursor" / "mcp.json"
    config.write_text('{"mcpServers": {}}\n', encoding="utf-8")
    _git("init", cwd=ws)
    _git("config", "user.email", "test@example.com", cwd=ws)
    _git("config", "user.name", "Test", cwd=ws)
    (ws / "okf").mkdir()
    (ws / "okf" / "status.md").write_text("team-owned\n", encoding="utf-8")
    _git("add", "okf/status.md", ".cursor/mcp.json", cwd=ws)
    _git("commit", "-m", "track team scaffold", cwd=ws)
    config_before = config.read_bytes()
    runner = CliRunner()

    result = runner.invoke(
        main,
        ["init", "--workspace", str(ws), "--producer-id", "codex", "--apply"],
    )

    assert result.exit_code != 0
    assert "scaffold_conflict" in result.output
    assert config.read_bytes() == config_before
    assert not (ws / ".cursor" / "mcp.json.agentbus.bak").exists()
    assert (ws / "okf" / "status.md").read_text(encoding="utf-8") == "team-owned\n"
    assert not (ws / MANIFEST_PATH).exists()


def test_distill_cli_materializes_handoffs(tmp_path: Path) -> None:
    ws = tmp_path / "ws"
    ws.mkdir()
    runner = CliRunner()
    init = runner.invoke(
        main,
        ["init", "--workspace", str(ws), "--producer-id", "codex", "--apply"],
    )
    assert init.exit_code == 0, init.output
    event_id = _publish_handoff(ws)

    result = runner.invoke(main, ["distill", "--workspace", str(ws)])

    assert result.exit_code == 0, result.output
    summary = json.loads(result.output)
    assert summary["count"] == 1
    assert summary["high_water_event_id"] == event_id
    assert (ws / "okf" / "log.md").is_file()
    assert list((ws / "okf" / "handoffs").rglob("*.md"))


def test_distill_cli_stdout_prints_markdown(tmp_path: Path) -> None:
    ws = tmp_path / "ws"
    ws.mkdir()
    runner = CliRunner()
    init = runner.invoke(
        main,
        ["init", "--workspace", str(ws), "--producer-id", "codex", "--apply"],
    )
    assert init.exit_code == 0, init.output
    _publish_handoff(ws, summary="preview only")

    result = runner.invoke(main, ["distill", "--workspace", str(ws), "--stdout"])

    assert result.exit_code == 0, result.output
    assert "type: Handoff" in result.output
    assert "> preview only" in result.output
    assert not (ws / "okf" / "handoffs").exists()


def test_distill_cli_dry_run_writes_nothing(tmp_path: Path) -> None:
    ws = tmp_path / "ws"
    ws.mkdir()
    runner = CliRunner()
    init = runner.invoke(
        main,
        ["init", "--workspace", str(ws), "--producer-id", "codex", "--apply"],
    )
    assert init.exit_code == 0, init.output
    _publish_handoff(ws)

    result = runner.invoke(main, ["distill", "--workspace", str(ws), "--dry-run"])

    assert result.exit_code == 0, result.output
    summary = json.loads(result.output.split("---")[0].strip())
    assert summary["count"] == 1
    assert not (ws / "okf" / "handoffs").exists()
    assert not (ws / ".agentbus" / ".distill-state.json").exists()


def test_distill_cli_custom_out_directory(tmp_path: Path) -> None:
    ws = tmp_path / "ws"
    ws.mkdir()
    out = tmp_path / "knowledge"
    runner = CliRunner()
    init = runner.invoke(
        main,
        ["init", "--workspace", str(ws), "--producer-id", "codex", "--apply"],
    )
    assert init.exit_code == 0, init.output
    _publish_handoff(ws)

    result = runner.invoke(
        main, ["distill", "--workspace", str(ws), "--out", str(out)]
    )

    assert result.exit_code == 0, result.output
    assert (out / "log.md").is_file()
    assert list((out / "handoffs").rglob("*.md"))


def test_distill_cli_custom_out_keeps_default_plane_fresh(tmp_path: Path) -> None:
    ws = tmp_path / "ws"
    ws.mkdir()
    out = tmp_path / "knowledge"
    runner = CliRunner()
    init = runner.invoke(
        main,
        ["init", "--workspace", str(ws), "--producer-id", "codex", "--apply"],
    )
    assert init.exit_code == 0, init.output
    event_id = _publish_handoff(ws)

    custom = runner.invoke(main, ["distill", "--workspace", str(ws), "--out", str(out)])

    assert custom.exit_code == 0, custom.output
    assert json.loads(custom.output)["count"] == 1
    assert not (ws / ".agentbus" / ".distill-state.json").exists()

    default = runner.invoke(main, ["distill", "--workspace", str(ws)])

    assert default.exit_code == 0, default.output
    summary = json.loads(default.output)
    assert summary["count"] == 1
    assert summary["high_water_event_id"] == event_id
    assert (ws / "okf" / "log.md").is_file()


def test_distill_cli_rejects_a_missing_workspace(tmp_path: Path) -> None:
    runner = CliRunner()

    result = runner.invoke(
        main, ["distill", "--workspace", str(tmp_path / "absent")]
    )

    assert result.exit_code != 0
    assert "Workspace not found" in result.output
    assert "Traceback" not in result.output
    assert result.exception is None or isinstance(result.exception, SystemExit)


def test_project_log_routes_to_distillation_when_plane_exists(tmp_path: Path) -> None:
    ws = tmp_path / "ws"
    ws.mkdir()
    runner = CliRunner()
    init = runner.invoke(
        main,
        ["init", "--workspace", str(ws), "--producer-id", "codex", "--apply"],
    )
    assert init.exit_code == 0, init.output
    _publish_handoff(ws)

    result = runner.invoke(main, ["project-log", "--workspace", str(ws)])

    assert result.exit_code == 0, result.output
    summary = json.loads(result.output)
    assert summary["projected"] == 1
    assert summary["knowledge_plane"] == "okf"
    assert (ws / "okf" / "log.md").is_file()
    assert not (ws / "log.md").exists()


def test_project_log_keeps_legacy_path_without_plane(tmp_path: Path) -> None:
    ws = tmp_path / "ws"
    ws.mkdir()
    runner = CliRunner()
    _publish_handoff(ws)

    result = runner.invoke(main, ["project-log", "--workspace", str(ws)])

    assert result.exit_code == 0, result.output
    summary = json.loads(result.output)
    assert summary["projected"] == 1
    assert "knowledge_plane" not in summary
    assert (ws / "log.md").is_file()


def test_project_log_custom_log_file_stays_legacy(tmp_path: Path) -> None:
    ws = tmp_path / "ws"
    ws.mkdir()
    runner = CliRunner()
    init = runner.invoke(
        main,
        ["init", "--workspace", str(ws), "--producer-id", "codex", "--apply"],
    )
    assert init.exit_code == 0, init.output
    _publish_handoff(ws)

    result = runner.invoke(
        main, ["project-log", "--workspace", str(ws), "--log-file", "custom.md"]
    )

    assert result.exit_code == 0, result.output
    assert (ws / "custom.md").is_file()
