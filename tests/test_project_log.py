"""project-log projection tests."""

from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from agentbus.cli import main
from agentbus.project_log import format_handoff_line, load_state, project_handoffs
from agentbus.store import Event, EventStore


def _publish(store: EventStore, event_id_hint: str, summary: str) -> None:
    store.publish(
        topic="okf/handoff",
        producer_id="grok",
        schema_version="1.0",
        payload={
            "from": "grok",
            "to": "agy",
            "summary": summary,
            "links": ["/initiatives/agentbus/status.md"],
            "initiative": "agentbus",
        },
        idempotency_key=f"proj-{event_id_hint}",
    )


def test_format_handoff_line():
    event = Event(
        event_id=3,
        topic="okf/handoff",
        producer_id="grok",
        timestamp="2026-07-05T12:00:00Z",
        schema_version="1.0",
        payload={
            "from": "grok",
            "to": "hermes",
            "summary": "test handoff",
            "links": ["/initiatives/agentbus/status.md"],
        },
        causation_id=None,
        idempotency_key=None,
    )
    text = format_handoff_line(event)
    assert "**Handoff | grok → hermes**" in text
    assert "[status](/initiatives/agentbus/status.md)" in text


def test_project_handoffs_appends_to_log(tmp_path):
    ws = tmp_path / "ws"
    ws.mkdir()
    log_path = ws / "log.md"
    log_path.write_text("# Workspace Update Log\n\n## 2026-07-04\n* old entry\n", encoding="utf-8")

    store = EventStore(ws)
    _publish(store, "1", "first projection")
    _publish(store, "2", "second projection")

    result = project_handoffs(store, ws, log_path)
    assert result["projected"] == 2
    assert load_state(ws)["last_event_id"] == 2

    log_text = log_path.read_text(encoding="utf-8")
    assert "first projection" in log_text
    assert "second projection" in log_text
    assert "## 2026-07-05" in log_text or "## 2026-07-04" in log_text
    store.close()


def test_project_handoffs_idempotent(tmp_path):
    ws = tmp_path / "ws"
    ws.mkdir()
    log_path = ws / "log.md"
    log_path.write_text("# Workspace Update Log\n\n", encoding="utf-8")

    store = EventStore(ws)
    _publish(store, "a", "once")

    first = project_handoffs(store, ws, log_path)
    second = project_handoffs(store, ws, log_path)
    assert first["projected"] == 1
    assert second["projected"] == 0
    store.close()


def test_cli_project_log_dry_run(tmp_path):
    ws_path = tmp_path / "ws"
    ws_path.mkdir()
    ws = str(ws_path)
    runner = CliRunner()
    store = EventStore(ws_path)
    _publish(store, "cli", "dry run test")
    store.close()

    result = runner.invoke(
        main,
        ["project-log", "--workspace", ws, "--dry-run"],
    )
    assert result.exit_code == 0
    data = json.loads(result.output.split("---")[0].strip())
    assert data["projected"] == 1
    assert data["dry_run"] is True