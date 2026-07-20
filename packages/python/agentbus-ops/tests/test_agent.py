"""SREWatchdogAgent integration-ish tests (mock probe + state; no live bus publish)."""

from __future__ import annotations

import json
import stat
import textwrap
from pathlib import Path
from unittest.mock import patch

from agentbus_ops.agent import SREWatchdogAgent
from agentbus_ops.probe import HealthSnapshot
from agentbus_ops.state import load_state


def _fake_probe(level: str = "healthy") -> HealthSnapshot:
    return HealthSnapshot(
        level=level,
        workspace="/tmp/ws",
        checked_at="2026-07-20T12:00:00Z",
        notes=[],
        exit_code={"healthy": 0, "degraded": 1, "critical": 2}[level],
    )


def test_run_once_bootstrap_seed_dry_run(tmp_path: Path):
    state = tmp_path / "sre.json"
    agent = SREWatchdogAgent(workspace=tmp_path, state_file=state)
    with patch.object(agent, "probe", return_value=_fake_probe("healthy")):
        d = agent.run_once(dry_run=True)
    assert d.action == "bootstrap_seed"
    assert d.should_publish is False
    assert not state.exists()  # pure dry-run


def test_run_once_bootstrap_write_state(tmp_path: Path):
    state = tmp_path / "sre.json"
    agent = SREWatchdogAgent(workspace=tmp_path, state_file=state)
    with patch.object(agent, "probe", return_value=_fake_probe("healthy")):
        d = agent.run_once(dry_run=True, write_state=True)
    assert d.action == "bootstrap_seed"
    loaded = load_state(state)
    assert loaded is not None
    assert loaded.level == "healthy"
    assert loaded.last_action == "bootstrap_seed"


def test_run_once_transition_dry_run(tmp_path: Path):
    state = tmp_path / "sre.json"
    state.write_text(
        json.dumps(
            {
                "level": "healthy",
                "last_published_level": "healthy",
                "last_published_epoch": 1,
            }
        ),
        encoding="utf-8",
    )
    agent = SREWatchdogAgent(workspace=tmp_path, state_file=state)
    with patch.object(agent, "probe", return_value=_fake_probe("degraded")):
        d = agent.run_once(dry_run=True)
    assert d.action == "publish_transition"
    assert d.should_publish is True
    assert d.payload is not None
    assert d.dry_run is True


def test_run_once_publish_mocked(tmp_path: Path):
    state = tmp_path / "sre.json"
    state.write_text(
        json.dumps(
            {
                "level": "healthy",
                "last_published_level": "healthy",
                "last_published_epoch": 1,
            }
        ),
        encoding="utf-8",
    )
    agent = SREWatchdogAgent(workspace=tmp_path, state_file=state, producer_id="aider")
    hooks: list[str] = []

    def fake_on_transition(prev, cur, decision):
        hooks.append("transition")
        decision.publish_event_id = 999
        decision.publish_ok = True

    def fake_critical(cur, decision):
        hooks.append("critical")

    agent.on_transition = fake_on_transition  # type: ignore[method-assign]
    agent.on_critical_alert = fake_critical  # type: ignore[method-assign]

    with patch.object(agent, "probe", return_value=_fake_probe("critical")):
        d = agent.run_once(dry_run=False)
    assert d.action == "publish_transition"
    assert hooks == ["transition", "critical"]
    assert d.publish_event_id == 999
    loaded = load_state(state)
    assert loaded is not None
    assert loaded.level == "critical"
    assert loaded.last_published_level == "critical"


def test_run_once_publish_failure_no_state_write(tmp_path: Path):
    from agentbus_ops.publish import PublishError

    state = tmp_path / "sre.json"
    original = {
        "level": "healthy",
        "last_published_level": "healthy",
        "last_published_epoch": 1,
        "last_action": "silence",
    }
    state.write_text(json.dumps(original), encoding="utf-8")
    agent = SREWatchdogAgent(workspace=tmp_path, state_file=state)

    def boom(prev, cur, decision):
        raise PublishError("bus down")

    agent.on_transition = boom  # type: ignore[method-assign]
    with patch.object(agent, "probe", return_value=_fake_probe("degraded")):
        d = agent.run_once(dry_run=False)
    assert d.publish_ok is False
    # State file should remain original (bash exits before write on publish fail)
    loaded = load_state(state)
    assert loaded is not None
    assert loaded.level == "healthy"


def test_subclass_critical_hook_only_on_critical(tmp_path: Path):
    state = tmp_path / "sre.json"
    state.write_text(
        json.dumps({"level": "healthy", "last_published_level": "healthy", "last_published_epoch": 1}),
        encoding="utf-8",
    )
    called: list[str] = []

    class Sub(SREWatchdogAgent):
        def on_transition(self, prev, cur, decision):
            decision.publish_event_id = 1
            decision.publish_ok = True

        def on_critical_alert(self, cur, decision):
            called.append(cur.level)

    agent = Sub(workspace=tmp_path, state_file=state)
    with patch.object(agent, "probe", return_value=_fake_probe("degraded")):
        agent.run_once()
    assert called == []

    with patch.object(agent, "probe", return_value=_fake_probe("critical")):
        # prev is now degraded from prior write
        agent.run_once()
    assert called == ["critical"]


def test_cli_watchdog_dry_run_json(tmp_path: Path):
    """CLI smoke with fake health script."""
    from click.testing import CliRunner

    from agentbus_ops.cli import main

    script = tmp_path / "h.sh"
    payload = {
        "ok": True,
        "level": "healthy",
        "exit_code": 0,
        "workspace": str(tmp_path),
        "checked_at": "2026-07-20T00:00:00Z",
        "notes": [],
        "disabled_services": [],
    }
    script.write_text(
        textwrap.dedent(
            f"""\
            #!/usr/bin/env bash
            echo '{json.dumps(payload)}'
            exit 0
            """
        ),
        encoding="utf-8",
    )
    script.chmod(script.stat().st_mode | stat.S_IXUSR)
    state = tmp_path / "state.json"
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "watchdog",
            "--workspace",
            str(tmp_path),
            "--health-script",
            str(script),
            "--state-file",
            str(state),
            "--dry-run",
            "--json",
        ],
    )
    assert result.exit_code == 0, result.output
    data = json.loads(result.output.strip().splitlines()[-1])
    assert data["action"] == "bootstrap_seed"
    assert data["level"] == "healthy"
    assert data["should_publish"] is False
