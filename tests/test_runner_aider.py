"""Aider SRE adapter tests (mocked subprocess)."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from unittest.mock import MagicMock

import yaml

from agentbus.runner import load_runner_config, run_once
from agentbus.runner.adapters.aider import (
    AiderAdapter,
    build_aider_command,
    build_aider_prompt,
)
from agentbus.runner.types import WakeEnvelope
from agentbus.store import EventStore


def _wake(eid: int = 9) -> WakeEnvelope:
    return WakeEnvelope(
        event_id=eid,
        topic="okf/handoff",
        from_agent="grok",
        to="aider",
        summary="SRE: check swarm health and report",
        payload={
            "from": "grok",
            "to": "aider",
            "summary": "SRE: check swarm health and report",
        },
        source="wake_file",
    )


def test_build_aider_prompt_and_command():
    p = build_aider_prompt(_wake(3), budget_remaining=2)
    assert "ops" in p.lower()
    assert "SRE" in p or "sre" in p.lower()
    assert "devops" in p.lower()
    assert "event_id: 3" in p
    assert "swarm_health_check" in p
    cmd = build_aider_command(
        aider_bin="aider",
        message=p,
        cwd=Path("/tmp"),
        yes_always=True,
        model="gpt-4o",
        extra_args=["--no-git"],
    )
    assert cmd[0:2] == ["aider", "--message"]
    assert p in cmd
    assert "--yes-always" in cmd
    assert "--model" in cmd and "gpt-4o" in cmd
    assert "--no-git" in cmd


def test_aider_dry_run(tmp_path: Path):
    ad = AiderAdapter(workspace=tmp_path, options={"dry_run": True})
    r = ad.start_turn(_wake(1), budget_remaining=1)
    assert r.ok is True
    assert "dry_run" in r.summary
    assert (tmp_path / ".agentbus" / "runs" / "1" / "prompt.md").is_file()


def test_aider_success_mocked(tmp_path: Path):
    mock = MagicMock(
        return_value=subprocess.CompletedProcess(
            args=["aider"],
            returncode=0,
            stdout="SRE_STATUS: healthy\n",
            stderr="",
        )
    )
    ad = AiderAdapter(workspace=tmp_path, options={}, run_fn=mock)
    r = ad.start_turn(_wake(2), budget_remaining=3)
    assert r.ok is True
    assert "RUNNER_ACK" in r.summary
    assert "healthy" in r.summary
    mock.assert_called_once()


def test_runner_loop_aider_wake_file_dry_run(tmp_path: Path):
    cfg_path = tmp_path / "runner.yaml"
    cfg_path.write_text(
        yaml.safe_dump(
            {
                "version": "1.0",
                "runner_id": "aider-r1",
                "producer_id": "aider",
                "intake": {
                    "mode": "wake_file",
                    "wake_file": ".agentbus/WAKE.aider.json",
                },
                "adapter": {"type": "aider", "dry_run": True},
                "accept_to": ["aider", "sre", "health"],
                "allow_broadcast": False,
                "budget": {"max_turns_per_chain": 8},
            }
        ),
        encoding="utf-8",
    )
    wake = tmp_path / ".agentbus" / "WAKE.aider.json"
    wake.parent.mkdir(parents=True)
    wake.write_text(
        json.dumps(
            {
                "event_id": 44,
                "topic": "okf/handoff",
                "payload": {
                    "from": "grok",
                    "to": "aider",
                    "summary": "health check",
                },
            }
        ),
        encoding="utf-8",
    )
    cfg = load_runner_config(cfg_path)
    results = run_once(tmp_path, cfg)
    assert len(results) == 1
    assert results[0]["ok"] is True
    store = EventStore(tmp_path)
    try:
        evs = store.poll("okf/handoff", since_id=0)["events"]
        assert evs[0]["causation_id"] == 44
        assert evs[0]["payload"]["from"] == "aider"
    finally:
        store.close()
