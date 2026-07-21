"""Phase C Hermes TurnAdapter tests (mocked subprocess — no live LLM)."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from unittest.mock import MagicMock

import yaml

from agentbus.runner import load_runner_config, run_once
from agentbus.runner.adapters.hermes import (
    HermesAdapter,
    build_hermes_command,
    build_hermes_prompt,
)
from agentbus.runner.types import WakeEnvelope
from agentbus.store import EventStore


def _wake(eid: int = 10) -> WakeEnvelope:
    return WakeEnvelope(
        event_id=eid,
        topic="okf/handoff",
        from_agent="agy",
        to="hermes",
        summary="check disk and reply",
        payload={"from": "agy", "to": "hermes", "summary": "check disk and reply"},
        source="webhook_queue",
    )


def test_build_hermes_prompt_includes_event():
    p = build_hermes_prompt(_wake(99), budget_remaining=7)
    assert "event_id=99" in p
    assert "check disk" in p
    assert "budget_remaining_turns_on_chain=7" in p
    assert "causation_id=99" in p


def test_build_hermes_prompt_includes_telegram_standing_orders():
    """Lock Mode A standing orders so suppress list cannot be silently trimmed."""
    p = build_hermes_prompt(_wake(99), budget_remaining=7)
    assert "Telegram Relay Standing Orders" in p
    for prefix in (
        "RUNNER_ACK",
        "RUNNER_ERROR",
        "RUNNER_SUSPEND",
        "NO-OP",
        "TERMINAL_IDLE",
        "CHAIN_BREAK",
    ):
        assert prefix in p, f"standing-order suppress list missing {prefix}"
    assert "RESUME:" in p
    assert "Relay substance" in p or "substance handoffs" in p.lower()
    assert "bridge role" in p


def test_build_hermes_command_shape():
    cmd = build_hermes_command(
        hermes_bin="hermes",
        prompt="hello",
        max_turns=5,
        model="x",
        provider="y",
        extra_args=["--safe-mode"],
    )
    assert cmd[0] == "hermes"
    assert cmd[1:4] == ["chat", "-q", "hello"]
    assert "-Q" in cmd
    assert "--max-turns" in cmd and "5" in cmd
    assert "-m" in cmd and "x" in cmd
    assert "--provider" in cmd and "y" in cmd
    assert "--safe-mode" in cmd


def test_hermes_dry_run(tmp_path: Path):
    ad = HermesAdapter(
        workspace=tmp_path,
        options={"dry_run": True, "max_turns": 3},
    )
    r = ad.start_turn(_wake(1), budget_remaining=3)
    assert r.ok is True
    assert "RUNNER_ACK" in r.summary
    assert "dry_run" in r.summary
    assert r.detail and r.detail.get("dry_run") is True


def test_hermes_success_mocked(tmp_path: Path):
    mock_run = MagicMock(
        return_value=subprocess.CompletedProcess(
            args=["hermes"],
            returncode=0,
            stdout="ops ok: disk 40% free\n",
            stderr="",
        )
    )
    ad = HermesAdapter(
        workspace=tmp_path,
        options={"command": "hermes", "max_turns": 2},
        run_fn=mock_run,
    )
    r = ad.start_turn(_wake(2), budget_remaining=5)
    assert r.ok is True
    assert "RUNNER_ACK" in r.summary
    assert "ops ok" in r.summary
    mock_run.assert_called_once()
    args, kwargs = mock_run.call_args
    assert args[0][0] == "hermes"
    assert args[0][1] == "chat"
    assert kwargs["cwd"] == str(tmp_path.resolve())
    assert kwargs["timeout"] == 600


def test_hermes_nonzero_exit(tmp_path: Path):
    mock_run = MagicMock(
        return_value=subprocess.CompletedProcess(
            args=["hermes"],
            returncode=2,
            stdout="",
            stderr="boom",
        )
    )
    ad = HermesAdapter(workspace=tmp_path, options={}, run_fn=mock_run)
    r = ad.start_turn(_wake(3), budget_remaining=1)
    assert r.ok is False
    assert "RUNNER_ERROR" in r.summary
    assert "exit=2" in r.summary


def test_hermes_timeout(tmp_path: Path):
    def boom(*_a, **_k):
        raise subprocess.TimeoutExpired(cmd="hermes", timeout=1)

    ad = HermesAdapter(
        workspace=tmp_path,
        options={"timeout_seconds": 1},
        run_fn=boom,
    )
    r = ad.start_turn(_wake(4), budget_remaining=1)
    assert r.ok is False
    assert "timeout" in r.summary


def test_runner_loop_hermes_dry_run_end_to_end(tmp_path: Path):
    cfg_path = tmp_path / "runner.yaml"
    cfg_path.write_text(
        yaml.safe_dump(
            {
                "version": "1.0",
                "runner_id": "hermes-r1",
                "producer_id": "hermes",
                "intake": {"mode": "webhook_queue", "runtime": "hermes"},
                "adapter": {"type": "hermes", "dry_run": True, "max_turns": 2},
                "accept_to": ["hermes"],
                "allow_broadcast": False,
                "budget": {"max_turns_per_chain": 10},
            }
        ),
        encoding="utf-8",
    )
    qdir = tmp_path / ".agentbus" / "ingress"
    qdir.mkdir(parents=True)
    rec = {
        "event_id": 77,
        "from": "agy",
        "to": "hermes",
        "summary": "phase c smoke",
        "topic": "okf/handoff",
        "raw": {
            "event_id": 77,
            "payload": {"from": "agy", "to": "hermes", "summary": "phase c smoke"},
        },
    }
    (qdir / "hermes_wake_queue.jsonl").write_text(json.dumps(rec) + "\n", encoding="utf-8")

    cfg = load_runner_config(cfg_path)
    results = run_once(tmp_path, cfg)
    assert len(results) == 1
    assert results[0]["status"] == "processed"
    assert results[0]["ok"] is True
    assert "hermes" in results[0]["summary"].lower()

    store = EventStore(tmp_path)
    try:
        evs = store.poll("okf/handoff", since_id=0)["events"]
        assert len(evs) == 1
        assert evs[0]["causation_id"] == 77
        assert evs[0]["payload"]["from"] == "hermes"
        assert "RUNNER_ACK" in evs[0]["payload"]["summary"]
    finally:
        store.close()
