"""Phase D Factory TurnAdapter tests (mocked subprocess — no live droid)."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from unittest.mock import MagicMock

import yaml

from agentbus.runner import load_runner_config, run_once
from agentbus.runner.adapters.factory import (
    FactoryAdapter,
    build_factory_command,
    build_factory_prompt,
)
from agentbus.runner.types import WakeEnvelope
from agentbus.store import EventStore


def _wake(eid: int = 50) -> WakeEnvelope:
    return WakeEnvelope(
        event_id=eid,
        topic="okf/handoff",
        from_agent="grok",
        to="factory",
        summary="FACTORY_QA_MISSION: run unit checks",
        payload={
            "from": "grok",
            "to": "factory",
            "summary": "FACTORY_QA_MISSION: run unit checks",
        },
        source="webhook_queue",
    )


def test_build_factory_prompt_includes_event():
    p = build_factory_prompt(_wake(88), budget_remaining=4)
    assert "event_id: 88" in p
    assert "FACTORY_QA_MISSION" in p
    assert "causation_id=88" in p
    assert "budget_remaining_turns_on_chain: 4" in p


def test_build_factory_command_shape(tmp_path: Path):
    prompt = tmp_path / "prompt.md"
    prompt.write_text("hi", encoding="utf-8")
    cmd = build_factory_command(
        droid_bin="droid",
        prompt_path=prompt,
        cwd=tmp_path,
        auto="medium",
        output_format="text",
        model="m1",
        mission=True,
        tag="factory-runner",
        extra_args=["--use-spec"],
        skip_permissions=False,
    )
    assert cmd[0:2] == ["droid", "exec"]
    assert "-f" in cmd and str(prompt) in cmd
    assert "--cwd" in cmd and str(tmp_path) in cmd
    assert "--auto" in cmd and "medium" in cmd
    assert "-o" in cmd and "text" in cmd
    assert "--mission" in cmd
    assert "-m" in cmd and "m1" in cmd
    assert "--tag" in cmd and "factory-runner" in cmd
    assert "--use-spec" in cmd
    assert "--skip-permissions-unsafe" not in cmd


def test_build_factory_command_skip_permissions_omits_auto(tmp_path: Path):
    prompt = tmp_path / "prompt.md"
    prompt.write_text("hi", encoding="utf-8")
    cmd = build_factory_command(
        droid_bin="droid",
        prompt_path=prompt,
        cwd=tmp_path,
        auto="high",
        output_format="text",
        model=None,
        mission=False,
        tag=None,
        extra_args=[],
        skip_permissions=True,
    )
    assert "--skip-permissions-unsafe" in cmd
    assert "--auto" not in cmd


def test_factory_default_skip_permissions(tmp_path: Path):
    """Headless dogfood default is skip_permissions=True (droid --auto high insufficient)."""
    mock_run = MagicMock(
        return_value=subprocess.CompletedProcess(
            args=["droid"],
            returncode=0,
            stdout="ok\n",
            stderr="",
        )
    )
    ad = FactoryAdapter(
        workspace=tmp_path,
        options={"command": "droid", "timeout_seconds": 90},
        run_fn=mock_run,
    )
    r = ad.start_turn(_wake(9), budget_remaining=3)
    assert r.ok is True
    cmd = mock_run.call_args[0][0]
    assert "--skip-permissions-unsafe" in cmd
    assert "--auto" not in cmd


def test_factory_dry_run(tmp_path: Path):
    ad = FactoryAdapter(
        workspace=tmp_path,
        options={"dry_run": True, "auto": "medium"},
    )
    r = ad.start_turn(_wake(1), budget_remaining=3)
    assert r.ok is True
    assert "RUNNER_ACK" in r.summary
    assert "dry_run" in r.summary
    assert (tmp_path / ".agentbus" / "runs" / "1" / "prompt.md").is_file()


def test_factory_success_mocked(tmp_path: Path):
    mock_run = MagicMock(
        return_value=subprocess.CompletedProcess(
            args=["droid"],
            returncode=0,
            stdout="QA checks complete; suite green\n",
            stderr="",
        )
    )
    ad = FactoryAdapter(
        workspace=tmp_path,
        options={"command": "droid", "auto": "medium", "timeout_seconds": 90},
        run_fn=mock_run,
    )
    r = ad.start_turn(_wake(2), budget_remaining=5)
    assert r.ok is True
    assert "RUNNER_ACK" in r.summary
    assert "suite green" in r.summary or "QA checks" in r.summary
    mock_run.assert_called_once()
    args, kwargs = mock_run.call_args
    assert args[0][0] == "droid"
    assert args[0][1] == "exec"
    assert kwargs["timeout"] == 90
    assert kwargs["cwd"] == str(tmp_path.resolve())


def test_factory_nonzero_exit(tmp_path: Path):
    mock_run = MagicMock(
        return_value=subprocess.CompletedProcess(
            args=["droid"],
            returncode=3,
            stdout="",
            stderr="droid boom",
        )
    )
    ad = FactoryAdapter(workspace=tmp_path, options={}, run_fn=mock_run)
    r = ad.start_turn(_wake(3), budget_remaining=1)
    assert r.ok is False
    assert "RUNNER_ERROR" in r.summary
    assert "exit=3" in r.summary


def test_factory_timeout(tmp_path: Path):
    def boom(*_a, **_k):
        raise subprocess.TimeoutExpired(cmd="droid", timeout=2)

    ad = FactoryAdapter(
        workspace=tmp_path,
        options={"timeout_seconds": 2},
        run_fn=boom,
    )
    r = ad.start_turn(_wake(4), budget_remaining=1)
    assert r.ok is False
    assert "timeout" in r.summary


def test_factory_invalid_auto(tmp_path: Path):
    # Invalid auto only checked when skip_permissions is false (auto is used).
    ad = FactoryAdapter(
        workspace=tmp_path,
        options={"auto": "ultra", "skip_permissions": False},
    )
    r = ad.start_turn(_wake(5), budget_remaining=1)
    assert r.ok is False
    assert "invalid auto" in r.summary


def test_runner_loop_factory_dry_run_end_to_end(tmp_path: Path):
    cfg_path = tmp_path / "runner.yaml"
    cfg_path.write_text(
        yaml.safe_dump(
            {
                "version": "1.0",
                "runner_id": "factory-r1",
                "producer_id": "factory",
                "intake": {"mode": "webhook_queue", "runtime": "factory"},
                "adapter": {"type": "factory", "dry_run": True, "auto": "low"},
                "accept_to": ["factory", "qa"],
                "allow_broadcast": False,
                "budget": {"max_turns_per_chain": 10},
            }
        ),
        encoding="utf-8",
    )
    qdir = tmp_path / ".agentbus" / "ingress"
    qdir.mkdir(parents=True)
    rec = {
        "event_id": 66,
        "from": "grok",
        "to": "factory",
        "summary": "phase d smoke",
        "topic": "okf/handoff",
        "raw": {
            "event_id": 66,
            "payload": {
                "from": "grok",
                "to": "factory",
                "summary": "phase d smoke",
            },
        },
    }
    (qdir / "factory_wake_queue.jsonl").write_text(
        json.dumps(rec) + "\n", encoding="utf-8"
    )

    cfg = load_runner_config(cfg_path)
    results = run_once(tmp_path, cfg)
    assert len(results) == 1
    assert results[0]["status"] == "processed"
    assert results[0]["ok"] is True
    assert "factory" in results[0]["summary"].lower()

    store = EventStore(tmp_path)
    try:
        evs = store.poll("okf/handoff", since_id=0)["events"]
        assert len(evs) == 1
        assert evs[0]["causation_id"] == 66
        assert evs[0]["payload"]["from"] == "factory"
        assert "RUNNER_ACK" in evs[0]["payload"]["summary"]
    finally:
        store.close()
