"""Phase E Grok/Agy CLI wrapper tests (mocked subprocess)."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from unittest.mock import MagicMock

import yaml

from agentbus.runner import load_runner_config, run_once
from agentbus.runner.adapters.agy import (
    AgyAdapter,
    build_agy_command,
    build_agy_prompt,
)
from agentbus.runner.adapters.grok import (
    GrokAdapter,
    build_grok_command,
    build_grok_prompt,
)
from agentbus.runner.types import WakeEnvelope
from agentbus.store import EventStore


def _wake(eid: int, *, to: str, frm: str, summary: str) -> WakeEnvelope:
    return WakeEnvelope(
        event_id=eid,
        topic="okf/handoff",
        from_agent=frm,
        to=to,
        summary=summary,
        payload={"from": frm, "to": to, "summary": summary},
        source="wake_file",
    )


def test_build_grok_prompt_and_command(tmp_path: Path):
    w = _wake(11, to="grok", frm="agy", summary="implement feature")
    p = build_grok_prompt(w, budget_remaining=5)
    assert "event_id: 11" in p
    assert "implement feature" in p
    assert "engineer" in p
    prompt_path = tmp_path / "prompt.md"
    prompt_path.write_text(p, encoding="utf-8")
    cmd = build_grok_command(
        grok_bin="grok",
        prompt_path=prompt_path,
        cwd=tmp_path,
        max_turns=8,
        always_approve=True,
        output_format="plain",
        model="m",
        extra_args=["--debug"],
    )
    assert cmd[0] == "grok"
    assert "--prompt-file" in cmd and str(prompt_path) in cmd
    assert "--always-approve" in cmd
    assert "--max-turns" in cmd and "8" in cmd
    assert "--cwd" in cmd
    assert "-m" in cmd and "m" in cmd
    assert "--debug" in cmd


def test_build_agy_prompt_and_command(tmp_path: Path):
    w = _wake(12, to="agy", frm="grok", summary="review architecture")
    p = build_agy_prompt(w, budget_remaining=3)
    assert "architect" in p
    assert "review architecture" in p
    cmd = build_agy_command(
        agy_bin="agy",
        prompt=p,
        workspace=tmp_path,
        print_timeout="10m",
        skip_permissions=True,
        model=None,
        extra_args=[],
    )
    assert cmd[0:2] == ["agy", "--print"]
    assert p in cmd
    assert "--print-timeout" in cmd and "10m" in cmd
    assert "--dangerously-skip-permissions" in cmd
    assert "--add-dir" in cmd and str(tmp_path) in cmd


def test_grok_dry_run(tmp_path: Path):
    ad = GrokAdapter(workspace=tmp_path, options={"dry_run": True})
    r = ad.start_turn(
        _wake(1, to="grok", frm="agy", summary="task"), budget_remaining=2
    )
    assert r.ok is True
    assert "dry_run" in r.summary
    assert (tmp_path / ".agentbus" / "runs" / "1" / "prompt.md").is_file()


def test_agy_dry_run(tmp_path: Path):
    ad = AgyAdapter(workspace=tmp_path, options={"dry_run": True})
    r = ad.start_turn(
        _wake(2, to="agy", frm="grok", summary="task"), budget_remaining=2
    )
    assert r.ok is True
    assert "dry_run" in r.summary


def test_grok_success_mocked(tmp_path: Path):
    mock = MagicMock(
        return_value=subprocess.CompletedProcess(
            args=["grok"], returncode=0, stdout="done feature\n", stderr=""
        )
    )
    ad = GrokAdapter(
        workspace=tmp_path,
        options={"timeout_seconds": 60, "max_turns": 4},
        run_fn=mock,
    )
    r = ad.start_turn(
        _wake(3, to="grok", frm="agy", summary="ship it"), budget_remaining=4
    )
    assert r.ok is True
    assert "RUNNER_ACK" in r.summary
    assert "done feature" in r.summary
    mock.assert_called_once()
    assert mock.call_args.kwargs["timeout"] == 60


def test_agy_nonzero_exit(tmp_path: Path):
    mock = MagicMock(
        return_value=subprocess.CompletedProcess(
            args=["agy"], returncode=1, stdout="", stderr="nope"
        )
    )
    ad = AgyAdapter(workspace=tmp_path, options={}, run_fn=mock)
    r = ad.start_turn(
        _wake(4, to="agy", frm="grok", summary="x"), budget_remaining=1
    )
    assert r.ok is False
    assert "RUNNER_ERROR" in r.summary
    assert "exit=1" in r.summary


def test_grok_timeout(tmp_path: Path):
    def boom(*_a, **_k):
        raise subprocess.TimeoutExpired(cmd="grok", timeout=3)

    ad = GrokAdapter(
        workspace=tmp_path, options={"timeout_seconds": 3}, run_fn=boom
    )
    r = ad.start_turn(
        _wake(5, to="grok", frm="agy", summary="x"), budget_remaining=1
    )
    assert r.ok is False
    assert "timeout" in r.summary


def test_runner_loop_grok_wake_file_dry_run(tmp_path: Path):
    cfg_path = tmp_path / "runner.yaml"
    cfg_path.write_text(
        yaml.safe_dump(
            {
                "version": "1.0",
                "runner_id": "grok-r1",
                "producer_id": "grok",
                "intake": {
                    "mode": "wake_file",
                    "wake_file": ".agentbus/WAKE.grok.json",
                },
                "adapter": {"type": "grok", "dry_run": True},
                "accept_to": ["grok", "engineer"],
                "allow_broadcast": False,
                "budget": {"max_turns_per_chain": 10},
            }
        ),
        encoding="utf-8",
    )
    wake = tmp_path / ".agentbus" / "WAKE.grok.json"
    wake.parent.mkdir(parents=True)
    wake.write_text(
        json.dumps(
            {
                "event_id": 77,
                "topic": "okf/handoff",
                "payload": {
                    "from": "agy",
                    "to": "grok",
                    "summary": "phase e smoke",
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
        assert len(evs) == 1
        assert evs[0]["causation_id"] == 77
        assert evs[0]["payload"]["from"] == "grok"
    finally:
        store.close()


def test_runner_loop_agy_wake_file_dry_run(tmp_path: Path):
    cfg_path = tmp_path / "runner.yaml"
    cfg_path.write_text(
        yaml.safe_dump(
            {
                "version": "1.0",
                "runner_id": "agy-r1",
                "producer_id": "agy",
                "intake": {
                    "mode": "wake_file",
                    "wake_file": ".agentbus/WAKE.agy.json",
                },
                "adapter": {"type": "agy", "dry_run": True},
                "accept_to": ["agy", "architect"],
                "allow_broadcast": False,
                "budget": {"max_turns_per_chain": 10},
            }
        ),
        encoding="utf-8",
    )
    wake = tmp_path / ".agentbus" / "WAKE.agy.json"
    wake.parent.mkdir(parents=True)
    wake.write_text(
        json.dumps(
            {
                "event_id": 88,
                "topic": "okf/handoff",
                "payload": {
                    "from": "grok",
                    "to": "agy",
                    "summary": "agy phase e smoke",
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
        assert evs[0]["causation_id"] == 88
        assert evs[0]["payload"]["from"] == "agy"
    finally:
        store.close()
