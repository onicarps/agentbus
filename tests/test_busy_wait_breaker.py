"""v0.16.1 busy-wait / companion-ACK circuit breaker hard gates."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import yaml

from agentbus.runner import load_runner_config, run_once
from agentbus.runner.adapters.prompt_common import (
    preview_suppresses_ack,
    turn_result_from_cli_exit,
)
from agentbus.runner.types import AWAIT_EXIT_CODE, TurnResult
from agentbus.store import EventStore


def _write_runner_yaml(path: Path, **overrides) -> Path:
    data = {
        "version": "1.0",
        "runner_id": "test-runner-1",
        "producer_id": "grok",
        "intake": {"mode": "webhook_queue", "runtime": "grok"},
        "adapter": {"type": "echo"},
        "accept_to": ["grok", "engineer"],
        "allow_broadcast": False,
        "budget": {"max_turns_per_chain": 10},
        "poll_interval_ms": 50,
    }
    for k, v in overrides.items():
        if isinstance(v, dict) and isinstance(data.get(k), dict):
            data[k] = {**data[k], **v}
        else:
            data[k] = v
    path.write_text(yaml.safe_dump(data), encoding="utf-8")
    return path


def _enqueue(
    ws: Path,
    event_id: int,
    *,
    to: str = "grok",
    frm: str = "factory",
    summary: str = "idle companion",
    runtime: str = "grok",
):
    qdir = ws / ".agentbus" / "ingress"
    qdir.mkdir(parents=True, exist_ok=True)
    rec = {
        "received_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "event_id": event_id,
        "runtime": runtime,
        "from": frm,
        "to": to,
        "summary": summary,
        "topic": "okf/handoff",
        "raw": {
            "event_id": event_id,
            "topic": "okf/handoff",
            "payload": {"from": frm, "to": to, "summary": summary},
        },
    }
    with (qdir / f"{runtime}_wake_queue.jsonl").open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(rec) + "\n")


def test_preview_suppresses_ack_markers():
    assert preview_suppresses_ack("CHAIN_BREAK: idle wake") is True
    assert preview_suppresses_ack("status TERMINAL_IDLE done") is True
    assert preview_suppresses_ack("this is a NO-OP") is True
    assert preview_suppresses_ack("normal work completed") is False
    assert preview_suppresses_ack("") is False
    assert preview_suppresses_ack(None) is False
    # case-sensitive exact substrings
    assert preview_suppresses_ack("chain_break lower") is False
    assert preview_suppresses_ack("no-op lower hyphen") is False


def test_turn_result_chain_break_sets_suppress_ack():
    r = turn_result_from_cli_exit(
        adapter="grok",
        event_id=543,
        returncode=0,
        preview="Ship loop CLOSED. CHAIN_BREAK — no further action.",
    )
    assert r.ok is True
    assert r.status == "ok"
    assert r.suppress_ack is True
    assert r.detail is not None
    assert r.detail.get("circuit_break") is True


def test_turn_result_noop_and_terminal_idle():
    for marker in ("NO-OP", "TERMINAL_IDLE"):
        r = turn_result_from_cli_exit(
            adapter="grok",
            event_id=1,
            returncode=0,
            preview=f"idle wake {marker}",
        )
        assert r.suppress_ack is True, marker


def test_turn_result_normal_does_not_suppress():
    r = turn_result_from_cli_exit(
        adapter="grok",
        event_id=2,
        returncode=0,
        preview="implemented feature and dispatched QA",
    )
    assert r.suppress_ack is False
    assert "RUNNER_ACK" in r.summary


def test_turn_result_error_with_chain_break_still_suppresses():
    r = turn_result_from_cli_exit(
        adapter="factory",
        event_id=590,
        returncode=1,
        preview="CHAIN_BREAK spurious companion ack",
    )
    assert r.ok is False
    assert r.suppress_ack is True


def test_turn_result_suspend_does_not_suppress():
    r = turn_result_from_cli_exit(
        adapter="grok",
        event_id=10,
        returncode=AWAIT_EXIT_CODE,
        preview="awaiting factory NO-OP",
    )
    assert r.status == "suspended"
    assert r.suppress_ack is False


def test_turn_result_explicit_suppress_ack_kwarg():
    r = TurnResult(summary="x", suppress_ack=True)
    assert r.suppress_ack is True
    assert r.ok is True


def test_loop_suppresses_runner_ack_on_chain_break(tmp_path: Path, monkeypatch):
    """Outer loop must not publish okf/handoff when suppress_ack is set."""
    cfg_path = _write_runner_yaml(tmp_path / "runner.yaml")
    _enqueue(tmp_path, 583, to="grok", frm="factory", summary="spurious idle")

    class _BreakAdapter:
        def start_turn(self, wake, budget_remaining: int = 0) -> TurnResult:
            return TurnResult(
                ok=True,
                summary=(
                    f"RUNNER_ACK: grok completed event_id={wake.event_id} "
                    "out=CHAIN_BREAK idle"
                ),
                detail={"adapter": "grok", "circuit_break": True},
                suppress_ack=True,
            )

    monkeypatch.setattr(
        "agentbus.runner.loop.get_adapter",
        lambda *a, **k: _BreakAdapter(),
    )

    cfg = load_runner_config(cfg_path)
    results = run_once(tmp_path, cfg)
    assert len(results) == 1
    assert results[0]["status"] == "processed"
    assert results[0]["ok"] is True
    assert results[0]["circuit_break"] is True
    assert results[0]["ack_event_id"] is None

    # Wake marked done (no re-delivery)
    done = (tmp_path / ".agentbus" / "ingress" / "grok_wake_done.ids").read_text()
    assert "583" in done

    # No companion handoff published
    store = EventStore(tmp_path)
    try:
        polled = store.poll("okf/handoff", since_id=0)
        assert polled["events"] == []
    finally:
        store.close()

    # Run log still written
    assert (tmp_path / ".agentbus" / "runs" / "583" / "result.json").is_file()


def test_loop_still_publishes_ack_without_suppress(tmp_path: Path, monkeypatch):
    cfg_path = _write_runner_yaml(tmp_path / "runner.yaml")
    _enqueue(tmp_path, 100, to="grok", frm="agy", summary="real work")

    class _OkAdapter:
        def start_turn(self, wake, budget_remaining: int = 0) -> TurnResult:
            return TurnResult(
                ok=True,
                summary=f"RUNNER_ACK: grok completed event_id={wake.event_id}",
                suppress_ack=False,
            )

    monkeypatch.setattr(
        "agentbus.runner.loop.get_adapter",
        lambda *a, **k: _OkAdapter(),
    )

    cfg = load_runner_config(cfg_path)
    results = run_once(tmp_path, cfg)
    assert results[0]["circuit_break"] is False
    assert results[0]["ack_event_id"] is not None

    store = EventStore(tmp_path)
    try:
        events = store.poll("okf/handoff", since_id=0)["events"]
        assert len(events) == 1
        assert events[0]["payload"]["to"] == "agy"
        assert "RUNNER_ACK" in events[0]["payload"]["summary"]
    finally:
        store.close()


def test_cli_adapter_chain_break_end_to_end(tmp_path: Path, monkeypatch):
    """Grok adapter path: CHAIN_BREAK in stdout → no bus ACK."""
    import subprocess

    from agentbus.runner.adapters.grok import GrokAdapter

    cfg_path = _write_runner_yaml(
        tmp_path / "runner.yaml",
        producer_id="grok",
        adapter={"type": "grok"},
        accept_to=["grok", "engineer"],
        intake={"mode": "webhook_queue", "runtime": "grok"},
    )
    _enqueue(tmp_path, 590, to="grok", frm="factory", summary="error loop")

    def fake_run(*_a, **_k):
        return subprocess.CompletedProcess(
            args=["grok"],
            returncode=0,
            stdout="Idle wake. CHAIN_BREAK\n",
            stderr="",
        )

    def _adapter(typ, *, workspace, options=None):
        assert typ == "grok"
        return GrokAdapter(workspace=workspace, options=options, run_fn=fake_run)

    monkeypatch.setattr("agentbus.runner.loop.get_adapter", _adapter)

    cfg = load_runner_config(cfg_path)
    results = run_once(tmp_path, cfg)
    assert results[0]["circuit_break"] is True
    assert results[0]["ack_event_id"] is None

    store = EventStore(tmp_path)
    try:
        assert store.poll("okf/handoff", since_id=0)["events"] == []
    finally:
        store.close()
