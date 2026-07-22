"""v0.16.1 busy-wait / companion-ACK circuit breaker hard gates."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import yaml

from agentbus.runner import load_runner_config, run_once
from agentbus.runner.adapters.prompt_common import (
    HANDOFF_SUMMARY_MAX,
    build_cli_role_prompt,
    format_cli_out_preview,
    is_ops_noise_summary,
    preview_suppresses_ack,
    turn_result_from_cli_exit,
)
from agentbus.runner.types import AWAIT_EXIT_CODE, TurnResult, WakeEnvelope
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


def test_is_ops_noise_summary_prefixes():
    """Structural skip must catch companion ACK / error / suspend prefixes."""
    assert is_ops_noise_summary("RUNNER_ACK: aider completed event_id=1516 out=...") is True
    assert is_ops_noise_summary("RUNNER_ERROR: hermes exit=1 event_id=9") is True
    assert is_ops_noise_summary("RUNNER_SUSPEND: event_id=10 wait_id=w_x") is True
    assert is_ops_noise_summary("NO-OP/TERMINAL_IDLE CHAIN_BREAK") is True
    assert is_ops_noise_summary("TERMINAL_IDLE") is True
    assert is_ops_noise_summary("CHAIN_BREAK: done") is True
    assert is_ops_noise_summary("SUPPRESS ACK: relay skipped") is True
    # case-insensitive prefix
    assert is_ops_noise_summary("runner_ack: peer") is True
    # substance must NOT be treated as ops noise
    assert is_ops_noise_summary("Wake mechanism: timer-poll ~5s") is False
    assert is_ops_noise_summary("TELEGRAM_SMOKE_SUBSTANCE: please relay") is False
    assert is_ops_noise_summary("SRE_STATUS: healthy") is False
    assert is_ops_noise_summary("") is False
    assert is_ops_noise_summary(None) is False


def test_loop_skips_ops_noise_without_llm_or_ack(tmp_path: Path, monkeypatch):
    """Inbound RUNNER_ACK must not spawn adapter or publish a re-ACK (storm fix)."""
    cfg_path = _write_runner_yaml(tmp_path / "runner.yaml")
    _enqueue(
        tmp_path,
        1518,
        to="grok",
        frm="aider",
        summary=(
            "RUNNER_ACK: aider completed event_id=1516 out="
            "Wake mechanism: Timer-poll (default 5s)"
        ),
    )

    called: list[int] = []

    class _MustNotRun:
        def start_turn(self, wake, budget_remaining: int = 0) -> TurnResult:
            called.append(wake.event_id)
            return TurnResult(ok=True, summary="should not run")

    monkeypatch.setattr(
        "agentbus.runner.loop.get_adapter",
        lambda *a, **k: _MustNotRun(),
    )

    cfg = load_runner_config(cfg_path)
    results = run_once(tmp_path, cfg)
    assert len(results) == 1
    assert results[0]["status"] == "skipped"
    assert results[0]["reason"] == "ops_noise"
    assert called == []

    done = (tmp_path / ".agentbus" / "ingress" / "grok_wake_done.ids").read_text()
    assert "1518" in done

    store = EventStore(tmp_path)
    try:
        polled = store.poll("okf/handoff", since_id=0)
        assert polled["events"] == []
    finally:
        store.close()


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


def test_out_preview_preserves_newlines_and_fills_budget():
    """P1: out= must not collapse multi-line answers to a 500-char single line."""
    body = "line one\n\nline two\n" + ("word " * 400)
    preview = format_cli_out_preview(body, 1800)
    assert "\n" in preview
    assert "line one" in preview
    assert "line two" in preview
    assert len(preview) <= 1800

    r = turn_result_from_cli_exit(
        adapter="grok",
        event_id=99,
        returncode=0,
        preview="## Title\n\n" + ("paragraph text " * 200),
    )
    assert "RUNNER_ACK" in r.summary
    assert "out=" in r.summary
    assert len(r.summary) <= HANDOFF_SUMMARY_MAX
    # Multi-line content preserved after out=
    out = r.summary.split("out=", 1)[1]
    assert "\n" in out or "Title" in out
    # Larger than the historical 500-char collapse budget when input is long
    assert len(out) > 500


def test_prompt_common_slack_primary_guidance():
    wake = WakeEnvelope(
        event_id=1,
        topic="okf/handoff",
        from_agent="slack",
        to="grok",
        summary="hello",
        payload={"from": "slack", "to": "grok", "summary": "hello"},
        source="wake_file",
    )
    prompt = build_cli_role_prompt(
        role_name="Grok",
        role_hint="engineer",
        wake=wake,
        budget_remaining=5,
    )
    assert "primary UI = Slack" in prompt
    assert "to `slack`" in prompt or "substance handoff to `slack`" in prompt
    assert "legacy Telegram" in prompt
    assert "Telegram bridge" not in prompt


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
