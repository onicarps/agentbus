"""Phase B headless runner tests (echo adapter, dual intake)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import yaml

from agentbus.runner import load_runner_config, run_once
from agentbus.store import EventStore


def _write_runner_yaml(path: Path, **overrides) -> Path:
    data = {
        "version": "1.0",
        "runner_id": "test-runner-1",
        "producer_id": "hermes",
        "intake": {"mode": "webhook_queue", "runtime": "hermes"},
        "adapter": {"type": "echo"},
        "accept_to": ["hermes", "devops"],
        "allow_broadcast": False,
        "budget": {"max_turns_per_chain": 10},
        "poll_interval_ms": 50,
    }
    # deep merge shallow overrides
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
    to: str = "hermes",
    frm: str = "agy",
    summary: str = "do work",
    received_at: str | None = None,
):
    qdir = ws / ".agentbus" / "ingress"
    qdir.mkdir(parents=True, exist_ok=True)
    ts = received_at or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    rec = {
        "received_at": ts,
        "event_id": event_id,
        "runtime": "hermes",
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
    with (qdir / "hermes_wake_queue.jsonl").open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(rec) + "\n")


def test_echo_queue_once(tmp_path: Path):
    cfg_path = _write_runner_yaml(tmp_path / "runner.yaml")
    _enqueue(tmp_path, 42, to="hermes", frm="agy", summary="ops check")
    cfg = load_runner_config(cfg_path)
    results = run_once(tmp_path, cfg)
    assert len(results) == 1
    assert results[0]["status"] == "processed"
    assert results[0]["ok"] is True
    assert results[0]["event_id"] == 42

    done = (tmp_path / ".agentbus" / "ingress" / "hermes_wake_done.ids").read_text()
    assert "42" in done

    store = EventStore(tmp_path)
    try:
        polled = store.poll("okf/handoff", since_id=0)
        assert len(polled["events"]) == 1
        ack = polled["events"][0]
        assert ack["causation_id"] == 42
        assert ack["payload"]["from"] == "hermes"
        assert ack["payload"]["to"] == "agy"
        assert "RUNNER_ACK" in ack["payload"]["summary"]
    finally:
        store.close()

    run_log = tmp_path / ".agentbus" / "runs" / "42" / "result.json"
    assert run_log.is_file()

    # second drain: already done
    results2 = run_once(tmp_path, cfg)
    assert results2 == []


def test_drop_broadcast(tmp_path: Path):
    cfg_path = _write_runner_yaml(tmp_path / "runner.yaml")
    _enqueue(tmp_path, 7, to="all", frm="grok", summary="broadcast")
    cfg = load_runner_config(cfg_path)
    results = run_once(tmp_path, cfg)
    assert results[0]["status"] == "skipped"
    assert results[0]["reason"] == "broadcast"
    store = EventStore(tmp_path)
    try:
        assert store.poll("okf/handoff", since_id=0)["events"] == []
    finally:
        store.close()


def test_accept_to_filter(tmp_path: Path):
    cfg_path = _write_runner_yaml(tmp_path / "runner.yaml")
    _enqueue(tmp_path, 8, to="factory", frm="agy", summary="wrong target")
    cfg = load_runner_config(cfg_path)
    results = run_once(tmp_path, cfg)
    assert results[0]["status"] == "skipped"
    assert "to_not_accepted" in results[0]["reason"]


def test_budget_exceeded(tmp_path: Path):
    cfg_path = _write_runner_yaml(
        tmp_path / "runner.yaml",
        budget={"max_turns_per_chain": 2},
    )
    cfg = load_runner_config(cfg_path)
    # same causation chain root: use causation_id=1 on all, distinct event ids
    for eid in (101, 102, 103):
        qdir = tmp_path / ".agentbus" / "ingress"
        qdir.mkdir(parents=True, exist_ok=True)
        rec = {
            "event_id": eid,
            "from": "agy",
            "to": "hermes",
            "summary": f"task {eid}",
            "topic": "okf/handoff",
            "causation_id": 1,
            "raw": {
                "event_id": eid,
                "causation_id": 1,
                "payload": {"from": "agy", "to": "hermes", "summary": f"task {eid}"},
            },
        }
        with (qdir / "hermes_wake_queue.jsonl").open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(rec) + "\n")

    results = run_once(tmp_path, cfg)
    assert len(results) == 3
    oks = [r for r in results if r.get("ok") is True]
    errs = [r for r in results if r.get("ok") is False]
    assert len(oks) == 2
    assert len(errs) == 1
    assert "RUNNER_ERROR" in errs[0]["summary"]
    assert "budget" in errs[0]["summary"]


def test_wake_file_intake(tmp_path: Path):
    cfg_path = _write_runner_yaml(
        tmp_path / "runner.yaml",
        intake={"mode": "wake_file", "wake_file": ".agentbus/WAKE.hermes.json"},
        accept_to=["hermes"],
    )
    wake_path = tmp_path / ".agentbus" / "WAKE.hermes.json"
    wake_path.parent.mkdir(parents=True, exist_ok=True)
    wake_path.write_text(
        json.dumps(
            {
                "event_id": 55,
                "topic": "okf/handoff",
                "payload": {
                    "from": "agy",
                    "to": "hermes",
                    "summary": "file wake task",
                },
            }
        ),
        encoding="utf-8",
    )
    cfg = load_runner_config(cfg_path)
    results = run_once(tmp_path, cfg)
    assert len(results) == 1
    assert results[0]["status"] == "processed"
    assert results[0]["event_id"] == 55

    store = EventStore(tmp_path)
    try:
        events = store.poll("okf/handoff", since_id=0)["events"]
        assert len(events) == 1
        assert events[0]["causation_id"] == 55
    finally:
        store.close()


def test_idempotent_ack(tmp_path: Path):
    cfg_path = _write_runner_yaml(tmp_path / "runner.yaml")
    _enqueue(tmp_path, 99, to="hermes", frm="agy")
    cfg = load_runner_config(cfg_path)
    r1 = run_once(tmp_path, cfg)
    assert r1[0]["status"] == "processed"
    # Force re-process by clearing done but keeping same idempotency key path
    done_path = tmp_path / ".agentbus" / "ingress" / "hermes_wake_done.ids"
    done_path.write_text("", encoding="utf-8")
    r2 = run_once(tmp_path, cfg)
    assert r2[0]["status"] == "processed"
    assert r2[0]["duplicate_ack"] is True

    store = EventStore(tmp_path)
    try:
        events = store.poll("okf/handoff", since_id=0)["events"]
        assert len(events) == 1
    finally:
        store.close()


def test_companion_ack_copies_slack_links(tmp_path: Path):
    """P0: Slack-origin wakes must put slack:// links on companion RUNNER_ACK."""
    cfg_path = _write_runner_yaml(
        tmp_path / "runner.yaml",
        producer_id="grok",
        intake={"mode": "webhook_queue", "runtime": "grok"},
        accept_to=["grok"],
    )
    qdir = tmp_path / ".agentbus" / "ingress"
    qdir.mkdir(parents=True, exist_ok=True)
    link = "slack://C123/1710000000.000100"
    rec = {
        "received_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "event_id": 1877,
        "runtime": "grok",
        "from": "slack",
        "to": "grok",
        "summary": "please fix",
        "topic": "okf/handoff",
        "raw": {
            "event_id": 1877,
            "topic": "okf/handoff",
            "payload": {
                "from": "slack",
                "to": "grok",
                "summary": "please fix",
                "links": [link],
            },
        },
        "payload": {
            "from": "slack",
            "to": "grok",
            "summary": "please fix",
            "links": [link],
        },
    }
    with (qdir / "grok_wake_queue.jsonl").open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(rec) + "\n")

    cfg = load_runner_config(cfg_path)
    results = run_once(tmp_path, cfg)
    assert results[0]["status"] == "processed"

    store = EventStore(tmp_path)
    try:
        events = store.poll("okf/handoff", since_id=0)["events"]
        assert len(events) == 1
        ack = events[0]
        assert ack["payload"]["to"] == "slack"
        assert ack["payload"]["links"] == [link]
        assert "RUNNER_ACK" in ack["payload"]["summary"]
    finally:
        store.close()
