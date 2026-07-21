"""Resilient messaging: dead-letter RETRY_EXHAUSTED + spillover DLQ."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from unittest.mock import MagicMock

from agentbus.resilience import (
    RETRY_EXHAUSTED_REASON,
    append_spillover,
    escalate_retry_exhausted,
    publish_or_spill,
    spillover_path,
)
from agentbus.schemas import DEAD_LETTER_TOPIC, validate_payload
from agentbus.store import EventStore


def test_dead_letter_schema_accepts_retry_exhausted():
    payload = validate_payload(
        DEAD_LETTER_TOPIC,
        {
            "reason": RETRY_EXHAUSTED_REASON,
            "original_event_id": 42,
            "original_event": {"topic": "okf/handoff", "summary": "x"},
            "summary": "publish retry exhausted",
        },
    )
    assert payload["reason"] == "RETRY_EXHAUSTED"


def test_append_spillover_jsonl(tmp_path: Path):
    p = append_spillover(
        tmp_path,
        {
            "reason": RETRY_EXHAUSTED_REASON,
            "original_event_id": 1,
            "summary": "test spill",
        },
    )
    assert p == spillover_path(tmp_path)
    lines = p.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    rec = json.loads(lines[0])
    assert rec["reason"] == "RETRY_EXHAUSTED"
    assert "spilled_at" in rec
    assert rec["summary"] == "test spill"


def test_escalate_retry_exhausted_publishes_bus(tmp_path: Path):
    store = EventStore(tmp_path)
    try:
        # Seed an original event so causation looks realistic.
        orig, _ = store.publish(
            topic="okf/handoff",
            producer_id="grok",
            schema_version="1.0",
            payload={"from": "grok", "to": "agy", "summary": "original work"},
        )
        result = escalate_retry_exhausted(
            store,
            original_event_id=orig.event_id,
            original_event=orig.to_dict(),
            summary=f"Webhook delivery retry exhausted event_id={orig.event_id}",
            producer_id="agentbus",
            causation_id=orig.event_id,
            idempotency_key=f"retry-exhausted:test:{orig.event_id}",
            workspace=tmp_path,
        )
        assert result["ok"] is True
        assert result["channel"] == "bus"
        assert result["event_id"] is not None

        polled = store.poll(DEAD_LETTER_TOPIC, since_id=0)
        assert len(polled["events"]) == 1
        dl = polled["events"][0]
        assert dl["payload"]["reason"] == "RETRY_EXHAUSTED"
        assert dl["payload"]["original_event_id"] == orig.event_id
        assert "retry exhausted" in dl["payload"]["summary"].lower()
    finally:
        store.close()


def test_publish_or_spill_happy_path(tmp_path: Path):
    store = EventStore(tmp_path)
    try:
        res = publish_or_spill(
            store,
            workspace=tmp_path,
            publish_kwargs={
                "topic": "okf/handoff",
                "producer_id": "grok",
                "schema_version": "1.0",
                "payload": {"from": "grok", "to": "agy", "summary": "hi"},
            },
        )
        assert res["ok"] is True
        assert res["event"].event_id >= 1
        assert res["duplicate"] is False
    finally:
        store.close()


def test_publish_or_spill_on_lock_writes_spillover(tmp_path: Path, monkeypatch):
    store = EventStore(tmp_path)

    def always_locked(**_kwargs):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(store, "publish", always_locked)
    try:
        res = publish_or_spill(
            store,
            workspace=tmp_path,
            publish_kwargs={
                "topic": "okf/handoff",
                "producer_id": "grok",
                "schema_version": "1.0",
                "payload": {"from": "grok", "to": "agy", "summary": "ack"},
                "causation_id": 99,
                "idempotency_key": "runner-ack:test:99",
            },
            spill_context={"kind": "runner_ack"},
        )
        assert res["ok"] is False
        assert res["channel"] == "spillover"
        path = Path(res["path"])
        assert path.is_file()
        rec = json.loads(path.read_text(encoding="utf-8").strip().splitlines()[0])
        assert rec["reason"] == "RETRY_EXHAUSTED"
        assert rec["original_event_id"] == 99
    finally:
        store.close()


def test_escalate_falls_back_to_spillover_when_publish_fails(
    tmp_path: Path, monkeypatch
):
    store = MagicMock()
    store.workspace = tmp_path
    store.publish.side_effect = sqlite3.OperationalError("database is locked")

    # Bypass nested retries: force immediate exhaust by making publish always fail
    # and max attempts 1 via env.
    monkeypatch.setenv("AGENTBUS_PUBLISH_MAX_ATTEMPTS", "1")
    monkeypatch.setenv("AGENTBUS_PUBLISH_BASE_DELAY", "0")

    result = escalate_retry_exhausted(
        store,
        original_event_id=7,
        original_event={"event_id": 7},
        summary="test escalate fallback",
        workspace=tmp_path,
    )
    assert result["ok"] is True
    assert result["channel"] == "spillover"
    assert Path(result["path"]).is_file()
