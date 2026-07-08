"""Event store unit tests."""

from __future__ import annotations

import pytest

from agentbus.schemas import validate_payload
from agentbus.store import EventStore


@pytest.fixture
def store(tmp_path):
    s = EventStore(tmp_path)
    yield s
    s.close()


def test_publish_and_poll(store):
    payload = {"from": "grok", "to": "agy", "summary": "hello"}
    validate_payload("okf/handoff", payload)
    event, dup = store.publish(
        topic="okf/handoff",
        producer_id="grok",
        schema_version="1.0",
        payload=payload,
    )
    assert not dup
    assert event.event_id == 1

    result = store.poll("okf/handoff", since_id=0)
    assert len(result["events"]) == 1
    assert result["latest_id"] == 1
    assert result["has_more"] is False


def test_content_dedup_within_window(store):
    payload = {"from": "grok", "to": "hermes", "summary": "same content"}
    e1, dup1 = store.publish(
        topic="okf/handoff",
        producer_id="grok",
        schema_version="1.0",
        payload=payload,
    )
    e2, dup2 = store.publish(
        topic="okf/handoff",
        producer_id="grok",
        schema_version="1.0",
        payload=payload,
    )
    assert not dup1
    assert dup2
    assert e1.event_id == e2.event_id


def test_status_aliases(store):
    payload = {"from": "grok", "to": "agy", "summary": "status aliases"}
    store.publish(
        topic="okf/handoff",
        producer_id="grok",
        schema_version="1.0",
        payload=payload,
    )
    st = store.status()
    assert st["event_count"] == st["total_events"] == 1
    assert st["pending_approval_count"] == st["pending_count"] == 0


def test_idempotency(store):
    payload = {"from": "grok", "to": "hermes", "summary": "dup test"}
    e1, _ = store.publish(
        topic="okf/handoff",
        producer_id="grok",
        schema_version="1.0",
        payload=payload,
        idempotency_key="key-1",
    )
    e2, dup = store.publish(
        topic="okf/handoff",
        producer_id="grok",
        schema_version="1.0",
        payload=payload,
        idempotency_key="key-1",
    )
    assert dup
    assert e1.event_id == e2.event_id


def test_poll_empty(store):
    result = store.poll("okf/handoff", since_id=0)
    assert result["events"] == []
    assert result["latest_id"] == 0


def test_invalid_payload_rejected():
    with pytest.raises(ValueError, match="invalid_payload"):
        validate_payload("okf/handoff", {"from": "grok"})