"""SLA timeout tests — PDD v0.4 acceptance criteria."""

from __future__ import annotations

import pytest

from agentbus.schemas import DEAD_LETTER_TOPIC, validate_payload
from agentbus.store import STATUS_PUBLISHED, STATUS_TIMEOUT_FAILED, EventStore


@pytest.fixture
def store(tmp_path):
    s = EventStore(tmp_path)
    yield s
    s.close()


def _handoff(summary: str, **kwargs) -> dict:
    return validate_payload(
        "okf/handoff",
        {"from": "grok", "to": "hermes", "summary": summary, **kwargs},
    )


def _backdate_sla_deadline(store: EventStore, event_id: int, deadline: str) -> None:
    store._conn.execute(
        "UPDATE events SET sla_deadline = ? WHERE event_id = ?",
        (deadline, event_id),
    )
    store._conn.commit()


def test_sla_breach_creates_dead_letter(store):
    """PDD AC1: expired SLA marks TIMEOUT_FAILED and publishes okf/dead-letter."""
    event, _ = store.publish(
        topic="okf/handoff",
        producer_id="grok",
        schema_version="1.0",
        payload=_handoff("Run QA validation"),
        sla_timeout_minutes=1,
    )
    assert event.sla_deadline is not None

    _backdate_sla_deadline(store, event.event_id, "2020-01-01T00:00:00Z")
    timed_out = store.expire_sla_breaches()
    assert event.event_id in timed_out

    updated = store.get_event(event.event_id)
    assert updated is not None
    assert updated.status == STATUS_TIMEOUT_FAILED

    dead_letters = store.poll(DEAD_LETTER_TOPIC, since_id=0)
    assert len(dead_letters["events"]) == 1
    dl = dead_letters["events"][0]
    assert dl["payload"]["reason"] == "SLA_BREACH"
    assert dl["payload"]["original_event_id"] == event.event_id
    assert dl["causation_id"] == event.event_id

    handoffs = store.poll("okf/handoff", since_id=0)
    assert handoffs["events"] == []


def test_sla_clearance_on_causation_reply(store):
    """PDD AC2: timely causation_id reply clears SLA — no breach at deadline."""
    parent, _ = store.publish(
        topic="okf/handoff",
        producer_id="grok",
        schema_version="1.0",
        payload=_handoff("Assign QA task"),
        sla_timeout_minutes=5,
    )
    store.publish(
        topic="okf/handoff",
        producer_id="hermes",
        schema_version="1.0",
        payload=validate_payload(
            "okf/handoff",
            {"from": "hermes", "to": "grok", "summary": "QA complete"},
        ),
        causation_id=parent.event_id,
    )

    updated = store.get_event(parent.event_id)
    assert updated is not None
    assert updated.sla_cleared is True

    _backdate_sla_deadline(store, parent.event_id, "2020-01-01T00:00:00Z")
    timed_out = store.expire_sla_breaches()
    assert parent.event_id not in timed_out
    assert store.get_event(parent.event_id).status == STATUS_PUBLISHED
    assert store.poll(DEAD_LETTER_TOPIC, since_id=0)["events"] == []


def test_poll_triggers_sla_expiry(store):
    event, _ = store.publish(
        topic="okf/handoff",
        producer_id="grok",
        schema_version="1.0",
        payload=_handoff("Ghost task"),
        sla_timeout_minutes=1,
    )
    _backdate_sla_deadline(store, event.event_id, "2020-01-01T00:00:00Z")
    store.poll("okf/handoff", since_id=0)
    assert store.get_event(event.event_id).status == STATUS_TIMEOUT_FAILED


def test_invalid_sla_minutes_rejected(store):
    with pytest.raises(ValueError, match="invalid_sla_timeout_minutes"):
        store.publish(
            topic="okf/handoff",
            producer_id="grok",
            schema_version="1.0",
            payload=_handoff("bad sla"),
            sla_timeout_minutes=0,
        )