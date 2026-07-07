"""Retention pruning tests."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from agentbus.store import EventStore


def test_prune_expired_removes_old_events(tmp_path):
    store = EventStore(tmp_path, retention_days=7)
    conn = store._conn
    old_ts = (datetime.now(timezone.utc) - timedelta(days=10)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    conn.execute(
        """
        INSERT INTO events
            (topic, producer_id, timestamp, schema_version, payload,
             causation_id, idempotency_key)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "okf/handoff",
            "grok",
            old_ts,
            "1.0",
            '{"from":"grok","to":"agy","summary":"old"}',
            None,
            None,
        ),
    )
    conn.commit()

    removed = store.prune_expired()
    assert removed == 1
    assert store.poll("okf/handoff", since_id=0)["events"] == []
    store.close()