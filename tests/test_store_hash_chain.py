"""AgentBus-Fast hash-chain and constant-time projection contracts."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone

from click.testing import CliRunner

from agentbus.cli import main
from agentbus.store import STATUS_PENDING, EventStore


def _publish(store: EventStore, summary: str, **kwargs):
    return store.publish(
        topic="okf/handoff",
        producer_id="codex",
        schema_version="1.0",
        payload={"from": "codex", "to": "agy", "summary": summary},
        **kwargs,
    )[0]


def test_forward_chain_verifies_and_pinpoints_payload_tamper(tmp_path):
    store = EventStore(tmp_path, auto_prune=False)
    _publish(store, "one")
    second = _publish(store, "two")
    third = _publish(store, "three")

    report = store.verify_chain()
    assert report["ok"] is True
    assert report["events_checked"] == 3
    rows = store._conn.execute(
        "SELECT event_id, prev_hash, hash FROM events ORDER BY event_id"
    ).fetchall()
    assert rows[0]["prev_hash"] == "0" * 64
    assert rows[1]["prev_hash"] == rows[0]["hash"]
    assert rows[2]["prev_hash"] == rows[1]["hash"]

    payload = json.dumps({"from": "codex", "to": "agy", "summary": "TWO"})
    store._conn.execute(
        "UPDATE events SET payload = ? WHERE event_id = ?", (payload, second.event_id)
    )
    store._conn.commit()
    report = store.verify_chain()
    assert report["ok"] is False
    assert report["divergence"]["event_id"] == second.event_id
    assert report["divergence"]["reason"] == "hash_mismatch"
    assert report["last_event_id"] == third.event_id
    store.close()


def test_chain_reports_deleted_sequence_and_broken_predecessor(tmp_path):
    store = EventStore(tmp_path, auto_prune=False)
    _publish(store, "one")
    second = _publish(store, "two")
    third = _publish(store, "three")
    store._conn.execute("DELETE FROM events WHERE event_id = ?", (second.event_id,))
    store._conn.commit()

    report = store.verify_chain()
    assert report["ok"] is False
    assert report["gaps"] == [
        {"expected_event_id": second.event_id, "found_event_id": third.event_id}
    ]
    assert report["divergence"]["event_id"] == third.event_id
    assert report["divergence"]["reason"] == "prev_hash_mismatch"
    store.close()


def test_legacy_cutover_preserves_rows_and_anchors_checkpoint(tmp_path):
    db_dir = tmp_path / ".agentbus"
    db_dir.mkdir()
    conn = sqlite3.connect(db_dir / "events.db")
    conn.execute(
        """
        CREATE TABLE events (
            event_id INTEGER PRIMARY KEY AUTOINCREMENT,
            topic TEXT NOT NULL,
            producer_id TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            schema_version TEXT NOT NULL,
            payload TEXT NOT NULL,
            causation_id INTEGER,
            idempotency_key TEXT UNIQUE
        )
        """
    )
    conn.execute(
        "INSERT INTO events(topic, producer_id, timestamp, schema_version, payload) "
        "VALUES('okf/handoff', 'legacy', '2026-01-01T00:00:00Z', '1.0', ?)",
        (json.dumps({"from": "legacy", "to": "agy", "summary": "old"}),),
    )
    conn.commit()
    conn.close()

    store = EventStore(tmp_path, retention_days=0, auto_prune=False)
    checkpoint = store.verify_chain()["checkpoint"]
    assert checkpoint["event_id"] == 1
    assert checkpoint["legacy"] is True
    legacy_hashes = store._conn.execute(
        "SELECT prev_hash, hash FROM events WHERE event_id = 1"
    ).fetchone()
    assert tuple(legacy_hashes) == (None, None)

    new = _publish(store, "new")
    row = store._conn.execute(
        "SELECT prev_hash, hash FROM events WHERE event_id = ?", (new.event_id,)
    ).fetchone()
    assert row["prev_hash"] == checkpoint["hash"]
    assert store.verify_chain()["ok"] is True

    store._conn.execute("UPDATE events SET producer_id = 'tampered' WHERE event_id = 1")
    store._conn.commit()
    full = store.verify_chain()
    assert full["ok"] is False
    assert full["divergence"]["reason"] == "checkpoint_hash_mismatch"
    assert store.verify_chain(since_checkpoint=True)["ok"] is True
    store.close()


def test_status_uses_transactional_counters_and_tracks_workflow_changes(tmp_path):
    store = EventStore(tmp_path, auto_prune=False)
    _publish(store, "published")
    pending = _publish(
        store,
        "pending",
        status=STATUS_PENDING,
        skip_intercept=True,
        sla_timeout_minutes=5,
    )
    status = store.status()
    assert status["event_count"] == 2
    assert status["pending_count"] == 1
    assert status["sla_active_count"] == 0
    assert status["topics"] == ["okf/handoff"]

    store.approve_event(pending.event_id, reviewer_id="agy")
    status = store.status()
    assert status["pending_count"] == 0
    assert status["sla_active_count"] == 1

    statements: list[str] = []
    store._conn.set_trace_callback(statements.append)
    store.status()
    store._conn.set_trace_callback(None)
    assert not any("FROM EVENTS" in statement.upper() for statement in statements)
    store.close()


def test_prune_advances_checkpoint_and_rebuilds_counters(tmp_path):
    store = EventStore(tmp_path, retention_days=7, auto_prune=False)
    first = _publish(store, "old")
    second = _publish(store, "new")
    old_ts = (datetime.now(timezone.utc) - timedelta(days=10)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    store._conn.execute(
        "UPDATE events SET timestamp = ? WHERE event_id = ?", (old_ts, first.event_id)
    )
    store._conn.commit()

    assert store.prune_expired() == 1
    status = store.status()
    assert status["event_count"] == 1
    assert status["latest_event_id"] == second.event_id
    report = store.verify_chain()
    assert report["ok"] is True
    assert report["checkpoint"]["event_id"] == first.event_id
    store.close()


def test_verify_cli_returns_one_and_exact_divergence(tmp_path):
    store = EventStore(tmp_path, auto_prune=False)
    event = _publish(store, "original")
    store._conn.execute(
        "UPDATE events SET topic = 'system/fs' WHERE event_id = ?", (event.event_id,)
    )
    store._conn.commit()
    store.close()

    result = CliRunner().invoke(main, ["verify", "--workspace", str(tmp_path)])
    assert result.exit_code == 1
    report = json.loads(result.output)
    assert report["ok"] is False
    assert report["divergence"]["event_id"] == event.event_id
    assert report["divergence"]["reason"] == "hash_mismatch"
