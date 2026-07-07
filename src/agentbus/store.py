"""SQLite-backed event store."""

from __future__ import annotations

import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path


@dataclass
class Event:
    event_id: int
    topic: str
    producer_id: str
    timestamp: str
    schema_version: str
    payload: dict
    causation_id: int | None
    idempotency_key: str | None

    def to_dict(self) -> dict:
        return {
            "event_id": self.event_id,
            "topic": self.topic,
            "producer_id": self.producer_id,
            "timestamp": self.timestamp,
            "schema_version": self.schema_version,
            "payload": self.payload,
            "causation_id": self.causation_id,
            "idempotency_key": self.idempotency_key,
        }


class EventStore:
    def __init__(self, workspace: Path, retention_days: int = 7) -> None:
        self.workspace = workspace.resolve()
        self.retention_days = retention_days
        db_dir = self.workspace / ".agentbus"
        db_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = db_dir / "events.db"
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._init_schema()
        self.prune_expired()

    def _init_schema(self) -> None:
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS events (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                topic TEXT NOT NULL,
                producer_id TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                schema_version TEXT NOT NULL,
                payload TEXT NOT NULL,
                causation_id INTEGER,
                idempotency_key TEXT UNIQUE
            );
            CREATE INDEX IF NOT EXISTS idx_events_topic_id ON events(topic, event_id);
            """
        )
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def prune_expired(self) -> int:
        """Delete events older than retention_days. Returns rows removed."""
        if self.retention_days <= 0:
            return 0
        cutoff = datetime.now(timezone.utc) - timedelta(days=self.retention_days)
        cutoff_str = cutoff.strftime("%Y-%m-%dT%H:%M:%SZ")
        cur = self._conn.execute(
            "DELETE FROM events WHERE timestamp < ?",
            (cutoff_str,),
        )
        self._conn.commit()
        return cur.rowcount

    def publish(
        self,
        *,
        topic: str,
        producer_id: str,
        schema_version: str,
        payload: dict,
        causation_id: int | None = None,
        idempotency_key: str | None = None,
    ) -> tuple[Event, bool]:
        """Return (event, duplicate)."""
        if idempotency_key:
            existing = self._conn.execute(
                "SELECT * FROM events WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
            if existing:
                return self._row_to_event(existing), True

        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        cur = self._conn.execute(
            """
            INSERT INTO events
                (topic, producer_id, timestamp, schema_version, payload,
                 causation_id, idempotency_key)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                topic,
                producer_id,
                ts,
                schema_version,
                json.dumps(payload),
                causation_id,
                idempotency_key,
            ),
        )
        self._conn.commit()
        self.prune_expired()
        row = self._conn.execute(
            "SELECT * FROM events WHERE event_id = ?", (cur.lastrowid,)
        ).fetchone()
        return self._row_to_event(row), False

    def poll(self, topic: str, since_id: int = 0, limit: int = 50) -> dict:
        rows = self._conn.execute(
            """
            SELECT * FROM events
            WHERE topic = ? AND event_id > ?
            ORDER BY event_id ASC
            LIMIT ?
            """,
            (topic, since_id, limit + 1),
        ).fetchall()
        has_more = len(rows) > limit
        rows = rows[:limit]
        events = [self._row_to_event(r).to_dict() for r in rows]
        latest_id = events[-1]["event_id"] if events else since_id
        return {"events": events, "latest_id": latest_id, "has_more": has_more}

    def status(self, producer_id: str | None = None) -> dict:
        count = self._conn.execute("SELECT COUNT(*) AS c FROM events").fetchone()["c"]
        latest = self._conn.execute("SELECT MAX(event_id) AS m FROM events").fetchone()["m"]
        topics = [
            r["topic"]
            for r in self._conn.execute(
                "SELECT DISTINCT topic FROM events ORDER BY topic"
            ).fetchall()
        ]
        return {
            "workspace": str(self.workspace),
            "event_count": count,
            "latest_event_id": latest or 0,
            "topics": topics,
            "retention_days": self.retention_days,
            "producer_id": producer_id or os.environ.get("AGENTBUS_PRODUCER_ID", ""),
        }

    @staticmethod
    def _row_to_event(row: sqlite3.Row) -> Event:
        return Event(
            event_id=row["event_id"],
            topic=row["topic"],
            producer_id=row["producer_id"],
            timestamp=row["timestamp"],
            schema_version=row["schema_version"],
            payload=json.loads(row["payload"]),
            causation_id=row["causation_id"],
            idempotency_key=row["idempotency_key"],
        )