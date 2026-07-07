"""SQLite-backed event store."""

from __future__ import annotations

import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from agentbus.artifacts import extract_artifacts
from agentbus.intercepts import DEFAULT_TTL_MINUTES, hitl_disabled, match_rule
from agentbus.rbac import ForbiddenError, check_approve_rbac, check_publish_rbac
from agentbus.schemas import DEAD_LETTER_TOPIC, validate_payload
from agentbus.tracing import generate_span_id, normalize_parent_span_id, normalize_trace_id

STATUS_PUBLISHED = "PUBLISHED"
STATUS_PENDING = "PENDING_APPROVAL"
STATUS_REJECTED = "REJECTED"
STATUS_TIMEOUT_FAILED = "TIMEOUT_FAILED"
SLA_BREACH_REASON = "SLA_BREACH"


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
    status: str = STATUS_PUBLISHED
    pending_until: str | None = None
    rejection_reason: str | None = None
    sla_timeout_minutes: int | None = None
    sla_deadline: str | None = None
    sla_cleared: bool = False
    trace_id: str | None = None
    span_id: str | None = None
    parent_span_id: str | None = None

    def to_dict(self) -> dict:
        data = {
            "event_id": self.event_id,
            "topic": self.topic,
            "producer_id": self.producer_id,
            "timestamp": self.timestamp,
            "schema_version": self.schema_version,
            "payload": self.payload,
            "causation_id": self.causation_id,
            "idempotency_key": self.idempotency_key,
            "status": self.status,
        }
        if self.pending_until:
            data["pending_until"] = self.pending_until
        if self.rejection_reason:
            data["rejection_reason"] = self.rejection_reason
        if self.sla_timeout_minutes is not None:
            data["sla_timeout_minutes"] = self.sla_timeout_minutes
        if self.sla_deadline:
            data["sla_deadline"] = self.sla_deadline
        if self.sla_cleared:
            data["sla_cleared"] = True
        if self.trace_id:
            data["trace_id"] = self.trace_id
        if self.span_id:
            data["span_id"] = self.span_id
        if self.parent_span_id:
            data["parent_span_id"] = self.parent_span_id
        return data


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
        self._migrate_hitl_columns()
        self._migrate_sla_columns()
        self._migrate_trace_columns()
        self._migrate_artifacts_table()

    def _migrate_artifacts_table(self) -> None:
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS artifacts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id INTEGER NOT NULL,
                type TEXT NOT NULL,
                name TEXT NOT NULL,
                content_blob TEXT NOT NULL,
                FOREIGN KEY (event_id) REFERENCES events(event_id)
            );
            CREATE INDEX IF NOT EXISTS idx_artifacts_event_id ON artifacts(event_id);
            """
        )
        self._conn.commit()

    def _migrate_trace_columns(self) -> None:
        cols = {row[1] for row in self._conn.execute("PRAGMA table_info(events)").fetchall()}
        if "trace_id" not in cols:
            self._conn.execute("ALTER TABLE events ADD COLUMN trace_id TEXT")
        if "span_id" not in cols:
            self._conn.execute("ALTER TABLE events ADD COLUMN span_id TEXT")
        if "parent_span_id" not in cols:
            self._conn.execute("ALTER TABLE events ADD COLUMN parent_span_id TEXT")
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_events_trace_id ON events(trace_id)"
        )
        self._conn.commit()

    def _migrate_sla_columns(self) -> None:
        cols = {row[1] for row in self._conn.execute("PRAGMA table_info(events)").fetchall()}
        if "sla_timeout_minutes" not in cols:
            self._conn.execute("ALTER TABLE events ADD COLUMN sla_timeout_minutes INTEGER")
        if "sla_deadline" not in cols:
            self._conn.execute("ALTER TABLE events ADD COLUMN sla_deadline TEXT")
        if "sla_cleared" not in cols:
            self._conn.execute(
                "ALTER TABLE events ADD COLUMN sla_cleared INTEGER NOT NULL DEFAULT 0"
            )
        self._conn.commit()

    def _migrate_hitl_columns(self) -> None:
        cols = {row[1] for row in self._conn.execute("PRAGMA table_info(events)").fetchall()}
        if "status" not in cols:
            self._conn.execute(
                f"ALTER TABLE events ADD COLUMN status TEXT NOT NULL DEFAULT '{STATUS_PUBLISHED}'"
            )
        if "pending_until" not in cols:
            self._conn.execute("ALTER TABLE events ADD COLUMN pending_until TEXT")
        if "rejection_reason" not in cols:
            self._conn.execute("ALTER TABLE events ADD COLUMN rejection_reason TEXT")
        if "projected_to_log" not in cols:
            self._conn.execute(
                "ALTER TABLE events ADD COLUMN projected_to_log INTEGER NOT NULL DEFAULT 0"
            )
            self._conn.execute("UPDATE events SET projected_to_log = 1")
        self._conn.execute(
            f"UPDATE events SET status = '{STATUS_PUBLISHED}' WHERE status IS NULL OR status = ''"
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

    def expire_pending(self) -> list[int]:
        """Auto-reject pending events past pending_until. Returns rejected event ids."""
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        rows = self._conn.execute(
            """
            SELECT event_id FROM events
            WHERE status = ? AND pending_until IS NOT NULL AND pending_until < ?
            """,
            (STATUS_PENDING, now),
        ).fetchall()
        rejected: list[int] = []
        for row in rows:
            result = self.reject_event(
                row["event_id"],
                reviewer_id="hitl",
                reason="auto-rejected: approval TTL expired",
            )
            rejected.append(result["event_id"])
        return rejected

    def expire_sla_breaches(self) -> list[int]:
        """Mark SLA-expired events TIMEOUT_FAILED and publish okf/dead-letter escalations."""
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        rows = self._conn.execute(
            """
            SELECT * FROM events
            WHERE status = ? AND sla_cleared = 0 AND sla_deadline IS NOT NULL
              AND sla_deadline < ?
            ORDER BY event_id ASC
            """,
            (STATUS_PUBLISHED, now),
        ).fetchall()
        timed_out: list[int] = []
        for row in rows:
            event = self._row_to_event(row)
            self._conn.execute(
                "UPDATE events SET status = ? WHERE event_id = ?",
                (STATUS_TIMEOUT_FAILED, event.event_id),
            )
            self._conn.commit()
            dead_letter_payload = validate_payload(
                DEAD_LETTER_TOPIC,
                {
                    "reason": SLA_BREACH_REASON,
                    "original_event_id": event.event_id,
                    "original_event": event.to_dict(),
                    "summary": (
                        f"SLA breach: no response within "
                        f"{event.sla_timeout_minutes}m for event {event.event_id}"
                    ),
                },
            )
            self.publish(
                topic=DEAD_LETTER_TOPIC,
                producer_id="agentbus",
                schema_version=event.schema_version,
                payload=dead_letter_payload,
                causation_id=event.event_id,
                skip_intercept=True,
                skip_rbac=True,
            )
            timed_out.append(event.event_id)
        return timed_out

    def _clear_sla(self, event_id: int) -> None:
        self._conn.execute(
            """
            UPDATE events SET sla_cleared = 1
            WHERE event_id = ? AND sla_cleared = 0 AND sla_deadline IS NOT NULL
            """,
            (event_id,),
        )
        self._conn.commit()

    def _sla_deadline_from_now(self, minutes: int) -> str:
        return (
            datetime.now(timezone.utc) + timedelta(minutes=minutes)
        ).strftime("%Y-%m-%dT%H:%M:%SZ")

    def publish(
        self,
        *,
        topic: str,
        producer_id: str,
        schema_version: str,
        payload: dict,
        causation_id: int | None = None,
        idempotency_key: str | None = None,
        status: str | None = None,
        pending_until: str | None = None,
        skip_intercept: bool = False,
        auth_token: str | None = None,
        skip_rbac: bool = False,
        sla_timeout_minutes: int | None = None,
        trace_id: str | None = None,
        parent_span_id: str | None = None,
    ) -> tuple[Event, bool]:
        """Return (event, duplicate)."""
        if idempotency_key:
            existing = self._conn.execute(
                "SELECT * FROM events WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
            if existing:
                return self._hydrate_event(self._row_to_event(existing)), True

        stored_payload, artifacts = extract_artifacts(payload)

        if not skip_rbac:
            check_publish_rbac(
                self.workspace,
                producer_id=producer_id,
                topic=topic,
                payload=stored_payload,
                auth_token=auth_token,
            )

        if causation_id is not None:
            self._clear_sla(causation_id)

        if sla_timeout_minutes is not None:
            if sla_timeout_minutes < 1:
                raise ValueError("invalid_sla_timeout_minutes: must be >= 1")

        trace_id = normalize_trace_id(trace_id)
        parent_span_id = normalize_parent_span_id(parent_span_id)
        span_id = generate_span_id()

        event_status = status or STATUS_PUBLISHED
        event_pending_until = pending_until

        if not skip_intercept and event_status == STATUS_PUBLISHED:
            rule = match_rule(self.workspace, topic, stored_payload)
            if rule:
                event_status = STATUS_PENDING
                ttl = timedelta(minutes=rule.ttl_minutes or DEFAULT_TTL_MINUTES)
                event_pending_until = (
                    datetime.now(timezone.utc) + ttl
                ).strftime("%Y-%m-%dT%H:%M:%SZ")

        sla_deadline = None
        if sla_timeout_minutes is not None and event_status == STATUS_PUBLISHED:
            sla_deadline = self._sla_deadline_from_now(sla_timeout_minutes)

        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        cur = self._conn.execute(
            """
            INSERT INTO events
                (topic, producer_id, timestamp, schema_version, payload,
                 causation_id, idempotency_key, status, pending_until,
                 sla_timeout_minutes, sla_deadline, sla_cleared,
                 trace_id, span_id, parent_span_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?)
            """,
            (
                topic,
                producer_id,
                ts,
                schema_version,
                json.dumps(stored_payload),
                causation_id,
                idempotency_key,
                event_status,
                event_pending_until,
                sla_timeout_minutes,
                sla_deadline,
                trace_id,
                span_id,
                parent_span_id,
            ),
        )
        event_id = cur.lastrowid
        self._save_artifacts(event_id, artifacts)
        self._conn.commit()
        self.prune_expired()
        row = self._conn.execute(
            "SELECT * FROM events WHERE event_id = ?", (event_id,)
        ).fetchone()
        event = self._row_to_event(row)
        return self._hydrate_event(event), False

    def _save_artifacts(self, event_id: int, artifacts: list[dict]) -> None:
        for art in artifacts:
            self._conn.execute(
                """
                INSERT INTO artifacts (event_id, type, name, content_blob)
                VALUES (?, ?, ?, ?)
                """,
                (event_id, art["type"], art["name"], art["content"]),
            )

    def _fetch_artifacts(self, event_ids: list[int]) -> dict[int, list[dict]]:
        if not event_ids:
            return {}
        placeholders = ",".join("?" for _ in event_ids)
        rows = self._conn.execute(
            f"""
            SELECT event_id, type, name, content_blob
            FROM artifacts
            WHERE event_id IN ({placeholders})
            ORDER BY id ASC
            """,
            event_ids,
        ).fetchall()
        result: dict[int, list[dict]] = {}
        for row in rows:
            result.setdefault(row["event_id"], []).append(
                {
                    "type": row["type"],
                    "name": row["name"],
                    "content": row["content_blob"],
                }
            )
        return result

    def _hydrate_event(self, event: Event) -> Event:
        arts = self._fetch_artifacts([event.event_id]).get(event.event_id)
        if arts:
            event.payload = {**event.payload, "artifacts": arts}
        return event

    def _hydrate_event_dicts(self, events: list[dict]) -> list[dict]:
        by_event = self._fetch_artifacts([e["event_id"] for e in events])
        hydrated: list[dict] = []
        for ev in events:
            data = dict(ev)
            arts = by_event.get(ev["event_id"])
            if arts:
                data["payload"] = {**data["payload"], "artifacts": arts}
            hydrated.append(data)
        return hydrated

    def poll(self, topic: str, since_id: int = 0, limit: int = 50) -> dict:
        self.expire_pending()
        self.expire_sla_breaches()
        rows = self._conn.execute(
            """
            SELECT * FROM events
            WHERE topic = ? AND event_id > ? AND status = ?
            ORDER BY event_id ASC
            LIMIT ?
            """,
            (topic, since_id, STATUS_PUBLISHED, limit + 1),
        ).fetchall()
        has_more = len(rows) > limit
        rows = rows[:limit]
        events = self._hydrate_event_dicts(
            [self._row_to_event(r).to_dict() for r in rows]
        )
        latest_id = events[-1]["event_id"] if events else since_id
        return {"events": events, "latest_id": latest_id, "has_more": has_more}

    def review_pending(self, topic: str | None = None, limit: int = 50) -> list[dict]:
        self.expire_pending()
        self.expire_sla_breaches()
        if topic:
            rows = self._conn.execute(
                """
                SELECT * FROM events
                WHERE status = ? AND topic = ?
                ORDER BY event_id ASC
                LIMIT ?
                """,
                (STATUS_PENDING, topic, limit),
            ).fetchall()
        else:
            rows = self._conn.execute(
                """
                SELECT * FROM events
                WHERE status = ?
                ORDER BY event_id ASC
                LIMIT ?
                """,
                (STATUS_PENDING, limit),
            ).fetchall()
        return [self._row_to_event(r).to_dict() for r in rows]

    def fetch_trace_events(self, trace_id: str) -> list[dict]:
        rows = self._conn.execute(
            """
            SELECT * FROM events
            WHERE trace_id = ?
            ORDER BY event_id ASC
            """,
            (trace_id,),
        ).fetchall()
        return [self._row_to_event(r).to_dict() for r in rows]

    def get_event(self, event_id: int) -> Event | None:
        row = self._conn.execute(
            "SELECT * FROM events WHERE event_id = ?", (event_id,)
        ).fetchone()
        if not row:
            return None
        return self._row_to_event(row)

    def approve_event(
        self,
        event_id: int,
        reviewer_id: str,
        *,
        auth_token: str | None = None,
    ) -> dict:
        check_approve_rbac(
            self.workspace,
            reviewer_id=reviewer_id,
            auth_token=auth_token,
        )
        self.expire_pending()
        row = self._conn.execute(
            "SELECT * FROM events WHERE event_id = ?", (event_id,)
        ).fetchone()
        if not row:
            raise ValueError(f"event_not_found: {event_id}")
        event = self._row_to_event(row)
        if event.status != STATUS_PENDING:
            raise ValueError(f"event_not_pending: {event_id} status={event.status}")

        updates = {
            "status": STATUS_PUBLISHED,
            "pending_until": None,
            "rejection_reason": None,
        }
        if event.sla_timeout_minutes and not event.sla_deadline:
            updates["sla_deadline"] = self._sla_deadline_from_now(
                event.sla_timeout_minutes
            )
        self._conn.execute(
            """
            UPDATE events
            SET status = ?, pending_until = NULL, rejection_reason = NULL,
                sla_deadline = COALESCE(?, sla_deadline)
            WHERE event_id = ?
            """,
            (
                STATUS_PUBLISHED,
                updates.get("sla_deadline"),
                event_id,
            ),
        )
        self._conn.commit()
        updated = self.get_event(event_id)
        assert updated is not None
        return {
            "event_id": event_id,
            "status": STATUS_PUBLISHED,
            "reviewer_id": reviewer_id,
            "event": updated.to_dict(),
        }

    def reject_event(
        self,
        event_id: int,
        reviewer_id: str,
        reason: str = "rejected by human reviewer",
        *,
        auth_token: str | None = None,
    ) -> dict:
        check_approve_rbac(
            self.workspace,
            reviewer_id=reviewer_id,
            auth_token=auth_token,
        )
        row = self._conn.execute(
            "SELECT * FROM events WHERE event_id = ?", (event_id,)
        ).fetchone()
        if not row:
            raise ValueError(f"event_not_found: {event_id}")
        event = self._row_to_event(row)
        if event.status not in (STATUS_PENDING,):
            raise ValueError(f"event_not_pending: {event_id} status={event.status}")

        self._conn.execute(
            """
            UPDATE events
            SET status = ?, pending_until = NULL, rejection_reason = ?
            WHERE event_id = ?
            """,
            (STATUS_REJECTED, reason, event_id),
        )
        self._conn.commit()

        notice_payload = {
            "from": "hitl",
            "to": event.producer_id,
            "summary": (
                f"REJECTED event {event_id} on {event.topic}: {reason}. "
                f"Original: {event.payload.get('summary', '')[:200]}"
            ),
            "links": [f"event://{event_id}"],
        }
        notice, _ = self.publish(
            topic="okf/handoff",
            producer_id=reviewer_id,
            schema_version=event.schema_version,
            payload=notice_payload,
            causation_id=event_id,
            skip_intercept=True,
            skip_rbac=True,
        )
        return {
            "event_id": event_id,
            "status": STATUS_REJECTED,
            "reviewer_id": reviewer_id,
            "reason": reason,
            "rejection_notice_event_id": notice.event_id,
        }

    def fetch_unprojected_handoffs(self, limit: int = 100) -> list[Event]:
        rows = self._conn.execute(
            """
            SELECT * FROM events
            WHERE topic = 'okf/handoff' AND status = ? AND projected_to_log = 0
            ORDER BY event_id ASC
            LIMIT ?
            """,
            (STATUS_PUBLISHED, limit),
        ).fetchall()
        return [self._row_to_event(r) for r in rows]

    def mark_projected(self, event_ids: list[int]) -> None:
        if not event_ids:
            return
        placeholders = ",".join("?" for _ in event_ids)
        self._conn.execute(
            f"UPDATE events SET projected_to_log = 1 WHERE event_id IN ({placeholders})",
            event_ids,
        )
        self._conn.commit()

    def status(self, producer_id: str | None = None) -> dict:
        self.expire_pending()
        self.expire_sla_breaches()
        count = self._conn.execute("SELECT COUNT(*) AS c FROM events").fetchone()["c"]
        latest = self._conn.execute("SELECT MAX(event_id) AS m FROM events").fetchone()["m"]
        pending = self._conn.execute(
            "SELECT COUNT(*) AS c FROM events WHERE status = ?",
            (STATUS_PENDING,),
        ).fetchone()["c"]
        sla_active = self._conn.execute(
            """
            SELECT COUNT(*) AS c FROM events
            WHERE status = ? AND sla_cleared = 0 AND sla_deadline IS NOT NULL
            """,
            (STATUS_PUBLISHED,),
        ).fetchone()["c"]
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
            "pending_approval_count": pending,
            "sla_active_count": sla_active,
            "hitl_enabled": not hitl_disabled(),
            "topics": topics,
            "retention_days": self.retention_days,
            "producer_id": producer_id or os.environ.get("AGENTBUS_PRODUCER_ID", ""),
        }

    @staticmethod
    def _row_to_event(row: sqlite3.Row) -> Event:
        keys = row.keys()
        return Event(
            event_id=row["event_id"],
            topic=row["topic"],
            producer_id=row["producer_id"],
            timestamp=row["timestamp"],
            schema_version=row["schema_version"],
            payload=json.loads(row["payload"]),
            causation_id=row["causation_id"],
            idempotency_key=row["idempotency_key"],
            status=row["status"] if "status" in keys else STATUS_PUBLISHED,
            pending_until=row["pending_until"] if "pending_until" in keys else None,
            rejection_reason=row["rejection_reason"] if "rejection_reason" in keys else None,
            sla_timeout_minutes=(
                row["sla_timeout_minutes"] if "sla_timeout_minutes" in keys else None
            ),
            sla_deadline=row["sla_deadline"] if "sla_deadline" in keys else None,
            sla_cleared=bool(row["sla_cleared"]) if "sla_cleared" in keys else False,
            trace_id=row["trace_id"] if "trace_id" in keys else None,
            span_id=row["span_id"] if "span_id" in keys else None,
            parent_span_id=row["parent_span_id"] if "parent_span_id" in keys else None,
        )