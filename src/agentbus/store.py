"""SQLite-backed event store."""

from __future__ import annotations

import importlib
import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING

from agentbus.artifacts import extract_artifacts
from agentbus.canonical import GENESIS_HASH, event_digest
from agentbus.intercepts import DEFAULT_TTL_MINUTES, hitl_disabled, match_rule
from agentbus.retry import (
    RetryExhaustedError,
    call_with_retry,
    default_publish_policy,
    is_transient_sqlite_error,
)
from agentbus.tracing import (
    generate_span_id,
    normalize_parent_span_id,
    normalize_trace_id,
)

if TYPE_CHECKING:
    from agentbus.mcpsafe import PolicyEnforcer


def _lazy(module: str, name: str):
    def invoke(*args, **kwargs):
        return getattr(importlib.import_module(module), name)(*args, **kwargs)

    invoke.__name__ = name
    return invoke


def _identity_error():
    return getattr(importlib.import_module("agentbus.identity"), "IdentityError")


VerificationResult = _lazy("agentbus.identity", "VerificationResult")
identity_configured = _lazy("agentbus.identity", "configured")
load_trust_state = _lazy("agentbus.identity", "load_trust_state")
prune_expired_delegations = _lazy("agentbus.identity", "prune_expired_delegations")
sign_event_envelope = _lazy("agentbus.identity", "sign_event_envelope")
topic_restricted = _lazy("agentbus.identity", "topic_restricted")
validate_typed_action = _lazy("agentbus.identity", "validate_typed_action")
verify_envelope = _lazy("agentbus.identity", "verify_envelope")
check_approve_rbac = _lazy("agentbus.rbac", "check_approve_rbac")
check_publish_rbac = _lazy("agentbus.rbac", "check_publish_rbac")
validate_payload = _lazy("agentbus.schemas", "validate_payload")

DEAD_LETTER_TOPIC = "okf/dead-letter"

STATUS_PUBLISHED = "PUBLISHED"
STATUS_PENDING = "PENDING_APPROVAL"
STATUS_REJECTED = "REJECTED"
STATUS_TIMEOUT_FAILED = "TIMEOUT_FAILED"
SLA_BREACH_REASON = "SLA_BREACH"
CONTENT_DEDUP_SECONDS = int(os.environ.get("AGENTBUS_CONTENT_DEDUP_SECONDS", "60"))


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
    identity_envelope: dict | None = None
    verification_status: str = "legacy_unverified"
    verification_reason: str | None = None

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
        data["verification_status"] = self.verification_status
        if self.identity_envelope:
            data["identity_envelope"] = self.identity_envelope
        if self.verification_reason:
            data["verification_reason"] = self.verification_reason
        return data


class EventStore:
    def __init__(
        self,
        workspace: Path,
        retention_days: int = 7,
        *,
        auto_prune: bool = True,
    ) -> None:
        from agentbus.workspace_guard import assert_workspace_supported

        self.workspace = assert_workspace_supported(workspace)
        self.retention_days = retention_days
        self._mcpsafe: PolicyEnforcer | None = None
        db_dir = self.workspace / ".agentbus"
        db_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = db_dir / "events.db"
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._configure_pragmas()
        self._init_schema()
        # Monitor/TUI hot paths pass auto_prune=False so a 1s refresh never
        # competes for a write lock (DELETE) under publish storms.
        if auto_prune:
            self.prune_expired()

    def set_mcpsafe(self, enforcer: PolicyEnforcer | None) -> None:
        """Attach or clear optional mcpsafe PolicyEnforcer."""
        self._mcpsafe = enforcer

    def _configure_pragmas(self) -> None:
        """OS-aware SQLite PRAGMAs (Windows avoids WAL under concurrent AV locks).

        Defaults:
        - POSIX: ``WAL`` + ``busy_timeout=5000``
        - Windows: ``MEMORY`` + ``busy_timeout=10000`` (best-effort under EDR/AV;
          weaker durability than DELETE/WAL on hard crash)

        Override with ``AGENTBUS_SQLITE_JOURNAL`` (case-insensitive) to one of:
        ``WAL``, ``MEMORY``, ``DELETE``, ``TRUNCATE``, ``PERSIST``, ``OFF``.
        Optional ``AGENTBUS_SQLITE_BUSY_TIMEOUT`` (milliseconds).
        Prefer a single MCP writer process; PRAGMAs only reduce lock storms.
        """
        self._conn.execute("PRAGMA synchronous = NORMAL")
        self._conn.execute("PRAGMA foreign_keys = ON")
        # Whitelist must match the docstring above.
        _JOURNAL_MODES = frozenset(
            {"WAL", "MEMORY", "DELETE", "TRUNCATE", "PERSIST", "OFF"}
        )
        override = (os.environ.get("AGENTBUS_SQLITE_JOURNAL") or "").strip().upper()
        if override in _JOURNAL_MODES:
            journal = override
        elif os.name == "nt":
            journal = "MEMORY"
        else:
            journal = "WAL"
        try:
            busy = int(os.environ.get("AGENTBUS_SQLITE_BUSY_TIMEOUT") or "0")
        except ValueError:
            busy = 0
        if busy <= 0:
            busy = 10000 if os.name == "nt" else 5000
        self._conn.execute(f"PRAGMA journal_mode = {journal}")
        self._conn.execute(f"PRAGMA busy_timeout = {busy}")
        self._journal_mode = journal
        self._busy_timeout_ms = busy

    def _init_schema(self) -> None:
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS schema_version (
                component TEXT PRIMARY KEY,
                version INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS identity_nonces (
                key_id TEXT NOT NULL,
                nonce TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                event_id INTEGER,
                PRIMARY KEY (key_id, nonce)
            );
            CREATE TABLE IF NOT EXISTS identity_key_high_water (
                key_id TEXT PRIMARY KEY,
                max_timestamp TEXT NOT NULL
            );
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
        self._migrate_identity_columns()
        self._migrate_scoped_idempotency()
        self._migrate_fast_ledger()
        self._conn.execute(
            "INSERT INTO schema_version(component, version) VALUES('event_store', 21) "
            "ON CONFLICT(component) DO UPDATE SET version=MAX(version, 21)"
        )
        self._conn.commit()

    def _migrate_fast_ledger(self) -> None:
        """Add the forward hash chain and materialized status projections.

        Existing rows are deliberately not rewritten. Their highest event id is
        retained as a legacy checkpoint; the first new row chains from its
        deterministic immutable-envelope digest.
        """
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS counters (
                name TEXT PRIMARY KEY,
                value INTEGER NOT NULL
            );
            """
        )
        self._conn.commit()
        self._add_column_if_missing(
            "prev_hash", "ALTER TABLE events ADD COLUMN prev_hash TEXT"
        )
        self._add_column_if_missing("hash", "ALTER TABLE events ADD COLUMN hash TEXT")
        self._conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_events_hash "
            "ON events(hash) WHERE hash IS NOT NULL"
        )
        self._conn.commit()

        self._conn.execute("BEGIN IMMEDIATE")
        try:
            checkpoint = self._meta("checkpoint_event_id")
            if checkpoint is None:
                row = self._conn.execute(
                    "SELECT * FROM events ORDER BY event_id DESC LIMIT 1"
                ).fetchone()
                if row is None:
                    checkpoint_id = 0
                    checkpoint_hash = GENESIS_HASH
                    legacy_checkpoint = "0"
                else:
                    checkpoint_id = int(row["event_id"])
                    checkpoint_hash = event_digest(
                        GENESIS_HASH,
                        self._chain_body_from_row(row, prev_hash=GENESIS_HASH),
                    )
                    legacy_checkpoint = "1"
                self._set_meta("checkpoint_event_id", str(checkpoint_id))
                self._set_meta("checkpoint_hash", checkpoint_hash)
                self._set_meta("checkpoint_legacy", legacy_checkpoint)

            if self._meta("counter_projection_version") != "1":
                self._rebuild_counters_locked()
                self._set_meta("counter_projection_version", "1")
            self._conn.execute("COMMIT")
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    def _meta(self, key: str) -> str | None:
        row = self._conn.execute(
            "SELECT value FROM meta WHERE key = ?", (key,)
        ).fetchone()
        return None if row is None else str(row["value"])

    def _set_meta(self, key: str, value: str) -> None:
        self._conn.execute(
            "INSERT INTO meta(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )

    def _counter(self, name: str, delta: int) -> None:
        if delta == 0:
            return
        self._conn.execute(
            "INSERT INTO counters(name, value) VALUES(?, ?) "
            "ON CONFLICT(name) DO UPDATE SET value = value + excluded.value",
            (name, delta),
        )

    def _set_counter(self, name: str, value: int) -> None:
        self._conn.execute(
            "INSERT INTO counters(name, value) VALUES(?, ?) "
            "ON CONFLICT(name) DO UPDATE SET value = excluded.value",
            (name, int(value)),
        )

    def _rebuild_counters_locked(self) -> None:
        self._conn.execute("DELETE FROM counters")
        row = self._conn.execute(
            """
            SELECT COUNT(*) AS total,
                   COALESCE(MAX(event_id), 0) AS latest,
                   COALESCE(SUM(status = ?), 0) AS pending,
                   COALESCE(SUM(status = ? AND sla_cleared = 0
                                AND sla_deadline IS NOT NULL), 0) AS sla_active
            FROM events
            """,
            (STATUS_PENDING, STATUS_PUBLISHED),
        ).fetchone()
        self._set_counter("total_events", int(row["total"]))
        self._set_counter("latest_event_id", int(row["latest"]))
        self._set_counter("pending_approval", int(row["pending"]))
        self._set_counter("sla_active", int(row["sla_active"]))
        for topic_row in self._conn.execute(
            "SELECT topic, COUNT(*) AS total FROM events GROUP BY topic"
        ).fetchall():
            self._set_counter(f"topic.{topic_row['topic']}", int(topic_row["total"]))

    @staticmethod
    def _chain_body_from_row(
        row: sqlite3.Row, *, prev_hash: str | None = None
    ) -> dict:
        """Return the immutable stored envelope covered by the hash chain.

        Mutable workflow columns (approval status, SLA clearance, and log
        projection state) are intentionally excluded; changing those through
        their supported transitions must not invalidate later events.
        """
        keys = set(row.keys())

        def value(name: str):
            return row[name] if name in keys else None

        return {
            "event_id": int(row["event_id"]),
            "prev_hash": prev_hash if prev_hash is not None else value("prev_hash"),
            "topic": row["topic"],
            "producer_id": row["producer_id"],
            "timestamp": row["timestamp"],
            "schema_version": row["schema_version"],
            # Bind the exact stored JSON text, not only its parsed meaning.
            "payload": row["payload"],
            "causation_id": value("causation_id"),
            "idempotency_key": value("idempotency_key"),
            "scoped_idempotency_key": value("scoped_idempotency_key"),
            "sla_timeout_minutes": value("sla_timeout_minutes"),
            "trace_id": value("trace_id"),
            "span_id": value("span_id"),
            "parent_span_id": value("parent_span_id"),
            "identity_envelope": value("identity_envelope"),
            "verification_status": value("verification_status"),
            "verification_reason": value("verification_reason"),
        }

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
        artifact_cols = {
            row[1]
            for row in self._conn.execute("PRAGMA table_info(artifacts)").fetchall()
        }
        if "sha256" not in artifact_cols:
            self._conn.execute("ALTER TABLE artifacts ADD COLUMN sha256 TEXT")
        if "size_bytes" not in artifact_cols:
            self._conn.execute("ALTER TABLE artifacts ADD COLUMN size_bytes INTEGER")
        self._conn.commit()

    def _migrate_identity_columns(self) -> None:
        self._add_column_if_missing(
            "identity_envelope", "ALTER TABLE events ADD COLUMN identity_envelope TEXT"
        )
        self._add_column_if_missing(
            "verification_status",
            "ALTER TABLE events ADD COLUMN verification_status TEXT NOT NULL "
            "DEFAULT 'legacy_unverified'",
        )
        self._add_column_if_missing(
            "verification_reason",
            "ALTER TABLE events ADD COLUMN verification_reason TEXT",
        )
        self._conn.commit()

    def _migrate_scoped_idempotency(self) -> None:
        self._add_column_if_missing(
            "scoped_idempotency_key",
            "ALTER TABLE events ADD COLUMN scoped_idempotency_key TEXT",
        )
        self._conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_events_producer_idempotency "
            "ON events(producer_id, scoped_idempotency_key) "
            "WHERE scoped_idempotency_key IS NOT NULL"
        )
        self._conn.commit()

    def _add_column_if_missing(self, column: str, ddl: str) -> bool:
        """Idempotent ADD COLUMN; tolerate concurrent migrators (duplicate column)."""
        cols = {
            row[1] for row in self._conn.execute("PRAGMA table_info(events)").fetchall()
        }
        if column in cols:
            return False
        try:
            self._conn.execute(ddl)
            return True
        except sqlite3.OperationalError as exc:
            # Concurrent EventStore opens can both pass the PRAGMA check.
            if "duplicate column" in str(exc).lower():
                return False
            raise

    def _migrate_trace_columns(self) -> None:
        self._add_column_if_missing(
            "trace_id", "ALTER TABLE events ADD COLUMN trace_id TEXT"
        )
        self._add_column_if_missing(
            "span_id", "ALTER TABLE events ADD COLUMN span_id TEXT"
        )
        self._add_column_if_missing(
            "parent_span_id", "ALTER TABLE events ADD COLUMN parent_span_id TEXT"
        )
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_events_trace_id ON events(trace_id)"
        )
        self._conn.commit()

    def _migrate_sla_columns(self) -> None:
        self._add_column_if_missing(
            "sla_timeout_minutes",
            "ALTER TABLE events ADD COLUMN sla_timeout_minutes INTEGER",
        )
        self._add_column_if_missing(
            "sla_deadline", "ALTER TABLE events ADD COLUMN sla_deadline TEXT"
        )
        self._add_column_if_missing(
            "sla_cleared",
            "ALTER TABLE events ADD COLUMN sla_cleared INTEGER NOT NULL DEFAULT 0",
        )
        self._conn.commit()

    def _migrate_hitl_columns(self) -> None:
        self._add_column_if_missing(
            "status",
            f"ALTER TABLE events ADD COLUMN status TEXT NOT NULL DEFAULT '{STATUS_PUBLISHED}'",
        )
        self._add_column_if_missing(
            "pending_until", "ALTER TABLE events ADD COLUMN pending_until TEXT"
        )
        self._add_column_if_missing(
            "rejection_reason", "ALTER TABLE events ADD COLUMN rejection_reason TEXT"
        )
        added_proj = self._add_column_if_missing(
            "projected_to_log",
            "ALTER TABLE events ADD COLUMN projected_to_log INTEGER NOT NULL DEFAULT 0",
        )
        if added_proj:
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
        expired_rows = self._conn.execute(
            "SELECT event_id, hash FROM events WHERE timestamp < ? "
            "ORDER BY event_id ASC",
            (cutoff_str,),
        ).fetchall()
        if expired_rows:
            checkpoint_id = int(self._meta("checkpoint_event_id") or 0)
            next_id = checkpoint_id + 1
            checkpoint_row = None
            for row in expired_rows:
                event_id = int(row["event_id"])
                if event_id < next_id:
                    continue
                if event_id != next_id or row["hash"] is None:
                    break
                checkpoint_row = row
                next_id += 1
            if checkpoint_row is not None:
                max_prunable_id = int(checkpoint_row["event_id"])
            elif all(row["hash"] is None for row in expired_rows):
                max_prunable_id = int(expired_rows[-1]["event_id"])
            else:
                max_prunable_id = checkpoint_id
            prunable_rows = [
                row
                for row in expired_rows
                if int(row["event_id"]) <= max_prunable_id
            ]
            expired_ids = [int(row["event_id"]) for row in prunable_rows]
            if not expired_ids:
                return 0
            placeholders = ",".join("?" for _ in expired_ids)
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._conn.execute(
                    f"DELETE FROM artifacts WHERE event_id IN ({placeholders})",
                    expired_ids,
                )
                cur = self._conn.execute(
                    f"DELETE FROM events WHERE event_id IN ({placeholders})",
                    expired_ids,
                )
                if checkpoint_row is not None:
                    self._set_meta(
                        "checkpoint_event_id", str(checkpoint_row["event_id"])
                    )
                    self._set_meta("checkpoint_hash", str(checkpoint_row["hash"]))
                    self._set_meta("checkpoint_legacy", "0")
                elif all(row["hash"] is None for row in expired_rows) and max_prunable_id > checkpoint_id:
                    self._set_meta("checkpoint_event_id", str(max_prunable_id))
                self._rebuild_counters_locked()
                self._conn.execute("COMMIT")
                return cur.rowcount
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
        return 0

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
            try:
                result = self.reject_event(
                    row["event_id"],
                    reviewer_id="hitl",
                    reason="auto-rejected: approval TTL expired",
                )
            except ValueError as exc:
                if str(exc).startswith("event_not_pending:"):
                    continue
                raise
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
            event = self._authoritative_event_from_row(row)
            if event is None:
                continue
            cur = self._conn.execute(
                """
                UPDATE events SET status = ?
                WHERE event_id = ? AND status = ? AND sla_cleared = 0
                  AND sla_deadline IS NOT NULL AND sla_deadline < ?
                """,
                (STATUS_TIMEOUT_FAILED, event.event_id, STATUS_PUBLISHED, now),
            )
            if cur.rowcount != 1:
                self._conn.commit()
                continue
            self._counter("sla_active", -1)
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
        cur = self._conn.execute(
            """
            UPDATE events SET sla_cleared = 1
            WHERE event_id = ? AND sla_cleared = 0 AND sla_deadline IS NOT NULL
              AND status = ?
            """,
            (event_id, STATUS_PUBLISHED),
        )
        if cur.rowcount:
            self._counter("sla_active", -1)
        self._conn.execute(
            """
            UPDATE events SET sla_cleared = 1
            WHERE event_id = ? AND sla_cleared = 0 AND sla_deadline IS NOT NULL
            """,
            (event_id,),
        )
        self._conn.commit()

    def _sla_deadline_from_now(self, minutes: int) -> str:
        return (datetime.now(timezone.utc) + timedelta(minutes=minutes)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )

    def _find_recent_content_duplicate(
        self,
        topic: str,
        producer_id: str,
        payload: dict,
        *,
        window_seconds: int = CONTENT_DEDUP_SECONDS,
    ) -> sqlite3.Row | None:
        if window_seconds < 1:
            return None
        cutoff = (
            datetime.now(timezone.utc) - timedelta(seconds=window_seconds)
        ).strftime("%Y-%m-%dT%H:%M:%SZ")
        payload_json = json.dumps(payload)
        return self._conn.execute(
            """
            SELECT * FROM events
            WHERE topic = ? AND producer_id = ? AND payload = ?
              AND timestamp >= ? AND status != ?
            ORDER BY event_id DESC LIMIT 1
            """,
            (topic, producer_id, payload_json, cutoff, STATUS_REJECTED),
        ).fetchone()

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
        identity_envelope: dict | None = None,
        action: dict | None = None,
        auto_sign: bool = True,
        signing_key_path: Path | None = None,
    ) -> tuple[Event, bool]:
        """Return (event, duplicate)."""
        stored_payload, artifacts = extract_artifacts(payload)
        if action is not None:
            action = validate_typed_action(action)
            if "action" in stored_payload and stored_payload["action"] != action:
                raise _identity_error()("payload_action_mismatch")

        if sla_timeout_minutes is not None:
            if sla_timeout_minutes < 1:
                raise ValueError("invalid_sla_timeout_minutes: must be >= 1")

        trace_id = normalize_trace_id(trace_id)
        parent_span_id = normalize_parent_span_id(parent_span_id)
        span_id = generate_span_id()

        verification_status = "legacy_unverified"
        verification_reason: str | None = None
        identity_state = None
        if identity_configured(self.workspace):
            # Keep expired child credentials from making every trust-state
            # verification grow with historical delegation traffic.
            prune_expired_delegations(self.workspace)
            identity_state = load_trust_state(self.workspace, update_high_water=True)
            if identity_envelope is None and auto_sign:
                try:
                    identity_envelope = sign_event_envelope(
                        self.workspace,
                        topic=topic,
                        producer_id=producer_id,
                        schema_version=schema_version,
                        payload=stored_payload,
                        artifacts=artifacts,
                        causation_id=causation_id,
                        idempotency_key=idempotency_key,
                        trace_id=trace_id,
                        action=action,
                        private_key_path=signing_key_path,
                    )
                except _identity_error() as exc:
                    verification_reason = str(exc)
            if identity_envelope is not None:
                if action is not None and (
                    not isinstance(identity_envelope.get("signed"), dict)
                    or identity_envelope["signed"].get("action") != action
                ):
                    raise _identity_error()("identity_action_mismatch")
                verification = verify_envelope(
                    self.workspace,
                    identity_envelope,
                    stored_payload=stored_payload,
                    artifacts=artifacts,
                    expected_topic=topic,
                    expected_producer=producer_id,
                    expected_schema_version=schema_version,
                    expected_causation_id=causation_id,
                    expected_idempotency_key=idempotency_key,
                    expected_trace_id=trace_id,
                )
                if verification.verified:
                    verification_status = "verified"
                    verification_reason = None
                else:
                    verification_reason = verification.reason
            if verification_status == "verified" and identity_envelope is not None:
                signed = identity_envelope["signed"]
                replay = self._conn.execute(
                    "SELECT 1 FROM identity_nonces WHERE key_id = ? AND nonce = ?",
                    (str(signed["key_id"]), str(signed["nonce"])),
                ).fetchone()
                if replay is not None:
                    raise _identity_error()("identity_nonce_replay")
            if (
                identity_state.mode in {"protected", "strict"}
                and topic_restricted(topic, identity_state)
                and verification_status != "verified"
            ):
                raise _identity_error()(
                    "403 Forbidden: identity_verification_required: "
                    f"{verification_reason or 'unsigned'}"
                )

        # Authentication precedes authorization and every deduplication lookup.
        # This prevents an unverified caller from probing whether another peer
        # already published a particular idempotency key or payload.
        if not skip_rbac:
            check_publish_rbac(
                self.workspace,
                producer_id=producer_id,
                topic=topic,
                payload=stored_payload,
                auth_token=auth_token,
                identity_verified=verification_status == "verified",
            )

        if self._mcpsafe is not None:
            self._mcpsafe.require_payload(stored_payload)

        existing: sqlite3.Row | None = None
        if idempotency_key:
            existing = self._conn.execute(
                """
                SELECT * FROM events
                WHERE producer_id = ?
                  AND (scoped_idempotency_key = ? OR
                       (scoped_idempotency_key IS NULL AND idempotency_key = ?))
                ORDER BY event_id DESC LIMIT 1
                """,
                (producer_id, idempotency_key, idempotency_key),
            ).fetchone()
        elif verification_status != "verified":
            # Advisory content deduplication predates AgentID and cannot consume
            # or authorize a signed nonce.  Sending an identical signed envelope
            # is a replay, not a successful duplicate; let the transactional
            # identity_nonces constraint decide it below.  A caller that wants
            # signed deduplication must bind an explicit idempotency key into the
            # envelope.
            existing = self._find_recent_content_duplicate(
                topic, producer_id, stored_payload
            )
        if existing is not None:
            # A concurrent publisher may have committed the same signed
            # envelope after the replay check above but before this content
            # deduplication lookup. Re-check the nonce here so content dedup
            # cannot turn a signed replay into an apparent successful publish.
            if verification_status == "verified" and identity_envelope is not None:
                signed = identity_envelope["signed"]
                replay = self._conn.execute(
                    "SELECT 1 FROM identity_nonces WHERE key_id = ? AND nonce = ?",
                    (str(signed["key_id"]), str(signed["nonce"])),
                ).fetchone()
                if replay is not None:
                    raise _identity_error()("identity_nonce_replay")
            duplicate = self._authoritative_event_from_row(existing)
            if duplicate is None:
                raise _identity_error()("403 Forbidden: unverified_duplicate_record")
            return duplicate, True

        if causation_id is not None:
            self._clear_sla(causation_id)

        event_status = status or STATUS_PUBLISHED
        event_pending_until = pending_until

        if not skip_intercept and event_status == STATUS_PUBLISHED:
            rule = match_rule(self.workspace, topic, stored_payload)
            if rule:
                event_status = STATUS_PENDING
                ttl = timedelta(minutes=rule.ttl_minutes or DEFAULT_TTL_MINUTES)
                event_pending_until = (datetime.now(timezone.utc) + ttl).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                )

        if event_status == STATUS_PENDING and event_pending_until is None:
            ttl = timedelta(minutes=DEFAULT_TTL_MINUTES)
            event_pending_until = (datetime.now(timezone.utc) + ttl).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )

        sla_deadline = None
        if sla_timeout_minutes is not None and event_status == STATUS_PUBLISHED:
            sla_deadline = self._sla_deadline_from_now(sla_timeout_minutes)

        # Insert+commit under retry with exp backoff + jitter so lock storms
        # (companion-ACK / concurrent writers) do not fail the first busy_timeout.
        # Only the write is retried — never re-INSERT after a successful commit
        # (would duplicate when idempotency_key is absent).
        concurrent_duplicate_event_id: int | None = None

        class _ConcurrentDuplicate(Exception):
            """Internal: a concurrent publisher committed this idempotency key first."""

        def _insert_commit() -> int:
            try:
                # Serialize writers before selecting the current hash head.
                self._conn.execute("BEGIN IMMEDIATE")
                ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                checkpoint_id = int(self._meta("checkpoint_event_id") or 0)
                checkpoint_hash = self._meta("checkpoint_hash") or GENESIS_HASH
                head = self._conn.execute(
                    "SELECT event_id, hash FROM events "
                    "WHERE event_id > ? ORDER BY event_id DESC LIMIT 1",
                    (checkpoint_id,),
                ).fetchone()
                prev_hash = (
                    str(head["hash"])
                    if head is not None and head["hash"] is not None
                    else checkpoint_hash
                )
                identity_key_id = None
                identity_nonce = None
                identity_timestamp = None
                if verification_status == "verified" and identity_envelope is not None:
                    signed = identity_envelope["signed"]
                    identity_key_id = str(signed["key_id"])
                    identity_nonce = str(signed["nonce"])
                    identity_timestamp = str(signed["timestamp"])
                    high = self._conn.execute(
                        "SELECT max_timestamp FROM identity_key_high_water "
                        "WHERE key_id = ?",
                        (identity_key_id,),
                    ).fetchone()
                    if high is not None and identity_timestamp < high["max_timestamp"]:
                        raise _identity_error()("event_timestamp_rollback")
                    try:
                        self._conn.execute(
                            "INSERT INTO identity_nonces(key_id, nonce, timestamp) "
                            "VALUES (?, ?, ?)",
                            (identity_key_id, identity_nonce, identity_timestamp),
                        )
                    except sqlite3.IntegrityError as exc:
                        raise _identity_error()("identity_nonce_replay") from exc
                cur = self._conn.execute(
                    """
                    INSERT INTO events
                        (topic, producer_id, timestamp, schema_version, payload,
                         causation_id, idempotency_key, scoped_idempotency_key,
                         status, pending_until,
                         sla_timeout_minutes, sla_deadline, sla_cleared,
                         trace_id, span_id, parent_span_id, identity_envelope,
                         verification_status, verification_reason, prev_hash, hash)
                    VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?, NULL)
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
                        json.dumps(identity_envelope) if identity_envelope else None,
                        verification_status,
                        verification_reason,
                        prev_hash,
                    ),
                )
                event_id = int(cur.lastrowid)
                inserted = self._conn.execute(
                    "SELECT * FROM events WHERE event_id = ?", (event_id,)
                ).fetchone()
                assert inserted is not None
                digest = event_digest(prev_hash, self._chain_body_from_row(inserted))
                self._conn.execute(
                    "UPDATE events SET hash = ? WHERE event_id = ?",
                    (digest, event_id),
                )
                if identity_key_id is not None:
                    self._conn.execute(
                        "UPDATE identity_nonces SET event_id = ? "
                        "WHERE key_id = ? AND nonce = ?",
                        (event_id, identity_key_id, identity_nonce),
                    )
                    self._conn.execute(
                        "INSERT INTO identity_key_high_water(key_id, max_timestamp) "
                        "VALUES (?, ?) ON CONFLICT(key_id) DO UPDATE SET "
                        "max_timestamp=MAX(max_timestamp, excluded.max_timestamp)",
                        (identity_key_id, identity_timestamp),
                    )
                self._counter("total_events", 1)
                self._set_counter("latest_event_id", event_id)
                self._counter(f"topic.{topic}", 1)
                if event_status == STATUS_PENDING:
                    self._counter("pending_approval", 1)
                if (
                    event_status == STATUS_PUBLISHED
                    and sla_deadline is not None
                ):
                    self._counter("sla_active", 1)
                self._save_artifacts(event_id, artifacts)
                self._conn.commit()
                return event_id
            except sqlite3.IntegrityError as exc:
                # A concurrent publisher may have committed the same
                # (producer_id, scoped_idempotency_key) after this
                # transaction's dedup precheck.  That is a benign duplicate,
                # not an insertion failure: roll back, resolve the winning
                # event, and report it as a duplicate.  Any IntegrityError
                # that is not explained by a concurrent winner is re-raised.
                try:
                    self._conn.rollback()
                except sqlite3.Error:
                    pass
                winner: sqlite3.Row | None = None
                if idempotency_key:
                    winner = self._conn.execute(
                        """
                        SELECT event_id FROM events
                        WHERE producer_id = ?
                          AND (scoped_idempotency_key = ? OR
                               (scoped_idempotency_key IS NULL AND idempotency_key = ?))
                        ORDER BY event_id DESC LIMIT 1
                        """,
                        (producer_id, idempotency_key, idempotency_key),
                    ).fetchone()
                if winner is not None:
                    raise _ConcurrentDuplicate(int(winner["event_id"])) from exc
                raise
            except Exception:
                try:
                    self._conn.rollback()
                except sqlite3.Error:
                    pass
                raise

        try:
            event_id = call_with_retry(
                _insert_commit,
                policy=default_publish_policy(),
                is_retryable=is_transient_sqlite_error,
            )
        except _ConcurrentDuplicate as exc:
            concurrent_duplicate_event_id = exc.args[0]
        except RetryExhaustedError as exc:
            # Re-raise the underlying SQLite error for API compatibility; callers
            # that want DLQ escalation should use agentbus.resilience helpers.
            if exc.last_error is not None:
                raise exc.last_error from exc
            raise

        if concurrent_duplicate_event_id is not None:
            row = self._conn.execute(
                "SELECT * FROM events WHERE event_id = ?",
                (concurrent_duplicate_event_id,),
            ).fetchone()
            event = self._authoritative_event_from_row(row)
            if event is None:  # fail closed if storage changed after insertion
                raise _identity_error()(
                    "403 Forbidden: inserted_event_failed_verification"
                )
            return event, True

        # Prune is best-effort; lock on prune must not fail a successful insert.
        try:
            self.prune_expired()
        except sqlite3.OperationalError:
            pass
        row = self._conn.execute(
            "SELECT * FROM events WHERE event_id = ?", (event_id,)
        ).fetchone()
        event = self._authoritative_event_from_row(row)
        if event is None:  # fail closed if storage changed after insertion
            raise _identity_error()(
                "403 Forbidden: inserted_event_failed_verification"
            )
        return event, False

    def _save_artifacts(self, event_id: int, artifacts: list[dict]) -> None:
        import hashlib

        for art in artifacts:
            raw = art["content"].encode("utf-8")
            self._conn.execute(
                """
                INSERT INTO artifacts
                    (event_id, type, name, content_blob, sha256, size_bytes)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    event_id,
                    art["type"],
                    art["name"],
                    art["content"],
                    hashlib.sha256(raw).hexdigest(),
                    len(raw),
                ),
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

    def _authoritative_events_from_rows(
        self, rows: list[sqlite3.Row]
    ) -> list[Event]:
        """Recompute identity/artifact integrity before exposing stored events."""
        if not rows:
            return []
        events = [self._row_to_event(row) for row in rows]
        artifacts = self._fetch_artifacts([event.event_id for event in events])
        state = (
            load_trust_state(self.workspace, update_high_water=False)
            if identity_configured(self.workspace)
            else None
        )
        authoritative: list[Event] = []
        for event in events:
            event_artifacts = artifacts.get(event.event_id, [])
            if state is not None and event.identity_envelope is not None:
                result = verify_envelope(
                    self.workspace,
                    event.identity_envelope,
                    stored_payload=event.payload,
                    artifacts=event_artifacts,
                    expected_topic=event.topic,
                    expected_producer=event.producer_id,
                    expected_schema_version=event.schema_version,
                    expected_causation_id=event.causation_id,
                    expected_idempotency_key=event.idempotency_key,
                    expected_trace_id=event.trace_id,
                )
                event.verification_status = (
                    "verified" if result.verified else "unverified"
                )
                event.verification_reason = result.reason
            elif state is not None:
                event.verification_status = "legacy_unverified"
                event.verification_reason = "legacy_unverified"
            if (
                state is not None
                and state.mode in {"protected", "strict"}
                and topic_restricted(event.topic, state)
                and event.verification_status != "verified"
            ):
                continue
            if event_artifacts:
                event.payload = {**event.payload, "artifacts": event_artifacts}
            authoritative.append(event)
        return authoritative

    def _authoritative_event_from_row(self, row: sqlite3.Row) -> Event | None:
        events = self._authoritative_events_from_rows([row])
        return events[0] if events else None

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
        events = [
            event.to_dict() for event in self._authoritative_events_from_rows(rows)
        ]
        # Advance past filtered tampered rows; otherwise protected consumers can
        # be pinned forever on the same invalid database record.
        latest_id = rows[-1]["event_id"] if rows else since_id
        return {"events": events, "latest_id": latest_id, "has_more": has_more}

    def poll_all(self, since_id: int = 0, limit: int = 50) -> dict:
        """Read published events across topics in global event-id order.

        This is the cursor contract used by read-only exporters.  As with
        :meth:`poll`, the returned cursor advances past records hidden by
        protected-mode verification so a consumer cannot become pinned on a
        tampered row.
        """
        if since_id < 0:
            raise ValueError("since_id must be >= 0")
        if limit < 1 or limit > 1000:
            raise ValueError("limit must be between 1 and 1000")
        rows = self._conn.execute(
            """
            SELECT * FROM events
            WHERE event_id > ? AND status = ?
            ORDER BY event_id ASC
            LIMIT ?
            """,
            (since_id, STATUS_PUBLISHED, limit + 1),
        ).fetchall()
        has_more = len(rows) > limit
        rows = rows[:limit]
        events = [
            event.to_dict() for event in self._authoritative_events_from_rows(rows)
        ]
        latest_id = rows[-1]["event_id"] if rows else since_id
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
        return [event.to_dict() for event in self._authoritative_events_from_rows(rows)]

    def fetch_trace_events(self, trace_id: str) -> list[dict]:
        rows = self._conn.execute(
            """
            SELECT * FROM events
            WHERE trace_id = ?
            ORDER BY event_id ASC
            """,
            (trace_id,),
        ).fetchall()
        return [event.to_dict() for event in self._authoritative_events_from_rows(rows)]

    def get_event(self, event_id: int) -> Event | None:
        row = self._conn.execute(
            "SELECT * FROM events WHERE event_id = ?", (event_id,)
        ).fetchone()
        if not row:
            return None
        return self._authoritative_event_from_row(row)

    def verify_event(self, event_id: int) -> VerificationResult:
        row = self._conn.execute(
            "SELECT * FROM events WHERE event_id = ?", (event_id,)
        ).fetchone()
        if row is None:
            return VerificationResult(False, None, None, None, "event_not_found")
        # Verification is itself a diagnostic read and must report invalid
        # records rather than filtering them through protected-mode policy.
        event = self._row_to_event(row)
        if not event.identity_envelope:
            return VerificationResult(False, None, None, None, "legacy_unverified")
        artifacts = self._fetch_artifacts([event_id]).get(event_id, [])
        return verify_envelope(
            self.workspace,
            event.identity_envelope,
            stored_payload=event.payload,
            artifacts=artifacts,
            expected_topic=event.topic,
            expected_producer=event.producer_id,
            expected_schema_version=event.schema_version,
            expected_causation_id=event.causation_id,
            expected_idempotency_key=event.idempotency_key,
            expected_trace_id=event.trace_id,
        )

    def get_verified_event(
        self, event_id: int
    ) -> tuple[Event | None, VerificationResult]:
        event = self.get_event(event_id)
        result = self.verify_event(event_id)
        if event is not None:
            event.verification_status = "verified" if result.verified else "unverified"
            event.verification_reason = result.reason
        return event, result

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
        event = self._authoritative_event_from_row(row)
        if event is None:
            raise _identity_error()("403 Forbidden: unverified_event")
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
        cur = self._conn.execute(
            """
            UPDATE events
            SET status = ?, pending_until = NULL, rejection_reason = NULL,
                sla_deadline = COALESCE(?, sla_deadline)
            WHERE event_id = ? AND status = ?
            """,
            (
                STATUS_PUBLISHED,
                updates.get("sla_deadline"),
                event_id,
                STATUS_PENDING,
            ),
        )
        if cur.rowcount != 1:
            self._conn.commit()
            raise ValueError(f"event_not_pending: {event_id}")
        self._counter("pending_approval", -1)
        if updates.get("sla_deadline") is not None:
            self._counter("sla_active", 1)
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
        event = self._authoritative_event_from_row(row)
        if event is None:
            raise _identity_error()("403 Forbidden: unverified_event")
        if event.status not in (STATUS_PENDING,):
            raise ValueError(f"event_not_pending: {event_id} status={event.status}")

        cur = self._conn.execute(
            """
            UPDATE events
            SET status = ?, pending_until = NULL, rejection_reason = ?
            WHERE event_id = ? AND status = ?
            """,
            (STATUS_REJECTED, reason, event_id, STATUS_PENDING),
        )
        if cur.rowcount != 1:
            self._conn.commit()
            raise ValueError(f"event_not_pending: {event_id}")
        self._counter("pending_approval", -1)
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
        return self._authoritative_events_from_rows(rows)

    def mark_projected(self, event_ids: list[int]) -> None:
        if not event_ids:
            return
        placeholders = ",".join("?" for _ in event_ids)
        self._conn.execute(
            f"UPDATE events SET projected_to_log = 1 WHERE event_id IN ({placeholders})",
            event_ids,
        )
        self._conn.commit()

    def list_active_slas(self) -> dict:
        self.expire_sla_breaches()
        rows = self._conn.execute(
            """
            SELECT *
            FROM events
            WHERE status = ? AND sla_cleared = 0 AND sla_deadline IS NOT NULL
            ORDER BY sla_deadline
            """,
            (STATUS_PUBLISHED,),
        ).fetchall()
        events = self._authoritative_events_from_rows(rows)
        active = [
            {
                "event_id": event.event_id,
                "topic": event.topic,
                "producer_id": event.producer_id,
                "sla_timeout_minutes": event.sla_timeout_minutes,
                "sla_deadline": event.sla_deadline,
            }
            for event in events
        ]
        return {"active": active, "sla_active_count": len(active)}

    def latest_event_id(self) -> int:
        row = self._conn.execute(
            "SELECT value FROM counters WHERE name = 'latest_event_id'"
        ).fetchone()
        return int(row["value"] if row is not None else 0)

    def verify_chain(self, *, since_checkpoint: bool = False) -> dict:
        """Verify the retained forward SHA-256 chain and legacy checkpoint.

        AgentBus v0.26 and earlier rows remain untouched. The immutable envelope
        of the last such row is the cutover anchor while it is retained; rows
        after it must be consecutive and fully chained.
        """
        checkpoint_id = int(self._meta("checkpoint_event_id") or 0)
        checkpoint_hash = self._meta("checkpoint_hash") or GENESIS_HASH
        legacy_checkpoint = self._meta("checkpoint_legacy") == "1"
        divergence: dict | None = None

        if legacy_checkpoint and not since_checkpoint:
            anchor = self._conn.execute(
                "SELECT * FROM events WHERE event_id = ?", (checkpoint_id,)
            ).fetchone()
            if anchor is not None:
                recomputed_anchor = event_digest(
                    GENESIS_HASH,
                    self._chain_body_from_row(anchor, prev_hash=GENESIS_HASH),
                )
                if recomputed_anchor != checkpoint_hash:
                    divergence = {
                        "event_id": checkpoint_id,
                        "reason": "checkpoint_hash_mismatch",
                        "expected": recomputed_anchor,
                        "found": checkpoint_hash,
                    }

        rows = self._conn.execute(
            "SELECT * FROM events WHERE event_id > ? ORDER BY event_id ASC",
            (checkpoint_id,),
        ).fetchall()
        expected_prev = checkpoint_hash
        expected_id = checkpoint_id + 1
        gaps: list[dict[str, int]] = []
        checked = 0
        for row in rows:
            event_id = int(row["event_id"])
            if event_id != expected_id:
                gaps.append({"expected_event_id": expected_id, "found_event_id": event_id})
            stored_prev = row["prev_hash"]
            stored_hash = row["hash"]
            if stored_prev != expected_prev and divergence is None:
                divergence = {
                    "event_id": event_id,
                    "reason": "prev_hash_mismatch",
                    "expected": expected_prev,
                    "found": stored_prev,
                }
            try:
                recomputed = event_digest(
                    str(stored_prev), self._chain_body_from_row(row)
                )
            except ValueError as exc:
                recomputed = None
                if divergence is None:
                    divergence = {
                        "event_id": event_id,
                        "reason": "malformed_prev_hash",
                        "detail": str(exc),
                    }
            if recomputed is not None and recomputed != stored_hash and divergence is None:
                divergence = {
                    "event_id": event_id,
                    "reason": "hash_mismatch",
                    "expected": recomputed,
                    "found": stored_hash,
                }
            expected_prev = str(stored_hash)
            expected_id = event_id + 1
            checked += 1

        return {
            "ok": divergence is None and not gaps,
            "events_checked": checked,
            "first_event_id": int(rows[0]["event_id"]) if rows else None,
            "last_event_id": int(rows[-1]["event_id"]) if rows else None,
            "checkpoint": {
                "event_id": checkpoint_id,
                "hash": checkpoint_hash,
                "legacy": legacy_checkpoint,
            },
            "head_hash": expected_prev,
            "gaps": gaps,
            "divergence": divergence,
        }

    def status(self, producer_id: str | None = None) -> dict:
        rows = self._conn.execute("SELECT name, value FROM counters").fetchall()
        counters = {str(row["name"]): int(row["value"]) for row in rows}
        count = counters.get("total_events", 0)
        latest = counters.get("latest_event_id", 0)
        pending = counters.get("pending_approval", 0)
        sla_active = counters.get("sla_active", 0)
        topics = sorted(
            name[len("topic.") :]
            for name, value in counters.items()
            if name.startswith("topic.") and value > 0
        )
        # Live PRAGMA read when available (more accurate than remembered override).
        try:
            live_journal = self._conn.execute("PRAGMA journal_mode").fetchone()[0]
        except Exception:  # pragma: no cover
            live_journal = getattr(self, "_journal_mode", None)
        try:
            live_busy = int(self._conn.execute("PRAGMA busy_timeout").fetchone()[0])
        except Exception:  # pragma: no cover
            live_busy = getattr(self, "_busy_timeout_ms", None)
        return {
            "workspace": str(self.workspace),
            "event_count": count,
            "total_events": count,
            "latest_event_id": latest or 0,
            "pending_approval_count": pending,
            "pending_count": pending,
            "sla_active_count": sla_active,
            "hitl_enabled": not hitl_disabled(),
            "topics": topics,
            "retention_days": self.retention_days,
            "producer_id": producer_id or os.environ.get("AGENTBUS_PRODUCER_ID", ""),
            "sqlite_journal_mode": str(live_journal).upper() if live_journal else None,
            "sqlite_busy_timeout_ms": live_busy,
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
            idempotency_key=(
                row["scoped_idempotency_key"]
                if "scoped_idempotency_key" in keys
                and row["scoped_idempotency_key"] is not None
                else row["idempotency_key"]
            ),
            status=row["status"] if "status" in keys else STATUS_PUBLISHED,
            pending_until=row["pending_until"] if "pending_until" in keys else None,
            rejection_reason=row["rejection_reason"]
            if "rejection_reason" in keys
            else None,
            sla_timeout_minutes=(
                row["sla_timeout_minutes"] if "sla_timeout_minutes" in keys else None
            ),
            sla_deadline=row["sla_deadline"] if "sla_deadline" in keys else None,
            sla_cleared=bool(row["sla_cleared"]) if "sla_cleared" in keys else False,
            trace_id=row["trace_id"] if "trace_id" in keys else None,
            span_id=row["span_id"] if "span_id" in keys else None,
            parent_span_id=row["parent_span_id"] if "parent_span_id" in keys else None,
            identity_envelope=(
                json.loads(row["identity_envelope"])
                if "identity_envelope" in keys and row["identity_envelope"]
                else None
            ),
            verification_status=(
                row["verification_status"]
                if "verification_status" in keys and row["verification_status"]
                else "legacy_unverified"
            ),
            verification_reason=(
                row["verification_reason"] if "verification_reason" in keys else None
            ),
        )
