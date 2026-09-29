"""Advisory lease locks — Phase 5 (persisted in events.db)."""

from __future__ import annotations

import re
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

DEFAULT_TTL_SECONDS = 300
MAX_TTL_SECONDS = 3600
OWNER_PATTERN = re.compile(r"^[a-z][a-z0-9_-]*$")


def validate_owner_id(owner_id: str) -> None:
    if not owner_id or not OWNER_PATTERN.match(owner_id):
        raise ValueError(f"invalid_owner_id: {owner_id}")


def normalize_resource(workspace: Path, resource: str) -> str:
    if not resource:
        raise ValueError("invalid_resource: empty path")
    path = Path(resource).expanduser().resolve()
    ws = workspace.resolve()
    try:
        path.relative_to(ws)
    except ValueError as exc:
        raise ValueError(
            f"resource_outside_workspace: {resource} (lease resources must live "
            f"inside the workspace at {ws}; use a path under it, e.g. "
            f"{ws}/<shared-file>)"
        ) from exc
    return str(path)


def clamp_ttl(ttl_seconds: int | None, default: int = DEFAULT_TTL_SECONDS) -> int:
    if ttl_seconds is None:
        return default
    if ttl_seconds < 1:
        raise ValueError("invalid_ttl: must be >= 1")
    if ttl_seconds > MAX_TTL_SECONDS:
        raise ValueError(f"invalid_ttl: max {MAX_TTL_SECONDS}")
    return ttl_seconds


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _fmt(dt: datetime) -> str:
    """Serialize UTC timestamps without discarding sub-second lease lifetime."""
    return dt.astimezone(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def _parse(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)


class LeaseStore:
    """Lease locks stored in the workspace events.db `leases` table."""

    def __init__(self, workspace: Path, default_ttl: int = DEFAULT_TTL_SECONDS) -> None:
        self.workspace = workspace.resolve()
        self.default_ttl = default_ttl
        db_dir = self.workspace / ".agentbus"
        db_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = db_dir / "events.db"
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS leases (
                lease_id TEXT PRIMARY KEY,
                resource TEXT NOT NULL UNIQUE,
                owner_id TEXT NOT NULL,
                acquired_at TEXT NOT NULL,
                expires_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_leases_resource ON leases(resource);
            CREATE INDEX IF NOT EXISTS idx_leases_expires ON leases(expires_at);
            """
        )
        columns = {
            row[1] for row in self._conn.execute("PRAGMA table_info(leases)").fetchall()
        }
        if "renew_count" not in columns:
            try:
                self._conn.execute(
                    "ALTER TABLE leases ADD COLUMN renew_count INTEGER NOT NULL DEFAULT 0"
                )
            except sqlite3.OperationalError as exc:
                if "duplicate column" not in str(exc).lower():
                    raise
        self._conn.commit()

    def _emit_lock_event(
        self,
        *,
        path: str,
        owner_id: str,
        action: str,
        lease_id: str,
        duration_held_s: float,
        renew_count: int,
    ) -> None:
        """Best-effort metadata hook; lock correctness never depends on telemetry."""
        try:
            from agentbus.store import EventStore

            lock_name = Path(path).relative_to(self.workspace).as_posix()
            events = EventStore(self.workspace, auto_prune=False)
            try:
                events.publish(
                    topic="system/lock",
                    producer_id="lock-telemetry",
                    schema_version="1.0",
                    payload={
                        "type": "swarm_lock_event",
                        "lock_name": lock_name,
                        "holder": owner_id,
                        "action": action,
                        "duration_held_s": round(max(0.0, duration_held_s), 3),
                        "renew_count": max(0, renew_count),
                    },
                    idempotency_key=f"lock:{lease_id}:{action}:{renew_count}",
                    skip_rbac=True,
                )
            finally:
                events.close()
        except Exception:
            return

    def close(self) -> None:
        self._conn.close()

    def _purge_expired(self) -> None:
        cutoff = _fmt(_utc_now())
        # `julianday` handles both legacy second-precision rows and new
        # microsecond-precision rows. Raw string comparison would order the
        # two ISO variants incorrectly at the same second.
        self._conn.execute(
            "DELETE FROM leases WHERE julianday(expires_at) <= julianday(?)",
            (cutoff,),
        )
        self._conn.commit()

    def _active_row(self, resource: str) -> sqlite3.Row | None:
        self._purge_expired()
        return self._conn.execute(
            "SELECT * FROM leases WHERE resource = ?",
            (resource,),
        ).fetchone()

    def lock_acquire(
        self,
        resource: str,
        owner_id: str,
        ttl_seconds: int | None = None,
    ) -> dict:
        validate_owner_id(owner_id)
        path = normalize_resource(self.workspace, resource)
        ttl = clamp_ttl(ttl_seconds, self.default_ttl)
        now = _utc_now()
        expires = now + timedelta(seconds=ttl)

        existing = self._active_row(path)
        if existing:
            if existing["owner_id"] == owner_id:
                return {
                    "acquired": True,
                    "lease_id": existing["lease_id"],
                    "expires_at": existing["expires_at"],
                    "resource": path,
                }
            return {
                "acquired": False,
                "current_owner": existing["owner_id"],
                "expires_at": existing["expires_at"],
                "resource": path,
            }

        lease_id = str(uuid.uuid4())
        self._conn.execute(
            """
            INSERT INTO leases (lease_id, resource, owner_id, acquired_at, expires_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (lease_id, path, owner_id, _fmt(now), _fmt(expires)),
        )
        self._conn.commit()
        self._emit_lock_event(
            path=path,
            owner_id=owner_id,
            action="acquire",
            lease_id=lease_id,
            duration_held_s=0,
            renew_count=0,
        )
        return {
            "acquired": True,
            "lease_id": lease_id,
            "expires_at": _fmt(expires),
            "resource": path,
        }

    def lock_release(self, resource: str, lease_id: str, owner_id: str) -> dict:
        validate_owner_id(owner_id)
        path = normalize_resource(self.workspace, resource)
        self._purge_expired()
        row = self._conn.execute(
            "SELECT * FROM leases WHERE resource = ? AND lease_id = ?",
            (path, lease_id),
        ).fetchone()
        if not row:
            return {"released": True, "resource": path}
        if row["owner_id"] != owner_id:
            raise ValueError("invalid_owner: owner_id does not hold this lease")
        duration = max(0.0, (_utc_now() - _parse(row["acquired_at"])).total_seconds())
        renew_count = int(row["renew_count"] or 0)
        self._conn.execute(
            "DELETE FROM leases WHERE lease_id = ?",
            (lease_id,),
        )
        self._conn.commit()
        self._emit_lock_event(
            path=path,
            owner_id=owner_id,
            action="release",
            lease_id=lease_id,
            duration_held_s=duration,
            renew_count=renew_count,
        )
        return {"released": True, "resource": path}

    def lock_renew(
        self,
        resource: str,
        lease_id: str,
        owner_id: str,
        ttl_seconds: int | None = None,
    ) -> dict:
        validate_owner_id(owner_id)
        path = normalize_resource(self.workspace, resource)
        ttl = clamp_ttl(ttl_seconds, self.default_ttl)
        self._purge_expired()
        row = self._conn.execute(
            "SELECT * FROM leases WHERE resource = ? AND lease_id = ?",
            (path, lease_id),
        ).fetchone()
        if not row or row["owner_id"] != owner_id:
            return {"renewed": False, "resource": path}
        now = _utc_now()
        expires = now + timedelta(seconds=ttl)
        renew_count = int(row["renew_count"] or 0) + 1
        self._conn.execute(
            "UPDATE leases SET expires_at = ?, renew_count = ? WHERE lease_id = ?",
            (_fmt(expires), renew_count, lease_id),
        )
        self._conn.commit()
        duration = max(0.0, (now - _parse(row["acquired_at"])).total_seconds())
        self._emit_lock_event(
            path=path,
            owner_id=owner_id,
            action="renew",
            lease_id=lease_id,
            duration_held_s=duration,
            renew_count=renew_count,
        )
        return {"renewed": True, "expires_at": _fmt(expires), "resource": path}

    def lock_status(self, resource: str) -> dict:
        path = normalize_resource(self.workspace, resource)
        row = self._active_row(path)
        if not row:
            return {
                "locked": False,
                "resource": path,
            }
        return {
            "locked": True,
            "resource": path,
            "lease_id": row["lease_id"],
            "current_owner": row["owner_id"],
            "acquired_at": row["acquired_at"],
            "expires_at": row["expires_at"],
        }
