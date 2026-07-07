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
        raise ValueError(f"resource_outside_workspace: {resource}") from exc
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
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse(ts: str) -> datetime:
    return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


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
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def _purge_expired(self) -> None:
        cutoff = _fmt(_utc_now())
        self._conn.execute("DELETE FROM leases WHERE expires_at <= ?", (cutoff,))
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
        self._conn.execute(
            "DELETE FROM leases WHERE lease_id = ?",
            (lease_id,),
        )
        self._conn.commit()
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
        expires = _utc_now() + timedelta(seconds=ttl)
        self._conn.execute(
            "UPDATE leases SET expires_at = ? WHERE lease_id = ?",
            (_fmt(expires), lease_id),
        )
        self._conn.commit()
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