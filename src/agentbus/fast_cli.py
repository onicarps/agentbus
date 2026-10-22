"""Tiered AgentBus console entrypoint.

The status projection is intentionally handled without importing Click, MCP,
AgentID, JSON Schema, Rich, or Textual. All other commands are delegated to the
fully compatible Click command tree on demand.
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
from pathlib import Path


def _legacy() -> None:
    from agentbus.cli import main as legacy_main

    legacy_main()


def _option(args: list[str], name: str, default: str | None = None) -> str | None:
    prefix = name + "="
    for index, item in enumerate(args):
        if item.startswith(prefix):
            return item[len(prefix) :]
        if item == name and index + 1 < len(args):
            return args[index + 1]
    return default


def _workspace(args: list[str]) -> Path:
    value = _option(args, "--workspace") or os.environ.get("AGENTBUS_WORKSPACE")
    return Path(value).expanduser().resolve() if value else Path.cwd().resolve()


def _can_read_projection(workspace: Path, args: list[str]) -> bool:
    allowed = {"--workspace", "--producer-id", "--retention-days"}
    index = 0
    while index < len(args):
        item = args[index]
        if item.startswith("--"):
            name = item.split("=", 1)[0]
            if name not in allowed:
                return False
            if "=" not in item:
                index += 1
                if index >= len(args):
                    return False
        else:
            return False
        index += 1

    if os.environ.get("AGENTBUS_BROKER_SOCKET"):
        return False
    try:
        if Path("/run/agentbus/agentbus.sock").is_socket():
            return False
    except OSError:
        return False
    identity = workspace / ".agentbus" / "identity"
    if all((identity / name).is_file() for name in (
        "trust-root.json", "policy.json", "registry.json"
    )):
        return False
    return True


def _retention_days(workspace: Path, args: list[str]) -> int:
    requested = int(_option(args, "--retention-days", "7") or "7")
    if requested != 7:
        # The compatibility path persists explicit non-default settings.
        raise ValueError("persist_retention_via_legacy")
    path = workspace / ".agentbus" / "config.json"
    if path.is_file():
        value = json.loads(path.read_text(encoding="utf-8")).get("retention_days")
        if value is not None:
            return int(value)
    return requested


def _fast_status(args: list[str]) -> bool:
    """Read the O(1) projection without running retention maintenance.

    Counts describe rows currently retained in the database. Writers and full
    EventStore opens perform retention cleanup; this read-only path never
    deletes rows or changes SQLite settings.
    """
    workspace = _workspace(args)
    if not _can_read_projection(workspace, args):
        return False
    db_path = workspace / ".agentbus" / "events.db"
    if not db_path.is_file():
        return False
    try:
        retention_days = _retention_days(workspace, args)
        uri = f"file:{db_path.as_posix()}?mode=ro"
        conn = sqlite3.connect(uri, uri=True)
        conn.row_factory = sqlite3.Row
        try:
            tables = {
                str(row["name"])
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table' "
                    "AND name IN ('counters', 'meta')"
                ).fetchall()
            }
            if tables != {"counters", "meta"}:
                return False
            counters = {
                str(row["name"]): int(row["value"])
                for row in conn.execute("SELECT name, value FROM counters").fetchall()
            }
            journal = str(conn.execute("PRAGMA journal_mode").fetchone()[0]).upper()
            busy = int(conn.execute("PRAGMA busy_timeout").fetchone()[0])
        finally:
            conn.close()
    except (OSError, ValueError, sqlite3.Error, json.JSONDecodeError):
        return False

    topics = sorted(
        name[len("topic.") :]
        for name, value in counters.items()
        if name.startswith("topic.") and value > 0
    )
    pending = counters.get("pending_approval", 0)
    count = counters.get("total_events", 0)
    disabled = os.environ.get("AGENTBUS_DISABLE_HITL", "").strip().lower() in {
        "1", "true", "yes", "on"
    }
    result = {
        "workspace": str(workspace),
        "event_count": count,
        "total_events": count,
        "latest_event_id": counters.get("latest_event_id", 0),
        "pending_approval_count": pending,
        "pending_count": pending,
        "sla_active_count": counters.get("sla_active", 0),
        "hitl_enabled": not disabled,
        "topics": topics,
        "retention_days": retention_days,
        "producer_id": _option(args, "--producer-id")
        or os.environ.get("AGENTBUS_PRODUCER_ID", ""),
        "sqlite_journal_mode": journal,
        "sqlite_busy_timeout_ms": busy,
    }
    sys.stdout.write(json.dumps(result) + "\n")
    return True


def _fast_verify(args: list[str]) -> bool:
    allowed = {"--workspace", "--retention-days", "--since-checkpoint"}
    index = 0
    while index < len(args):
        item = args[index]
        name = item.split("=", 1)[0]
        if name not in allowed:
            return False
        if name != "--since-checkpoint" and "=" not in item:
            index += 1
            if index >= len(args):
                return False
        index += 1

    workspace = _workspace(args)
    db_path = workspace / ".agentbus" / "events.db"
    if not db_path.is_file():
        return False
    from agentbus.store import EventStore

    store = EventStore(
        workspace,
        retention_days=int(_option(args, "--retention-days", "7") or "7"),
        auto_prune=False,
    )
    try:
        report = store.verify_chain(since_checkpoint="--since-checkpoint" in args)
    finally:
        store.close()
    sys.stdout.write(json.dumps(report, sort_keys=True) + "\n")
    if not report["ok"]:
        raise SystemExit(1)
    return True


def main() -> None:
    args = sys.argv[1:]
    if args and args[0] == "status" and _fast_status(args[1:]):
        return
    if args and args[0] == "verify" and _fast_verify(args[1:]):
        return
    _legacy()


if __name__ == "__main__":
    main()
