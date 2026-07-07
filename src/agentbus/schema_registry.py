"""Pluggable topic schema registry backed by SQLite."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path


def _connect(workspace: Path) -> sqlite3.Connection:
    db_dir = workspace.resolve() / ".agentbus"
    db_dir.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_dir / "events.db", check_same_thread=False)
    conn.row_factory = sqlite3.Row
    ensure_table(conn)
    return conn


def ensure_table(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS topic_schemas (
            topic_name TEXT PRIMARY KEY,
            json_schema TEXT NOT NULL,
            version TEXT NOT NULL DEFAULT '1.0',
            registered_at TEXT NOT NULL
        );
        """
    )
    conn.commit()


def register_schema(
    workspace: Path,
    topic_name: str,
    json_schema: dict,
    *,
    version: str = "1.0",
) -> dict:
    from datetime import datetime, timezone

    from agentbus.schemas import TOPIC_PATTERN, validate_topic_name

    validate_topic_name(topic_name)
    if not isinstance(json_schema, dict):
        raise ValueError("invalid_json_schema: expected object")

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    conn = _connect(workspace)
    try:
        conn.execute(
            """
            INSERT INTO topic_schemas (topic_name, json_schema, version, registered_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(topic_name) DO UPDATE SET
                json_schema = excluded.json_schema,
                version = excluded.version,
                registered_at = excluded.registered_at
            """,
            (topic_name, json.dumps(json_schema), version, ts),
        )
        conn.commit()
    finally:
        conn.close()
    return {"topic_name": topic_name, "version": version, "registered_at": ts}


def load_schema(workspace: Path, topic_name: str) -> dict | None:
    conn = _connect(workspace)
    try:
        row = conn.execute(
            "SELECT json_schema FROM topic_schemas WHERE topic_name = ?",
            (topic_name,),
        ).fetchone()
        if not row:
            return None
        return json.loads(row["json_schema"])
    finally:
        conn.close()


def list_schemas(workspace: Path) -> list[dict]:
    conn = _connect(workspace)
    try:
        rows = conn.execute(
            "SELECT topic_name, version, registered_at FROM topic_schemas ORDER BY topic_name"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def import_schema_file(workspace: Path, path: Path) -> dict:
    data = json.loads(path.read_text(encoding="utf-8"))
    if "topic" in data and "json_schema" in data:
        return register_schema(
            workspace, data["topic"], data["json_schema"], version=data.get("version", "1.0")
        )
    if "topic" in data and "schema" in data:
        return register_schema(
            workspace, data["topic"], data["schema"], version=data.get("version", "1.0")
        )
    raise ValueError("invalid_schema_file: require topic + json_schema (or schema)")


def topic_is_registered(workspace: Path, topic_name: str) -> bool:
    return load_schema(workspace, topic_name) is not None