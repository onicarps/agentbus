"""FastMCP server exposing AgentBus tools."""

from __future__ import annotations

import json
import os
from pathlib import Path

from mcp.server.fastmcp import FastMCP

from agentbus.auth import check_publish_token, ensure_ephemeral_token
from agentbus.schemas import validate_payload
from agentbus.store import EventStore

mcp = FastMCP("agentbus")

_store: EventStore | None = None
_workspace: Path | None = None


def _get_store() -> EventStore:
    if _store is None:
        raise RuntimeError("store not initialized — run via agentbus serve")
    return _store


def init_store(workspace: Path, retention_days: int = 7) -> EventStore:
    global _store, _workspace
    _workspace = workspace.resolve()
    _store = EventStore(workspace, retention_days=retention_days)
    return _store


def _auth_workspace() -> Path | None:
    return _workspace


def _producer_id(override: str | None) -> str:
    pid = override or os.environ.get("AGENTBUS_PRODUCER_ID", "")
    if not pid:
        raise ValueError("producer_id required (arg or AGENTBUS_PRODUCER_ID)")
    return pid


@mcp.tool()
def agentbus_publish(
    topic: str,
    payload: dict,
    schema_version: str = "1.0",
    producer_id: str | None = None,
    causation_id: int | None = None,
    idempotency_key: str | None = None,
    auth_token: str | None = None,
) -> str:
    """Append one event to the workspace event log."""
    check_publish_token(_auth_workspace(), auth_token=auth_token)
    validate_payload(topic, payload)
    event, duplicate = _get_store().publish(
        topic=topic,
        producer_id=_producer_id(producer_id),
        schema_version=schema_version,
        payload=payload,
        causation_id=causation_id,
        idempotency_key=idempotency_key,
    )
    return json.dumps(
        {
            "event_id": event.event_id,
            "topic": event.topic,
            "timestamp": event.timestamp,
            "duplicate": duplicate,
        }
    )


@mcp.tool()
def agentbus_poll(
    topic: str,
    since_id: int = 0,
    limit: int = 50,
) -> str:
    """Fetch events after cursor (at-least-once delivery)."""
    result = _get_store().poll(topic=topic, since_id=since_id, limit=min(limit, 100))
    return json.dumps(result)


@mcp.tool()
def agentbus_status() -> str:
    """Workspace bus health and topic list."""
    return json.dumps(_get_store().status())


def run_stdio(
    workspace: Path,
    retention_days: int = 7,
    *,
    rotate_token: bool = False,
) -> None:
    ws = workspace.resolve()
    ensure_ephemeral_token(ws, rotate=rotate_token)
    init_store(ws, retention_days)
    mcp.run(transport="stdio")