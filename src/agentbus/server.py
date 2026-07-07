"""FastMCP server exposing AgentBus tools."""

from __future__ import annotations

import json
import os
from pathlib import Path

from mcp.server.fastmcp import FastMCP

from agentbus.auth import check_publish_token, ensure_ephemeral_token
from agentbus.leases import LeaseStore
from agentbus.rbac import ForbiddenError
from agentbus.schemas import validate_payload
from agentbus.store import EventStore

mcp = FastMCP("agentbus")

_store: EventStore | None = None
_lease_store: LeaseStore | None = None
_workspace: Path | None = None


def _get_store() -> EventStore:
    if _store is None:
        raise RuntimeError("store not initialized — run via agentbus serve")
    return _store


def _get_lease_store() -> LeaseStore:
    if _lease_store is None:
        raise RuntimeError("lease store not initialized — run via agentbus serve")
    return _lease_store


def init_store(workspace: Path, retention_days: int = 7) -> EventStore:
    global _store, _lease_store, _workspace
    _workspace = workspace.resolve()
    _store = EventStore(_workspace, retention_days=retention_days)
    _lease_store = LeaseStore(_workspace)
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
    payload = validate_payload(topic, payload)
    try:
        event, duplicate = _get_store().publish(
            topic=topic,
            producer_id=_producer_id(producer_id),
            schema_version=schema_version,
            payload=payload,
            causation_id=causation_id,
            idempotency_key=idempotency_key,
            auth_token=auth_token,
        )
    except ForbiddenError as exc:
        return json.dumps({"error": str(exc), "code": exc.code})
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


@mcp.tool()
def agentbus_review(topic: str | None = None, limit: int = 50) -> str:
    """List events pending human approval (hidden from standard poll)."""
    return json.dumps(_get_store().review_pending(topic=topic, limit=min(limit, 100)))


@mcp.tool()
def agentbus_approve(
    event_id: int,
    reviewer_id: str | None = None,
    auth_token: str | None = None,
) -> str:
    """Approve a pending event so agents can see it on poll."""
    check_publish_token(_auth_workspace(), auth_token=auth_token)
    rid = reviewer_id or os.environ.get("AGENTBUS_PRODUCER_ID", "agy")
    try:
        return json.dumps(
            _get_store().approve_event(event_id, reviewer_id=rid, auth_token=auth_token)
        )
    except (ValueError, ForbiddenError) as exc:
        code = getattr(exc, "code", 400)
        return json.dumps({"error": str(exc), "code": code})


@mcp.tool()
def agentbus_reject(
    event_id: int,
    reason: str = "rejected by human reviewer",
    reviewer_id: str | None = None,
    auth_token: str | None = None,
) -> str:
    """Reject a pending event and notify the originating agent."""
    check_publish_token(_auth_workspace(), auth_token=auth_token)
    rid = reviewer_id or os.environ.get("AGENTBUS_PRODUCER_ID", "agy")
    try:
        return json.dumps(
            _get_store().reject_event(
                event_id, reviewer_id=rid, reason=reason, auth_token=auth_token
            )
        )
    except (ValueError, ForbiddenError) as exc:
        code = getattr(exc, "code", 400)
        return json.dumps({"error": str(exc), "code": code})


@mcp.tool()
def agentbus_lock_acquire(
    resource: str,
    owner_id: str,
    ttl_seconds: int | None = None,
    auth_token: str | None = None,
) -> str:
    """Acquire an exclusive advisory lease on a workspace resource."""
    check_publish_token(_auth_workspace(), auth_token=auth_token)
    return json.dumps(
        _get_lease_store().lock_acquire(resource, owner_id, ttl_seconds)
    )


@mcp.tool()
def agentbus_lock_release(
    resource: str,
    lease_id: str,
    owner_id: str,
    auth_token: str | None = None,
) -> str:
    """Release a held lease (idempotent if already expired)."""
    check_publish_token(_auth_workspace(), auth_token=auth_token)
    return json.dumps(
        _get_lease_store().lock_release(resource, lease_id, owner_id)
    )


@mcp.tool()
def agentbus_lock_renew(
    resource: str,
    lease_id: str,
    owner_id: str,
    ttl_seconds: int | None = None,
    auth_token: str | None = None,
) -> str:
    """Extend TTL on an active lease (heartbeat)."""
    check_publish_token(_auth_workspace(), auth_token=auth_token)
    return json.dumps(
        _get_lease_store().lock_renew(resource, lease_id, owner_id, ttl_seconds)
    )


@mcp.tool()
def agentbus_lock_status(resource: str) -> str:
    """Check lock state without acquiring (no auth required)."""
    return json.dumps(_get_lease_store().lock_status(resource))


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