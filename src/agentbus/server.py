"""FastMCP server exposing AgentBus tools."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from mcp.server.fastmcp import FastMCP

from agentbus.auth import check_publish_token, ensure_ephemeral_token
from agentbus.leases import LeaseStore
from agentbus.artifacts import PayloadTooLargeError
from agentbus.mcpsafe import AccessDeniedError, PolicyEnforcer, load_enforcer
from agentbus.rbac import ForbiddenError
from agentbus.schemas import set_validation_workspace, validate_payload
from agentbus.store import EventStore
from agentbus.wiretap import instrument_call

mcp = FastMCP("agentbus")

_store: EventStore | None = None
_lease_store: LeaseStore | None = None
_workspace: Path | None = None
_wiretap_enabled: bool = False
_wiretap_log: Path | None = None
_wiretap_client: str | None = None
_mcpsafe: PolicyEnforcer | None = None


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
    set_validation_workspace(_workspace)
    _store = EventStore(_workspace, retention_days=retention_days)
    if _mcpsafe is not None:
        _store.set_mcpsafe(_mcpsafe)
    _lease_store = LeaseStore(_workspace)
    return _store


def configure_wiretap(
    enabled: bool = False,
    *,
    log_path: Path | None = None,
    client: str | None = None,
) -> None:
    """Enable/disable God View MCP wiretap (system/mcp events). Default off."""
    global _wiretap_enabled, _wiretap_log, _wiretap_client
    _wiretap_enabled = enabled
    _wiretap_log = log_path
    _wiretap_client = client or os.environ.get("AGENTBUS_PRODUCER_ID")


def configure_mcpsafe(
    enabled: bool = False,
    *,
    lockfile: Path | str | None = None,
    workspace: Path | None = None,
) -> PolicyEnforcer | None:
    """Enable/disable mcpsafe PolicyEnforcer (default off)."""
    global _mcpsafe
    _mcpsafe = load_enforcer(
        workspace if workspace is not None else _workspace,
        enabled=enabled,
        lockfile=lockfile,
    )
    if _store is not None:
        _store.set_mcpsafe(_mcpsafe)
    return _mcpsafe


def _auth_workspace() -> Path | None:
    return _workspace


def _producer_id(override: str | None) -> str:
    pid = override or os.environ.get("AGENTBUS_PRODUCER_ID", "")
    if not pid:
        raise ValueError("producer_id required (arg or AGENTBUS_PRODUCER_ID)")
    return pid


def _check_mcpsafe_tool(tool: str) -> str | None:
    """Return JSON error if tool blocked; else None."""
    if _mcpsafe is None:
        return None
    try:
        _mcpsafe.require(tool)
    except AccessDeniedError as exc:
        return json.dumps({"error": str(exc), "code": exc.code})
    return None


def _wt(tool: str, arguments: dict[str, Any], fn: Any) -> Any:
    denied = _check_mcpsafe_tool(tool)
    if denied is not None:
        return denied

    def _guarded() -> Any:
        if _mcpsafe is not None and tool == "agentbus_publish":
            payload = arguments.get("payload")
            if isinstance(payload, dict):
                try:
                    _mcpsafe.require_payload(payload)
                except AccessDeniedError as exc:
                    return json.dumps({"error": str(exc), "code": exc.code})
        return fn()

    if not _wiretap_enabled:
        return _guarded()
    return instrument_call(
        _get_store(),
        tool,
        arguments,
        _guarded,
        wiretap_log=_wiretap_log,
        client=_wiretap_client,
    )


@mcp.tool()
def agentbus_publish(
    topic: str,
    payload: dict,
    schema_version: str = "1.0",
    producer_id: str | None = None,
    causation_id: int | None = None,
    idempotency_key: str | None = None,
    auth_token: str | None = None,
    sla_timeout_minutes: int | None = None,
    trace_id: str | None = None,
    parent_span_id: str | None = None,
) -> str:
    """Append one event to the workspace event log."""

    def _run() -> str:
        check_publish_token(_auth_workspace(), auth_token=auth_token)
        validated = validate_payload(topic, payload)
        try:
            event, duplicate = _get_store().publish(
                topic=topic,
                producer_id=_producer_id(producer_id),
                schema_version=schema_version,
                payload=validated,
                causation_id=causation_id,
                idempotency_key=idempotency_key,
                auth_token=auth_token,
                sla_timeout_minutes=sla_timeout_minutes,
                trace_id=trace_id,
                parent_span_id=parent_span_id,
            )
        except ForbiddenError as exc:
            return json.dumps({"error": str(exc), "code": exc.code})
        except AccessDeniedError as exc:
            return json.dumps({"error": str(exc), "code": exc.code})
        except PayloadTooLargeError as exc:
            return json.dumps({"error": str(exc), "code": exc.code})
        out = {
            "event_id": event.event_id,
            "topic": event.topic,
            "timestamp": event.timestamp,
            "duplicate": duplicate,
        }
        if event.span_id:
            out["span_id"] = event.span_id
        if event.trace_id:
            out["trace_id"] = event.trace_id
        return json.dumps(out)

    return _wt(
        "agentbus_publish",
        {
            "topic": topic,
            "payload": payload,
            "schema_version": schema_version,
            "producer_id": producer_id,
            "causation_id": causation_id,
            "idempotency_key": idempotency_key,
            "auth_token": auth_token,
            "sla_timeout_minutes": sla_timeout_minutes,
            "trace_id": trace_id,
            "parent_span_id": parent_span_id,
        },
        _run,
    )


@mcp.tool()
def agentbus_poll(
    topic: str,
    since_id: int = 0,
    limit: int = 50,
) -> str:
    """Fetch events after cursor (at-least-once delivery)."""

    def _run() -> str:
        result = _get_store().poll(topic=topic, since_id=since_id, limit=min(limit, 100))
        return json.dumps(result)

    return _wt(
        "agentbus_poll",
        {"topic": topic, "since_id": since_id, "limit": limit},
        _run,
    )


@mcp.tool()
def agentbus_status() -> str:
    """Workspace bus health and topic list."""

    def _run() -> str:
        return json.dumps(_get_store().status())

    return _wt("agentbus_status", {}, _run)


@mcp.tool()
def agentbus_review(topic: str | None = None, limit: int = 50) -> str:
    """List events pending human approval (hidden from standard poll)."""

    def _run() -> str:
        return json.dumps(_get_store().review_pending(topic=topic, limit=min(limit, 100)))

    return _wt("agentbus_review", {"topic": topic, "limit": limit}, _run)


@mcp.tool()
def agentbus_approve(
    event_id: int,
    reviewer_id: str | None = None,
    auth_token: str | None = None,
) -> str:
    """Approve a pending event so agents can see it on poll."""

    def _run() -> str:
        check_publish_token(_auth_workspace(), auth_token=auth_token)
        rid = reviewer_id or os.environ.get("AGENTBUS_PRODUCER_ID", "agy")
        try:
            return json.dumps(
                _get_store().approve_event(event_id, reviewer_id=rid, auth_token=auth_token)
            )
        except (ValueError, ForbiddenError) as exc:
            code = getattr(exc, "code", 400)
            return json.dumps({"error": str(exc), "code": code})

    return _wt(
        "agentbus_approve",
        {"event_id": event_id, "reviewer_id": reviewer_id, "auth_token": auth_token},
        _run,
    )


@mcp.tool()
def agentbus_reject(
    event_id: int,
    reason: str = "rejected by human reviewer",
    reviewer_id: str | None = None,
    auth_token: str | None = None,
) -> str:
    """Reject a pending event and notify the originating agent."""

    def _run() -> str:
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

    return _wt(
        "agentbus_reject",
        {
            "event_id": event_id,
            "reason": reason,
            "reviewer_id": reviewer_id,
            "auth_token": auth_token,
        },
        _run,
    )


@mcp.tool()
def agentbus_lock_acquire(
    resource: str,
    owner_id: str,
    ttl_seconds: int | None = None,
    auth_token: str | None = None,
) -> str:
    """Acquire an exclusive advisory lease on a workspace resource."""

    def _run() -> str:
        check_publish_token(_auth_workspace(), auth_token=auth_token)
        return json.dumps(
            _get_lease_store().lock_acquire(resource, owner_id, ttl_seconds)
        )

    return _wt(
        "agentbus_lock_acquire",
        {
            "resource": resource,
            "owner_id": owner_id,
            "ttl_seconds": ttl_seconds,
            "auth_token": auth_token,
        },
        _run,
    )


@mcp.tool()
def agentbus_lock_release(
    resource: str,
    lease_id: str,
    owner_id: str,
    auth_token: str | None = None,
) -> str:
    """Release a held lease (idempotent if already expired)."""

    def _run() -> str:
        check_publish_token(_auth_workspace(), auth_token=auth_token)
        return json.dumps(
            _get_lease_store().lock_release(resource, lease_id, owner_id)
        )

    return _wt(
        "agentbus_lock_release",
        {
            "resource": resource,
            "lease_id": lease_id,
            "owner_id": owner_id,
            "auth_token": auth_token,
        },
        _run,
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

    def _run() -> str:
        check_publish_token(_auth_workspace(), auth_token=auth_token)
        return json.dumps(
            _get_lease_store().lock_renew(resource, lease_id, owner_id, ttl_seconds)
        )

    return _wt(
        "agentbus_lock_renew",
        {
            "resource": resource,
            "lease_id": lease_id,
            "owner_id": owner_id,
            "ttl_seconds": ttl_seconds,
            "auth_token": auth_token,
        },
        _run,
    )


@mcp.tool()
def agentbus_lock_status(resource: str) -> str:
    """Check lock state without acquiring (no auth required)."""

    def _run() -> str:
        return json.dumps(_get_lease_store().lock_status(resource))

    return _wt("agentbus_lock_status", {"resource": resource}, _run)


def run_stdio(
    workspace: Path,
    retention_days: int = 7,
    *,
    rotate_token: bool = False,
    wiretap: bool = False,
    wiretap_log: Path | str | None = None,
    enable_mcpsafe: bool | None = None,
    mcpsafe_lock: Path | str | None = None,
) -> None:
    ws = workspace.resolve()
    ensure_ephemeral_token(ws, rotate=rotate_token)
    if enable_mcpsafe is None:
        from agentbus.mcpsafe import mcpsafe_enabled_from_env

        enable_mcpsafe = mcpsafe_enabled_from_env()
    configure_mcpsafe(enable_mcpsafe, lockfile=mcpsafe_lock, workspace=ws)
    init_store(ws, retention_days)
    log_path: Path | None = None
    if wiretap_log:
        log_path = Path(wiretap_log)
    elif wiretap:
        log_path = ws / ".agentbus" / "wiretap.jsonl"
    configure_wiretap(wiretap, log_path=log_path if wiretap else None)
    mcp.run(transport="stdio")
