"""Publish SRE_STATUS handoffs onto the AgentBus (okf/handoff)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from agentbus_ops.policy import DEFAULT_TOPIC


class PublishError(RuntimeError):
    """Bus publish failed."""


def publish_sre_status(
    workspace: str | Path,
    payload: dict[str, Any],
    *,
    producer_id: str,
    idempotency_key: str | None = None,
    topic: str = DEFAULT_TOPIC,
    schema_version: str = "1.0",
) -> dict[str, Any]:
    """Append one okf/handoff event via EventStore.

    Returns ``{"event_id", "topic", "duplicate", ...}``.
    """
    from agentbus.schemas import validate_payload
    from agentbus.store import EventStore

    ws = Path(workspace).resolve()
    store = EventStore(ws)
    try:
        body = validate_payload(topic, dict(payload), workspace=ws, producer_id=producer_id)
        event, duplicate = store.publish(
            topic=topic,
            producer_id=producer_id,
            schema_version=schema_version,
            payload=body,
            idempotency_key=idempotency_key,
        )
        return {
            "event_id": event.event_id,
            "topic": event.topic,
            "timestamp": event.timestamp,
            "duplicate": duplicate,
        }
    except Exception as exc:
        raise PublishError(str(exc)) from exc
    finally:
        store.close()


def compact_metrics_snippet(workspace: str | Path) -> str:
    """Best-effort compact metrics line (same shape as bash --include-metrics)."""
    try:
        from agentbus.metrics import collect_workspace_metrics
    except ImportError:
        return ""
    try:
        report = collect_workspace_metrics(Path(workspace).resolve())
    except Exception:
        return ""
    d = report.to_dict() if hasattr(report, "to_dict") else report
    if not isinstance(d, dict):
        return ""
    st = d.get("status") or {}
    sla = d.get("sla") or {}
    parts = [
        f"events={st.get('event_count', '?')}",
        f"latest={st.get('latest_event_id', '?')}",
        f"pending={st.get('pending_count', '?')}",
        f"sla_active={st.get('sla_active_count', sla.get('active_count', '?'))}",
    ]
    for i in (d.get("ingress") or [])[:4]:
        if not isinstance(i, dict):
            continue
        name = i.get("service") or i.get("runtime") or "?"
        q = i.get("queue") or {}
        und = q.get("undrained", i.get("undrained", "?"))
        en = "on" if i.get("enabled", True) else "off"
        parts.append(f"ingress:{name}={en}/undrained={und}")
    return "; ".join(parts)
