"""Resilient messaging helpers: dead-letter escalate + file spillover DLQ.

When product-side retries are exhausted (SQLite lock storms, webhook delivery
failures), escalate to ``okf/dead-letter`` (reason=RETRY_EXHAUSTED). If the bus
itself cannot accept the dead-letter, append a line to the spillover file so
ops can recover without silent loss or infinite retry thrash.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from agentbus.retry import (
    RetryExhaustedError,
    call_with_retry,
    default_publish_policy,
    is_transient_sqlite_error,
)
from agentbus.schemas import DEAD_LETTER_TOPIC, validate_payload

log = logging.getLogger("agentbus.resilience")

RETRY_EXHAUSTED_REASON = "RETRY_EXHAUSTED"
SPILLOVER_REL = Path(".agentbus") / "dead-letter" / "spillover.jsonl"


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def spillover_path(workspace: Path | str) -> Path:
    return Path(workspace).resolve() / SPILLOVER_REL


def append_spillover(
    workspace: Path | str,
    record: dict[str, Any],
) -> Path:
    """Append one JSONL record to the file-backed dead-letter spillover."""
    path = spillover_path(workspace)
    path.parent.mkdir(parents=True, exist_ok=True)
    line = dict(record)
    line.setdefault("spilled_at", _utc_now())
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(line, ensure_ascii=False, default=str) + "\n")
    return path


def escalate_retry_exhausted(
    store: Any,
    *,
    original_event_id: int,
    original_event: dict[str, Any],
    summary: str,
    producer_id: str = "agentbus",
    causation_id: int | None = None,
    idempotency_key: str | None = None,
    workspace: Path | str | None = None,
) -> dict[str, Any]:
    """Publish ``okf/dead-letter`` with reason RETRY_EXHAUSTED; spillover on failure.

    Returns a status dict::
        {"ok": True, "event_id": N, "channel": "bus"}
        {"ok": True, "channel": "spillover", "path": "..."}
        {"ok": False, "error": "..."}
    """
    oid = max(1, int(original_event_id) or 1)
    payload = {
        "reason": RETRY_EXHAUSTED_REASON,
        "original_event_id": oid,
        "original_event": original_event,
        "summary": (summary or "retry exhausted")[:2000],
    }
    # Validate early so permanent schema errors do not burn retries.
    try:
        payload = validate_payload(DEAD_LETTER_TOPIC, payload)
    except Exception as exc:  # noqa: BLE001
        log.warning("dead-letter payload invalid: %s", exc)
        ws = workspace or getattr(store, "workspace", None)
        if ws is not None:
            path = append_spillover(
                ws,
                {
                    "reason": RETRY_EXHAUSTED_REASON,
                    "original_event_id": oid,
                    "original_event": original_event,
                    "summary": summary,
                    "validate_error": str(exc),
                },
            )
            return {"ok": True, "channel": "spillover", "path": str(path)}
        return {"ok": False, "error": f"invalid_payload:{exc}"}

    def _publish() -> Any:
        event, _dup = store.publish(
            topic=DEAD_LETTER_TOPIC,
            producer_id=producer_id,
            schema_version="1.0",
            payload=payload,
            causation_id=causation_id if causation_id is not None else oid,
            idempotency_key=idempotency_key,
            skip_rbac=True,
            skip_intercept=True,
        )
        return event

    try:
        # Nested publish already retries; keep a thin outer attempt for safety.
        event = call_with_retry(
            _publish,
            policy=default_publish_policy(),
            is_retryable=is_transient_sqlite_error,
        )
        return {
            "ok": True,
            "channel": "bus",
            "event_id": getattr(event, "event_id", None),
        }
    except (RetryExhaustedError, Exception) as exc:  # noqa: BLE001
        log.warning(
            "dead-letter bus publish failed after retries: %s — spilling to file",
            exc,
        )
        ws = workspace or getattr(store, "workspace", None)
        if ws is None:
            return {"ok": False, "error": str(exc)}
        path = append_spillover(
            ws,
            {
                "reason": RETRY_EXHAUSTED_REASON,
                "original_event_id": oid,
                "original_event": original_event,
                "summary": summary,
                "publish_error": str(exc),
            },
        )
        return {"ok": True, "channel": "spillover", "path": str(path)}


def publish_or_spill(
    store: Any,
    *,
    workspace: Path | str,
    publish_kwargs: dict[str, Any],
    spill_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Publish via store (store already retries); on exhaustion spillover + DLQ.

    ``EventStore.publish`` applies exponential backoff + jitter. This helper
    catches terminal lock failures and escalates without hammering the DB.

    Returns::
        {"ok": True, "event": Event, "duplicate": bool}
        {"ok": False, "channel": "spillover", "path": "...", "error": "..."}
    """
    try:
        event, duplicate = store.publish(**publish_kwargs)
        return {"ok": True, "event": event, "duplicate": duplicate}
    except Exception as exc:  # noqa: BLE001 — classified below
        if not is_transient_sqlite_error(exc) and not isinstance(
            exc, RetryExhaustedError
        ):
            raise
        last: BaseException = (
            exc.last_error if isinstance(exc, RetryExhaustedError) and exc.last_error else exc
        )
        ctx = dict(spill_context or {})
        ctx.setdefault("topic", publish_kwargs.get("topic"))
        ctx.setdefault("producer_id", publish_kwargs.get("producer_id"))
        ctx.setdefault("payload", publish_kwargs.get("payload"))
        ctx.setdefault("causation_id", publish_kwargs.get("causation_id"))
        ctx["error"] = str(last)
        path = append_spillover(
            workspace,
            {
                "reason": RETRY_EXHAUSTED_REASON,
                "summary": (
                    f"publish retry exhausted topic={publish_kwargs.get('topic')} "
                    f"err={last!r}"
                )[:2000],
                "original_event_id": int(publish_kwargs.get("causation_id") or 1),
                "original_event": ctx,
            },
        )
        # Best-effort bus dead-letter (may also spillover if DB still locked).
        try:
            escalate_retry_exhausted(
                store,
                original_event_id=int(publish_kwargs.get("causation_id") or 1),
                original_event=ctx,
                summary=(
                    f"Publish retry exhausted: topic={publish_kwargs.get('topic')} "
                    f"producer={publish_kwargs.get('producer_id')}"
                )[:2000],
                producer_id="agentbus",
                causation_id=publish_kwargs.get("causation_id"),
                idempotency_key=(
                    f"retry-exhausted:{publish_kwargs.get('idempotency_key')}"
                    if publish_kwargs.get("idempotency_key")
                    else None
                ),
                workspace=workspace,
            )
        except Exception:  # noqa: BLE001
            log.exception("escalate_retry_exhausted failed")
        return {
            "ok": False,
            "channel": "spillover",
            "path": str(path),
            "error": str(last),
        }


def env_flag(name: str, default: bool = True) -> bool:
    """Parse truthy env flag (1/true/yes/on)."""
    raw = (os.environ.get(name) or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}
