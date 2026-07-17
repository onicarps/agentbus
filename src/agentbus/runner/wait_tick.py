"""Evaluate pending waits against the event log; emit resume wakes (v0.16)."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from agentbus.runner.config import default_queue_path
from agentbus.runner.wait_store import (
    WaitRegistration,
    WaitStore,
    build_resume_payload,
    match_predicate,
    resume_idempotency_key,
    utc_now,
    utc_now_iso,
)
from agentbus.schemas import DEAD_LETTER_TOPIC
from agentbus.store import EventStore

log = logging.getLogger("agentbus.runner.wait_tick")

Clock = Callable[[], datetime]


def _event_as_dict(ev: Any) -> dict[str, Any]:
    if isinstance(ev, dict):
        return ev
    if hasattr(ev, "to_dict"):
        return ev.to_dict()
    return {
        "event_id": getattr(ev, "event_id", None),
        "topic": getattr(ev, "topic", None),
        "producer_id": getattr(ev, "producer_id", None),
        "payload": getattr(ev, "payload", {}) or {},
        "causation_id": getattr(ev, "causation_id", None),
    }


def deliver_resume_intake(
    workspace: Path,
    wait: WaitRegistration,
    *,
    resume_event_id: int,
    payload: dict[str, Any],
    causation_id: int | None,
) -> None:
    """Write wake file and/or queue so the waiting runner can pick up the resume."""
    hint = wait.intake_hint or {}
    producer = wait.producer_id
    wake_body = {
        "event_id": resume_event_id,
        "topic": "okf/handoff",
        "causation_id": causation_id,
        "payload": payload,
        "source": "resume",
        "wait_id": wait.wait_id,
        "received_at": utc_now_iso(),
    }

    # Always write classical WAKE.<producer>.json (wake_file runners + fallback)
    wake_rel = hint.get("wake_file") or f".agentbus/WAKE.{producer}.json"
    wake_path = Path(wake_rel)
    if not wake_path.is_absolute():
        wake_path = workspace / wake_path
    wake_path.parent.mkdir(parents=True, exist_ok=True)
    wake_path.write_text(
        json.dumps(wake_body, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )

    # Append queue record when runtime known (webhook_queue runners)
    runtime = hint.get("runtime") or producer
    mode = hint.get("mode")
    if mode == "webhook_queue" or hint.get("queue_path") or runtime:
        if hint.get("queue_path"):
            qpath = Path(hint["queue_path"])
            if not qpath.is_absolute():
                qpath = workspace / qpath
        else:
            qpath = default_queue_path(workspace, str(runtime))
        qpath.parent.mkdir(parents=True, exist_ok=True)
        rec = {
            "received_at": utc_now_iso(),
            "event_id": resume_event_id,
            "runtime": runtime,
            "from": "agentbus",
            "to": producer,
            "summary": payload.get("summary"),
            "topic": "okf/handoff",
            "causation_id": causation_id,
            "raw": {
                "event_id": resume_event_id,
                "topic": "okf/handoff",
                "causation_id": causation_id,
                "payload": payload,
                "source": "resume",
            },
        }
        with qpath.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")


def _publish_resume(
    store: EventStore,
    wait: WaitRegistration,
    *,
    fulfilled_by: int,
    status: str,
    reason: str,
) -> tuple[Any, bool]:
    payload = build_resume_payload(
        wait, fulfilled_by=fulfilled_by, status=status, reason=reason
    )
    try:
        chain_causation = int(wait.chain_key)
    except (TypeError, ValueError):
        chain_causation = wait.origin_event_id
    event, duplicate = store.publish(
        topic="okf/handoff",
        producer_id="agentbus",
        schema_version="1.0",
        payload=payload,
        causation_id=chain_causation,
        idempotency_key=resume_idempotency_key(wait.wait_id, fulfilled_by),
        skip_rbac=True,
        skip_intercept=True,
    )
    return event, duplicate


def _publish_wait_timeout_dead_letter(
    store: EventStore, wait: WaitRegistration
) -> Any:
    """Escalate timed-out wait to okf/dead-letter (reason=WAIT_TIMEOUT)."""
    original = {
        "wait_id": wait.wait_id,
        "origin_event_id": wait.origin_event_id,
        "producer_id": wait.producer_id,
        "chain_key": wait.chain_key,
        "timeout_at": wait.timeout_at,
        "predicate": wait.predicate.to_dict(),
    }
    payload = {
        "reason": "WAIT_TIMEOUT",
        "original_event_id": max(1, int(wait.origin_event_id) or 1),
        "original_event": original,
        "summary": (
            f"Wait timeout: wait_id={wait.wait_id} origin={wait.origin_event_id} "
            f"timeout_at={wait.timeout_at}"
        ),
    }
    event, _ = store.publish(
        topic=DEAD_LETTER_TOPIC,
        producer_id="agentbus",
        schema_version="1.0",
        payload=payload,
        causation_id=wait.origin_event_id or None,
        idempotency_key=f"wait-timeout:{wait.wait_id}",
        skip_rbac=True,
        skip_intercept=True,
    )
    return event


def _wait_terminal_status(resume_status: str) -> str:
    """Map resume payload status (ok|timeout) → WaitRegistration terminal status."""
    if resume_status == "timeout":
        return "timeout"
    if resume_status == "cancelled":
        return "cancelled"
    # resume status "ok" and any other success-like value → fulfilled
    return "fulfilled"


def fulfill_wait(
    workspace: Path,
    store: EventStore,
    waits: WaitStore,
    wait: WaitRegistration,
    *,
    fulfilled_by: int,
    status: str,
    reason: str,
) -> dict[str, Any] | None:
    """Mark wait terminal, publish resume (idempotent), deliver intake.

    ``status`` is the resume payload status (``ok`` | ``timeout``).
    WaitRegistration is stored as ``fulfilled`` | ``timeout`` (terminal).

    Returns result dict or None if wait was already terminal / duplicate no-op.
    """
    if wait.is_terminal:
        return None

    # Re-load to reduce double-tick races
    current = waits.load(wait.wait_id) or wait
    if current.is_terminal:
        return None

    resume_status = status if status in ("ok", "timeout") else "ok"
    event, duplicate = _publish_resume(
        store,
        current,
        fulfilled_by=fulfilled_by,
        status=resume_status,
        reason=reason,
    )
    waits.mark_terminal(
        current,
        status=_wait_terminal_status(resume_status),
        fulfilled_by=fulfilled_by,
    )

    if not duplicate:
        try:
            chain_causation = int(current.chain_key)
        except (TypeError, ValueError):
            chain_causation = current.origin_event_id
        deliver_resume_intake(
            workspace,
            current,
            resume_event_id=event.event_id,
            payload=build_resume_payload(
                current,
                fulfilled_by=fulfilled_by,
                status=resume_status,
                reason=reason,
            ),
            causation_id=chain_causation,
        )

    return {
        "wait_id": current.wait_id,
        "status": resume_status,
        "wait_status": _wait_terminal_status(resume_status),
        "fulfilled_by": fulfilled_by,
        "resume_event_id": event.event_id,
        "duplicate": duplicate,
    }


def tick_waits(
    workspace: Path,
    store: EventStore,
    *,
    waits: WaitStore | None = None,
    now: datetime | None = None,
    topics: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Scan pending waits: timeouts + new events from durable cursor.

    Lost-wakeup safe: scans from last-seen event_id, not only live arrivals.
    Late fulfill after timeout: terminal waits are not re-opened.
    """
    wait_store = waits or WaitStore(workspace, now_fn=(lambda: now) if now else None)
    clock = now or utc_now()
    results: list[dict[str, Any]] = []
    pending = wait_store.list_waits(status="pending")
    if not pending:
        # Still advance cursor if events exist so we don't re-scan forever later
        return results

    # 1) Timeouts first
    for wait in pending:
        if not wait_store.is_expired(wait, now=clock):
            continue
        try:
            dl = _publish_wait_timeout_dead_letter(store, wait)
            fulfilled_by = int(dl.event_id)
        except Exception as exc:  # noqa: BLE001
            log.warning("dead-letter publish failed wait=%s: %s", wait.wait_id, exc)
            fulfilled_by = 0
        outcome = fulfill_wait(
            workspace,
            store,
            wait_store,
            wait,
            fulfilled_by=fulfilled_by or wait.origin_event_id,
            status="timeout",
            reason=f"wait timeout at {wait.timeout_at}",
        )
        if outcome:
            results.append(outcome)

    # Refresh pending after timeouts
    pending = wait_store.list_waits(status="pending")
    if not pending:
        return results

    cursor = wait_store.get_cursor()
    scan_topics = topics or ["okf/handoff"]
    # Also consider min origin so pre-cursor fulfillments for new waits resolve
    min_origin = min((w.origin_event_id for w in pending), default=cursor)
    since = min(cursor, max(0, min_origin - 1))

    candidates: list[dict[str, Any]] = []
    max_seen = cursor
    for topic in scan_topics:
        try:
            polled = store.poll(topic, since_id=since, limit=500)
        except Exception as exc:  # noqa: BLE001
            log.warning("poll %s failed during wait tick: %s", topic, exc)
            continue
        for raw in polled.get("events") or []:
            ev = _event_as_dict(raw)
            try:
                eid = int(ev.get("event_id") or 0)
            except (TypeError, ValueError):
                continue
            if eid > max_seen:
                max_seen = eid
            candidates.append(ev)

    # Stable order by event_id
    candidates.sort(key=lambda e: int(e.get("event_id") or 0))

    for wait in pending:
        if wait.is_terminal:
            continue
        # Skip if timed out mid-loop (already handled)
        if wait_store.is_expired(wait, now=clock):
            continue
        for ev in candidates:
            try:
                eid = int(ev.get("event_id") or 0)
            except (TypeError, ValueError):
                continue
            # Only events at or after wait registration origin matter for
            # lost-wakeup; allow any event_id > 0 that matches predicate.
            if not match_predicate(
                wait.predicate, ev, waiter_producer_id=wait.producer_id
            ):
                continue
            outcome = fulfill_wait(
                workspace,
                store,
                wait_store,
                wait,
                fulfilled_by=eid,
                status="ok",
                reason=(
                    f"predicate matched event_id={eid} "
                    f"from={(ev.get('payload') or {}).get('from')}"
                ),
            )
            if outcome:
                results.append(outcome)
            break  # one fulfillment per wait

    if max_seen > cursor:
        wait_store.set_cursor(max_seen)

    return results


def register_wait_from_await_drop(
    workspace: Path,
    *,
    runner_id: str,
    producer_id: str,
    chain_key: str,
    origin_event_id: int,
    await_data: dict[str, Any],
    intake_hint: dict[str, Any] | None = None,
    waits: WaitStore | None = None,
) -> WaitRegistration:
    """Upsert WaitRegistration from CLI await.json drop (or create defaults)."""
    from agentbus.runner.wait_store import WaitPredicate

    store = waits or WaitStore(workspace)
    wait_id = str(await_data.get("wait_id") or "")
    if wait_id:
        existing = store.load(wait_id)
        if existing is not None:
            if intake_hint and not existing.intake_hint:
                existing.intake_hint = dict(intake_hint)
                store.save(existing)
            return existing

    pred_raw = await_data.get("predicate")
    if not isinstance(pred_raw, dict):
        pred_raw = {
            "causation_id": await_data.get("causation_id"),
            "from_any": await_data.get("expect_from")
            or await_data.get("from_any")
            or [],
            "summary_contains": await_data.get("match")
            or await_data.get("summary_contains"),
            "topic": await_data.get("topic") or "okf/handoff",
        }
    predicate = WaitPredicate.from_dict(pred_raw)
    timeout_hours = await_data.get("timeout_hours")
    hint = intake_hint
    if hint is None and isinstance(await_data.get("intake_hint"), dict):
        hint = await_data["intake_hint"]
    snapshot = (
        await_data.get("task_snapshot")
        if isinstance(await_data.get("task_snapshot"), dict)
        else None
    )
    return store.create(
        runner_id=str(await_data.get("runner_id") or runner_id),
        producer_id=str(await_data.get("producer_id") or producer_id),
        chain_key=str(await_data.get("chain_key") or chain_key),
        origin_event_id=int(await_data.get("origin_event_id") or origin_event_id),
        predicate=predicate,
        reason=str(await_data.get("reason") or "await"),
        timeout_hours=timeout_hours,
        wait_id=wait_id or None,
        intake_hint=hint,
        task_snapshot=snapshot,
    )
