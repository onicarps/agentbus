"""Main runner loop — process wake envelopes, publish ACK, mark done."""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from agentbus.runner.adapters import get_adapter
from agentbus.runner.budget import ChainBudget
from agentbus.runner.config import (
    RunnerConfig,
    default_done_path,
    default_queue_path,
    runner_done_path,
    runner_state_path,
)
from agentbus.runner.intake import (
    append_done_id,
    iter_queue_envelopes,
    load_done_ids,
    read_wake_file,
)
from agentbus.runner.types import BROADCAST_TO, TurnResult, WakeEnvelope
from agentbus.store import EventStore

log = logging.getLogger("agentbus.runner")


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _done_path(workspace: Path, cfg: RunnerConfig) -> Path:
    intake = cfg.intake
    if intake.done_path:
        p = Path(intake.done_path)
        return p if p.is_absolute() else (workspace / p).resolve()
    if intake.mode == "webhook_queue" and intake.runtime:
        return default_done_path(workspace, intake.runtime)
    return runner_done_path(workspace, cfg.runner_id)


def _queue_path(workspace: Path, cfg: RunnerConfig) -> Path:
    intake = cfg.intake
    if intake.queue_path:
        p = Path(intake.queue_path)
        return p if p.is_absolute() else (workspace / p).resolve()
    if not intake.runtime:
        raise ValueError("intake.runtime required when queue_path unset")
    return default_queue_path(workspace, intake.runtime)


def _wake_file_path(workspace: Path, cfg: RunnerConfig) -> Path:
    intake = cfg.intake
    if intake.wake_file:
        p = Path(intake.wake_file)
        return p if p.is_absolute() else (workspace / p).resolve()
    # default WAKE.<runtime or producer>.json
    name = intake.runtime or cfg.producer_id
    return workspace / ".agentbus" / f"WAKE.{name}.json"


def _write_run_log(
    workspace: Path, cfg: RunnerConfig, wake: WakeEnvelope, result: TurnResult
) -> Path:
    runs = cfg.resolve(workspace, cfg.runs_dir) or (workspace / ".agentbus" / "runs")
    run_dir = runs / str(wake.event_id)
    run_dir.mkdir(parents=True, exist_ok=True)
    path = run_dir / "result.json"
    path.write_text(
        json.dumps(
            {
                "runner_id": cfg.runner_id,
                "processed_at": _utc_now(),
                "wake": {
                    "event_id": wake.event_id,
                    "from": wake.from_agent,
                    "to": wake.to,
                    "summary": wake.summary,
                    "source": wake.source,
                    "topic": wake.topic,
                },
                "result": {
                    "ok": result.ok,
                    "summary": result.summary,
                    "detail": result.detail,
                },
            },
            indent=2,
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )
    return path


def _parse_iso_utc(ts: str) -> datetime | None:
    raw = (ts or "").strip()
    if not raw:
        return None
    try:
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def wake_received_at(wake: WakeEnvelope) -> datetime | None:
    """Best-effort event time from queue/wake payload (for max_event_age_hours)."""
    raw = wake.raw or {}
    candidates: list[Any] = [
        raw.get("received_at"),
        raw.get("woken_at"),
        raw.get("timestamp"),
    ]
    nested = raw.get("raw")
    if isinstance(nested, dict):
        candidates.extend(
            [nested.get("received_at"), nested.get("woken_at"), nested.get("timestamp")]
        )
    for c in candidates:
        if isinstance(c, str):
            dt = _parse_iso_utc(c)
            if dt is not None:
                return dt
    return None


def should_skip(wake: WakeEnvelope, cfg: RunnerConfig) -> str | None:
    """Return skip reason or None if should process."""
    to = (wake.to or "").strip()
    if not cfg.allow_broadcast and to in BROADCAST_TO:
        return "broadcast"
    if to not in cfg.accept_to:
        return f"to_not_accepted:{to}"
    max_age = int(cfg.budget.max_event_age_hours or 0)
    if max_age > 0:
        received = wake_received_at(wake)
        if received is not None:
            age_h = (datetime.now(timezone.utc) - received).total_seconds() / 3600.0
            if age_h > max_age:
                return f"stale_age_hours:{age_h:.1f}>{max_age}"
    return None


def process_envelope(
    workspace: Path,
    cfg: RunnerConfig,
    wake: WakeEnvelope,
    *,
    store: EventStore,
    budget: ChainBudget,
    done_path: Path,
    done: set[int],
) -> dict[str, Any]:
    if wake.event_id in done:
        return {"event_id": wake.event_id, "status": "already_done"}

    skip = should_skip(wake, cfg)
    if skip:
        append_done_id(done_path, wake.event_id)
        done.add(wake.event_id)
        log.info("skip event_id=%s reason=%s", wake.event_id, skip)
        return {"event_id": wake.event_id, "status": "skipped", "reason": skip}

    chain = budget.chain_key(wake.event_id, wake.causation_id)
    if budget.would_exceed(chain):
        result = TurnResult(
            ok=False,
            summary=(
                f"RUNNER_ERROR: budget exceeded for chain={chain} "
                f"event_id={wake.event_id} max={cfg.budget.max_turns_per_chain}"
            ),
            detail={"chain": chain, "reason": "budget"},
        )
    else:
        adapter = get_adapter(
            cfg.adapter.type,
            workspace=workspace,
            options=cfg.adapter.options,
        )
        remaining = budget.remaining(chain)
        try:
            result = adapter.start_turn(wake, budget_remaining=remaining)
        except Exception as exc:  # noqa: BLE001 — poison-pill path
            log.exception("adapter error event_id=%s", wake.event_id)
            result = TurnResult(
                ok=False,
                summary=(
                    f"RUNNER_ERROR: adapter exception event_id={wake.event_id} "
                    f"err={type(exc).__name__}"
                ),
                detail={"error": str(exc)},
            )

    _write_run_log(workspace, cfg, wake, result)

    reply_to = wake.from_agent or "agy"
    if not reply_to or reply_to == cfg.producer_id:
        reply_to = wake.from_agent or "all"
        # avoid engineer RBAC issues: never use bare broadcast if we can help it
        if reply_to in BROADCAST_TO:
            reply_to = "agy"

    event, duplicate = store.publish(
        topic="okf/handoff",
        producer_id=cfg.producer_id,
        schema_version="1.0",
        payload={
            "from": cfg.producer_id,
            "to": reply_to if reply_to not in BROADCAST_TO else "agy",
            "summary": result.summary,
        },
        causation_id=wake.event_id,
        idempotency_key=f"runner-ack:{cfg.runner_id}:{wake.event_id}",
    )

    # Count every executed turn (success or error) toward chain budget.
    if not duplicate:
        budget.record(chain)

    append_done_id(done_path, wake.event_id)
    done.add(wake.event_id)

    return {
        "event_id": wake.event_id,
        "status": "processed",
        "ok": result.ok,
        "ack_event_id": event.event_id,
        "duplicate_ack": duplicate,
        "summary": result.summary,
    }


def collect_pending(
    workspace: Path, cfg: RunnerConfig, done: set[int]
) -> list[WakeEnvelope]:
    if cfg.intake.mode == "webhook_queue":
        q = _queue_path(workspace, cfg)
        return list(iter_queue_envelopes(q, done))
    if cfg.intake.mode == "wake_file":
        env = read_wake_file(_wake_file_path(workspace, cfg), done)
        return [env] if env else []
    raise ValueError(f"unsupported intake mode {cfg.intake.mode}")


def run_once(workspace: Path, cfg: RunnerConfig) -> list[dict[str, Any]]:
    """Process all currently pending envelopes; return result dicts."""
    from agentbus.workspace_guard import assert_workspace_supported

    assert_workspace_supported(workspace)
    done_path = _done_path(workspace, cfg)
    done = load_done_ids(done_path)
    budget = ChainBudget(
        runner_state_path(workspace, cfg.runner_id),
        cfg.budget.max_turns_per_chain,
    )
    pending = collect_pending(workspace, cfg, done)
    results: list[dict[str, Any]] = []
    store = EventStore(workspace)
    try:
        for wake in pending:
            results.append(
                process_envelope(
                    workspace,
                    cfg,
                    wake,
                    store=store,
                    budget=budget,
                    done_path=done_path,
                    done=done,
                )
            )
    finally:
        store.close()
    return results


def run_loop(workspace: Path, cfg: RunnerConfig, *, once: bool = False) -> int:
    """Run until interrupted, or a single drain if once=True. Return exit code."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    if once:
        results = run_once(workspace, cfg)
        log.info("run --once processed=%s", len(results))
        print(json.dumps({"processed": len(results), "results": results}, indent=2))
        return 0

    log.info(
        "runner up runner_id=%s intake=%s producer=%s",
        cfg.runner_id,
        cfg.intake.mode,
        cfg.producer_id,
    )
    while True:
        results = run_once(workspace, cfg)
        if results:
            log.info("batch size=%s", len(results))
        time.sleep(max(0.05, cfg.poll_interval_ms / 1000.0))
