"""Edge-triggered SRE policy (pure) — port of sre_edge_watchdog.sh decision engine."""

from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from agentbus_ops.probe import LEVELS, HealthSnapshot, _EXIT_FOR_LEVEL
from agentbus_ops.state import WatchdogState

DEFAULT_COOLDOWN_SECONDS = 60
DEFAULT_PRODUCER_ID = "aider"
DEFAULT_TO_AGENT = "all"
DEFAULT_TOPIC = "okf/handoff"
SUMMARY_MAX_LEN = 900

_LINKS = [
    "/runbooks/sre-edge-triggered.md",
    "/runbooks/aider-sre-health.md",
    "/scripts/sre_edge_watchdog.sh",
]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def notes_fingerprint(level: str, notes: list[str]) -> str:
    notes_key = ",".join(str(n) for n in notes)
    return hashlib.sha256(f"{level}|{notes_key}".encode()).hexdigest()[:12]


def build_summary(
    health: HealthSnapshot,
    *,
    prev_level: str | None,
    metrics_snippet: str = "",
) -> str:
    parts = [f"SRE_STATUS: {health.level}"]
    if prev_level and prev_level != health.level:
        parts.append(f"(was {prev_level})")
    notes = health.notes or []
    if notes:
        top = [str(n) for n in notes if not str(n).startswith("skip_")][:6]
        skips = sum(1 for n in notes if str(n).startswith("skip_"))
        if top:
            parts.append("notes=" + ",".join(top))
        if skips:
            parts.append(f"skipped={skips}")
    if metrics_snippet:
        parts.append(f"metrics[{metrics_snippet}]")
    parts.append(f"workspace={health.workspace or ''}")
    if health.latest_event_id is not None:
        parts.append(f"latest_event_id={health.latest_event_id}")
    summary = " | ".join(parts)
    if len(summary) > SUMMARY_MAX_LEN:
        summary = summary[: SUMMARY_MAX_LEN - 3] + "..."
    return summary


def build_idempotency_key(level: str, fingerprint: str, now_iso: str) -> str:
    hour_bucket = now_iso[:13].replace("-", "").replace("T", "")  # YYYYMMDDHH
    return f"sre-status-{level}-{hour_bucket}-{fingerprint}"


@dataclass
class Decision:
    """Result of one edge-policy evaluation."""

    action: str
    reason: str
    should_publish: bool
    level: str
    prev_level: str | None
    idempotency_key: str | None
    summary: str
    payload: dict[str, Any] | None
    state: WatchdogState
    health: HealthSnapshot
    cooldown_seconds: int
    state_file: str
    dry_run: bool = False
    publish_event_id: int | None = None
    publish_ok: bool = True
    publish_error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "action": self.action,
            "reason": self.reason,
            "should_publish": self.should_publish,
            "level": self.level,
            "prev_level": self.prev_level,
            "idempotency_key": self.idempotency_key,
            "summary": self.summary,
            "payload": self.payload,
            "state": self.state.to_dict(),
            "health": self.health.to_dict(),
            "cooldown_seconds": self.cooldown_seconds,
            "state_file": self.state_file,
            "dry_run": self.dry_run,
            "publish_event_id": self.publish_event_id,
            "publish_ok": self.publish_ok,
            "publish_error": self.publish_error,
        }


def decide(
    health: HealthSnapshot,
    prev: WatchdogState | None,
    *,
    state_file: str,
    cooldown_seconds: int = DEFAULT_COOLDOWN_SECONDS,
    force_bootstrap_publish: bool = False,
    seed_level: str | None = None,
    metrics_snippet: str = "",
    producer_id: str = DEFAULT_PRODUCER_ID,
    to_agent: str = DEFAULT_TO_AGENT,
    now_epoch: int | None = None,
    now_iso: str | None = None,
) -> Decision:
    """Pure edge policy. No I/O. Parity with bash decision engine."""
    level = health.level if health.level in LEVELS else "critical"
    now_epoch = int(now_epoch if now_epoch is not None else time.time())
    now_iso = now_iso or _utc_now_iso()
    notes = list(health.notes or [])
    fp = notes_fingerprint(level, notes)

    # Optional seed for tests (pretend previous level)
    if seed_level:
        seed = seed_level if seed_level in LEVELS else "critical"
        prev = WatchdogState(
            level=seed,
            sre_status=seed,
            last_published_level=seed,
            last_published_at=None,
            last_published_epoch=0,
            notes_fingerprint="",
            bootstrap=False,
        )

    bootstrap = prev is None
    prev_level = prev.level if prev else None
    last_pub_level = prev.last_published_level if prev else None
    last_pub_epoch = int(prev.last_published_epoch or 0) if prev else 0

    action = "silence"
    reason = ""
    should_publish = False

    if bootstrap and not force_bootstrap_publish:
        action = "bootstrap_seed"
        reason = "no prior state; seed current level without publish (anti-storm)"
        should_publish = False
    elif bootstrap and force_bootstrap_publish:
        action = "bootstrap_publish"
        reason = "no prior state; --force-bootstrap-publish"
        should_publish = True
    elif level != prev_level:
        # Real edge. Cooldown only suppresses *duplicate* same-level re-publish.
        if (
            last_pub_level == level
            and last_pub_epoch
            and (now_epoch - last_pub_epoch) < cooldown_seconds
        ):
            action = "cooldown_suppress"
            reason = (
                f"level={level} already published {now_epoch - last_pub_epoch}s ago "
                f"(<{cooldown_seconds}s cooldown)"
            )
            should_publish = False
        else:
            action = "publish_transition"
            reason = f"level {prev_level} -> {level}"
            should_publish = True
    else:
        action = "silence"
        reason = f"level unchanged ({level})"
        should_publish = False

    summary = build_summary(health, prev_level=prev_level, metrics_snippet=metrics_snippet)
    idem = build_idempotency_key(level, fp, now_iso) if should_publish else None

    exit_code = health.exit_code
    if exit_code not in (0, 1, 2):
        exit_code = _EXIT_FOR_LEVEL.get(level, 2)

    new_state = WatchdogState(
        schema_version="1.0",
        level=level,
        sre_status=level,
        exit_code=exit_code,
        last_checked_at=now_iso,
        last_checked_epoch=now_epoch,
        notes=notes,
        notes_fingerprint=fp,
        workspace=health.workspace,
        latest_event_id=health.latest_event_id,
        disabled_services=list(health.disabled_services or []),
        last_action=action,
        last_action_reason=reason,
        last_published_level=prev.last_published_level if prev else None,
        last_published_at=prev.last_published_at if prev else None,
        last_published_epoch=int(prev.last_published_epoch or 0) if prev else 0,
        last_idempotency_key=prev.last_idempotency_key if prev else None,
        bootstrap=bootstrap,
    )

    if should_publish:
        new_state.last_published_level = level
        new_state.last_published_at = now_iso
        new_state.last_published_epoch = now_epoch
        new_state.last_idempotency_key = idem
    elif bootstrap and not force_bootstrap_publish:
        # Seed as if we know current level without claiming a bus publish
        new_state.last_published_level = level
        new_state.last_published_at = None
        new_state.last_published_epoch = 0
        new_state.last_idempotency_key = None

    payload: dict[str, Any] | None = None
    if should_publish:
        # okf/handoff schema: additionalProperties=false — only known keys.
        payload = {
            "from": producer_id,
            "to": to_agent,
            "initiative": "agentbus",
            "summary": summary,
            "links": list(_LINKS),
        }

    return Decision(
        action=action,
        reason=reason,
        should_publish=should_publish,
        level=level,
        prev_level=prev_level,
        idempotency_key=idem,
        summary=summary,
        payload=payload,
        state=new_state,
        health=health,
        cooldown_seconds=cooldown_seconds,
        state_file=state_file,
    )
