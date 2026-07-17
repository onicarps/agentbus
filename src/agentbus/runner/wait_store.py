"""Durable wait registrations + pure predicate/resume helpers (v0.16)."""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from agentbus.runner.types import WakeEnvelope

log = logging.getLogger("agentbus.runner.wait_store")

DEFAULT_TIMEOUT_HOURS = 4
MAX_TIMEOUT_HOURS = 24
WAITS_DIRNAME = "waits"
CURSOR_FILENAME = "_cursor.json"

# Terminal wait statuses — no further live resumes.
TERMINAL_STATUSES = frozenset({"fulfilled", "timeout", "cancelled"})


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso(now: datetime | None = None) -> str:
    dt = now or utc_now()
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_iso_utc(ts: str) -> datetime | None:
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


def clamp_timeout_hours(hours: float | int | None) -> float:
    if hours is None:
        return float(DEFAULT_TIMEOUT_HOURS)
    h = float(hours)
    if h <= 0:
        return float(DEFAULT_TIMEOUT_HOURS)
    return min(h, float(MAX_TIMEOUT_HOURS))


def new_wait_id() -> str:
    return f"w_{uuid.uuid4().hex[:12]}"


@dataclass
class WaitPredicate:
    """Predicate for matching bus events against a wait.

    Primary keys: from_any + causation_id. summary_contains is secondary only.
    """

    causation_id: int | None = None
    from_any: list[str] = field(default_factory=list)
    summary_contains: str | None = None
    topic: str = "okf/handoff"

    def to_dict(self) -> dict[str, Any]:
        return {
            "causation_id": self.causation_id,
            "from_any": list(self.from_any),
            "summary_contains": self.summary_contains,
            "topic": self.topic,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> WaitPredicate:
        d = data if isinstance(data, dict) else {}
        from_any = d.get("from_any") or []
        if isinstance(from_any, str):
            from_any = [from_any]
        cid = d.get("causation_id")
        try:
            causation_id = int(cid) if cid is not None else None
        except (TypeError, ValueError):
            causation_id = None
        sc = d.get("summary_contains")
        return cls(
            causation_id=causation_id,
            from_any=[str(x) for x in from_any if x],
            summary_contains=str(sc) if sc else None,
            topic=str(d.get("topic") or "okf/handoff"),
        )


@dataclass
class WaitRegistration:
    wait_id: str
    runner_id: str
    producer_id: str
    chain_key: str
    origin_event_id: int
    suspended_at: str
    timeout_at: str
    reason: str
    predicate: WaitPredicate
    status: str = "pending"  # pending | fulfilled | timeout | cancelled
    fulfilled_by: int | None = None
    intake_hint: dict[str, Any] = field(default_factory=dict)
    task_snapshot: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["predicate"] = self.predicate.to_dict()
        return d

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> WaitRegistration:
        cid = data.get("origin_event_id")
        try:
            origin = int(cid) if cid is not None else 0
        except (TypeError, ValueError):
            origin = 0
        fb = data.get("fulfilled_by")
        try:
            fulfilled_by = int(fb) if fb is not None else None
        except (TypeError, ValueError):
            fulfilled_by = None
        return cls(
            wait_id=str(data.get("wait_id") or ""),
            runner_id=str(data.get("runner_id") or ""),
            producer_id=str(data.get("producer_id") or ""),
            chain_key=str(data.get("chain_key") or ""),
            origin_event_id=origin,
            suspended_at=str(data.get("suspended_at") or ""),
            timeout_at=str(data.get("timeout_at") or ""),
            reason=str(data.get("reason") or ""),
            predicate=WaitPredicate.from_dict(
                data.get("predicate") if isinstance(data.get("predicate"), dict) else {}
            ),
            status=str(data.get("status") or "pending"),
            fulfilled_by=fulfilled_by,
            intake_hint=dict(data.get("intake_hint") or {})
            if isinstance(data.get("intake_hint"), dict)
            else {},
            task_snapshot=dict(data.get("task_snapshot") or {})
            if isinstance(data.get("task_snapshot"), dict)
            else {},
        )

    @property
    def is_terminal(self) -> bool:
        return self.status in TERMINAL_STATUSES


def match_predicate(
    predicate: WaitPredicate | dict[str, Any],
    event: dict[str, Any],
    *,
    waiter_producer_id: str,
) -> bool:
    """Pure match: True if event fulfills the wait predicate.

    Self-fulfillment guard: events from the waiting producer never match.
    Primary filters: from_any, causation_id. summary_contains is secondary.
    """
    pred = (
        predicate
        if isinstance(predicate, WaitPredicate)
        else WaitPredicate.from_dict(predicate)
    )
    if not isinstance(event, dict):
        return False

    producer = str(event.get("producer_id") or "")
    payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
    from_agent = str(payload.get("from") or producer or "")

    # Self-fulfillment guard (primary correctness rule)
    if waiter_producer_id and (
        producer == waiter_producer_id or from_agent == waiter_producer_id
    ):
        return False

    topic = str(event.get("topic") or "")
    if pred.topic and topic and topic != pred.topic:
        return False

    # Primary: from_any
    if pred.from_any:
        allowed = {a.strip() for a in pred.from_any if a and str(a).strip()}
        if allowed and from_agent not in allowed and producer not in allowed:
            return False

    # Primary: causation_id
    if pred.causation_id is not None:
        try:
            event_cid = (
                int(event["causation_id"])
                if event.get("causation_id") is not None
                else None
            )
        except (TypeError, ValueError):
            event_cid = None
        if event_cid != pred.causation_id:
            return False

    # Secondary: summary_contains
    if pred.summary_contains:
        summary = str(payload.get("summary") or "")
        if pred.summary_contains not in summary:
            return False

    # Require at least one primary key for safety (no free-text-only waits)
    if not pred.from_any and pred.causation_id is None:
        return False

    return True


def build_resume_payload(
    wait: WaitRegistration,
    *,
    fulfilled_by: int,
    status: str,
    reason: str,
) -> dict[str, Any]:
    """Locked resume keys under payload['resume'] (design §3.A)."""
    return {
        "from": "agentbus",
        "to": wait.producer_id,
        "summary": f"RESUME: wait_id={wait.wait_id} status={status}",
        "resume": {
            "wait_id": wait.wait_id,
            "chain_key": wait.chain_key,
            "origin_event_id": wait.origin_event_id,
            "fulfilled_by": int(fulfilled_by),
            "status": status,
            "reason": reason,
        },
    }


def resume_idempotency_key(wait_id: str, fulfilled_by: int) -> str:
    return f"resume:{wait_id}:{fulfilled_by}"


def suspend_ack_idempotency_key(runner_id: str, event_id: int) -> str:
    return f"suspend-ack:{runner_id}:{event_id}"


def build_resume_wake(
    wait: WaitRegistration,
    *,
    resume_event_id: int,
    fulfilled_by: int,
    status: str,
    reason: str,
) -> WakeEnvelope:
    """Pure transform: WaitRegistration + fulfillment → WakeEnvelope for intake."""
    payload = build_resume_payload(
        wait, fulfilled_by=fulfilled_by, status=status, reason=reason
    )
    try:
        chain_causation = int(wait.chain_key)
    except (TypeError, ValueError):
        chain_causation = wait.origin_event_id
    return WakeEnvelope(
        event_id=resume_event_id,
        topic="okf/handoff",
        from_agent="agentbus",
        to=wait.producer_id,
        summary=str(payload["summary"]),
        payload=payload,
        source="resume",
        raw={
            "event_id": resume_event_id,
            "topic": "okf/handoff",
            "causation_id": chain_causation,
            "payload": payload,
            "wait_id": wait.wait_id,
            "source": "resume",
        },
        causation_id=chain_causation,
        trace_id=None,
    )


def await_drop_path(workspace: Path, event_id: int) -> Path:
    return workspace / ".agentbus" / "runs" / str(event_id) / "await.json"


def write_await_drop(workspace: Path, event_id: int, data: dict[str, Any]) -> Path:
    path = await_drop_path(workspace, event_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    return path


def load_await_drop(workspace: Path, event_id: int) -> dict[str, Any] | None:
    path = await_drop_path(workspace, event_id)
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return data if isinstance(data, dict) else None


class WaitStore:
    """JSON-file wait store under ``.agentbus/waits/``.

    Corrupt files are skipped + logged (never crash the runner loop).
    """

    def __init__(
        self,
        workspace: Path,
        *,
        now_fn: Callable[[], datetime] | None = None,
    ) -> None:
        self.workspace = workspace.resolve()
        self.waits_dir = self.workspace / ".agentbus" / WAITS_DIRNAME
        self.cursor_path = self.waits_dir / CURSOR_FILENAME
        self._now = now_fn or utc_now

    def ensure_dir(self) -> None:
        self.waits_dir.mkdir(parents=True, exist_ok=True)

    def path_for(self, wait_id: str) -> Path:
        safe = "".join(c for c in wait_id if c.isalnum() or c in "_-")
        if not safe:
            raise ValueError("invalid wait_id")
        return self.waits_dir / f"{safe}.json"

    def save(self, wait: WaitRegistration) -> Path:
        self.ensure_dir()
        path = self.path_for(wait.wait_id)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(
            json.dumps(wait.to_dict(), indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        tmp.replace(path)
        return path

    def load(self, wait_id: str) -> WaitRegistration | None:
        path = self.path_for(wait_id)
        if not path.is_file():
            return None
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            log.warning("corrupt wait file %s: %s", path, exc)
            return None
        if not isinstance(data, dict):
            return None
        try:
            return WaitRegistration.from_dict(data)
        except (TypeError, ValueError, KeyError) as exc:
            log.warning("invalid wait file %s: %s", path, exc)
            return None

    def list_waits(self, *, status: str | None = "pending") -> list[WaitRegistration]:
        self.ensure_dir()
        out: list[WaitRegistration] = []
        for path in sorted(self.waits_dir.glob("*.json")):
            if path.name.startswith("_"):
                continue
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                log.warning("skip corrupt wait %s: %s", path.name, exc)
                continue
            if not isinstance(data, dict):
                log.warning("skip non-object wait %s", path.name)
                continue
            try:
                wait = WaitRegistration.from_dict(data)
            except (TypeError, ValueError, KeyError) as exc:
                log.warning("skip invalid wait %s: %s", path.name, exc)
                continue
            if not wait.wait_id:
                continue
            if status is not None and wait.status != status:
                continue
            out.append(wait)
        return out

    def get_cursor(self) -> int:
        if not self.cursor_path.is_file():
            return 0
        try:
            data = json.loads(self.cursor_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return 0
        if not isinstance(data, dict):
            return 0
        try:
            return int(data.get("last_seen_event_id") or 0)
        except (TypeError, ValueError):
            return 0

    def set_cursor(self, event_id: int) -> None:
        self.ensure_dir()
        payload = {
            "last_seen_event_id": int(event_id),
            "updated_at": utc_now_iso(self._now()),
        }
        tmp = self.cursor_path.with_suffix(self.cursor_path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        tmp.replace(self.cursor_path)

    def create(
        self,
        *,
        runner_id: str,
        producer_id: str,
        chain_key: str,
        origin_event_id: int,
        predicate: WaitPredicate,
        reason: str = "await",
        timeout_hours: float | int | None = None,
        wait_id: str | None = None,
        intake_hint: dict[str, Any] | None = None,
        task_snapshot: dict[str, Any] | None = None,
        now: datetime | None = None,
    ) -> WaitRegistration:
        now_dt = now or self._now()
        hours = clamp_timeout_hours(timeout_hours)
        timeout_at = utc_now_iso(now_dt + timedelta(hours=hours))
        wait = WaitRegistration(
            wait_id=wait_id or new_wait_id(),
            runner_id=runner_id,
            producer_id=producer_id,
            chain_key=str(chain_key),
            origin_event_id=int(origin_event_id),
            suspended_at=utc_now_iso(now_dt),
            timeout_at=timeout_at,
            reason=reason,
            predicate=predicate,
            status="pending",
            intake_hint=dict(intake_hint or {}),
            task_snapshot=dict(task_snapshot or {}),
        )
        self.save(wait)
        return wait

    def mark_terminal(
        self,
        wait: WaitRegistration,
        *,
        status: str,
        fulfilled_by: int | None = None,
    ) -> WaitRegistration:
        wait.status = status
        if fulfilled_by is not None:
            wait.fulfilled_by = int(fulfilled_by)
        self.save(wait)
        return wait

    def is_expired(self, wait: WaitRegistration, *, now: datetime | None = None) -> bool:
        deadline = parse_iso_utc(wait.timeout_at)
        if deadline is None:
            return False
        return (now or self._now()) >= deadline
