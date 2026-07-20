"""SRE watchdog state file — same contract as bash sre_edge_watchdog.sh."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


SCHEMA_VERSION = "1.0"
DEFAULT_STATE_NAME = "sre_last_state.json"


@dataclass
class WatchdogState:
    """Persisted observation + publish markers under .agentbus/."""

    level: str = "healthy"
    sre_status: str = "healthy"
    exit_code: int = 0
    last_checked_at: str | None = None
    last_checked_epoch: int = 0
    notes: list[str] = field(default_factory=list)
    notes_fingerprint: str = ""
    workspace: str | None = None
    latest_event_id: int | None = None
    disabled_services: list[str] = field(default_factory=list)
    last_action: str | None = None
    last_action_reason: str | None = None
    last_published_level: str | None = None
    last_published_at: str | None = None
    last_published_epoch: int = 0
    last_idempotency_key: str | None = None
    bootstrap: bool = False
    schema_version: str = SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["schema_version"] = self.schema_version or SCHEMA_VERSION
        return d

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> WatchdogState:
        level = str(data.get("level") or data.get("sre_status") or "healthy")
        notes = data.get("notes") or []
        if not isinstance(notes, list):
            notes = [str(notes)]
        disabled = data.get("disabled_services") or []
        if not isinstance(disabled, list):
            disabled = [str(disabled)]
        eid = data.get("latest_event_id")
        if eid is not None and not isinstance(eid, int):
            try:
                eid = int(eid)
            except (TypeError, ValueError):
                eid = None

        def _int(key: str, default: int = 0) -> int:
            try:
                return int(data.get(key) or default)
            except (TypeError, ValueError):
                return default

        return cls(
            level=level,
            sre_status=str(data.get("sre_status") or level),
            exit_code=_int("exit_code", 0),
            last_checked_at=data.get("last_checked_at"),
            last_checked_epoch=_int("last_checked_epoch", 0),
            notes=[str(n) for n in notes],
            notes_fingerprint=str(data.get("notes_fingerprint") or ""),
            workspace=data.get("workspace"),
            latest_event_id=eid,
            disabled_services=[str(d) for d in disabled],
            last_action=data.get("last_action"),
            last_action_reason=data.get("last_action_reason"),
            last_published_level=data.get("last_published_level"),
            last_published_at=data.get("last_published_at"),
            last_published_epoch=_int("last_published_epoch", 0),
            last_idempotency_key=data.get("last_idempotency_key"),
            bootstrap=bool(data.get("bootstrap")),
            schema_version=str(data.get("schema_version") or SCHEMA_VERSION),
        )


def default_state_path(workspace: str | Path) -> Path:
    return Path(workspace).resolve() / ".agentbus" / DEFAULT_STATE_NAME


def load_state(path: str | Path) -> WatchdogState | None:
    """Load state file; return None if missing or corrupt."""
    p = Path(path)
    if not p.is_file():
        return None
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(data, dict):
        return None
    return WatchdogState.from_dict(data)


def save_state(path: str | Path, state: WatchdogState | dict[str, Any]) -> None:
    """Atomic-ish write of state JSON (mkdir parent)."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(state, WatchdogState):
        payload = state.to_dict()
    else:
        payload = dict(state)
    text = json.dumps(payload, indent=2) + "\n"
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(p)
