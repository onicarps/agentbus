"""Health probe — v0.1 wraps coordination-root bash swarm_health_check.sh --json."""

from __future__ import annotations

import json
import os
import subprocess
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


LEVELS = ("healthy", "degraded", "critical")
_EXIT_FOR_LEVEL = {"healthy": 0, "degraded": 1, "critical": 2}


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass
class HealthSnapshot:
    """Machine-readable health probe result (bash --json parity)."""

    level: str
    workspace: str
    checked_at: str
    ok: bool = True
    sre_status: str = ""
    exit_code: int = 0
    latest_event_id: int | None = None
    notes: list[str] = field(default_factory=list)
    disabled_services: list[str] = field(default_factory=list)
    raw: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.level not in LEVELS:
            self.level = "critical"
        if not self.sre_status:
            self.sre_status = self.level
        self.ok = self.level == "healthy"
        if self.exit_code not in (0, 1, 2):
            self.exit_code = _EXIT_FOR_LEVEL.get(self.level, 2)

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "level": self.level,
            "sre_status": self.sre_status,
            "exit_code": self.exit_code,
            "workspace": self.workspace,
            "checked_at": self.checked_at,
            "latest_event_id": self.latest_event_id,
            "notes": list(self.notes),
            "disabled_services": list(self.disabled_services),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any], *, workspace_fallback: str = "") -> HealthSnapshot:
        level = str(data.get("level") or data.get("sre_status") or "critical")
        if level not in LEVELS:
            level = "critical"
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
        exit_code = data.get("exit_code")
        if exit_code is None:
            exit_code = _EXIT_FOR_LEVEL.get(level, 2)
        else:
            try:
                exit_code = int(exit_code)
            except (TypeError, ValueError):
                exit_code = _EXIT_FOR_LEVEL.get(level, 2)
        return cls(
            level=level,
            workspace=str(data.get("workspace") or workspace_fallback),
            checked_at=str(data.get("checked_at") or _utc_now()),
            ok=level == "healthy",
            sre_status=level,
            exit_code=exit_code,
            latest_event_id=eid,
            notes=[str(n) for n in notes],
            disabled_services=[str(d) for d in disabled],
            raw=dict(data),
        )


def default_health_script(workspace: Path) -> Path:
    """Resolve swarm_health_check.sh under coordination root (or env override)."""
    env = os.environ.get("SRE_HEALTH_SCRIPT")
    if env:
        return Path(env)
    return workspace / "scripts" / "swarm_health_check.sh"


def probe_health(
    workspace: str | Path,
    *,
    health_script: str | Path | None = None,
    timeout: float = 120.0,
) -> HealthSnapshot:
    """Run bash probe with --json (MVP wrap). Pure-Python probe is deferred.

    Captures JSON even when the probe exits 1/2 (degraded/critical).
    """
    ws = Path(workspace).resolve()
    script = Path(health_script) if health_script else default_health_script(ws)
    if not script.is_file():
        return HealthSnapshot(
            level="critical",
            workspace=str(ws),
            checked_at=_utc_now(),
            ok=False,
            sre_status="critical",
            exit_code=2,
            notes=[f"health_script_missing:{script}"],
        )

    env = os.environ.copy()
    env["AGENTBUS_WORKSPACE"] = str(ws)
    try:
        proc = subprocess.run(
            [str(script), "--json"],
            cwd=str(ws),
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return HealthSnapshot(
            level="critical",
            workspace=str(ws),
            checked_at=_utc_now(),
            ok=False,
            sre_status="critical",
            exit_code=2,
            notes=["health_probe_timeout"],
        )
    except OSError as exc:
        return HealthSnapshot(
            level="critical",
            workspace=str(ws),
            checked_at=_utc_now(),
            ok=False,
            sre_status="critical",
            exit_code=2,
            notes=[f"health_probe_os_error:{exc}"],
        )

    raw_out = (proc.stdout or "").strip()
    if raw_out:
        # Prefer last non-empty line that parses as JSON (stdout should be pure).
        for line in reversed(raw_out.splitlines()):
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(data, dict):
                snap = HealthSnapshot.from_dict(data, workspace_fallback=str(ws))
                # Prefer probe's exit_code; fall back to process code.
                if snap.exit_code not in (0, 1, 2):
                    snap.exit_code = proc.returncode if proc.returncode in (0, 1, 2) else 2
                return snap

    # Fallback: parse text SRE_STATUS line from combined streams
    text = "\n".join(
        x for x in ((proc.stdout or ""), (proc.stderr or "")) if x
    )
    level = "critical"
    for line in text.splitlines():
        if line.startswith("SRE_STATUS:"):
            parts = line.split()
            if len(parts) >= 2 and parts[1] in LEVELS:
                level = parts[1]
            break
    return HealthSnapshot(
        level=level,
        workspace=str(ws),
        checked_at=_utc_now(),
        ok=level == "healthy",
        sre_status=level,
        exit_code=proc.returncode if proc.returncode in (0, 1, 2) else _EXIT_FOR_LEVEL[level],
        notes=["json_mode_fallback"],
    )
