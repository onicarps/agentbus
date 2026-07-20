"""Aggregated workspace metrics for SRE ops (P1).

Assembles bus status, SLA/dead-letter signals, and ingress queue health into
one read-only payload so Aider/ops can poll without juggling separate scripts.

CLI: ``agentbus metrics [--workspace] [--text] [--no-health] [--no-waits]``

Design notes (see decisions/grok-aider-ops-enhancements-review-2026-07-20):
- ``queue_depth`` from HTTP health is **total JSONL lines**, not backlog.
- True backlog is ``undrained`` = queue event_ids not present in the done set.
- Disabled ingress services are listed with ``enabled: false`` (no health probe).
"""

from __future__ import annotations

import json
import re
import shlex
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import yaml

from agentbus.runner.config import default_done_path, default_queue_path
from agentbus.runner.intake import envelope_from_queue_record, load_done_ids
from agentbus.runner.wait_store import WaitStore
from agentbus.schemas import DEAD_LETTER_TOPIC
from agentbus.store import STATUS_PUBLISHED, EventStore
from agentbus.swarm import ServiceSpec, SwarmConfig, load_swarm_config, swarm_yaml_path
from agentbus.wake_ingress import DEFAULT_PORTS, PATH_HEALTH

_INGRESS_SUFFIX = "-wake-ingress"
_RUNTIME_FROM_CMD = re.compile(r"--runtime\s+(\S+)", re.IGNORECASE)
_HOST_FROM_CMD = re.compile(r"--host\s+(\S+)", re.IGNORECASE)
_PORT_FROM_CMD = re.compile(r"--port\s+(\d+)", re.IGNORECASE)

# Cap lists in the payload so ops JSON stays small under storm retention.
_MAX_SLA_ACTIVE = 50
_MAX_DEAD_LETTER_RECENT = 10
_MAX_OPEN_WAITS = 50


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass
class MetricsReport:
    """Unified telemetry payload."""

    workspace: Path
    collected_at: str
    status: dict[str, Any] = field(default_factory=dict)
    sla: dict[str, Any] = field(default_factory=dict)
    ingress: list[dict[str, Any]] = field(default_factory=list)
    waits: dict[str, Any] | None = None
    errors: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        """True when core bus status was collected (ingress probe failures are soft)."""
        return bool(self.status) and not any(
            e.startswith("status:") for e in self.errors
        )

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "ok": self.ok,
            "workspace": str(self.workspace.resolve()),
            "collected_at": self.collected_at,
            "status": self.status,
            "sla": self.sla,
            "ingress": self.ingress,
        }
        if self.waits is not None:
            d["waits"] = self.waits
        if self.errors:
            d["errors"] = list(self.errors)
        return d


def _load_swarm_lenient(workspace: Path) -> SwarmConfig | None:
    """Load swarm.yaml; allow all-disabled (parked) swarms."""
    path = swarm_yaml_path(workspace)
    if not path.is_file():
        return None
    try:
        return load_swarm_config(workspace)
    except ValueError:
        # load_swarm_config may reject all-disabled; parse manually
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        if not isinstance(raw, dict):
            raise
        version = str(raw.get("version") or "1.0")
        services_raw = raw.get("services") or {}
        if not isinstance(services_raw, dict) or not services_raw:
            raise ValueError("swarm.yaml must define a non-empty 'services' mapping")
        services: dict[str, ServiceSpec] = {}
        for name, defn in services_raw.items():
            if not isinstance(name, str) or not name.strip():
                continue
            if isinstance(defn, str):
                command, env, cwd, enabled = defn, {}, None, True
            elif isinstance(defn, dict):
                enabled = bool(defn.get("enabled", True))
                command = defn.get("command")
                if not command or not isinstance(command, str):
                    continue
                env = {str(k): str(v) for k, v in (defn.get("env") or {}).items()}
                cwd = defn.get("cwd")
                if cwd is not None:
                    cwd = str(cwd)
            else:
                continue
            services[name] = ServiceSpec(
                name=name, command=command, env=env, cwd=cwd, enabled=enabled
            )
        if not services:
            return None
        return SwarmConfig(version=version, services=services, path=path)


def _is_ingress_service(name: str, command: str) -> bool:
    if name.endswith(_INGRESS_SUFFIX):
        return True
    return "wake-ingress" in command


def _flag_value(command: str, pattern: re.Pattern[str], *, shlex_flag: str) -> str | None:
    m = pattern.search(command)
    if m:
        return m.group(1).strip().strip("\"'")
    try:
        parts = shlex.split(command)
    except ValueError:
        return None
    for i, p in enumerate(parts):
        if p == shlex_flag and i + 1 < len(parts):
            return parts[i + 1]
        if p.startswith(f"{shlex_flag}="):
            return p.split("=", 1)[1]
    return None


def _parse_runtime(name: str, command: str) -> str:
    rt = _flag_value(command, _RUNTIME_FROM_CMD, shlex_flag="--runtime")
    if rt:
        return rt.lower()
    if name.endswith(_INGRESS_SUFFIX):
        return name[: -len(_INGRESS_SUFFIX)].lower()
    return ""


def _parse_host_port(command: str, runtime: str) -> tuple[str, int | None]:
    host = _flag_value(command, _HOST_FROM_CMD, shlex_flag="--host") or "127.0.0.1"
    port_s = _flag_value(command, _PORT_FROM_CMD, shlex_flag="--port")
    if port_s and port_s.isdigit():
        return host, int(port_s)
    default = DEFAULT_PORTS.get(runtime.lower()) if runtime else None
    return host, default


def _count_jsonl_lines(path: Path) -> int:
    if not path.is_file():
        return 0
    n = 0
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            if line.strip():
                n += 1
    return n


def _queue_event_ids(path: Path) -> list[int]:
    """Extract event_ids from a wake queue JSONL (best-effort)."""
    if not path.is_file():
        return []
    ids: list[int] = []
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(rec, dict):
                continue
            env = envelope_from_queue_record(rec)
            if env is not None:
                ids.append(env.event_id)
                continue
            # Fallback: top-level event_id
            try:
                eid = rec.get("event_id")
                if eid is None and isinstance(rec.get("event"), dict):
                    eid = rec["event"].get("event_id")
                if eid is not None:
                    ids.append(int(eid))
            except (TypeError, ValueError):
                continue
    return ids


def _queue_stats(workspace: Path, runtime: str) -> dict[str, Any]:
    qpath = default_queue_path(workspace, runtime)
    dpath = default_done_path(workspace, runtime)
    line_count = _count_jsonl_lines(qpath)
    event_ids = _queue_event_ids(qpath)
    done = load_done_ids(dpath)
    unique = set(event_ids)
    undrained_ids = sorted(unique - done)
    return {
        "queue_path": str(qpath),
        "done_path": str(dpath),
        "line_count": line_count,
        "event_id_count": len(event_ids),
        "unique_event_ids": len(unique),
        "done_count": len(done),
        "undrained": len(undrained_ids),
        # sample only — full list can be huge under long retention
        "undrained_sample": undrained_ids[:20],
        "exists": qpath.is_file(),
    }


def _probe_health(
    host: str,
    port: int,
    *,
    timeout_s: float = 1.5,
) -> dict[str, Any]:
    url = f"http://{host}:{port}{PATH_HEALTH}"
    out: dict[str, Any] = {
        "url": url,
        "reachable": False,
        "ok": False,
        "queue_depth": None,
        "error": None,
    }
    try:
        req = Request(url, method="GET")
        with urlopen(req, timeout=timeout_s) as resp:  # noqa: S310 — loopback ops probe
            body = resp.read().decode("utf-8", errors="replace")
            data = json.loads(body) if body.strip() else {}
            if not isinstance(data, dict):
                out["error"] = "non_object_body"
                return out
            out["reachable"] = True
            out["ok"] = bool(data.get("ok"))
            qd = data.get("queue_depth")
            out["queue_depth"] = int(qd) if qd is not None else None
            if data.get("runtime") is not None:
                out["runtime"] = data.get("runtime")
            return out
    except HTTPError as exc:
        out["error"] = f"http_{exc.code}"
        return out
    except (URLError, TimeoutError, OSError, json.JSONDecodeError, ValueError) as exc:
        out["error"] = type(exc).__name__
        if getattr(exc, "reason", None):
            out["error_detail"] = str(exc.reason)[:200]
        else:
            out["error_detail"] = str(exc)[:200]
        return out


def _dead_letter_stats(store: EventStore) -> dict[str, Any]:
    """Count okf/dead-letter events and break down by payload.reason."""
    rows = store._conn.execute(
        """
        SELECT event_id, payload, timestamp
        FROM events
        WHERE topic = ? AND status = ?
        ORDER BY event_id DESC
        """,
        (DEAD_LETTER_TOPIC, STATUS_PUBLISHED),
    ).fetchall()
    by_reason: dict[str, int] = {}
    recent: list[dict[str, Any]] = []
    for row in rows:
        try:
            payload = json.loads(row["payload"])
        except (TypeError, json.JSONDecodeError):
            payload = {}
        if not isinstance(payload, dict):
            payload = {}
        reason = str(payload.get("reason") or "UNKNOWN")
        by_reason[reason] = by_reason.get(reason, 0) + 1
        if len(recent) < _MAX_DEAD_LETTER_RECENT:
            recent.append(
                {
                    "event_id": row["event_id"],
                    "timestamp": row["timestamp"],
                    "reason": reason,
                    "original_event_id": payload.get("original_event_id"),
                    "summary": str(payload.get("summary") or "")[:200],
                }
            )
    return {
        "total": len(rows),
        "by_reason": by_reason,
        "recent": recent,
    }


def _wait_stats(workspace: Path) -> dict[str, Any]:
    store = WaitStore(workspace)
    if not store.waits_dir.is_dir():
        return {
            "open_count": 0,
            "by_status": {},
            "open": [],
            "waits_dir": str(store.waits_dir),
        }
    # list all statuses
    all_waits = store.list_waits(status=None)
    by_status: dict[str, int] = {}
    open_list: list[dict[str, Any]] = []
    for w in all_waits:
        by_status[w.status] = by_status.get(w.status, 0) + 1
        if w.status in ("pending", "fulfilling") and len(open_list) < _MAX_OPEN_WAITS:
            open_list.append(
                {
                    "wait_id": w.wait_id,
                    "status": w.status,
                    "producer_id": w.producer_id,
                    "runner_id": w.runner_id,
                    "origin_event_id": w.origin_event_id,
                    "timeout_at": w.timeout_at,
                    "chain_key": w.chain_key,
                }
            )
    open_count = by_status.get("pending", 0) + by_status.get("fulfilling", 0)
    return {
        "open_count": open_count,
        "by_status": by_status,
        "open": open_list,
        "waits_dir": str(store.waits_dir),
    }


def _collect_ingress(
    workspace: Path,
    swarm: SwarmConfig | None,
    *,
    probe_health: bool,
    health_timeout_s: float,
) -> list[dict[str, Any]]:
    if swarm is None:
        return []
    out: list[dict[str, Any]] = []
    for name, spec in swarm.services.items():
        if not _is_ingress_service(name, spec.command):
            continue
        runtime = _parse_runtime(name, spec.command)
        host, port = _parse_host_port(spec.command, runtime)
        entry: dict[str, Any] = {
            "service": name,
            "runtime": runtime or None,
            "enabled": spec.enabled,
            "host": host,
            "port": port,
            "queue": _queue_stats(workspace, runtime) if runtime else None,
            "health": None,
            "note": None,
        }
        if not spec.enabled:
            entry["note"] = "disabled_by_config"
            # still report residual queue for ops awareness
            out.append(entry)
            continue
        if not probe_health:
            entry["note"] = "health_probe_skipped"
            out.append(entry)
            continue
        if port is None:
            entry["note"] = "port_unknown"
            entry["health"] = {
                "reachable": False,
                "ok": False,
                "error": "port_unknown",
            }
            out.append(entry)
            continue
        entry["health"] = _probe_health(host, port, timeout_s=health_timeout_s)
        if not entry["health"].get("reachable"):
            entry["note"] = "health_unreachable"
        out.append(entry)
    return out


def collect_workspace_metrics(
    workspace: Path,
    *,
    retention_days: int = 7,
    probe_health: bool = True,
    include_waits: bool = True,
    health_timeout_s: float = 1.5,
    store: EventStore | None = None,
) -> MetricsReport:
    """Collect status + SLA + ingress (+ optional waits) for ``workspace``."""
    workspace = workspace.resolve()
    report = MetricsReport(workspace=workspace, collected_at=_utc_now())
    own_store = store is None
    event_store = store
    try:
        if event_store is None:
            event_store = EventStore(workspace, retention_days=retention_days)
        report.status = event_store.status()
        # Drop producer_id noise from status when not set for this aggregate
        report.status.pop("producer_id", None)

        sla_list = event_store.list_active_slas()
        active = list(sla_list.get("active") or [])
        report.sla = {
            "active_count": int(sla_list.get("sla_active_count") or len(active)),
            "active": active[:_MAX_SLA_ACTIVE],
            "active_truncated": len(active) > _MAX_SLA_ACTIVE,
            "dead_letter": _dead_letter_stats(event_store),
        }
    except Exception as exc:  # noqa: BLE001 — surface soft error in report
        report.errors.append(f"status: {type(exc).__name__}: {exc}")
    finally:
        if own_store and event_store is not None:
            try:
                event_store.close()
            except Exception:  # pragma: no cover
                pass

    try:
        swarm = _load_swarm_lenient(workspace)
    except Exception as exc:  # noqa: BLE001
        report.errors.append(f"swarm: {type(exc).__name__}: {exc}")
        swarm = None

    if swarm is None and not any(e.startswith("swarm:") for e in report.errors):
        if not swarm_yaml_path(workspace).is_file():
            report.errors.append("swarm: swarm.yaml missing")

    try:
        report.ingress = _collect_ingress(
            workspace,
            swarm,
            probe_health=probe_health,
            health_timeout_s=health_timeout_s,
        )
    except Exception as exc:  # noqa: BLE001
        report.errors.append(f"ingress: {type(exc).__name__}: {exc}")

    if include_waits:
        try:
            report.waits = _wait_stats(workspace)
        except Exception as exc:  # noqa: BLE001
            report.errors.append(f"waits: {type(exc).__name__}: {exc}")
            report.waits = {"open_count": 0, "by_status": {}, "open": [], "error": str(exc)}

    return report


def format_metrics_text(report: MetricsReport) -> str:
    """Human-readable multi-line summary for ops terminals."""
    lines: list[str] = []
    st = "OK" if report.ok else "DEGRADED"
    lines.append(f"metrics: {st}  workspace={report.workspace}  at={report.collected_at}")
    s = report.status or {}
    lines.append(
        "  status: events={event_count} latest={latest} pending={pending} "
        "sla_active={sla}".format(
            event_count=s.get("event_count", "?"),
            latest=s.get("latest_event_id", "?"),
            pending=s.get("pending_count", "?"),
            sla=s.get("sla_active_count", "?"),
        )
    )
    sla = report.sla or {}
    dl = sla.get("dead_letter") or {}
    by_reason = dl.get("by_reason") or {}
    reason_s = (
        ", ".join(f"{k}={v}" for k, v in sorted(by_reason.items()))
        if by_reason
        else "none"
    )
    lines.append(
        f"  sla: active={sla.get('active_count', 0)}  "
        f"dead_letter_total={dl.get('total', 0)} ({reason_s})"
    )
    if report.ingress:
        lines.append("  ingress:")
        for ing in report.ingress:
            q = ing.get("queue") or {}
            h = ing.get("health")
            enabled = "on" if ing.get("enabled") else "off"
            parts = [
                f"    - {ing.get('service')} runtime={ing.get('runtime')} "
                f"enabled={enabled}"
            ]
            if q:
                parts.append(
                    f"queue_lines={q.get('line_count', 0)} "
                    f"undrained={q.get('undrained', 0)} "
                    f"done={q.get('done_count', 0)}"
                )
            if h is not None:
                if h.get("reachable"):
                    parts.append(
                        f"health=ok/{h.get('ok')} depth={h.get('queue_depth')}"
                    )
                else:
                    parts.append(f"health=unreachable({h.get('error')})")
            if ing.get("note"):
                parts.append(f"note={ing['note']}")
            lines.append("  ".join(parts))
    else:
        lines.append("  ingress: (none)")
    if report.waits is not None:
        w = report.waits
        by = w.get("by_status") or {}
        by_s = ", ".join(f"{k}={v}" for k, v in sorted(by.items())) or "none"
        lines.append(f"  waits: open={w.get('open_count', 0)}  by_status=({by_s})")
    for err in report.errors:
        lines.append(f"  [error] {err}")
    return "\n".join(lines)
