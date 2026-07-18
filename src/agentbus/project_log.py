"""Project okf/handoff events into OKF log.md format."""

from __future__ import annotations

import json
from pathlib import Path

from agentbus.store import Event, EventStore

STATE_FILE = "project-log.json"
HANDOFF_TOPIC = "okf/handoff"
LOG_HEADER = "# Workspace Update Log"


def _state_path(workspace: Path) -> Path:
    return workspace / ".agentbus" / STATE_FILE


def load_state(workspace: Path) -> dict:
    path = _state_path(workspace)
    if not path.exists():
        return {"last_event_id": 0}
    return json.loads(path.read_text(encoding="utf-8"))


def save_state(workspace: Path, last_event_id: int) -> None:
    path = _state_path(workspace)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"last_event_id": last_event_id}, indent=2) + "\n",
        encoding="utf-8",
    )


def format_handoff_line(event: Event) -> str:
    payload = event.payload
    from_agent = payload["from"]
    to_agent = payload["to"]
    summary = payload["summary"]
    lines = [f"* **Handoff | {from_agent} → {to_agent}**: {summary}"]
    for link in payload.get("links", []):
        name = link.rstrip("/").rsplit("/", 1)[-1].replace(".md", "")
        lines.append(f"* **Update**: [{name}]({link}) — AgentBus event {event.event_id}.")
    return "\n".join(lines)


def _event_date(event: Event) -> str:
    return event.timestamp[:10]


def _insert_under_date(log_text: str, date: str, block: str) -> str:
    header = f"## {date}"
    if header in log_text:
        idx = log_text.index(header)
        line_end = log_text.index("\n", idx) + 1
        return log_text[:line_end] + block + log_text[line_end:]
    marker = f"{LOG_HEADER}\n\n"
    if marker in log_text:
        pos = log_text.index(marker) + len(marker)
        return log_text[:pos] + f"{header}\n{block}\n" + log_text[pos:]
    return f"{LOG_HEADER}\n\n{header}\n{block}\n"


def _dict_to_event(row: dict) -> Event:
    return Event(
        event_id=row["event_id"],
        topic=row["topic"],
        producer_id=row["producer_id"],
        timestamp=row["timestamp"],
        schema_version=row["schema_version"],
        payload=row["payload"],
        causation_id=row["causation_id"],
        idempotency_key=row["idempotency_key"],
        status=row.get("status", "PUBLISHED"),
        pending_until=row.get("pending_until"),
        rejection_reason=row.get("rejection_reason"),
    )


def project_handoffs(
    store: EventStore,
    workspace: Path,
    log_path: Path,
    *,
    dry_run: bool = False,
    reset: bool = False,
) -> dict:
    if reset:
        store._conn.execute(
            "UPDATE events SET projected_to_log = 0 WHERE topic = ?",
            (HANDOFF_TOPIC,),
        )
        store._conn.commit()

    events = store.fetch_unprojected_handoffs(limit=100)

    if not events:
        state = load_state(workspace)
        return {
            "projected": 0,
            "last_event_id": state.get("last_event_id", 0),
            "dry_run": dry_run,
            "lines": [],
        }

    log_text = log_path.read_text(encoding="utf-8") if log_path.exists() else f"{LOG_HEADER}\n\n"
    lines: list[str] = []
    for event in events:
        line = format_handoff_line(event)
        lines.append(line)
        log_text = _insert_under_date(log_text, _event_date(event), line + "\n")

    last_id = events[-1].event_id
    if not dry_run:
        log_path.write_text(log_text, encoding="utf-8")
        store.mark_projected([e.event_id for e in events])
        save_state(workspace, last_id)

    return {
        "projected": len(events),
        "last_event_id": last_id,
        "dry_run": dry_run,
        "lines": lines,
    }