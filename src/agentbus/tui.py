"""Textual mission-control TUI for agentbus monitor — God View (v0.9)."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from agentbus.devex import discover_config_targets, format_event_row
from agentbus.schemas import SYSTEM_TOPICS
from agentbus.store import EventStore
from agentbus.tracing import build_trace_tree, format_trace_tree_plain

DARK_AGENT_MINUTES = 5


def _reviewer_id() -> str:
    return os.environ.get("AGENTBUS_PRODUCER_ID", "monitor")


def _parse_ts(ts: str | None) -> datetime | None:
    if not ts:
        return None
    try:
        # Accept ...Z
        if ts.endswith("Z"):
            ts = ts[:-1] + "+00:00"
        return datetime.fromisoformat(ts)
    except ValueError:
        return None


def detect_dark_agents(
    events: list[dict[str, Any]],
    *,
    threshold_minutes: float = DARK_AGENT_MINUTES,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    """Flag producers that emit system/* without okf/* handoffs within threshold."""
    now = now or datetime.now(timezone.utc)
    last_okf: dict[str, datetime] = {}
    last_system: dict[str, datetime] = {}
    last_any: dict[str, datetime] = {}

    for ev in events:
        pid = ev.get("producer_id") or "?"
        topic = ev.get("topic") or ""
        ts = _parse_ts(ev.get("timestamp"))
        if not ts:
            continue
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        last_any[pid] = max(last_any.get(pid, ts), ts)
        if topic.startswith("okf/"):
            last_okf[pid] = max(last_okf.get(pid, ts), ts)
        if topic.startswith("system/"):
            last_system[pid] = max(last_system.get(pid, ts), ts)

    warnings: list[dict[str, Any]] = []
    # Dark agents: non-system producers with recent system/* activity but no recent okf/*.
    # Iterate last_system so pure okf publishers are never false-positived.
    for pid, sys_ts in last_system.items():
        if pid in {"wiretap", "os-watcher", "swarm-tail", "monitor"}:
            continue
        age_sys = (now - sys_ts).total_seconds() / 60.0
        if age_sys > threshold_minutes * 2:
            continue  # stale system noise
        okf_ts = last_okf.get(pid)
        last_ts = last_any.get(pid, sys_ts)
        if okf_ts is None:
            warnings.append(
                {
                    "producer_id": pid,
                    "reason": "system/* activity with no okf/* handoff",
                    "last_seen": last_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "minutes_since_okf": None,
                }
            )
            continue
        since_okf = (now - okf_ts).total_seconds() / 60.0
        if since_okf > threshold_minutes:
            warnings.append(
                {
                    "producer_id": pid,
                    "reason": f"system/* without okf/* for {since_okf:.1f}m (threshold {threshold_minutes}m)",
                    "last_seen": last_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "minutes_since_okf": round(since_okf, 1),
                }
            )
    return warnings


def fetch_monitor_state(
    workspace: Path,
    *,
    retention_days: int = 7,
    limit: int = 200,
) -> dict[str, Any]:
    store = EventStore(workspace, retention_days=retention_days)
    try:
        store.expire_pending()
        store.expire_sla_breaches()
        rows = store._conn.execute(
            """
            SELECT * FROM events
            ORDER BY event_id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        events = [store._row_to_event(r).to_dict() for r in reversed(rows)]
        pending = store.review_pending()
        producers = {e.get("producer_id") for e in events if e.get("producer_id")}
        system_events = [e for e in events if e.get("topic") in SYSTEM_TOPICS or str(e.get("topic", "")).startswith("system/")]
        dark = detect_dark_agents(events)
        return {
            "events": events,
            "pending": pending,
            "system_events": system_events,
            "dark_agents": dark,
            "mcp_configs": len(discover_config_targets(workspace)),
            "active_producers": len(producers),
        }
    finally:
        store.close()


def approve_pending_event(
    workspace: Path,
    event_id: int,
    *,
    reviewer_id: str | None = None,
    retention_days: int = 7,
) -> dict[str, Any]:
    store = EventStore(workspace, retention_days=retention_days)
    try:
        return store.approve_event(event_id, reviewer_id=reviewer_id or _reviewer_id())
    finally:
        store.close()


def reject_pending_event(
    workspace: Path,
    event_id: int,
    *,
    reviewer_id: str | None = None,
    reason: str = "rejected via monitor TUI",
    retention_days: int = 7,
) -> dict[str, Any]:
    store = EventStore(workspace, retention_days=retention_days)
    try:
        return store.reject_event(
            event_id,
            reviewer_id=reviewer_id or _reviewer_id(),
            reason=reason,
        )
    finally:
        store.close()


def _format_system_row(ev: dict[str, Any]) -> tuple[str, str, str, str]:
    topic = ev.get("topic", "")
    payload = ev.get("payload") or {}
    eid = str(ev.get("event_id", ""))
    ts = str(ev.get("timestamp", ""))[-8:]
    if topic == "system/mcp":
        summary = f"{payload.get('tool', '?')} {payload.get('latency_ms', '')}ms"
    elif topic == "system/fs":
        summary = f"{payload.get('event', '?')} {payload.get('path', '')}"
    elif topic == "system/shell":
        summary = f"pid={payload.get('pid')} {payload.get('name', '')}"
    elif topic == "system/monologue":
        text = str(payload.get("text", ""))[:60]
        summary = f"{payload.get('agent', '?')}: {text}"
    else:
        summary = str(payload)[:80]
    return eid, ts, topic.replace("system/", "s/"), summary


def run_monitor_tui(
    workspace: Path,
    *,
    interval: float = 1.0,
    retention_days: int = 7,
) -> None:
    try:
        from textual.app import App, ComposeResult
        from textual.binding import Binding
        from textual.containers import Horizontal, Vertical
        from textual.widgets import DataTable, Footer, Static
    except ImportError as exc:
        raise RuntimeError(
            "Textual required for interactive monitor — pip install 'okf-agentbus[devex]'"
        ) from exc

    ws = workspace.resolve()

    class _MonitorApp(App):
        TITLE = "AgentBus God View"
        CSS = """
        Screen { layout: vertical; }
        #header-bar {
            height: 3;
            background: $surface;
            color: $text;
            padding: 0 1;
        }
        #body { height: 1fr; }
        #stream { width: 2fr; min-width: 30; }
        #right { width: 1fr; }
        #trace-content, #hitl-help, #wiretap-help, #dark-bar {
            padding: 0 1;
        }
        #dark-bar { height: 2; color: $error; }
        """

        BINDINGS = [
            Binding("a", "approve_pending", "Approve", show=True),
            Binding("r", "reject_pending", "Reject", show=True),
            Binding("q", "quit", "Quit", show=True),
        ]

        def __init__(self) -> None:
            super().__init__()
            self._selected_event: dict[str, Any] | None = None
            self._focused_pending_id: int | None = None

        def compose(self) -> ComposeResult:
            yield Static("", id="header-bar")
            yield Static("", id="dark-bar")
            with Horizontal(id="body"):
                yield DataTable(id="stream", cursor_type="row")
                with Vertical(id="right"):
                    yield Static("Select an event with trace_id", id="trace-content")
                    yield DataTable(id="hitl", cursor_type="row")
                    yield Static(
                        "[dim]HITL: focus pending · a=approve · r=reject[/dim]",
                        id="hitl-help",
                    )
                    yield DataTable(id="wiretap", cursor_type="row")
                    yield Static(
                        "[dim]Wiretap: system/mcp · system/fs · system/shell · monologue[/dim]",
                        id="wiretap-help",
                    )
            yield Footer()

        def on_mount(self) -> None:
            stream = self.query_one("#stream", DataTable)
            stream.add_columns("id", "time", "topic", "status", "summary")
            hitl = self.query_one("#hitl", DataTable)
            hitl.add_columns("id", "topic", "from", "summary")
            wire = self.query_one("#wiretap", DataTable)
            wire.add_columns("id", "time", "topic", "detail")
            self.set_interval(interval, self.refresh_data)
            self.refresh_data()

        def refresh_data(self) -> None:
            state = fetch_monitor_state(ws, retention_days=retention_days)
            header = self.query_one("#header-bar", Static)
            header.update(
                f"Workspace: {ws}  |  MCP configs: {state['mcp_configs']}  |  "
                f"Active producers: {state['active_producers']}  |  "
                f"Pending: {len(state['pending'])}  |  "
                f"System: {len(state['system_events'])}"
            )
            dark_bar = self.query_one("#dark-bar", Static)
            dark = state.get("dark_agents") or []
            if dark:
                parts = [
                    f"{d['producer_id']}: {d['reason']}" for d in dark[:4]
                ]
                dark_bar.update("[b]DARK AGENT[/b] " + " · ".join(parts))
            else:
                dark_bar.update("[dim]No dark agents detected[/dim]")

            stream = self.query_one("#stream", DataTable)
            cursor = stream.cursor_row
            stream.clear()
            for ev in state["events"]:
                # Stream shows cooperative (okf/*) primarily; system still listed
                row = format_event_row(ev)
                status = ev.get("status", "PUBLISHED")
                stream.add_row(
                    row["id"],
                    row["time"],
                    row["topic"],
                    status,
                    row["summary"],
                    key=str(ev["event_id"]),
                )
            if stream.row_count and 0 <= cursor < stream.row_count:
                stream.move_cursor(row=cursor)

            hitl = self.query_one("#hitl", DataTable)
            hitl_cursor = hitl.cursor_row
            hitl.clear()
            for ev in state["pending"]:
                payload = ev.get("payload") or {}
                hitl.add_row(
                    str(ev["event_id"]),
                    ev.get("topic", ""),
                    str(payload.get("from", ev.get("producer_id", "?"))),
                    format_event_row(ev)["summary"],
                    key=str(ev["event_id"]),
                )
            if hitl.row_count and 0 <= hitl_cursor < hitl.row_count:
                hitl.move_cursor(row=hitl_cursor)

            wire = self.query_one("#wiretap", DataTable)
            wire_cursor = wire.cursor_row
            wire.clear()
            for ev in state["system_events"][-80:]:
                eid, ts, topic, detail = _format_system_row(ev)
                wire.add_row(eid, ts, topic, detail, key=f"sys-{ev['event_id']}")
            if wire.row_count and 0 <= wire_cursor < wire.row_count:
                wire.move_cursor(row=wire_cursor)

        def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
            table = event.data_table
            if table.id == "stream":
                event_id = int(event.row_key.value)
                state = fetch_monitor_state(ws, retention_days=retention_days)
                selected = next(
                    (e for e in state["events"] if e["event_id"] == event_id),
                    None,
                )
                self._selected_event = selected
                self._update_trace(selected)
            elif table.id == "hitl":
                self._focused_pending_id = int(event.row_key.value)

        def on_data_table_row_highlighted(self, event: DataTable.RowHighlighted) -> None:
            table = event.data_table
            if table.id == "hitl" and event.row_key is not None:
                self._focused_pending_id = int(event.row_key.value)

        def _update_trace(self, selected: dict[str, Any] | None) -> None:
            trace_widget = self.query_one("#trace-content", Static)
            if not selected:
                trace_widget.update("Select an event with trace_id")
                return
            trace_id = selected.get("trace_id")
            if not trace_id:
                import json

                payload_str = json.dumps(selected.get("payload", {}), indent=2)
                trace_widget.update(
                    f"Event {selected['event_id']} (No Trace)\n\n"
                    f"[dim]Payload Detail:[/dim]\n{payload_str}"
                )
                return
            store = EventStore(ws, retention_days=retention_days)
            try:
                events = store.fetch_trace_events(trace_id)
            finally:
                store.close()
            roots = build_trace_tree(events)
            trace_widget.update(format_trace_tree_plain(trace_id, roots))

        def action_approve_pending(self) -> None:
            event_id = self._focused_pending_id
            if event_id is None:
                self.notify("Focus a pending event in HITL pane", severity="warning")
                return
            try:
                approve_pending_event(ws, event_id, retention_days=retention_days)
            except Exception as exc:
                self.notify(str(exc), severity="error")
                return
            self.notify(f"Approved event {event_id}")
            self.refresh_data()

        def action_reject_pending(self) -> None:
            event_id = self._focused_pending_id
            if event_id is None:
                self.notify("Focus a pending event in HITL pane", severity="warning")
                return
            try:
                reject_pending_event(ws, event_id, retention_days=retention_days)
            except Exception as exc:
                self.notify(str(exc), severity="error")
                return
            self.notify(f"Rejected event {event_id}")
            self.refresh_data()

    _MonitorApp().run()
