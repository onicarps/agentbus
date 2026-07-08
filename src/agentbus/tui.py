"""Textual mission-control TUI for agentbus monitor — PDD v0.8."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from agentbus.devex import discover_config_targets, format_event_row
from agentbus.store import EventStore
from agentbus.tracing import build_trace_tree, format_trace_tree_plain


def _reviewer_id() -> str:
    return os.environ.get("AGENTBUS_PRODUCER_ID", "monitor")


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
        return {
            "events": events,
            "pending": pending,
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


def run_monitor_tui(
    workspace: Path,
    *,
    interval: float = 1.0,
    retention_days: int = 7,
) -> None:
    try:
        from textual.app import App, ComposeResult
        from textual.binding import Binding
        from textual.containers import Container, Horizontal, Vertical
        from textual.widgets import DataTable, Footer, Static
    except ImportError as exc:
        raise RuntimeError(
            "Textual required for interactive monitor — pip install 'okf-agentbus[devex]'"
        ) from exc

    ws = workspace.resolve()

    class _MonitorApp(App):
        TITLE = "AgentBus Mission Control"
        CSS = """
        Screen { layout: vertical; }
        #header-bar {
            height: 3;
            background: $surface;
            color: $text;
            padding: 0 1;
        }
        #body { height: 1fr; }
        #stream { width: 1fr; min-width: 36; }
        #right { width: 1fr; }
        #trace { height: 1fr; border: solid $primary; }
        #hitl { height: 1fr; border: solid $warning; }
        #trace-content, #hitl-help {
            padding: 0 1;
        }
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
            with Horizontal(id="body"):
                yield DataTable(id="stream", cursor_type="row")
                with Vertical(id="right"):
                    yield Static("Select an event with trace_id", id="trace-content")
                    yield DataTable(id="hitl", cursor_type="row")
                    yield Static(
                        "[dim]HITL: focus pending row · a=approve · r=reject[/dim]",
                        id="hitl-help",
                    )
            yield Footer()

        def on_mount(self) -> None:
            stream = self.query_one("#stream", DataTable)
            stream.add_columns("id", "time", "topic", "status", "summary")
            hitl = self.query_one("#hitl", DataTable)
            hitl.add_columns("id", "topic", "from", "summary")
            self.set_interval(interval, self.refresh_data)
            self.refresh_data()

        def refresh_data(self) -> None:
            state = fetch_monitor_state(ws, retention_days=retention_days)
            header = self.query_one("#header-bar", Static)
            header.update(
                f"Workspace: {ws}  |  MCP configs: {state['mcp_configs']}  |  "
                f"Active producers: {state['active_producers']}  |  "
                f"Pending: {len(state['pending'])}"
            )

            stream = self.query_one("#stream", DataTable)
            cursor = stream.cursor_row
            stream.clear()
            for ev in state["events"]:
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
                trace_widget.update(
                    f"Event {selected['event_id']} has no trace_id"
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