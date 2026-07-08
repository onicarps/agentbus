#!/usr/bin/env python3
"""Distributed trace_id + parent_span_id — AgentBus v0.5."""

from __future__ import annotations

import tempfile
from pathlib import Path

from agentbus.schemas import set_validation_workspace, validate_payload
from agentbus.store import EventStore
from agentbus.tracing import build_trace_tree


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="ab-ex05-") as td:
        ws = Path(td)
        set_validation_workspace(ws)
        store = EventStore(ws)
        try:
            parent_payload = validate_payload(
                "okf/handoff",
                {"from": "grok", "to": "hermes", "summary": "Parent handoff"},
            )
            parent, _ = store.publish(
                topic="okf/handoff",
                producer_id="grok",
                schema_version="1.0",
                payload=parent_payload,
                trace_id="trace-example-05",
                skip_rbac=True,
            )
            child_payload = validate_payload(
                "okf/handoff",
                {"from": "hermes", "to": "grok", "summary": "Child reply"},
            )
            child, _ = store.publish(
                topic="okf/handoff",
                producer_id="hermes",
                schema_version="1.0",
                payload=child_payload,
                trace_id="trace-example-05",
                parent_span_id=parent.span_id,
                skip_rbac=True,
            )
            assert child.trace_id == parent.trace_id
            assert child.parent_span_id == parent.span_id

            trace_events = store.fetch_trace_events("trace-example-05")
            roots = build_trace_tree(trace_events)
            assert len(roots) >= 1
            assert any(c.get("event_id") == child.event_id for c in roots[0].get("children", []))
            print(f"OK: trace {parent.trace_id} parent={parent.event_id} child={child.event_id}")
        finally:
            store.close()


if __name__ == "__main__":
    main()