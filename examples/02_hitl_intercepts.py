#!/usr/bin/env python3
"""HITL intercept rule + PENDING_APPROVAL flow — AgentBus v0.3."""

from __future__ import annotations

import tempfile
from pathlib import Path

from agentbus.intercepts import InterceptRule, add_rule
from agentbus.schemas import set_validation_workspace, validate_payload
from agentbus.store import STATUS_PENDING, STATUS_PUBLISHED, EventStore


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="ab-ex02-") as td:
        ws = Path(td)
        set_validation_workspace(ws)
        add_rule(ws, InterceptRule(topic="okf/handoff", contains="DELETE"))

        store = EventStore(ws)
        try:
            payload = validate_payload(
                "okf/handoff",
                {"from": "grok", "to": "all", "summary": "DELETE production database"},
            )
            event, _ = store.publish(
                topic="okf/handoff",
                producer_id="grok",
                schema_version="1.0",
                payload=payload,
                skip_rbac=True,
            )
            assert event.status == STATUS_PENDING

            poll = store.poll("okf/handoff", since_id=0)
            assert poll["events"] == [], "pending event must be hidden from poll"

            pending = store.review_pending()
            assert len(pending) == 1

            store.approve_event(event.event_id, reviewer_id="human")
        finally:
            store.close()

        store = EventStore(ws)
        try:
            poll2 = store.poll("okf/handoff", since_id=0)
            assert len(poll2["events"]) == 1
            assert poll2["events"][0]["status"] == STATUS_PUBLISHED
            print(f"OK: intercept blocked poll until approve (event {event.event_id})")
        finally:
            store.close()


if __name__ == "__main__":
    main()