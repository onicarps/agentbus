#!/usr/bin/env python3
"""Basic publish/poll loop — AgentBus v0.1 core pub/sub."""

from __future__ import annotations

import tempfile
from pathlib import Path

from agentbus.schemas import set_validation_workspace, validate_payload
from agentbus.store import EventStore


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="ab-ex01-") as td:
        ws = Path(td)
        set_validation_workspace(ws)
        store = EventStore(ws)
        try:
            payload = validate_payload(
                "okf/handoff",
                {"from": "demo", "to": "all", "summary": "Hello from example 01"},
            )
            event, _ = store.publish(
                topic="okf/handoff",
                producer_id="demo",
                schema_version="1.0",
                payload=payload,
                skip_rbac=True,
            )
            polled = store.poll("okf/handoff", since_id=0)
            assert len(polled["events"]) == 1
            assert polled["events"][0]["event_id"] == event.event_id
            print(f"OK: published event {event.event_id}, poll returned 1 event")
        finally:
            store.close()


if __name__ == "__main__":
    main()