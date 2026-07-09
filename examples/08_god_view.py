#!/usr/bin/env python3
"""God View observability — AgentBus v0.9 (wiretap emit + system topics)."""

from __future__ import annotations

import tempfile
from pathlib import Path

from agentbus.schemas import set_validation_workspace, validate_payload
from agentbus.store import EventStore
from agentbus.tui import detect_dark_agents
from agentbus.wiretap import emit_system_mcp, redact_value


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="ab-ex08-") as td:
        ws = Path(td)
        set_validation_workspace(ws)
        store = EventStore(ws)
        try:
            # Simulate a wiretapped tools/call (auth_token redacted automatically)
            emit_system_mcp(
                store,
                tool="agentbus_publish",
                arguments={
                    "topic": "okf/handoff",
                    "auth_token": "should-not-leak",
                    "payload": {"from": "grok", "to": "hermes", "summary": "work"},
                },
                latency_ms=4.2,
                result_summary='{"event_id":1}',
            )
            fs_payload = validate_payload(
                "system/fs",
                {"event": "created", "path": "src/feature.py", "is_directory": False},
            )
            store.publish(
                topic="system/fs",
                producer_id="os-watcher",
                schema_version="1.0",
                payload=fs_payload,
                skip_rbac=True,
            )
            mcp_events = store.poll("system/mcp")["events"]
            fs_events = store.poll("system/fs")["events"]
            print("system/mcp:", len(mcp_events), "events")
            print("redacted args:", redact_value(mcp_events[0]["payload"]["arguments"]))
            print("system/fs:", fs_events[0]["payload"]["path"])
            dark = detect_dark_agents(mcp_events + fs_events)
            print("dark_agents:", dark)
        finally:
            store.close()
        print("OK — God View example complete")


if __name__ == "__main__":
    main()
