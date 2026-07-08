#!/usr/bin/env python3
"""Artifact attachment on publish — AgentBus v0.6."""

from __future__ import annotations

import tempfile
from pathlib import Path

from agentbus.schemas import set_validation_workspace, validate_payload
from agentbus.store import EventStore


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="ab-ex06-") as td:
        ws = Path(td)
        set_validation_workspace(ws)
        artifact_path = ws / "sample.txt"
        artifact_path.write_text("artifact payload for example 06\n", encoding="utf-8")

        store = EventStore(ws)
        try:
            payload = validate_payload(
                "okf/handoff",
                {
                    "from": "grok",
                    "to": "all",
                    "summary": "Handoff with attached artifact",
                    "artifacts": [
                        {
                            "type": "file_content",
                            "name": "sample.txt",
                            "content": artifact_path.read_text(encoding="utf-8"),
                        }
                    ],
                },
            )
            event, _ = store.publish(
                topic="okf/handoff",
                producer_id="grok",
                schema_version="1.0",
                payload=payload,
                skip_rbac=True,
            )
            polled = store.poll("okf/handoff", since_id=0)
            arts = polled["events"][0]["payload"].get("artifacts") or []
            assert len(arts) == 1
            assert arts[0]["name"] == "sample.txt"
            print(f"OK: artifact stored for event {event.event_id}")
        finally:
            store.close()


if __name__ == "__main__":
    main()