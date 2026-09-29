#!/usr/bin/env python3
"""Example 09: a LangGraph-node handoff skeleton.

This is framework-neutral on purpose: call the publish block from a LangGraph
node after registering its producer in the workspace RBAC policy. It does not
import, execute, or wait on LangGraph itself.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from agentbus.rbac import ensure_default_roles, save_rbac_config
from agentbus.schemas import set_validation_workspace, validate_payload
from agentbus.store import EventStore


def run_langgraph_bridge_demo() -> None:
    with tempfile.TemporaryDirectory(prefix="ab-ex09-") as td:
        ws = Path(td)
        set_validation_workspace(ws)
        rbac = ensure_default_roles(ws)
        rbac.producers["langgraph-orchestrator"] = "engineer"
        save_rbac_config(ws, rbac)
        store = EventStore(ws)
        try:
            print("--- [LangGraph Node: Planner] ---")
            print("Generating feature specification and dispatching to AgentBus...")

            payload = validate_payload(
                "okf/handoff",
                {
                    "from": "langgraph-orchestrator",
                    "to": "codex",
                    "summary": "Implement JWT rotation middleware in FastAPI",
                    "initiative": "auth-service",
                    "links": ["/docs/specs/jwt-rotation.md"],
                },
            )
            event, _ = store.publish(
                topic="okf/handoff",
                producer_id="langgraph-orchestrator",
                schema_version="1.0",
                payload=payload,
            )
            print(f"Dispatched event #{event.event_id} to Codex.")

            polled = store.poll(topic="okf/handoff", since_id=0, limit=5)
            print(f"Polled {len(polled['events'])} events from the local bus.")
        finally:
            store.close()


if __name__ == "__main__":
    run_langgraph_bridge_demo()
