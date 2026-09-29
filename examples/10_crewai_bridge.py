#!/usr/bin/env python3
"""Example 10: a CrewAI-task handoff skeleton.

This is framework-neutral on purpose: call the publish block from a CrewAI
task after registering its producer in the workspace RBAC policy. It does not
import or execute CrewAI itself.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from agentbus.rbac import ensure_default_roles, save_rbac_config
from agentbus.schemas import set_validation_workspace, validate_payload
from agentbus.store import EventStore


def run_crewai_bridge_demo() -> None:
    with tempfile.TemporaryDirectory(prefix="ab-ex10-") as td:
        ws = Path(td)
        set_validation_workspace(ws)
        rbac = ensure_default_roles(ws)
        rbac.producers["crewai-agent"] = "engineer"
        save_rbac_config(ws, rbac)
        store = EventStore(ws)
        try:
            print("--- [CrewAI Task: Multi-Agent QA Delegation] ---")
            print("CrewAI task dispatching pre-push test validation to Pi...")

            payload = validate_payload(
                "okf/handoff",
                {
                    "from": "crewai-agent",
                    "to": "pi",
                    "summary": "PI_QA_MISSION: Validate unit test matrix and coverage across Python 3.11/3.12",
                    "initiative": "agentbus",
                    "links": ["/projects/agentbus/tests/"],
                },
            )
            event, _ = store.publish(
                topic="okf/handoff",
                producer_id="crewai-agent",
                schema_version="1.0",
                payload=payload,
            )
            print(f"Published CrewAI QA mission event #{event.event_id}.")
            print("CrewAI task can await PI_QA_VERDICT on the bus.")
        finally:
            store.close()


if __name__ == "__main__":
    run_crewai_bridge_demo()
