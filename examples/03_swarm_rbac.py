#!/usr/bin/env python3
"""Swarm RBAC + qa_droid cryptographic proof — AgentBus v0.3."""

from __future__ import annotations

import tempfile
from pathlib import Path

from agentbus.rbac import ensure_default_roles, mint_droid_proof
from agentbus.schemas import set_validation_workspace, validate_payload
from agentbus.store import EventStore


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="ab-ex03-") as td:
        ws = Path(td)
        set_validation_workspace(ws)
        ensure_default_roles(ws)

        minted = mint_droid_proof(ws, mission_id="example-mission")
        proof = minted["droid_proof"]

        store = EventStore(ws)
        try:
            payload = validate_payload(
                "okf/handoff",
                {
                    "from": "hermes",
                    "to": "all",
                    "summary": "QA droid validation complete",
                    "droid_proof": proof,
                },
            )
            event, _ = store.publish(
                topic="okf/handoff",
                producer_id="hermes",
                schema_version="1.0",
                payload=payload,
            )
            polled = store.poll("okf/handoff", since_id=0)
            assert len(polled["events"]) == 1
            assert polled["events"][0]["payload"]["droid_proof"] == proof
            print(f"OK: qa_droid published with proof (event {event.event_id})")
        finally:
            store.close()


if __name__ == "__main__":
    main()