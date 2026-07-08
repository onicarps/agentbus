#!/usr/bin/env python3
"""SLA timeout + dead-letter routing — AgentBus v0.4."""

from __future__ import annotations

import tempfile
from pathlib import Path

from agentbus.schemas import DEAD_LETTER_TOPIC, set_validation_workspace, validate_payload
from agentbus.store import STATUS_TIMEOUT_FAILED, EventStore


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="ab-ex04-") as td:
        ws = Path(td)
        set_validation_workspace(ws)
        store = EventStore(ws)
        try:
            payload = validate_payload(
                "okf/handoff",
                {"from": "grok", "to": "hermes", "summary": "Run QA validation"},
            )
            event, _ = store.publish(
                topic="okf/handoff",
                producer_id="grok",
                schema_version="1.0",
                payload=payload,
                sla_timeout_minutes=1,
                skip_rbac=True,
            )
            assert event.sla_deadline is not None

            store._conn.execute(
                "UPDATE events SET sla_deadline = ? WHERE event_id = ?",
                ("2020-01-01T00:00:00Z", event.event_id),
            )
            store._conn.commit()
            timed_out = store.expire_sla_breaches()
            assert event.event_id in timed_out

            updated = store.get_event(event.event_id)
            assert updated is not None
            assert updated.status == STATUS_TIMEOUT_FAILED

            dead = store.poll(DEAD_LETTER_TOPIC, since_id=0)
            assert len(dead["events"]) == 1
            assert dead["events"][0]["payload"]["reason"] == "SLA_BREACH"
            print(f"OK: SLA breach escalated to dead-letter (event {event.event_id})")
        finally:
            store.close()


if __name__ == "__main__":
    main()