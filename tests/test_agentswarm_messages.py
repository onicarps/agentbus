#!/usr/bin/env python3
"""Agentswarm integration test — simulate multi-agent pub/sub on the live workspace.

Run standalone:  python3 tests/test_agentswarm_messages.py
Not a pytest module (functions prefixed with _ to avoid fixture errors).
"""

from __future__ import annotations

import json
import secrets
import sys
import time
from pathlib import Path

# Ensure the project is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from agentbus.schemas import set_validation_workspace, validate_payload
from agentbus.store import EventStore


WORKSPACE = Path("/home/oni/okf_agent_workspace/projects/agentbus")
set_validation_workspace(WORKSPACE)


def banner(title: str) -> None:
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


def _test_basic_handoff(store: EventStore) -> int:
    """Test 1: Basic inter-agent handoff (hermes -> grok)."""
    banner("TEST 1: Basic Handoff (hermes -> grok)")

    payload = validate_payload(
        "okf/handoff",
        {"from": "hermes", "to": "grok", "summary": "Task assigned: analyze log patterns"},
    )
    event, dup = store.publish(
        topic="okf/handoff", producer_id="hermes", schema_version="1.0",
        payload=payload, skip_rbac=True,
    )
    print(f"  Published: event #{event.event_id} (dup={dup})")
    assert not dup

    polled = store.poll("okf/handoff", since_id=0)
    assert len(polled["events"]) >= 1
    received = polled["events"][-1]
    assert received["producer_id"] == "hermes"
    assert received["payload"]["from"] == "hermes"
    assert received["payload"]["to"] == "grok"
    print(f"  Polled: {len(polled['events'])} event(s), latest=#{received['event_id']}")
    print(f"  Payload: {json.dumps(received['payload'], indent=4)}")
    return event.event_id


def _test_broadcast(store: EventStore, prev_id: int) -> int:
    """Test 2: Broadcast to all agents."""
    banner("TEST 2: Broadcast to All Agents")

    payload = validate_payload(
        "okf/handoff",
        {"from": "factory", "to": "all", "summary": "System status check — all agents report in"},
    )
    event, _ = store.publish(
        topic="okf/handoff", producer_id="factory", schema_version="1.0",
        payload=payload, skip_rbac=True, causation_id=prev_id,
    )
    print(f"  Published broadcast: event #{event.event_id}")

    polled = store.poll("okf/handoff", since_id=0)
    assert len(polled["events"]) >= 1
    print(f"  okf/handoff total: {len(polled['events'])} event(s)")
    return event.event_id


def _test_traced_chain(store: EventStore, prev_id: int) -> int:
    """Test 3: Traced causal chain (hermes -> grok -> hermes)."""
    banner("TEST 3: Traced Causal Chain")

    trace_id = f"trace-{secrets.token_hex(8)}"

    payload_a = validate_payload("okf/handoff", {"from": "hermes", "to": "grok", "summary": "Step A: run diagnostics"})
    evt_a, _ = store.publish(topic="okf/handoff", producer_id="hermes", schema_version="1.0",
                             payload=payload_a, skip_rbac=True, causation_id=prev_id, trace_id=trace_id)
    print(f"  Step A: hermes -> grok (caused by #{prev_id}): event #{evt_a.event_id}")
    print(f"  Trace ID: {trace_id}, Span: {evt_a.span_id}")

    payload_b = validate_payload("okf/handoff", {"from": "grok", "to": "hermes", "summary": "Step B: diagnostics complete, 0 errors"})
    evt_b, _ = store.publish(topic="okf/handoff", producer_id="grok", schema_version="1.0",
                             payload=payload_b, skip_rbac=True, causation_id=evt_a.event_id, trace_id=trace_id)
    print(f"  Step B: grok -> hermes (caused by #{evt_a.event_id}): event #{evt_b.event_id}")

    payload_c = validate_payload("okf/handoff", {"from": "hermes", "to": "grok", "summary": "Step C: received, closing ticket"})
    evt_c, _ = store.publish(topic="okf/handoff", producer_id="hermes", schema_version="1.0",
                             payload=payload_c, skip_rbac=True, causation_id=evt_b.event_id, trace_id=trace_id)
    print(f"  Step C: hermes -> grok (caused by #{evt_b.event_id}): event #{evt_c.event_id}")

    traced = store.fetch_trace_events(trace_id)
    print(f"  Trace {trace_id}: {len(traced)} events in chain")
    for t_evt in traced:
        print(f"    #{t_evt['event_id']} ({t_evt['producer_id']} -> {t_evt.get('payload',{}).get('to','?')})")
    assert len(traced) >= 3
    return evt_c.event_id


def _test_artifact_transfer(store: EventStore, prev_id: int) -> int:
    """Test 4: Artifact transfer (hermes sends file content to grok)."""
    banner("TEST 4: Artifact Transfer")

    artifact_content = json.dumps({"metric": "cpu_usage", "value": 42.7, "unit": "%"})
    payload = validate_payload(
        "okf/handoff",
        {"from": "hermes", "to": "grok", "summary": "Performance data handoff",
         "artifacts": [{"type": "git_diff", "name": "metrics.json", "content": artifact_content}]},
    )
    event, _ = store.publish(
        topic="okf/handoff", producer_id="hermes", schema_version="1.0",
        payload=payload, skip_rbac=True, causation_id=prev_id,
    )
    print(f"  Published with artifact: event #{event.event_id}")

    polled = store.poll("okf/handoff", since_id=0)
    last = polled["events"][-1]
    arts = last["payload"].get("artifacts") or []
    assert len(arts) == 1
    assert arts[0]["name"] == "metrics.json"
    print(f"  Artifact verified: {arts[0]['name']} ({len(arts[0]['content'])} bytes)")
    return event.event_id


def _test_sla_timeout(store: EventStore, prev_id: int) -> int:
    """Test 5: SLA timeout — event with 1-minute deadline."""
    banner("TEST 5: SLA Timeout (1-minute deadline)")

    payload = validate_payload(
        "okf/handoff",
        {"from": "factory", "to": "hermes", "summary": "Urgent review required — SLA-bound"},
    )
    event, _ = store.publish(
        topic="okf/handoff", producer_id="factory", schema_version="1.0",
        payload=payload, skip_rbac=True, sla_timeout_minutes=1, causation_id=prev_id,
    )
    print(f"  Published SLA-bound event: #{event.event_id} (sla={event.sla_timeout_minutes}m)")
    print(f"  Deadline: {event.sla_deadline}")

    dl_polled = store.poll("okf/dead-letter", since_id=0)
    print(f"  Dead-letter events: {len(dl_polled['events'])}")
    return event.event_id


def _test_idempotency(store: EventStore, prev_id: int) -> int:
    """Test 6: Idempotency — same message twice should deduplicate."""
    banner("TEST 6: Idempotency Deduplication")

    payload = validate_payload("okf/handoff", {"from": "hermes", "to": "grok", "summary": "Idempotent heartbeat seq=1"})
    event1, dup1 = store.publish(
        topic="okf/handoff", producer_id="hermes", schema_version="1.0",
        payload=payload, skip_rbac=True, idempotency_key="test-idemp-001", causation_id=prev_id,
    )
    print(f"  First publish: event #{event1.event_id}, dup={dup1}")

    event2, dup2 = store.publish(
        topic="okf/handoff", producer_id="hermes", schema_version="1.0",
        payload=payload, skip_rbac=True, idempotency_key="test-idemp-001", causation_id=event1.event_id,
    )
    print(f"  Second publish (same key): event #{event2.event_id}, dup={dup2}")
    assert dup2, "Second publish with same idempotency_key MUST be a duplicate"
    assert event1.event_id == event2.event_id
    print(f"  Dedup confirmed: both point to event #{event1.event_id}")
    return event2.event_id


def _test_content_dedup(store: EventStore, prev_id: int) -> int:
    """Test 7: Content-based dedup (same topic+producer+payload within window)."""
    banner("TEST 7: Content-Based Deduplication")

    payload = validate_payload("okf/handoff", {"from": "grok", "to": "hermes", "summary": "Heartbeat beat=42"})
    event1, dup1 = store.publish(
        topic="okf/handoff", producer_id="grok", schema_version="1.0",
        payload=payload, skip_rbac=True, causation_id=prev_id,
    )
    print(f"  First heartbeat: event #{event1.event_id}, dup={dup1}")

    time.sleep(1.5)
    event2, dup2 = store.publish(
        topic="okf/handoff", producer_id="grok", schema_version="1.0",
        payload=payload, skip_rbac=True, causation_id=event1.event_id,
    )
    print(f"  Second heartbeat (identical payload): event #{event2.event_id}, dup={dup2}")
    if dup2:
        print(f"  Content dedup triggered: both point to #{event1.event_id}")
    else:
        print(f"  Content dedup did NOT trigger (outside window or policy)")
    return event2.event_id


def _test_approval_flow(store: EventStore, prev_id: int) -> int:
    """Test 8: HITL approval — publish an approval record referencing a prior event."""
    banner("TEST 8: HITL Approval Flow")

    task_payload = validate_payload("okf/handoff", {"from": "hermes", "to": "grok", "summary": "Pending approval task"})
    task_evt, _ = store.publish(
        topic="okf/handoff", producer_id="hermes", schema_version="1.0",
        payload=task_payload, skip_rbac=True, causation_id=prev_id,
    )
    print(f"  Task published: #{task_evt.event_id}")

    approval_payload = validate_payload(
        "okf/approval",
        {"event_id": task_evt.event_id, "approver": "reviewer", "decision": "approve", "reason": "approved after review"},
    )
    approval_evt, _ = store.publish(
        topic="okf/approval", producer_id="reviewer", schema_version="1.0",
        payload=approval_payload, skip_rbac=True, causation_id=task_evt.event_id,
    )
    print(f"  Approval recorded: #{approval_evt.event_id} (decision={approval_evt.payload['decision']})")

    polled = store.poll("okf/approval", since_id=0)
    print(f"  okf/approval total: {len(polled['events'])} event(s)")
    assert len(polled["events"]) >= 1
    return approval_evt.event_id


def _test_multi_topic_poll(store: EventStore) -> dict:
    """Test 9: Cross-topic summary."""
    banner("TEST 9: Cross-Topic Summary")

    topics = ["okf/handoff", "okf/approval", "okf/dead-letter"]
    summary = {}
    for topic in topics:
        polled = store.poll(topic, since_id=0)
        summary[topic] = len(polled["events"])
        if polled["events"]:
            latest = polled["events"][-1]
            print(f"  {topic}: {len(polled['events'])} event(s), latest=#{latest['event_id']} by {latest['producer_id']}")
        else:
            print(f"  {topic}: {len(polled['events'])} event(s) (empty)")
    return summary


def main() -> None:
    print("AgentBus Agentswarm Integration Test")
    print(f"Workspace: {WORKSPACE}")
    print(f"DB: {WORKSPACE / '.agentbus' / 'events.db'}")

    store = EventStore(WORKSPACE)
    try:
        results = {}
        eid = 0

        eid = _test_basic_handoff(store); results["basic_handoff"] = eid
        eid = _test_broadcast(store, eid); results["broadcast"] = eid
        eid = _test_traced_chain(store, eid); results["traced_chain"] = eid
        eid = _test_artifact_transfer(store, eid); results["artifact_transfer"] = eid
        eid = _test_sla_timeout(store, eid); results["sla_timeout"] = eid
        eid = _test_idempotency(store, eid); results["idempotency"] = eid
        eid = _test_content_dedup(store, eid); results["content_dedup"] = eid
        eid = _test_approval_flow(store, eid); results["approval_flow"] = eid
        summary = _test_multi_topic_poll(store); results["topic_summary"] = summary

        banner("SUMMARY")
        print(json.dumps(results, indent=2, default=str))
        print("\nAll tests passed.")
    finally:
        store.close()


if __name__ == "__main__":
    main()