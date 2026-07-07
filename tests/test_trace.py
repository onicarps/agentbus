"""Observability trace tests — PDD v0.5 acceptance criteria."""

from __future__ import annotations

import pytest

from agentbus.schemas import validate_payload
from agentbus.store import EventStore
from agentbus.tracing import build_trace_tree


@pytest.fixture
def store(tmp_path):
    s = EventStore(tmp_path)
    yield s
    s.close()


def _handoff(summary: str, **kwargs) -> dict:
    return validate_payload(
        "okf/handoff",
        {"from": "grok", "to": "hermes", "summary": summary, **kwargs},
    )


def test_trace_id_preserved_on_poll(store):
    """PDD AC1: trace_id survives publish → poll roundtrip."""
    event, _ = store.publish(
        topic="okf/handoff",
        producer_id="grok",
        schema_version="1.0",
        payload=_handoff("Run QA"),
        trace_id="trace-550e8400-e29b",
    )
    assert event.trace_id == "trace-550e8400-e29b"

    polled = store.poll("okf/handoff", since_id=0)["events"][0]
    assert polled["trace_id"] == "trace-550e8400-e29b"


def test_span_id_auto_generated(store):
    """PDD AC2: bus assigns unique span_id per event."""
    e1, _ = store.publish(
        topic="okf/handoff",
        producer_id="agy",
        schema_version="1.0",
        payload=_handoff("Published PDD"),
        trace_id="trace-abc",
    )
    e2, _ = store.publish(
        topic="okf/handoff",
        producer_id="grok",
        schema_version="1.0",
        payload=_handoff("Implemented feature"),
        trace_id="trace-abc",
        parent_span_id=e1.span_id,
    )
    assert e1.span_id
    assert e2.span_id
    assert e1.span_id != e2.span_id
    assert e2.parent_span_id == e1.span_id


def test_trace_tree_hierarchy(store):
    """PDD AC3: parent_span_id links build multi-level tree."""
    root, _ = store.publish(
        topic="okf/handoff",
        producer_id="agy",
        schema_version="1.0",
        payload=_handoff("PDD"),
        trace_id="trace-tree",
    )
    child, _ = store.publish(
        topic="okf/handoff",
        producer_id="grok",
        schema_version="1.0",
        payload=_handoff("Implement"),
        trace_id="trace-tree",
        parent_span_id=root.span_id,
    )
    leaf, _ = store.publish(
        topic="okf/handoff",
        producer_id="hermes",
        schema_version="1.0",
        payload=validate_payload(
            "okf/handoff",
            {"from": "hermes", "to": "grok", "summary": "QA failed"},
        ),
        trace_id="trace-tree",
        parent_span_id=child.span_id,
    )

    events = store.fetch_trace_events("trace-tree")
    roots = build_trace_tree(events)
    assert len(roots) == 1
    assert roots[0]["span_id"] == root.span_id
    assert len(roots[0]["children"]) == 1
    assert roots[0]["children"][0]["span_id"] == child.span_id
    assert len(roots[0]["children"][0]["children"]) == 1
    assert roots[0]["children"][0]["children"][0]["span_id"] == leaf.span_id


def test_fetch_trace_events_isolated(store):
    store.publish(
        topic="okf/handoff",
        producer_id="grok",
        schema_version="1.0",
        payload=_handoff("other"),
        trace_id="trace-other",
    )
    store.publish(
        topic="okf/handoff",
        producer_id="grok",
        schema_version="1.0",
        payload=_handoff("target"),
        trace_id="trace-target",
    )
    assert len(store.fetch_trace_events("trace-target")) == 1