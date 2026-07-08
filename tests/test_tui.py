"""Textual TUI helpers — PDD v0.8 acceptance."""

from __future__ import annotations

from agentbus.intercepts import InterceptRule, add_rule
from agentbus.rbac import ensure_default_roles
from agentbus.schemas import validate_payload, set_validation_workspace
from agentbus.store import STATUS_PUBLISHED, EventStore
from agentbus.tui import approve_pending_event, fetch_monitor_state
from agentbus.tracing import build_trace_tree, format_trace_tree_plain


def test_approve_pending_via_tui_helper(tmp_path):
    set_validation_workspace(tmp_path)
    ensure_default_roles(tmp_path)
    add_rule(tmp_path, InterceptRule(topic="okf/handoff", contains="PyPI"))

    store = EventStore(tmp_path)
    try:
        payload = validate_payload(
            "okf/handoff",
            {"from": "grok", "to": "all", "summary": "Push to PyPI now"},
        )
        pending, _ = store.publish(
            topic="okf/handoff",
            producer_id="grok",
            schema_version="1.0",
            payload=payload,
        )
        assert pending.status == "PENDING_APPROVAL"
    finally:
        store.close()

    result = approve_pending_event(tmp_path, pending.event_id, reviewer_id="agy")
    assert result["status"] == STATUS_PUBLISHED

    store = EventStore(tmp_path)
    try:
        updated = store.get_event(pending.event_id)
        assert updated is not None
        assert updated.status == STATUS_PUBLISHED
    finally:
        store.close()


def test_fetch_monitor_state_includes_pending(tmp_path):
    set_validation_workspace(tmp_path)
    add_rule(tmp_path, InterceptRule(topic="okf/handoff", contains="BLOCK"))

    store = EventStore(tmp_path)
    try:
        payload = validate_payload(
            "okf/handoff",
            {"from": "grok", "to": "all", "summary": "BLOCK this action"},
        )
        store.publish(
            topic="okf/handoff",
            producer_id="grok",
            schema_version="1.0",
            payload=payload,
            skip_rbac=True,
        )
    finally:
        store.close()

    state = fetch_monitor_state(tmp_path)
    assert len(state["pending"]) == 1
    assert len(state["events"]) >= 1


def test_format_trace_tree_plain():
    events = [
        {
            "event_id": 1,
            "span_id": "span-a",
            "parent_span_id": None,
            "producer_id": "grok",
            "payload": {"from": "grok", "summary": "root"},
        },
        {
            "event_id": 2,
            "span_id": "span-b",
            "parent_span_id": "span-a",
            "producer_id": "hermes",
            "payload": {"from": "hermes", "summary": "child"},
        },
    ]
    roots = build_trace_tree(events)
    text = format_trace_tree_plain("trace-1", roots)
    assert "trace-1" in text
    assert "grok" in text
    assert "hermes" in text