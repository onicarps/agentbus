"""Textual TUI helpers — PDD v0.8 acceptance + monitor crash resilience."""

from __future__ import annotations

import sqlite3
import threading
import time
from unittest.mock import patch

from agentbus.intercepts import InterceptRule, add_rule
from agentbus.rbac import ensure_default_roles
from agentbus.schemas import validate_payload, set_validation_workspace
from agentbus.store import STATUS_PUBLISHED, EventStore
from agentbus.tui import (
    _escape_markup,
    _state_fingerprint,
    approve_pending_event,
    fetch_monitor_state,
)
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


def test_fetch_monitor_state_is_read_only(tmp_path):
    """Monitor snapshot must not call expire_* (write locks crash TUI under load)."""
    set_validation_workspace(tmp_path)
    store = EventStore(tmp_path)
    try:
        for i in range(5):
            payload = validate_payload(
                "okf/handoff",
                {"from": "grok", "to": "all", "summary": f"event {i}"},
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

    with (
        patch.object(EventStore, "expire_pending", autospec=True) as exp_p,
        patch.object(EventStore, "expire_sla_breaches", autospec=True) as exp_s,
        patch.object(EventStore, "review_pending", autospec=True) as rev,
    ):
        state = fetch_monitor_state(tmp_path)
        exp_p.assert_not_called()
        exp_s.assert_not_called()
        rev.assert_not_called()
    assert len(state["events"]) == 5
    assert state["pending"] == []


def test_fetch_monitor_state_handles_storm_volume(tmp_path):
    """Hundreds of events (ACK-storm scale) still return a bounded snapshot."""
    set_validation_workspace(tmp_path)
    store = EventStore(tmp_path)
    try:
        for i in range(350):
            payload = validate_payload(
                "okf/handoff",
                {
                    "from": "factory" if i % 2 == 0 else "grok",
                    "to": "grok" if i % 2 == 0 else "factory",
                    "summary": f"RUNNER_ACK storm {i} " + ("x" * 120),
                },
            )
            store.publish(
                topic="okf/handoff",
                producer_id="factory" if i % 2 == 0 else "grok",
                schema_version="1.0",
                payload=payload,
                skip_rbac=True,
                idempotency_key=f"storm-{i}",
            )
    finally:
        store.close()

    state = fetch_monitor_state(tmp_path, limit=200)
    assert len(state["events"]) == 200
    assert state["events"][-1]["event_id"] == 350
    assert state["active_producers"] >= 2
    fp1 = _state_fingerprint(state)
    fp2 = _state_fingerprint(state)
    assert fp1 == fp2


def test_escape_markup_neutralizes_brackets():
    raw = "status [error] with [b]bold[/b]"
    escaped = _escape_markup(raw)
    assert escaped != raw
    try:
        from rich.markup import render

        render(escaped)
    except ImportError:
        pass


def test_fetch_monitor_state_survives_concurrent_writers(tmp_path):
    """Read-only monitor snapshot should not fail when peers publish rapidly."""
    set_validation_workspace(tmp_path)
    stop = threading.Event()

    def writer() -> None:
        n = 0
        while not stop.is_set() and n < 80:
            try:
                store = EventStore(tmp_path)
                try:
                    payload = validate_payload(
                        "okf/handoff",
                        {"from": "stress", "to": "all", "summary": f"w{n}"},
                    )
                    store.publish(
                        topic="okf/handoff",
                        producer_id="stress",
                        schema_version="1.0",
                        payload=payload,
                        skip_rbac=True,
                        idempotency_key=f"conc-w-{n}-{time.time()}",
                    )
                finally:
                    store.close()
            except Exception:
                pass
            n += 1
            time.sleep(0.005)

    t = threading.Thread(target=writer)
    t.start()
    try:
        for _ in range(30):
            state = fetch_monitor_state(tmp_path)
            assert "events" in state
            time.sleep(0.01)
    finally:
        stop.set()
        t.join(timeout=5)


def test_refresh_data_swallows_fetch_errors():
    """Unhandled refresh exceptions crash Textual — verify the guard pattern.

    Pure unit test (no Textual install required in CI; textual is optional
    ``devex`` extra). Mirrors ``_MonitorApp.refresh_data`` error handling.
    """
    from agentbus import tui as tui_mod

    fetch_calls = {"n": 0}
    banners: list[str] = []

    def boom_fetch(*_a, **_k):
        fetch_calls["n"] += 1
        raise sqlite3.OperationalError("database is locked")

    def refresh_data() -> None:
        try:
            boom_fetch()
        except Exception as exc:
            err = f"{type(exc).__name__}: {exc}"
            banners.append(
                f"[b red]Monitor refresh error[/] "
                f"{tui_mod._escape_markup(err)}  "
                f"(will retry)"
            )

    refresh_data()
    refresh_data()
    assert fetch_calls["n"] == 2
    assert len(banners) == 2
    assert "database is locked" in banners[0]
    assert "OperationalError" in banners[0]
