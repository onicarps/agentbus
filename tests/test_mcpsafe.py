"""mcpsafe PolicyEnforcer + bus integration tests."""

from __future__ import annotations

import json

import pytest

from agentbus.mcpsafe import AccessDeniedError, PolicyEnforcer, load_enforcer
from agentbus.schemas import validate_payload
from agentbus.store import EventStore


def _write_lock(path, *, allowed=None, blocked=None):
    data = {}
    if allowed is not None:
        data["allowed_tools"] = allowed
    if blocked is not None:
        data["blocked_tools"] = blocked
    path.write_text(json.dumps(data), encoding="utf-8")


def test_evaluate_blocked_tool(tmp_path):
    lock = tmp_path / ".mcpsafe.lock"
    _write_lock(lock, blocked=["shell_exec", "rm"])
    enf = PolicyEnforcer(lock)
    assert enf.evaluate("shell_exec") is False
    assert enf.evaluate("agentbus_publish") is True


def test_evaluate_allowlist(tmp_path):
    lock = tmp_path / ".mcpsafe.lock"
    _write_lock(lock, allowed=["agentbus_publish", "agentbus_poll"])
    enf = PolicyEnforcer(lock)
    assert enf.evaluate("agentbus_publish") is True
    assert enf.evaluate("agentbus_status") is False


def test_blocked_overrides_allowlist(tmp_path):
    lock = tmp_path / ".mcpsafe.lock"
    _write_lock(lock, allowed=["shell_exec"], blocked=["shell_exec"])
    enf = PolicyEnforcer(lock)
    assert enf.evaluate("shell_exec") is False


def test_missing_lockfile_allows_all(tmp_path):
    enf = PolicyEnforcer(tmp_path / "missing.lock")
    assert enf.evaluate("anything") is True


def test_require_raises(tmp_path):
    lock = tmp_path / ".mcpsafe.lock"
    _write_lock(lock, blocked=["bad_tool"])
    enf = PolicyEnforcer(lock)
    with pytest.raises(AccessDeniedError, match="AccessDenied"):
        enf.require("bad_tool")


def test_payload_tool_fields(tmp_path):
    lock = tmp_path / ".mcpsafe.lock"
    _write_lock(lock, blocked=["dangerous"])
    enf = PolicyEnforcer(lock)
    assert enf.evaluate_payload({"summary": "ok"}) is True
    assert enf.evaluate_payload({"tool": "dangerous"}) is False
    assert enf.evaluate_payload({"tool_name": "safe"}) is True


def test_store_publish_blocked_payload_tool(tmp_path):
    lock = tmp_path / ".mcpsafe.lock"
    _write_lock(lock, blocked=["file_delete"])
    store = EventStore(tmp_path)
    store.set_mcpsafe(PolicyEnforcer(lock))
    with pytest.raises(AccessDeniedError):
        store.publish(
            topic="okf/handoff",
            producer_id="grok",
            schema_version="1.0",
            payload=validate_payload(
                "okf/handoff",
                {
                    "from": "grok",
                    "to": "swarm",
                    "summary": "try delete",
                    "tool": "file_delete",
                },
            ),
        )
    store.close()


def test_store_publish_allowed_without_tool(tmp_path):
    lock = tmp_path / ".mcpsafe.lock"
    _write_lock(lock, blocked=["file_delete"])
    store = EventStore(tmp_path)
    store.set_mcpsafe(PolicyEnforcer(lock))
    event, dup = store.publish(
        topic="okf/handoff",
        producer_id="grok",
        schema_version="1.0",
        payload=validate_payload(
            "okf/handoff",
            {"from": "grok", "to": "swarm", "summary": "normal handoff"},
        ),
    )
    assert not dup
    assert event.event_id == 1
    store.close()


def test_load_enforcer_off_by_default(tmp_path, monkeypatch):
    monkeypatch.delenv("AGENTBUS_ENABLE_MCPSAFE", raising=False)
    assert load_enforcer(tmp_path) is None
    monkeypatch.setenv("AGENTBUS_ENABLE_MCPSAFE", "1")
    enf = load_enforcer(tmp_path)
    assert enf is not None
    assert enf.lockfile_path == (tmp_path / ".mcpsafe.lock").resolve()
