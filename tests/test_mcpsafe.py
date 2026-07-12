"""mcpsafe PolicyEnforcer + bus integration tests."""

from __future__ import annotations

import json

import pytest
from click.testing import CliRunner

from agentbus.cli import main as cli_main
from agentbus.mcpsafe import AccessDeniedError, PolicyEnforcer, load_enforcer
from agentbus.schemas import validate_payload
from agentbus.server import configure_mcpsafe, init_store
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


def test_invalid_lockfile_allows_all(tmp_path):
    lock = tmp_path / ".mcpsafe.lock"
    lock.write_text("{not-json", encoding="utf-8")
    enf = PolicyEnforcer(lock)
    assert enf.evaluate("anything") is True


def test_require_raises_without_path_leak(tmp_path):
    lock = tmp_path / ".mcpsafe.lock"
    _write_lock(lock, blocked=["bad_tool"])
    enf = PolicyEnforcer(lock)
    with pytest.raises(AccessDeniedError, match="AccessDenied") as ei:
        enf.require("bad_tool")
    assert str(tmp_path) not in str(ei.value)
    assert ".mcpsafe.lock" not in str(ei.value)


def test_payload_tool_fields(tmp_path):
    lock = tmp_path / ".mcpsafe.lock"
    _write_lock(lock, blocked=["dangerous"])
    enf = PolicyEnforcer(lock)
    assert enf.evaluate_payload({"summary": "ok"}) is True
    assert enf.evaluate_payload({"tool": "dangerous"}) is False
    assert enf.evaluate_payload({"tool_name": "safe"}) is True
    assert enf.evaluate_payload({"mcp_tool": "dangerous"}) is False


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


def test_configure_mcpsafe_attaches_to_store(tmp_path):
    lock = tmp_path / ".mcpsafe.lock"
    _write_lock(lock, blocked=["x"])
    configure_mcpsafe(True, lockfile=lock, workspace=tmp_path)
    store = init_store(tmp_path)
    assert store._mcpsafe is not None
    assert store._mcpsafe.evaluate("x") is False
    store.close()
    configure_mcpsafe(False)


def test_cli_publish_enable_mcpsafe_blocks(tmp_path, monkeypatch):
    monkeypatch.setenv("AGENTBUS_AUTH", "off")
    monkeypatch.delenv("AGENTBUS_ENABLE_MCPSAFE", raising=False)
    lock = tmp_path / ".mcpsafe.lock"
    _write_lock(lock, blocked=["file_delete"])
    runner = CliRunner()
    result = runner.invoke(
        cli_main,
        [
            "publish",
            "--workspace",
            str(tmp_path),
            "--topic",
            "okf/handoff",
            "--payload",
            json.dumps(
                {
                    "from": "grok",
                    "to": "swarm",
                    "summary": "blocked tool",
                    "tool": "file_delete",
                }
            ),
            "--producer-id",
            "grok",
            "--enable-mcpsafe",
            "--mcpsafe-lock",
            str(lock),
        ],
    )
    assert result.exit_code != 0
    assert "AccessDenied" in (result.output or "") + (str(result.exception) or "")
