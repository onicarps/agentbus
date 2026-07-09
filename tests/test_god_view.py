"""God View (v0.9) — schemas, wiretap redaction, watch/tail helpers, dark agents."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from agentbus.rbac import default_rbac_config, ensure_default_roles
from agentbus.schemas import SYSTEM_TOPICS, set_validation_workspace, validate_payload
from agentbus.server import configure_wiretap, init_store
from agentbus.store import EventStore
from agentbus.tail import (
    _monologue_idempotency_key,
    _safe_source_path,
    expand_registry,
    list_agent_logs,
    parse_line,
)
from agentbus.tui import detect_dark_agents, fetch_monitor_state
from agentbus.watch import _cwd_under_workspace
from agentbus.wiretap import REDACTED, emit_system_mcp, instrument_call, redact_value


def test_system_topics_registered():
    assert SYSTEM_TOPICS == {
        "system/mcp",
        "system/fs",
        "system/shell",
        "system/monologue",
    }
    for topic in SYSTEM_TOPICS:
        payload = {
            "system/mcp": {"tool": "agentbus_status", "latency_ms": 1.2},
            "system/fs": {"event": "created", "path": "src/a.py"},
            "system/shell": {"event": "process_start", "pid": 1234, "name": "bash"},
            "system/monologue": {"agent": "grok", "text": "thinking..."},
        }[topic]
        out = validate_payload(topic, payload)
        assert out


def test_observer_role_in_default_rbac():
    cfg = default_rbac_config()
    assert "observer" in cfg.roles
    assert cfg.producers["wiretap"] == "observer"
    assert cfg.producers["os-watcher"] == "observer"
    assert "system/*" in cfg.roles["observer"].can_publish_topics


def test_redact_auth_token():
    raw = {
        "topic": "okf/handoff",
        "auth_token": "super-secret-token-value-12345",
        "nested": {"api_key": "sk-abc", "ok": "yes"},
    }
    cleaned = redact_value(raw)
    assert cleaned["auth_token"] == REDACTED
    assert cleaned["nested"]["api_key"] == REDACTED
    assert cleaned["nested"]["ok"] == "yes"


def test_emit_system_mcp_publishes(tmp_path: Path):
    set_validation_workspace(tmp_path)
    store = EventStore(tmp_path)
    try:
        eid = emit_system_mcp(
            store,
            tool="agentbus_publish",
            arguments={"topic": "okf/handoff", "auth_token": "secret-token-xxx"},
            latency_ms=12.5,
            result_summary='{"event_id":1}',
        )
        assert eid is not None
        polled = store.poll("system/mcp", since_id=0)
        assert polled["events"]
        args = polled["events"][0]["payload"]["arguments"]
        assert args["auth_token"] == REDACTED
    finally:
        store.close()


def test_instrument_call_records_latency(tmp_path: Path):
    set_validation_workspace(tmp_path)
    store = EventStore(tmp_path)
    try:
        result = instrument_call(
            store,
            "agentbus_status",
            {},
            lambda: '{"ok": true}',
        )
        assert "ok" in result
        events = store.poll("system/mcp")["events"]
        assert len(events) == 1
        assert events[0]["payload"]["tool"] == "agentbus_status"
        assert "latency_ms" in events[0]["payload"]
    finally:
        store.close()


def test_wiretap_default_off_does_not_break_init(tmp_path: Path):
    configure_wiretap(False)
    store = init_store(tmp_path)
    try:
        assert store is not None
    finally:
        store.close()
        configure_wiretap(False)


def test_parse_line_grok_jsonl():
    line = json.dumps({"src": "shell", "lvl": "info", "msg": "hello world"})
    parsed = parse_line("grok", line, "jsonl")
    assert parsed is not None
    assert parsed["agent"] == "grok"
    assert "hello" in parsed["text"]


def test_list_agent_logs_shape():
    rows = list_agent_logs(["grok", "hermes"])
    assert len(rows) == 2
    assert {r["agent"] for r in rows} == {"grok", "hermes"}
    assert "present" in rows[0]


def test_expand_registry_finds_temp_log(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    home = tmp_path / "home"
    log_dir = home / ".grok" / "logs"
    log_dir.mkdir(parents=True)
    log_path = log_dir / "unified.jsonl"
    log_path.write_text('{"msg":"hi"}\n', encoding="utf-8")
    sources = expand_registry(["grok"], home=home, cwd=tmp_path)
    assert any(s.path == log_path for s in sources)


def test_system_fs_publish_via_store(tmp_path: Path):
    set_validation_workspace(tmp_path)
    store = EventStore(tmp_path)
    try:
        payload = validate_payload(
            "system/fs",
            {"event": "modified", "path": "README.md", "is_directory": False},
        )
        event, _ = store.publish(
            topic="system/fs",
            producer_id="os-watcher",
            schema_version="1.0",
            payload=payload,
            skip_rbac=True,
        )
        assert event.topic == "system/fs"
    finally:
        store.close()


def test_detect_dark_agents():
    now = datetime.now(timezone.utc)
    events = [
        {
            "producer_id": "ghost",
            "topic": "system/fs",
            "timestamp": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        },
        {
            "producer_id": "grok",
            "topic": "okf/handoff",
            "timestamp": (now - timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        },
        {
            # okf-only publisher must not false-positive as dark
            "producer_id": "agy",
            "topic": "okf/handoff",
            "timestamp": (now - timedelta(minutes=20)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        },
    ]
    dark = detect_dark_agents(events, threshold_minutes=5, now=now)
    pids = {d["producer_id"] for d in dark}
    assert "ghost" in pids
    assert "grok" not in pids
    assert "agy" not in pids


def test_safe_source_path_abbreviates_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: home))
    log = home / ".grok" / "logs" / "unified.jsonl"
    log.parent.mkdir(parents=True)
    log.write_text("x\n")
    assert _safe_source_path(str(log)).startswith("~/")


def test_monologue_idempotency_stable():
    a = _monologue_idempotency_key(
        agent="grok", source_path="~/x", role="log", text="hi", byte_offset=10
    )
    b = _monologue_idempotency_key(
        agent="grok", source_path="~/x", role="log", text="hi", byte_offset=10
    )
    assert a == b
    assert a.startswith("mono-")


def test_cwd_path_containment(tmp_path: Path):
    ws = tmp_path / "workspace"
    ws.mkdir()
    sibling = tmp_path / "workspace-other"
    sibling.mkdir()
    assert _cwd_under_workspace(str(ws / "src"), ws)
    assert not _cwd_under_workspace(str(sibling), ws)


def test_result_summary_redacted(tmp_path: Path):
    set_validation_workspace(tmp_path)
    store = EventStore(tmp_path)
    try:
        secret = "a" * 40  # long token-like
        emit_system_mcp(
            store,
            tool="agentbus_status",
            arguments={},
            latency_ms=1.0,
            result_summary=f'{{"token": "{secret}"}}',
        )
        ev = store.poll("system/mcp")["events"][0]
        assert secret not in ev["payload"].get("result_summary", "")

        # Embedded token in a non-sensitive JSON value must also be masked
        emit_system_mcp(
            store,
            tool="agentbus_status",
            arguments={},
            latency_ms=1.0,
            result_summary=f'{{"message": "received bearer {secret} from tool"}}',
        )
        ev2 = store.poll("system/mcp")["events"][-1]
        assert secret not in ev2["payload"].get("result_summary", "")
    finally:
        store.close()


def test_fetch_monitor_state_includes_system(tmp_path: Path):
    set_validation_workspace(tmp_path)
    ensure_default_roles(tmp_path)
    store = EventStore(tmp_path)
    try:
        store.publish(
            topic="system/mcp",
            producer_id="wiretap",
            schema_version="1.0",
            payload={"tool": "agentbus_poll", "latency_ms": 3},
            skip_rbac=True,
        )
    finally:
        store.close()
    state = fetch_monitor_state(tmp_path)
    assert "system_events" in state
    assert len(state["system_events"]) >= 1
    assert "dark_agents" in state


@pytest.mark.skipif(
    __import__("importlib").util.find_spec("watchdog") is None
    or __import__("importlib").util.find_spec("psutil") is None,
    reason="obs extras not installed",
)
def test_watch_fs_event(tmp_path: Path):
    from agentbus.watch import run_watch

    set_validation_workspace(tmp_path)
    target = tmp_path / "src"
    target.mkdir()
    # short duration; create file mid-run in thread
    import threading
    import time

    def _touch() -> None:
        time.sleep(0.6)
        (target / "hello.py").write_text("print(1)\n", encoding="utf-8")

    threading.Thread(target=_touch, daemon=True).start()
    count = run_watch(
        tmp_path,
        enable_fs=True,
        enable_shell=False,
        debounce_ms=100,
        duration=2.0,
    )
    assert count >= 1
    store = EventStore(tmp_path)
    try:
        events = store.poll("system/fs")["events"]
        assert any(e["payload"].get("path", "").endswith("hello.py") for e in events)
    finally:
        store.close()
