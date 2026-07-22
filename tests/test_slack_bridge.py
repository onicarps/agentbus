"""Unit / dogfood smoke tests for scripts/slack_bridge.py (no Slack tokens)."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "slack_bridge.py"


def _load_bridge():
    spec = importlib.util.spec_from_file_location("slack_bridge_under_test", SCRIPT)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    # Avoid polluting sys.modules permanently across test runs if reloaded
    sys.modules["slack_bridge_under_test"] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def sb():
    return _load_bridge()


# ---------------------------------------------------------------------------
# Ops-noise suppress (Hermes parity)
# ---------------------------------------------------------------------------


def test_ops_noise_prefixes_match_hermes(sb):
    assert sb.is_ops_noise_summary("RUNNER_ACK: grok completed event_id=1") is True
    assert sb.is_ops_noise_summary("RUNNER_ERROR: exit=1") is True
    assert sb.is_ops_noise_summary("RUNNER_SUSPEND: wait_id=w") is True
    assert sb.is_ops_noise_summary("NO-OP/TERMINAL_IDLE") is True
    assert sb.is_ops_noise_summary("TERMINAL_IDLE") is True
    assert sb.is_ops_noise_summary("CHAIN_BREAK: done") is True
    assert sb.is_ops_noise_summary("SUPPRESS ACK: skipped") is True
    assert sb.is_ops_noise_summary("runner_ack: peer") is True  # case-insensitive
    assert sb.is_ops_noise_summary("Ship closed. CHAIN_BREAK") is False  # not prefix
    assert sb.is_ops_noise_summary("SLACK_SMOKE: please reply") is False
    assert sb.is_ops_noise_summary("") is False
    assert sb.is_ops_noise_summary(None) is False


# ---------------------------------------------------------------------------
# Target routing — never default to swarm
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "text,expected_to,contains",
    [
        ("hello swarm", "agy", "hello swarm"),
        ("@grok ship it", "grok", "ship it"),
        ("/ask factory run QA", "factory", "run QA"),
        ("/hermes status?", "hermes", "status?"),
        ("aider: restart health check", "aider", "restart health check"),
        ("@agy triage this", "agy", "triage this"),
        ("@unknown do stuff", "agy", "@unknown do stuff"),
        ("@swarm broadcast please", "swarm", "broadcast please"),
    ],
)
def test_parse_target_agent(sb, text, expected_to, contains):
    to, cleaned = sb.parse_target_agent(text)
    assert to == expected_to
    assert contains in cleaned


def test_default_to_never_swarm_for_plain_chat(sb):
    to, _ = sb.parse_target_agent("what is the status?")
    assert to == "agy"
    assert to != "swarm"


# ---------------------------------------------------------------------------
# Inbound payload schema shape
# ---------------------------------------------------------------------------


def test_build_inbound_payload_schema_shape(sb):
    payload = sb.build_inbound_payload(
        text="@grok fix the bridge",
        channel="C123",
        ts="1710000000.000100",
        user="U99",
    )
    assert set(payload.keys()) <= {
        "from",
        "to",
        "summary",
        "links",
        "initiative",
    }
    assert payload["from"] == "slack"
    assert payload["to"] == "grok"
    assert payload["links"] == ["slack://C123/1710000000.000100"]
    assert "slack_channel" not in payload
    assert "[slack:U99]" in payload["summary"]
    assert "fix the bridge" in payload["summary"]
    assert (
        sb.inbound_idempotency_key("C123", "1710000000.000100")
        == "slack:C123:1710000000.000100"
    )


def test_inbound_payload_passes_jsonschema(sb):
    from agentbus.schemas import validate_payload

    payload = sb.build_inbound_payload(
        text="plain question for triage",
        channel="D456",
        ts="99.1",
        user="oni",
    )
    # Must not raise
    validate_payload("okf/handoff", payload)


def test_summary_truncated_to_2000(sb):
    long = "x" * 5000
    payload = sb.build_inbound_payload(
        text=long, channel="C1", ts="1.0", user="u"
    )
    assert len(payload["summary"]) <= 2000
    assert payload["summary"].endswith("...")


# ---------------------------------------------------------------------------
# Links parsing / outbound gate
# ---------------------------------------------------------------------------


def test_channel_from_links(sb):
    assert sb.channel_from_links(["slack://C1/1.2"]) == ("C1", "1.2")
    assert sb.channel_from_links(["https://example.com", "slack://D9/3.4"]) == (
        "D9",
        "3.4",
    )
    assert sb.channel_from_links([]) is None
    assert sb.channel_from_links(None) is None
    assert sb.parse_slack_uri("not-a-uri") is None


def test_should_post_outbound_only_to_slack(sb):
    assert sb.should_post_outbound({"to": "slack", "summary": "hello human"}) is True
    assert sb.should_post_outbound({"to": "human", "summary": "hello"}) is False
    assert sb.should_post_outbound({"to": "hermes", "summary": "hello"}) is False
    assert (
        sb.should_post_outbound(
            {"to": "slack", "summary": "RUNNER_ACK: done event_id=1"}
        )
        is False
    )
    assert (
        sb.should_post_outbound({"to": "slack", "summary": "CHAIN_BREAK: idle"})
        is False
    )


def test_format_outbound_message(sb):
    msg = sb.format_outbound_message(
        {"from": "grok", "summary": "Bridge rewrite complete."}
    )
    assert msg.startswith("*grok*")
    assert "Bridge rewrite complete." in msg


# ---------------------------------------------------------------------------
# Slack event accept filter
# ---------------------------------------------------------------------------


def test_should_accept_slack_message(sb):
    good = {
        "text": "hello",
        "user": "U1",
        "channel": "C1",
        "ts": "1.0",
    }
    assert sb.should_accept_slack_message(good) is True
    assert sb.should_accept_slack_message({**good, "bot_id": "B1"}) is False
    assert (
        sb.should_accept_slack_message({**good, "subtype": "message_changed"})
        is False
    )
    assert sb.should_accept_slack_message({**good, "text": "  "}) is False
    assert sb.should_accept_slack_message({**good, "channel": None}) is False


# ---------------------------------------------------------------------------
# Cursor persistence (no history spam)
# ---------------------------------------------------------------------------


def test_cursor_roundtrip(sb, tmp_path: Path):
    # Fake workspace with .agentbus
    ws = tmp_path / "okf"
    (ws / ".agentbus").mkdir(parents=True)
    assert sb.load_cursor(ws) is None
    sb.save_cursor(ws, 1787)
    assert sb.load_cursor(ws) == 1787
    assert (ws / ".agentbus" / "slack_bridge.cursor").read_text().strip() == "1787"


def test_resolve_start_cursor_uses_existing(sb, tmp_path: Path, monkeypatch):
    ws = tmp_path / "okf"
    (ws / ".agentbus").mkdir(parents=True)
    sb.save_cursor(ws, 100)

    def boom(_ws):
        raise AssertionError("should not seek when cursor exists")

    monkeypatch.setattr(sb, "seek_bus_head", boom)
    assert sb.resolve_start_cursor(ws) == 100


def test_resolve_start_cursor_seeks_head_when_missing(
    sb, tmp_path: Path, monkeypatch
):
    ws = tmp_path / "okf"
    (ws / ".agentbus").mkdir(parents=True)
    monkeypatch.setattr(sb, "seek_bus_head", lambda _ws: 9999)
    assert sb.resolve_start_cursor(ws) == 9999
    assert sb.load_cursor(ws) == 9999


def test_seek_bus_head_parses_status(sb, tmp_path: Path, monkeypatch):
    ws = tmp_path / "okf"
    (ws / ".agentbus").mkdir(parents=True)

    class Fake:
        returncode = 0
        stdout = json.dumps({"latest_event_id": 42, "event_count": 40})
        stderr = ""

    monkeypatch.setattr(sb, "run_agentbus", lambda *a, **k: Fake())
    assert sb.seek_bus_head(ws) == 42


# ---------------------------------------------------------------------------
# Dogfood: inbound → schema + outbound loop pure path
# ---------------------------------------------------------------------------


def test_inbound_outbound_loop_smoke(sb):
    """Simulate Slack DM → bus payload → agent reply → Slack post decision."""
    inbound = sb.build_inbound_payload(
        text="@grok status of slack bridge?",
        channel="D999",
        ts="200.5",
        user="oni",
    )
    assert inbound["to"] == "grok"
    assert inbound["from"] == "slack"
    link = inbound["links"][0]

    # Agent substance reply routes only to slack with same link for threading
    reply = {
        "from": "grok",
        "to": "slack",
        "summary": "Rewrite complete; stay 0.16.3. CHAIN_BREAK",
        "links": [link],
    }
    # Note: summary starting with substance is fine; trailing CHAIN_BREAK is OK
    # for suppress only when *prefix* is ops noise.
    assert sb.should_post_outbound(reply) is True
    ch, ts = sb.channel_from_links(reply["links"])
    assert ch == "D999"
    assert ts == "200.5"

    # Companion ACK must not post
    ack = {
        "from": "grok",
        "to": "slack",
        "summary": "RUNNER_ACK: grok completed event_id=1",
        "links": [link],
    }
    assert sb.should_post_outbound(ack) is False
