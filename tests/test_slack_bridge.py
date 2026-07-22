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
    assert "Reply directly to 'slack' on the bus" in payload["summary"]
    assert (
        sb.inbound_idempotency_key("C123", "1710000000.000100")
        == "slack:C123:1710000000.000100"
    )


def test_inbound_payload_injects_a2a_routing_directive(sb):
    """Slack primary-comms: agents must see explicit to=slack reply context."""
    payload = sb.build_inbound_payload(
        text="plain question for triage",
        channel="D456",
        ts="99.1",
        user="oni",
    )
    assert payload["summary"].startswith("[slack:oni]")
    assert "Reply directly to 'slack' on the bus" in payload["summary"]
    assert payload["links"] == ["slack://D456/99.1"]


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
    # User body may ellipsize; SYSTEM directive is reserved and must remain.
    assert "Reply directly to 'slack' on the bus" in payload["summary"]


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


def test_runner_ack_out_allow_and_extract(sb):
    """RUNNER_ACK with out= is the Slack safety-net exception to ops suppress."""
    ack = {
        "to": "slack",
        "from": "grok",
        "summary": (
            "RUNNER_ACK: grok completed event_id=1877 out="
            "Fix pack closed.\nSecond line."
        ),
    }
    assert sb.should_post_outbound(ack) is True
    msg = sb.format_outbound_message(ack)
    assert msg.startswith("*grok*")
    assert "Fix pack closed." in msg
    assert "Second line." in msg
    assert "RUNNER_ACK" not in msg
    # Pure ACK without out= still suppressed
    pure = {
        "to": "slack",
        "summary": "RUNNER_ACK: grok completed event_id=1",
    }
    assert sb.should_post_outbound(pure) is False


def test_strip_bot_mention_before_target_parse(sb):
    """app_mention text is often '<@UBOT> @grok fix it' — must still route."""
    to, cleaned = sb.parse_target_agent("<@U0BOTID> @grok fix the bridge")
    assert to == "grok"
    assert cleaned == "fix the bridge"
    to2, cleaned2 = sb.parse_target_agent("<@U0BOTID> <@U0BOTID> /ask factory run QA")
    assert to2 == "factory"
    assert "run QA" in cleaned2
    # Plain text without bot token unchanged
    to3, cleaned3 = sb.parse_target_agent("@agy triage")
    assert to3 == "agy"
    assert cleaned3 == "triage"


def test_correlation_ts_prefers_thread_root(sb):
    assert sb.correlation_ts({"ts": "2.0", "thread_ts": "1.0"}) == "1.0"
    assert sb.correlation_ts({"ts": "2.0"}) == "2.0"
    assert sb.correlation_ts({}) == ""


def test_inbound_uses_thread_ts_in_links(sb):
    """When building from a thread reply, links should use thread root."""
    payload = sb.build_inbound_payload(
        text="@grok continue",
        channel="C9",
        ts="100.0",  # caller passes thread_ts or ts
        user="U1",
    )
    assert payload["links"] == ["slack://C9/100.0"]


def test_system_directive_survives_long_user_text(sb):
    long = "x" * 5000
    payload = sb.build_inbound_payload(
        text=long, channel="C1", ts="1.0", user="u"
    )
    assert len(payload["summary"]) <= 2000
    assert "Reply directly to 'slack' on the bus" in payload["summary"]
    assert 'slack://C1/1.0' in payload["summary"]


def test_resolve_slack_channel_ts_direct_links(sb):
    assert sb.resolve_slack_channel_ts(
        {"links": ["slack://C1/9.9"], "to": "slack"}
    ) == ("C1", "9.9")


def test_resolve_slack_channel_ts_causation_backfill(sb):
    """When links absent, walk causation chain to nearest slack:// ancestor."""
    chain = {
        10: {
            "event_id": 10,
            "causation_id": None,
            "payload": {
                "from": "slack",
                "to": "grok",
                "summary": "hi",
                "links": ["slack://C99/1.234"],
            },
        },
        11: {
            "event_id": 11,
            "causation_id": 10,
            "payload": {"from": "agy", "to": "grok", "summary": "forward"},
        },
        12: {
            "event_id": 12,
            "causation_id": 11,
            "payload": {
                "from": "grok",
                "to": "slack",
                "summary": "RUNNER_ACK: done out=ok",
            },
        },
    }

    def fetch(eid: int):
        return chain.get(eid)

    # Companion ACK: causation_id = wake event (11) → parent 10 has slack://
    assert sb.resolve_slack_channel_ts(
        {"to": "slack", "summary": "done", "links": []},
        causation_id=11,
        fetch_event=fetch,
    ) == ("C99", "1.234")

    # Deeper walk: start at 12 → 11 → 10
    assert sb.resolve_slack_channel_ts(
        {"to": "slack", "summary": "x", "links": []},
        causation_id=12,
        fetch_event=fetch,
    ) == ("C99", "1.234")

    # No fetch → no backfill
    assert (
        sb.resolve_slack_channel_ts(
            {"to": "slack", "summary": "x", "links": []},
            causation_id=11,
            fetch_event=None,
        )
        is None
    )


def test_idempotency_key_stable_for_double_delivery(sb):
    """message + app_mention share the same message ts → same key."""
    ch, ts = "C1", "1710000000.000100"
    assert sb.inbound_idempotency_key(ch, ts) == sb.inbound_idempotency_key(ch, ts)
    assert sb.inbound_idempotency_key(ch, ts) == f"slack:{ch}:{ts}"


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
