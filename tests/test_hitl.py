"""HITL intercept tests — PDD v0.3 acceptance criteria."""

from __future__ import annotations

import json

import pytest
from click.testing import CliRunner

from agentbus.cli import main
from agentbus.intercepts import InterceptRule, add_rule
from agentbus.schemas import validate_payload
from agentbus.store import STATUS_PENDING, STATUS_PUBLISHED, STATUS_REJECTED, EventStore


@pytest.fixture
def store(tmp_path):
    s = EventStore(tmp_path)
    yield s
    s.close()


def _handoff(summary: str, **kwargs) -> dict:
    return validate_payload(
        "okf/handoff",
        {"from": "grok", "to": "all", "summary": summary, **kwargs},
    )


def test_intercept_hides_from_poll_until_approved(store, tmp_path):
    add_rule(tmp_path, InterceptRule(topic="okf/handoff", contains="PyPI"))

    event, _ = store.publish(
        topic="okf/handoff",
        producer_id="grok",
        schema_version="1.0",
        payload=_handoff("PyPI v0.2.4 release ready"),
    )
    assert event.status == STATUS_PENDING

    poll = store.poll("okf/handoff", since_id=0)
    assert poll["events"] == []

    review = store.review_pending()
    assert len(review) == 1
    assert review[0]["event_id"] == event.event_id

    store.approve_event(event.event_id, reviewer_id="human")
    poll2 = store.poll("okf/handoff", since_id=0)
    assert len(poll2["events"]) == 1
    assert "PyPI" in poll2["events"][0]["payload"]["summary"]


def test_reject_notifies_originator(store, tmp_path):
    add_rule(tmp_path, InterceptRule(topic="okf/handoff", contains="PyPI"))

    event, _ = store.publish(
        topic="okf/handoff",
        producer_id="grok",
        schema_version="1.0",
        payload=_handoff("Push to PyPI now"),
    )
    result = store.reject_event(event.event_id, reviewer_id="agy", reason="wait for QA")
    assert result["status"] == STATUS_REJECTED
    assert result["rejection_notice_event_id"] > event.event_id

    notices = store.poll("okf/handoff", since_id=event.event_id)
    assert len(notices["events"]) == 1
    notice = notices["events"][0]["payload"]
    assert notice["to"] == "grok"
    assert "REJECTED" in notice["summary"]
    assert "wait for QA" in notice["summary"]


def test_auto_reject_on_ttl_expiry(store, tmp_path):
    event, _ = store.publish(
        topic="okf/handoff",
        producer_id="grok",
        schema_version="1.0",
        payload=_handoff("blocked action"),
        status=STATUS_PENDING,
        pending_until="2020-01-01T00:00:00Z",
        skip_intercept=True,
    )
    rejected_ids = store.expire_pending()
    assert event.event_id in rejected_ids

    updated = store.get_event(event.event_id)
    assert updated is not None
    assert updated.status == STATUS_REJECTED


def test_cli_config_review_approve(tmp_path):
    ws = str(tmp_path)
    runner = CliRunner()
    runner.invoke(main, ["token", "ensure", "--workspace", ws, "--quiet"])

    cfg = runner.invoke(
        main,
        [
            "config",
            "set-intercept",
            "--workspace",
            ws,
            "--topic",
            "okf/handoff",
            "--contains",
            "PyPI",
        ],
    )
    assert cfg.exit_code == 0, cfg.output

    pub = runner.invoke(
        main,
        [
            "publish",
            "--workspace",
            ws,
            "--topic",
            "okf/handoff",
            "--payload",
            json.dumps(_handoff("PyPI publish attempt")),
            "--producer-id",
            "grok",
        ],
    )
    assert pub.exit_code == 0, pub.output
    event_id = json.loads(pub.output)["event_id"]

    poll = runner.invoke(
        main,
        ["poll", "--workspace", ws, "--topic", "okf/handoff", "--since-id", "0"],
    )
    assert json.loads(poll.output)["events"] == []

    rev = runner.invoke(main, ["review", "--workspace", ws])
    assert rev.exit_code == 0
    pending = json.loads(rev.output)
    assert any(e["event_id"] == event_id for e in pending)

    appr = runner.invoke(main, ["approve", "--workspace", ws, str(event_id)])
    assert appr.exit_code == 0, appr.output

    poll2 = runner.invoke(
        main,
        ["poll", "--workspace", ws, "--topic", "okf/handoff", "--since-id", "0"],
    )
    assert len(json.loads(poll2.output)["events"]) == 1


def test_disable_hitl_env_bypasses_intercept(store, tmp_path, monkeypatch):
    add_rule(tmp_path, InterceptRule(topic="okf/handoff", contains="PyPI"))
    monkeypatch.setenv("AGENTBUS_DISABLE_HITL", "1")

    event, _ = store.publish(
        topic="okf/handoff",
        producer_id="grok",
        schema_version="1.0",
        payload=_handoff("PyPI emergency release"),
    )
    assert event.status == STATUS_PUBLISHED
    assert len(store.poll("okf/handoff", since_id=0)["events"]) == 1

    st = store.status()
    assert st["hitl_enabled"] is False


def test_non_matching_events_publish_immediately(store, tmp_path):
    add_rule(tmp_path, InterceptRule(topic="okf/handoff", contains="PyPI"))
    event, _ = store.publish(
        topic="okf/handoff",
        producer_id="grok",
        schema_version="1.0",
        payload=_handoff("routine status update"),
    )
    assert event.status == STATUS_PUBLISHED
    assert len(store.poll("okf/handoff", since_id=0)["events"]) == 1