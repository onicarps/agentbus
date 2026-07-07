"""Swarm RBAC tests — PDD v0.3 acceptance criteria."""

from __future__ import annotations

import pytest

from agentbus.rbac import ForbiddenError, ensure_default_roles, mint_droid_proof
from agentbus.schemas import validate_payload
from agentbus.store import EventStore


@pytest.fixture
def rbac_workspace(tmp_path):
    ensure_default_roles(tmp_path)
    return tmp_path


@pytest.fixture
def store(rbac_workspace):
    s = EventStore(rbac_workspace)
    yield s
    s.close()


def _handoff(summary: str, **kwargs) -> dict:
    return validate_payload(
        "okf/handoff",
        {"from": "grok", "to": "all", "summary": summary, **kwargs},
    )


def test_engineer_pass_payload_blocked(store):
    """PDD AC1: engineer cannot publish QA validation payloads."""
    with pytest.raises(ForbiddenError, match="403 Forbidden.*PASS"):
        store.publish(
            topic="okf/handoff",
            producer_id="grok",
            schema_version="1.0",
            payload=_handoff("DevEx validation PASS — 8/8 tests green"),
        )


def test_qa_droid_without_proof_blocked(store):
    """PDD AC2: qa_droid requires valid droid_proof."""
    with pytest.raises(ForbiddenError, match="droid_proof"):
        store.publish(
            topic="okf/handoff",
            producer_id="hermes",
            schema_version="1.0",
            payload=validate_payload(
                "okf/handoff",
                {"from": "hermes", "to": "all", "summary": "QA complete"},
            ),
        )


def test_valid_qa_publish_with_droid_proof(store, rbac_workspace):
    """PDD AC3: qa_droid with minted proof publishes and polls."""
    minted = mint_droid_proof(rbac_workspace, mission_id="mission-abc")
    proof = minted["droid_proof"]

    event, dup = store.publish(
        topic="okf/handoff",
        producer_id="hermes",
        schema_version="1.0",
        payload=validate_payload(
            "okf/handoff",
            {
                "from": "hermes",
                "to": "all",
                "summary": "RBAC QA validation complete",
                "droid_proof": proof,
            },
        ),
    )
    assert not dup
    assert event.event_id == 1

    result = store.poll("okf/handoff", since_id=0)
    assert len(result["events"]) == 1
    assert result["events"][0]["payload"]["droid_proof"] == proof


def test_engineer_normal_handoff_allowed(store):
    payload = _handoff("Phase 2 RBAC implementation complete")
    event, dup = store.publish(
        topic="okf/handoff",
        producer_id="grok",
        schema_version="1.0",
        payload=payload,
    )
    assert not dup
    assert event.event_id >= 1


def test_architect_can_approve_pending(store, rbac_workspace):
    from agentbus.intercepts import InterceptRule, add_rule

    add_rule(rbac_workspace, InterceptRule(topic="okf/handoff", contains="PyPI"))
    pending, _ = store.publish(
        topic="okf/handoff",
        producer_id="grok",
        schema_version="1.0",
        payload=_handoff("PyPI v0.3.2 release candidate"),
    )
    result = store.approve_event(pending.event_id, reviewer_id="agy")
    assert result["status"] == "PUBLISHED"


def test_engineer_cannot_approve(store, rbac_workspace):
    from agentbus.intercepts import InterceptRule, add_rule

    add_rule(rbac_workspace, InterceptRule(topic="okf/handoff", contains="PyPI"))
    pending, _ = store.publish(
        topic="okf/handoff",
        producer_id="grok",
        schema_version="1.0",
        payload=_handoff("PyPI deploy request"),
    )
    with pytest.raises(ForbiddenError, match="cannot approve"):
        store.approve_event(pending.event_id, reviewer_id="grok")


def test_droid_proof_single_use(store, rbac_workspace):
    minted = mint_droid_proof(rbac_workspace)
    proof = minted["droid_proof"]
    payload = validate_payload(
        "okf/handoff",
        {
            "from": "hermes",
            "to": "all",
            "summary": "first publish",
            "droid_proof": proof,
        },
    )
    store.publish(
        topic="okf/handoff",
        producer_id="hermes",
        schema_version="1.0",
        payload=payload,
    )
    with pytest.raises(ForbiddenError, match="droid_proof"):
        store.publish(
            topic="okf/handoff",
            producer_id="hermes",
            schema_version="1.0",
            payload=validate_payload(
                "okf/handoff",
                {
                    "from": "hermes",
                    "to": "all",
                    "summary": "reuse proof",
                    "droid_proof": proof,
                },
            ),
        )


def test_rbac_disabled_env(tmp_path, monkeypatch):
    ensure_default_roles(tmp_path)
    monkeypatch.setenv("AGENTBUS_DISABLE_RBAC", "1")
    s = EventStore(tmp_path)
    try:
        event, _ = s.publish(
            topic="okf/handoff",
            producer_id="grok",
            schema_version="1.0",
            payload=_handoff("Self-reported PASS without proof"),
        )
        assert event.event_id == 1
    finally:
        s.close()