"""Swarm RBAC tests — PDD v0.3 acceptance criteria."""

from __future__ import annotations

import pytest

from agentbus.identity import (
    IdentityError,
    bootstrap_workspace_identity,
    enroll_identity,
    set_policy_mode,
)
from agentbus.rbac import (
    ForbiddenError,
    check_publish_rbac,
    ensure_default_roles,
    mint_droid_proof,
    rbac_disabled,
    resolve_role,
)
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
            producer_id="factory_droid",
            schema_version="1.0",
            payload=validate_payload(
                "okf/handoff",
                {"from": "factory_droid", "to": "all", "summary": "QA complete"},
            ),
        )


def test_valid_qa_publish_with_droid_proof(store, rbac_workspace):
    """PDD AC3: qa_droid with minted proof publishes and polls."""
    minted = mint_droid_proof(rbac_workspace, mission_id="mission-abc")
    proof = minted["droid_proof"]

    event, dup = store.publish(
        topic="okf/handoff",
        producer_id="factory_droid",
        schema_version="1.0",
        payload=validate_payload(
            "okf/handoff",
            {
                "from": "factory_droid",
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
            "from": "factory_droid",
            "to": "all",
            "summary": "first publish",
            "droid_proof": proof,
        },
    )
    store.publish(
        topic="okf/handoff",
        producer_id="factory_droid",
        schema_version="1.0",
        payload=payload,
    )
    with pytest.raises(ForbiddenError, match="droid_proof"):
        store.publish(
            topic="okf/handoff",
            producer_id="factory_droid",
            schema_version="1.0",
            payload=validate_payload(
                "okf/handoff",
                {
                    "from": "factory_droid",
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


def test_protected_mode_ignores_rbac_disable_and_token_roles(tmp_path, monkeypatch):
    config = ensure_default_roles(tmp_path)
    config.token_roles["qa-token"] = "qa"
    from agentbus.rbac import save_rbac_config

    save_rbac_config(tmp_path, config)
    bootstrap_workspace_identity(tmp_path)
    set_policy_mode(tmp_path, "protected")
    monkeypatch.setenv("AGENTBUS_DISABLE_RBAC", "1")
    assert rbac_disabled(tmp_path) is False
    assert resolve_role(
        tmp_path, producer_id="grok", auth_token="qa-token"
    ) == "engineer"
    with pytest.raises(ForbiddenError, match="blocked by pattern"):
        check_publish_rbac(
            tmp_path,
            producer_id="grok",
            topic="okf/handoff",
            payload=_handoff("Self-reported PASS"),
            auth_token="qa-token",
        )


def test_protected_qa_droid_requires_agentid_not_legacy_proof(tmp_path):
    """Legacy evidence symbol retained; Pi is the protected QA gate today."""
    ensure_default_roles(tmp_path)
    bootstrap_workspace_identity(tmp_path)
    enroll_identity(
        tmp_path,
        "pi",
        capabilities=("message", "qa_verdict"),
        topics=("okf/handoff",),
    )
    set_policy_mode(tmp_path, "protected")
    store = EventStore(tmp_path)
    payload = validate_payload(
        "okf/handoff",
        {
            "from": "pi",
            "to": "codex",
            "summary": "GREEN",
        },
    )
    with pytest.raises(IdentityError, match="identity_verification_required"):
        store.publish(
            topic="okf/handoff",
            producer_id="pi",
            schema_version="1.0",
            payload=payload,
            auto_sign=False,
        )
    event, _ = store.publish(
        topic="okf/handoff",
        producer_id="pi",
        schema_version="1.0",
        payload=payload,
        action={"type": "qa_verdict", "result": "green"},
    )
    assert event.verification_status == "verified"
    store.close()
