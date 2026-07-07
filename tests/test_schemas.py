"""Payload normalization and schema tests."""

from __future__ import annotations

import pytest

from agentbus.schemas import normalize_handoff_payload, validate_payload


def test_normalize_content_to_summary():
    out = normalize_handoff_payload(
        {"from": "hermes", "to": "grok", "content": "hello"}
    )
    assert out["summary"] == "hello"
    assert "content" not in out


def test_normalize_comma_to_to_all():
    out = normalize_handoff_payload(
        {"from": "hermes", "to": "hermes,grok,agy", "summary": "hi"}
    )
    assert out["to"] == "all"


def test_normalize_strips_extra_fields():
    out = normalize_handoff_payload(
        {
            "from": "hermes",
            "to": "grok",
            "summary": "done",
            "mission_id": "m1",
            "idempotency_key": "should-not-be-here",
        }
    )
    assert "mission_id" not in out
    assert "idempotency_key" not in out


def test_factory_droid_agent_id():
    payload = validate_payload(
        "okf/handoff",
        {"from": "factory_droid", "to": "grok", "summary": "test"},
    )
    assert payload["from"] == "factory_droid"


def test_hermes_bad_payload_still_fails_without_summary():
    with pytest.raises(ValueError, match="invalid_payload"):
        validate_payload(
            "okf/handoff",
            {"from": "hermes", "to": "grok", "mission_id": "only"},
        )