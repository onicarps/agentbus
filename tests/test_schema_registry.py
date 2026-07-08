"""Pluggable schema registry tests — PDD v0.7 acceptance criteria."""

from __future__ import annotations

import json

import pytest

from agentbus.schema_registry import import_schema_file, list_schemas, register_schema
from agentbus.schemas import set_validation_workspace, validate_payload
from agentbus.store import EventStore

CI_SCHEMA = {
    "type": "object",
    "required": ["build_id", "status"],
    "additionalProperties": False,
    "properties": {
        "build_id": {"type": "string"},
        "status": {"type": "string"},
        "failure_reason": {"type": ["string", "null"]},
    },
}


@pytest.fixture
def store(tmp_path):
    set_validation_workspace(tmp_path)
    s = EventStore(tmp_path)
    yield s
    s.close()


def test_okf_approval_builtin_topic():
    validate_payload(
        "okf/approval",
        {"event_id": 42, "approver": "grok", "decision": "approve"},
    )
    with pytest.raises(ValueError, match="invalid_payload"):
        validate_payload("okf/approval", {"event_id": 1})


def test_strict_validation_rejects_wrong_type(store, tmp_path):
    """PDD AC1: registered schema rejects invalid payload types."""
    register_schema(tmp_path, "ci/build-alert", CI_SCHEMA)
    with pytest.raises(ValueError, match="invalid_payload"):
        validate_payload(
            "ci/build-alert",
            {"build_id": "123", "status": 123},
            workspace=tmp_path,
        )


def test_valid_custom_topic_publish(store, tmp_path):
    register_schema(tmp_path, "ci/build-alert", CI_SCHEMA)
    event, _ = store.publish(
        topic="ci/build-alert",
        producer_id="grok",
        schema_version="1.0",
        payload={"build_id": "b1", "status": "FAILED", "failure_reason": "syntax"},
    )
    assert event.event_id == 1
    polled = store.poll("ci/build-alert", since_id=0)
    assert len(polled["events"]) == 1


def test_cli_import_registers_schema(tmp_path):
    """PDD AC3: schema import populates topic_schemas table."""
    mock = tmp_path / "mock.json"
    mock.write_text(
        json.dumps(
            {
                "topic": "security/vuln-scan",
                "json_schema": {
                    "type": "object",
                    "required": ["severity"],
                    "properties": {"severity": {"type": "string"}},
                },
            }
        ),
        encoding="utf-8",
    )
    result = import_schema_file(tmp_path, mock)
    assert result["topic_name"] == "security/vuln-scan"
    assert len(list_schemas(tmp_path)) == 1


def test_sdk_topic_registration(tmp_path):
    """PDD AC2: @bus.topic decorator registers schema in SQLite."""
    pytest.importorskip("pydantic")
    from pydantic import BaseModel

    from agentbus.sdk import AgentBus

    bus = AgentBus(tmp_path)

    @bus.topic("ci/build-alert")
    class CIBuildAlert(BaseModel):
        build_id: str
        status: str
        failure_reason: str | None = None

    rows = list_schemas(tmp_path)
    assert any(r["topic_name"] == "ci/build-alert" for r in rows)
    assert CIBuildAlert.__agentbus_topic__ == "ci/build-alert"