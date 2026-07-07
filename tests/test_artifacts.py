"""Distributed context artifact tests — PDD v0.6 acceptance criteria."""

from __future__ import annotations

import pytest

from agentbus.artifacts import MAX_ARTIFACT_BYTES, PayloadTooLargeError, validate_artifact
from agentbus.schemas import validate_payload
from agentbus.store import EventStore


@pytest.fixture
def store(tmp_path):
    s = EventStore(tmp_path)
    yield s
    s.close()


def _handoff(summary: str, **kwargs) -> dict:
    return validate_payload(
        "okf/handoff",
        {"from": "grok", "to": "hermes", "summary": summary, **kwargs},
    )


def test_publish_and_poll_artifact(store):
    """PDD AC1+AC2: 10kb artifact attaches and returns on poll."""
    content = "x" * 10_240
    payload = _handoff(
        "Please review",
        artifacts=[
            {
                "type": "file_content",
                "name": "review.txt",
                "content": content,
            }
        ],
    )
    event, _ = store.publish(
        topic="okf/handoff",
        producer_id="grok",
        schema_version="1.0",
        payload=payload,
    )
    raw = store._conn.execute(
        "SELECT payload FROM events WHERE event_id = ?", (event.event_id,)
    ).fetchone()[0]
    assert "review.txt" not in raw

    polled = store.poll("okf/handoff", since_id=0)["events"][0]
    assert len(polled["payload"]["artifacts"]) == 1
    assert polled["payload"]["artifacts"][0]["content"] == content
    assert polled["payload"]["artifacts"][0]["name"] == "review.txt"


def test_oversized_artifact_rejected():
    """PDD AC3: 2MB artifact triggers Payload Too Large."""
    big = "a" * (2 * 1024 * 1024)
    with pytest.raises(PayloadTooLargeError, match="413 Payload Too Large"):
        validate_artifact(
            {"type": "file_content", "name": "huge.txt", "content": big}
        )


def test_artifact_at_limit_ok():
    content = "b" * MAX_ARTIFACT_BYTES
    art = validate_artifact(
        {"type": "git_diff", "name": "limit.patch", "content": content}
    )
    assert len(art["content"].encode()) == MAX_ARTIFACT_BYTES


def test_artifacts_not_in_events_table_blob(store, tmp_path):
    payload = _handoff(
        "with artifact",
        artifacts=[
            {"type": "error_trace", "name": "err.log", "content": "trace line"}
        ],
    )
    store.publish(
        topic="okf/handoff",
        producer_id="grok",
        schema_version="1.0",
        payload=payload,
    )
    raw = store._conn.execute("SELECT payload FROM events WHERE event_id = 1").fetchone()[
        0
    ]
    assert "trace line" not in raw
    count = store._conn.execute("SELECT COUNT(*) FROM artifacts").fetchone()[0]
    assert count == 1