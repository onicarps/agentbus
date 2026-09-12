from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from agentbus.cli import main
from agentbus.leases import LeaseStore
from agentbus.posthog import (
    Cursor,
    HTTPResult,
    PostHogConfig,
    PostHogExporter,
    deterministic_uuid,
    load_config,
    map_event,
    runner_generation_payload,
)
from agentbus.store import EventStore


FIXTURES = Path(__file__).parent / "fixtures" / "posthog"
WS_ID = "00000000-0000-0000-0000-000000000123"


@pytest.mark.parametrize("fixture_name", ["handoff", "qa", "ai_generation", "lock"])
def test_mapping_matches_golden_schema_and_drops_content(fixture_name: str) -> None:
    fixture = json.loads((FIXTURES / f"{fixture_name}.json").read_text())
    mapped = map_event(fixture["source"], WS_ID)
    assert mapped is not None
    assert mapped["event"] == fixture["event"]
    assert mapped["properties"] == fixture["properties"]
    assert mapped["uuid"] == deterministic_uuid(
        WS_ID, fixture["source"]["event_id"], fixture["event"]
    )
    encoded = json.dumps(mapped)
    for prohibited in ("never export", "private evidence", "api_key", "prompt"):
        assert prohibited not in encoded


def _publish_handoff(workspace: Path, summary: str = "sensitive content") -> int:
    store = EventStore(workspace)
    try:
        event, _ = store.publish(
            topic="okf/handoff",
            producer_id="agy",
            schema_version="1.0",
            payload={
                "from": "agy",
                "to": "codex",
                "summary": summary,
                "api_key": "phc_abcdefghijklmnopqrstuvwxyz0123456789",
            },
            skip_rbac=True,
        )
        return event.event_id
    finally:
        store.close()


def test_delivery_advances_atomic_cursor_and_never_sends_secret(tmp_path: Path) -> None:
    event_id = _publish_handoff(tmp_path)
    requests: list[dict] = []

    def send(_url: str, body: bytes, _timeout: float) -> HTTPResult:
        requests.append(json.loads(body))
        return HTTPResult(200, {})

    exporter = PostHogExporter(tmp_path, PostHogConfig("phc_test"), send=send)
    result = exporter.process_once()
    assert result == {"status": "delivered", "delivered": 1, "last_event_id": event_id}
    cursor = json.loads((tmp_path / ".agentbus/posthog_cursor.json").read_text())
    assert cursor["last_event_id"] == event_id
    encoded = json.dumps(requests)
    assert requests[0]["batch"][0]["properties"]["distinct_id"].startswith("agentbus:")
    assert "distinct_id" not in requests[0]["batch"][0]
    assert "sensitive content" not in encoded
    assert "abcdefghijklmnopqrstuvwxyz" not in encoded


def test_retry_after_429_then_success_without_loss(tmp_path: Path) -> None:
    event_id = _publish_handoff(tmp_path)
    responses = [HTTPResult(429, {"Retry-After": "5"}), HTTPResult(200, {})]
    sleeps: list[float] = []
    exporter = PostHogExporter(
        tmp_path,
        PostHogConfig("phc_test"),
        send=lambda *_: responses.pop(0),
        sleep=sleeps.append,
    )
    assert exporter.process_once()["status"] == "delivered"
    assert sleeps == [5.0]
    assert exporter.cursor.last_event_id == event_id


def test_non_retryable_batch_is_quarantined_without_payload(tmp_path: Path) -> None:
    event_id = _publish_handoff(tmp_path, "top secret summary")
    exporter = PostHogExporter(
        tmp_path,
        PostHogConfig("phc_test"),
        send=lambda *_: HTTPResult(400, {}),
    )
    result = exporter.process_once()
    assert result["status"] == "quarantined"
    assert exporter.cursor.last_event_id == event_id
    quarantine = (tmp_path / ".agentbus/posthog_quarantine.jsonl").read_text()
    assert "top secret summary" not in quarantine
    assert json.loads(quarantine)["event_ids"] == [event_id]


def test_crash_between_send_and_cursor_save_replays_same_uuid(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _publish_handoff(tmp_path)
    batches: list[dict] = []

    def send(_url: str, body: bytes, _timeout: float) -> HTTPResult:
        batches.append(json.loads(body))
        return HTTPResult(200, {})

    original_save = Cursor.save
    monkeypatch.setattr(Cursor, "save", lambda *_: (_ for _ in ()).throw(OSError("killed")))
    with pytest.raises(OSError, match="killed"):
        PostHogExporter(tmp_path, PostHogConfig("phc_test"), send=send).process_once()
    monkeypatch.setattr(Cursor, "save", original_save)
    PostHogExporter(tmp_path, PostHogConfig("phc_test"), send=send).process_once()
    assert batches[0]["batch"][0]["uuid"] == batches[1]["batch"][0]["uuid"]


def test_runner_usage_extraction_is_numeric_only() -> None:
    payload = runner_generation_payload(
        adapter="codex",
        model="gpt-x",
        trace_id="trace-x",
        latency_ms=12.5,
        is_error=False,
        detail={
            "stdout": '{"type":"turn.completed","usage":{"input_tokens":12,"output_tokens":3,"cached_input_tokens":8},"secret":"do not copy"}',
            "prompt": "private prompt",
        },
    )
    assert payload["$ai_input_tokens"] == 12
    assert payload["$ai_output_tokens"] == 3
    assert payload["$ai_cache_read_tokens"] == 8
    assert "private" not in json.dumps(payload)
    assert "secret" not in json.dumps(payload)


def test_https_configuration_is_mandatory(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTHOG_PROJECT_API_KEY", "phc_test")
    monkeypatch.setenv("POSTHOG_HOST", "http://example.com")
    with pytest.raises(ValueError, match="HTTPS"):
        load_config()
    monkeypatch.setenv("POSTHOG_HOST", "https://user:pass@example.com?token=x")
    with pytest.raises(ValueError, match="credentials/query"):
        load_config()


def test_lock_operations_emit_allowlisted_source_events(tmp_path: Path) -> None:
    resource = tmp_path / "project" / "file.py"
    resource.parent.mkdir()
    leases = LeaseStore(tmp_path)
    acquired = leases.lock_acquire(str(resource), "codex")
    leases.lock_renew(str(resource), acquired["lease_id"], "codex")
    leases.lock_release(str(resource), acquired["lease_id"], "codex")
    leases.close()

    store = EventStore(tmp_path, auto_prune=False)
    try:
        events = store.poll("system/lock", limit=10)["events"]
    finally:
        store.close()
    assert [event["payload"]["action"] for event in events] == [
        "acquire",
        "renew",
        "release",
    ]
    assert all(event["payload"]["lock_name"] == "project/file.py" for event in events)
    assert events[-1]["payload"]["renew_count"] == 1


def test_cli_status_does_not_require_api_key(tmp_path: Path) -> None:
    result = CliRunner().invoke(
        main,
        ["posthog", "status", "--workspace", str(tmp_path)],
        env={"POSTHOG_PROJECT_API_KEY": ""},
    )
    assert result.exit_code == 0, result.output
    status = json.loads(result.output)
    assert status["event_lag"] == 0
    assert status["host"] == "https://us.i.posthog.com"
