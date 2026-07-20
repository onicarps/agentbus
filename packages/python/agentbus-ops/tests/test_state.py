"""State load/save tests."""

from __future__ import annotations

from pathlib import Path

from agentbus_ops.state import WatchdogState, load_state, save_state


def test_roundtrip(tmp_path: Path):
    path = tmp_path / "sre_last_state.json"
    st = WatchdogState(
        level="degraded",
        sre_status="degraded",
        exit_code=1,
        last_checked_at="2026-07-20T01:00:00Z",
        last_checked_epoch=100,
        notes=["missing:watch"],
        notes_fingerprint="deadbeef",
        workspace="/ws",
        latest_event_id=42,
        disabled_services=["hermes-wake-ingress"],
        last_action="publish_transition",
        last_action_reason="level healthy -> degraded",
        last_published_level="degraded",
        last_published_at="2026-07-20T01:00:00Z",
        last_published_epoch=100,
        last_idempotency_key="sre-status-degraded-2026072001-deadbeef",
        bootstrap=False,
    )
    save_state(path, st)
    loaded = load_state(path)
    assert loaded is not None
    assert loaded.level == "degraded"
    assert loaded.exit_code == 1
    assert loaded.notes == ["missing:watch"]
    assert loaded.latest_event_id == 42
    assert loaded.last_idempotency_key == st.last_idempotency_key
    assert loaded.disabled_services == ["hermes-wake-ingress"]


def test_missing_returns_none(tmp_path: Path):
    assert load_state(tmp_path / "nope.json") is None


def test_corrupt_returns_none(tmp_path: Path):
    p = tmp_path / "bad.json"
    p.write_text("{not json", encoding="utf-8")
    assert load_state(p) is None


def test_from_dict_tolerates_string_ids():
    st = WatchdogState.from_dict(
        {
            "level": "healthy",
            "latest_event_id": "99",
            "last_published_epoch": "12",
            "notes": "single",
        }
    )
    assert st.latest_event_id == 99
    assert st.last_published_epoch == 12
    assert st.notes == ["single"]
