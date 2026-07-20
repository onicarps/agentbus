"""Unit tests for pure edge policy (no bus / no bash)."""

from __future__ import annotations

from agentbus_ops.policy import build_idempotency_key, decide, notes_fingerprint
from agentbus_ops.probe import HealthSnapshot
from agentbus_ops.state import WatchdogState


def _health(level: str = "healthy", notes: list[str] | None = None) -> HealthSnapshot:
    return HealthSnapshot(
        level=level,
        workspace="/tmp/ws",
        checked_at="2026-07-20T12:00:00Z",
        notes=notes or [],
        exit_code={"healthy": 0, "degraded": 1, "critical": 2}[level],
    )


def test_bootstrap_seed_no_publish():
    d = decide(_health("healthy"), None, state_file="/tmp/s.json")
    assert d.action == "bootstrap_seed"
    assert d.should_publish is False
    assert d.state.last_published_level == "healthy"
    assert d.state.last_published_epoch == 0
    assert d.payload is None


def test_force_bootstrap_publish():
    d = decide(
        _health("degraded", ["missing:watch"]),
        None,
        state_file="/tmp/s.json",
        force_bootstrap_publish=True,
    )
    assert d.action == "bootstrap_publish"
    assert d.should_publish is True
    assert d.payload is not None
    assert d.payload["from"] == "aider"
    assert "SRE_STATUS: degraded" in d.summary
    assert d.idempotency_key is not None
    assert d.idempotency_key.startswith("sre-status-degraded-")


def test_silence_unchanged_level():
    prev = WatchdogState(level="healthy", last_published_level="healthy", last_published_epoch=1)
    d = decide(_health("healthy"), prev, state_file="/tmp/s.json")
    assert d.action == "silence"
    assert d.should_publish is False
    assert d.prev_level == "healthy"


def test_publish_on_transition():
    prev = WatchdogState(
        level="healthy",
        last_published_level="healthy",
        last_published_epoch=100,
    )
    d = decide(
        _health("critical", ["status_failed"]),
        prev,
        state_file="/tmp/s.json",
        now_epoch=1000,
        now_iso="2026-07-20T12:00:00Z",
    )
    assert d.action == "publish_transition"
    assert d.should_publish is True
    assert d.reason == "level healthy -> critical"
    assert d.state.last_published_level == "critical"
    assert d.state.last_published_epoch == 1000
    assert d.payload["to"] == "all"
    assert d.payload["initiative"] == "agentbus"
    assert "links" in d.payload


def test_cooldown_suppresses_same_level_republish():
    """Cooldown only when re-publishing same level after a real edge flap path."""
    # prev_level != level triggers edge; last_pub_level == new level within cooldown
    prev = WatchdogState(
        level="degraded",
        last_published_level="critical",
        last_published_epoch=950,
    )
    d = decide(
        _health("critical"),
        prev,
        state_file="/tmp/s.json",
        cooldown_seconds=60,
        now_epoch=980,  # 30s after last publish of critical
        now_iso="2026-07-20T12:00:00Z",
    )
    assert d.action == "cooldown_suppress"
    assert d.should_publish is False


def test_cooldown_allows_after_window():
    prev = WatchdogState(
        level="degraded",
        last_published_level="critical",
        last_published_epoch=900,
    )
    d = decide(
        _health("critical"),
        prev,
        state_file="/tmp/s.json",
        cooldown_seconds=60,
        now_epoch=1000,  # 100s later
        now_iso="2026-07-20T12:00:00Z",
    )
    assert d.action == "publish_transition"
    assert d.should_publish is True


def test_seed_level_forces_prev():
    d = decide(
        _health("healthy"),
        None,
        state_file="/tmp/s.json",
        seed_level="degraded",
        now_epoch=1000,
        now_iso="2026-07-20T12:00:00Z",
    )
    assert d.action == "publish_transition"
    assert d.prev_level == "degraded"
    assert d.should_publish is True


def test_notes_fingerprint_stable():
    assert notes_fingerprint("healthy", ["a", "b"]) == notes_fingerprint("healthy", ["a", "b"])
    assert notes_fingerprint("healthy", ["a"]) != notes_fingerprint("healthy", ["b"])


def test_idempotency_key_hour_bucket():
    key = build_idempotency_key("healthy", "abc123def456", "2026-07-20T15:42:00Z")
    assert key == "sre-status-healthy-2026072015-abc123def456"


def test_summary_skips_counted():
    d = decide(
        _health("healthy", ["skip_disabled:x", "skip_ingress_disabled:y", "real_note"]),
        WatchdogState(level="healthy"),
        state_file="/tmp/s.json",
        metrics_snippet="events=1",
    )
    assert "skipped=2" in d.summary
    assert "notes=real_note" in d.summary
    assert "metrics[events=1]" in d.summary


def test_invalid_level_becomes_critical():
    h = HealthSnapshot(level="weird", workspace="/tmp", checked_at="t")
    # from_dict / __post_init normalizes; construct raw then decide
    h.level = "weird"
    d = decide(h, WatchdogState(level="healthy"), state_file="/tmp/s.json", now_epoch=1, now_iso="2026-07-20T00:00:00Z")
    assert d.level == "critical"
    assert d.should_publish is True
