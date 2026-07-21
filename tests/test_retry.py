"""Unit tests for agentbus.retry (exp backoff + jitter + exhaust)."""

from __future__ import annotations

import random
import sqlite3

import pytest

from agentbus.retry import (
    RetryExhaustedError,
    RetryPolicy,
    call_with_retry,
    compute_backoff,
    default_publish_policy,
    is_transient_sqlite_error,
)


def test_compute_backoff_no_jitter_is_deterministic():
    pol = RetryPolicy(
        max_attempts=5,
        base_delay=0.1,
        max_delay=10.0,
        exponential_base=2.0,
        jitter_ratio=0.0,
    )
    assert compute_backoff(0, pol, rng=random.Random(0)) == pytest.approx(0.1)
    assert compute_backoff(1, pol, rng=random.Random(0)) == pytest.approx(0.2)
    assert compute_backoff(2, pol, rng=random.Random(0)) == pytest.approx(0.4)


def test_compute_backoff_caps_at_max_delay():
    pol = RetryPolicy(
        max_attempts=10,
        base_delay=1.0,
        max_delay=1.5,
        exponential_base=2.0,
        jitter_ratio=0.0,
    )
    assert compute_backoff(5, pol) == pytest.approx(1.5)


def test_compute_backoff_full_jitter_in_range():
    pol = RetryPolicy(
        max_attempts=5,
        base_delay=1.0,
        max_delay=10.0,
        exponential_base=2.0,
        jitter_ratio=1.0,
    )
    rng = random.Random(42)
    for _ in range(50):
        d = compute_backoff(1, pol, rng=rng)  # raw = 2.0
        assert 0.0 <= d <= 2.0


def test_is_transient_sqlite_error():
    assert is_transient_sqlite_error(sqlite3.OperationalError("database is locked"))
    assert is_transient_sqlite_error(sqlite3.OperationalError("database is busy"))
    assert not is_transient_sqlite_error(sqlite3.OperationalError("no such table"))
    assert not is_transient_sqlite_error(ValueError("nope"))


def test_call_with_retry_succeeds_after_transient_failures():
    attempts = {"n": 0}

    def flaky() -> str:
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise sqlite3.OperationalError("database is locked")
        return "ok"

    sleeps: list[float] = []
    result = call_with_retry(
        flaky,
        policy=RetryPolicy(
            max_attempts=5,
            base_delay=0.01,
            max_delay=1.0,
            jitter_ratio=0.0,
        ),
        sleep=sleeps.append,
    )
    assert result == "ok"
    assert attempts["n"] == 3
    assert len(sleeps) == 2  # slept after failures 1 and 2


def test_call_with_retry_exhausted():
    def always_locked() -> None:
        raise sqlite3.OperationalError("database is locked")

    with pytest.raises(RetryExhaustedError) as ei:
        call_with_retry(
            always_locked,
            policy=RetryPolicy(max_attempts=3, base_delay=0.0, jitter_ratio=0.0),
            sleep=lambda _d: None,
        )
    assert ei.value.attempts == 3
    assert isinstance(ei.value.last_error, sqlite3.OperationalError)
    assert len(ei.value.errors) == 3


def test_call_with_retry_non_retryable_raises_immediately():
    calls = {"n": 0}

    def boom() -> None:
        calls["n"] += 1
        raise ValueError("permanent")

    with pytest.raises(ValueError, match="permanent"):
        call_with_retry(
            boom,
            policy=RetryPolicy(max_attempts=5, base_delay=0.0),
            sleep=lambda _d: None,
        )
    assert calls["n"] == 1


def test_default_publish_policy_env(monkeypatch):
    monkeypatch.setenv("AGENTBUS_PUBLISH_MAX_ATTEMPTS", "7")
    monkeypatch.setenv("AGENTBUS_PUBLISH_BASE_DELAY", "0.1")
    monkeypatch.setenv("AGENTBUS_PUBLISH_MAX_DELAY", "3")
    monkeypatch.setenv("AGENTBUS_PUBLISH_JITTER", "0.5")
    pol = default_publish_policy()
    assert pol.max_attempts == 7
    assert pol.base_delay == pytest.approx(0.1)
    assert pol.max_delay == pytest.approx(3.0)
    assert pol.jitter_ratio == pytest.approx(0.5)


def test_retry_policy_validation():
    with pytest.raises(ValueError):
        RetryPolicy(max_attempts=0)
    with pytest.raises(ValueError):
        RetryPolicy(jitter_ratio=1.5)
    with pytest.raises(ValueError):
        RetryPolicy(exponential_base=0.5)
