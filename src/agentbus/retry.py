"""Resilient call retries: exponential backoff + jitter.

Used by EventStore publish and messaging helpers so concurrent writers under
companion-ACK / storm load do not thrash SQLite or webhook endpoints.
"""

from __future__ import annotations

import os
import random
import sqlite3
import time
from dataclasses import dataclass
from typing import Any, Callable, TypeVar

T = TypeVar("T")


@dataclass(frozen=True)
class RetryPolicy:
    """Retry schedule with capped exponential backoff and optional jitter.

    ``jitter_ratio``:
      * ``1.0`` — full jitter (uniform in ``[0, delay]``) — default, AWS style
      * ``0.0`` — no jitter (deterministic delay)
      * ``(0, 1)`` — equal jitter blend toward the raw delay
    """

    max_attempts: int = 5
    base_delay: float = 0.05
    max_delay: float = 2.0
    exponential_base: float = 2.0
    jitter_ratio: float = 1.0

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.base_delay < 0 or self.max_delay < 0:
            raise ValueError("delays must be >= 0")
        if self.exponential_base < 1.0:
            raise ValueError("exponential_base must be >= 1.0")
        if not (0.0 <= self.jitter_ratio <= 1.0):
            raise ValueError("jitter_ratio must be in [0, 1]")


def default_publish_policy() -> RetryPolicy:
    """Policy for SQLite publish under lock contention (env-overridable)."""

    def _int(name: str, default: int) -> int:
        raw = (os.environ.get(name) or "").strip()
        if not raw:
            return default
        try:
            return int(raw)
        except ValueError:
            return default

    def _float(name: str, default: float) -> float:
        raw = (os.environ.get(name) or "").strip()
        if not raw:
            return default
        try:
            return float(raw)
        except ValueError:
            return default

    return RetryPolicy(
        max_attempts=max(1, _int("AGENTBUS_PUBLISH_MAX_ATTEMPTS", 5)),
        base_delay=max(0.0, _float("AGENTBUS_PUBLISH_BASE_DELAY", 0.05)),
        max_delay=max(0.0, _float("AGENTBUS_PUBLISH_MAX_DELAY", 2.0)),
        exponential_base=max(1.0, _float("AGENTBUS_PUBLISH_EXP_BASE", 2.0)),
        jitter_ratio=min(1.0, max(0.0, _float("AGENTBUS_PUBLISH_JITTER", 1.0))),
    )


def default_delivery_policy() -> RetryPolicy:
    """Policy for outbound delivery (webhook / bridge) retries."""

    def _int(name: str, default: int) -> int:
        raw = (os.environ.get(name) or "").strip()
        if not raw:
            return default
        try:
            return int(raw)
        except ValueError:
            return default

    def _float(name: str, default: float) -> float:
        raw = (os.environ.get(name) or "").strip()
        if not raw:
            return default
        try:
            return float(raw)
        except ValueError:
            return default

    return RetryPolicy(
        max_attempts=max(1, _int("AGENTBUS_DELIVERY_MAX_ATTEMPTS", 3)),
        base_delay=max(0.0, _float("AGENTBUS_DELIVERY_BASE_DELAY", 0.2)),
        max_delay=max(0.0, _float("AGENTBUS_DELIVERY_MAX_DELAY", 2.0)),
        exponential_base=max(1.0, _float("AGENTBUS_DELIVERY_EXP_BASE", 2.0)),
        jitter_ratio=min(1.0, max(0.0, _float("AGENTBUS_DELIVERY_JITTER", 1.0))),
    )


class RetryExhaustedError(Exception):
    """All retry attempts failed."""

    def __init__(
        self,
        message: str,
        *,
        attempts: int,
        last_error: BaseException | None = None,
        errors: list[BaseException] | None = None,
    ) -> None:
        super().__init__(message)
        self.attempts = attempts
        self.last_error = last_error
        self.errors = list(errors or [])


def compute_backoff(
    attempt: int,
    policy: RetryPolicy,
    *,
    rng: random.Random | None = None,
) -> float:
    """Delay in seconds *before* attempt ``attempt`` (0-based after first failure).

    ``attempt=0`` is the first backoff (after the first failure).
    """
    if attempt < 0:
        attempt = 0
    raw = min(
        policy.max_delay,
        policy.base_delay * (policy.exponential_base**attempt),
    )
    if raw <= 0 or policy.jitter_ratio <= 0:
        return raw
    r = rng if rng is not None else random
    if policy.jitter_ratio >= 1.0:
        # Full jitter: pick uniformly in [0, raw]
        return float(r.uniform(0.0, raw))
    # Equal-ish jitter: mix deterministic delay with random component
    low = raw * (1.0 - policy.jitter_ratio)
    return float(r.uniform(low, raw))


def is_transient_sqlite_error(exc: BaseException) -> bool:
    """True for SQLite lock/busy errors that are safe to retry."""
    if isinstance(exc, sqlite3.OperationalError):
        msg = str(exc).lower()
        return "locked" in msg or "busy" in msg
    # modernc / some drivers wrap as generic Exception with same text
    if isinstance(exc, OSError):
        msg = str(exc).lower()
        return "database is locked" in msg or "sqlite_busy" in msg
    return False


def call_with_retry(
    fn: Callable[[], T],
    *,
    policy: RetryPolicy | None = None,
    is_retryable: Callable[[BaseException], bool] | None = None,
    sleep: Callable[[float], None] = time.sleep,
    rng: random.Random | None = None,
    on_retry: Callable[[int, BaseException, float], None] | None = None,
) -> T:
    """Invoke ``fn`` until success or attempts exhausted.

    Non-retryable errors are re-raised immediately. On exhaustion raises
    :class:`RetryExhaustedError` (with ``last_error`` set).
    """
    pol = policy or default_publish_policy()
    check = is_retryable or is_transient_sqlite_error
    errors: list[BaseException] = []
    last: BaseException | None = None

    for attempt in range(pol.max_attempts):
        try:
            return fn()
        except BaseException as exc:  # noqa: BLE001 — classified below
            last = exc
            errors.append(exc)
            if not check(exc):
                raise
            if attempt + 1 >= pol.max_attempts:
                break
            delay = compute_backoff(attempt, pol, rng=rng)
            if on_retry is not None:
                on_retry(attempt + 1, exc, delay)
            if delay > 0:
                sleep(delay)

    raise RetryExhaustedError(
        f"retry exhausted after {pol.max_attempts} attempt(s): {last!r}",
        attempts=pol.max_attempts,
        last_error=last,
        errors=errors,
    )


def retryable_call(
    fn: Callable[[], T],
    **kwargs: Any,
) -> T:
    """Alias for :func:`call_with_retry` (friendlier import name)."""
    return call_with_retry(fn, **kwargs)
