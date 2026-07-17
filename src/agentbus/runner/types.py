"""Shared types for agentbus run (Phase B)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


BROADCAST_TO = frozenset({"all", "swarm", "*"})


@dataclass
class WakeEnvelope:
    event_id: int
    topic: str
    from_agent: str
    to: str
    summary: str
    payload: dict[str, Any]
    source: str  # webhook_queue | wake_file
    raw: dict[str, Any] = field(default_factory=dict)
    causation_id: int | None = None
    trace_id: str | None = None


@dataclass
class TurnResult:
    ok: bool
    summary: str
    detail: dict[str, Any] | None = None
