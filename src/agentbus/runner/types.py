"""Shared types for agentbus run (Phase B + v0.16 suspend)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


BROADCAST_TO = frozenset({"all", "swarm", "*"})

TurnStatus = Literal["ok", "error", "suspended"]

# EX_TEMPFAIL-style: cooperative await (agentbus await)
AWAIT_EXIT_CODE = 75


@dataclass
class WakeEnvelope:
    event_id: int
    topic: str
    from_agent: str
    to: str
    summary: str
    payload: dict[str, Any]
    source: str  # webhook_queue | wake_file | resume
    raw: dict[str, Any] = field(default_factory=dict)
    causation_id: int | None = None
    trace_id: str | None = None


@dataclass
class TurnResult:
    """Adapter turn outcome.

    ``status`` is the v0.16 primary field. ``ok`` keyword remains accepted for
    back-compat (maps True→ok, False→error). Property ``ok`` is True for both
    ``ok`` and ``suspended`` (suspend is not a failure path).
    """

    summary: str
    detail: dict[str, Any] | None = None
    status: TurnStatus = "ok"

    def __init__(
        self,
        summary: str = "",
        detail: dict[str, Any] | None = None,
        *,
        status: TurnStatus | None = None,
        ok: bool | None = None,
    ) -> None:
        # Prefer explicit status; else map legacy ok= bool; default ok.
        if status is not None:
            self.status = status
        elif ok is not None:
            self.status = "ok" if ok else "error"
        else:
            self.status = "ok"
        self.summary = summary
        self.detail = detail

    @property
    def ok(self) -> bool:
        """True when the turn is not an error (includes suspended)."""
        return self.status != "error"
