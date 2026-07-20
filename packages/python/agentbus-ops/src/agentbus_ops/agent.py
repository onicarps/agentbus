"""SREWatchdogAgent — deterministic edge-triggered bus participant."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from agentbus_ops.policy import (
    DEFAULT_COOLDOWN_SECONDS,
    DEFAULT_PRODUCER_ID,
    DEFAULT_TO_AGENT,
    DEFAULT_TOPIC,
    Decision,
    decide,
)
from agentbus_ops.probe import HealthSnapshot, probe_health
from agentbus_ops.publish import PublishError, compact_metrics_snippet, publish_sre_status
from agentbus_ops.state import WatchdogState, default_state_path, load_state, save_state


class SREWatchdogAgent:
    """Deterministic SRE edge watchdog (not an LLM runner every tick).

    Lifecycle of ``run_once``:

    1. ``probe()`` — health snapshot (bash wrap in v0.1)
    2. ``decide()`` — pure edge policy vs state file
    3. On publish edge: ``on_transition`` (default publishes) +
       ``on_critical_alert`` when level is critical
    4. Persist state (unless pure dry-run)
    """

    def __init__(
        self,
        workspace: str | Path | None = None,
        *,
        state_file: str | Path | None = None,
        health_script: str | Path | None = None,
        cooldown_seconds: int = DEFAULT_COOLDOWN_SECONDS,
        producer_id: str = DEFAULT_PRODUCER_ID,
        to_agent: str = DEFAULT_TO_AGENT,
        topic: str = DEFAULT_TOPIC,
        probe_timeout: float = 120.0,
    ) -> None:
        env_ws = os.environ.get("AGENTBUS_WORKSPACE")
        raw = workspace or env_ws or Path.cwd()
        self.workspace = Path(raw).resolve()
        self.state_file = Path(state_file) if state_file else default_state_path(self.workspace)
        self.health_script = Path(health_script) if health_script else None
        self.cooldown_seconds = int(cooldown_seconds)
        self.producer_id = producer_id
        self.to_agent = to_agent
        self.topic = topic
        self.probe_timeout = probe_timeout

    def probe(self) -> HealthSnapshot:
        """Probe swarm health. Override to inject pure-Python probe later."""
        return probe_health(
            self.workspace,
            health_script=self.health_script,
            timeout=self.probe_timeout,
        )

    def load_prev(self, *, seed_level: str | None = None) -> WatchdogState | None:
        if seed_level:
            # decide() handles seed; return None here so policy sees seed path only.
            return None
        return load_state(self.state_file)

    def decide(
        self,
        health: HealthSnapshot,
        prev: WatchdogState | None,
        *,
        force_bootstrap_publish: bool = False,
        seed_level: str | None = None,
        metrics_snippet: str = "",
        now_epoch: int | None = None,
        now_iso: str | None = None,
    ) -> Decision:
        return decide(
            health,
            prev if not seed_level else None,
            state_file=str(self.state_file),
            cooldown_seconds=self.cooldown_seconds,
            force_bootstrap_publish=force_bootstrap_publish,
            seed_level=seed_level,
            metrics_snippet=metrics_snippet,
            producer_id=self.producer_id,
            to_agent=self.to_agent,
            now_epoch=now_epoch,
            now_iso=now_iso,
        )

    def on_transition(self, prev: WatchdogState | None, cur: HealthSnapshot, decision: Decision) -> None:
        """Default: publish SRE_STATUS. Subclass to add side effects."""
        if not decision.should_publish or not decision.payload:
            return
        result = publish_sre_status(
            self.workspace,
            decision.payload,
            producer_id=self.producer_id,
            idempotency_key=decision.idempotency_key,
            topic=self.topic,
        )
        decision.publish_event_id = result.get("event_id")
        decision.publish_ok = True

    def on_critical_alert(self, cur: HealthSnapshot, decision: Decision) -> None:
        """Default no-op. Subclass for LLM / paging — never default-on."""

    def run_once(
        self,
        *,
        dry_run: bool = False,
        write_state: bool = False,
        force_bootstrap_publish: bool = False,
        include_metrics: bool = False,
        seed_level: str | None = None,
        now_epoch: int | None = None,
        now_iso: str | None = None,
    ) -> Decision:
        """One observation tick. Cron-safe: exit logic is caller's concern."""
        health = self.probe()
        prev = None if seed_level else load_state(self.state_file)
        metrics = compact_metrics_snippet(self.workspace) if include_metrics else ""
        decision = self.decide(
            health,
            prev,
            force_bootstrap_publish=force_bootstrap_publish,
            seed_level=seed_level,
            metrics_snippet=metrics,
            now_epoch=now_epoch,
            now_iso=now_iso,
        )
        decision.dry_run = dry_run

        if decision.should_publish:
            if dry_run:
                decision.publish_ok = True
                decision.publish_event_id = None
            else:
                try:
                    self.on_transition(prev, health, decision)
                    if decision.level == "critical":
                        self.on_critical_alert(health, decision)
                except PublishError as exc:
                    decision.publish_ok = False
                    decision.publish_error = str(exc)
                    # Still return decision so CLI can exit 2; do not write publish markers
                    # if publish failed — keep prior publish markers by reloading? Policy
                    # already stamped publish markers into new_state assuming success.
                    # Revert publish markers on failure to match bash (bash exits 2 before
                    # state write on publish fail... actually bash writes state only after
                    # publish success path: on fail it exits 2 before state write).
                    return decision

        # Persist state (skip pure dry-run unless write_state)
        if not dry_run or write_state:
            # On successful path (or silence / bootstrap), write new_state.
            if decision.should_publish and not decision.publish_ok and not dry_run:
                return decision
            save_state(self.state_file, decision.state)

        return decision


def decision_to_public_dict(decision: Decision) -> dict[str, Any]:
    """JSON-serializable decision matching bash --json keys."""
    d = decision.to_dict()
    # Drop publish_error key when None for closer bash parity (optional).
    if d.get("publish_error") is None:
        d.pop("publish_error", None)
    return d
