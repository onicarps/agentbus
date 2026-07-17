"""CI-safe echo adapter — no LLM."""

from __future__ import annotations

from agentbus.runner.types import TurnResult, WakeEnvelope


class EchoAdapter:
    def start_turn(
        self, wake: WakeEnvelope, *, budget_remaining: int
    ) -> TurnResult:
        summary = (
            f"RUNNER_ACK: echo handled event_id={wake.event_id} "
            f"to={wake.to} from={wake.from_agent} "
            f"budget_remaining={budget_remaining}"
        )
        return TurnResult(
            ok=True,
            summary=summary,
            detail={
                "adapter": "echo",
                "event_id": wake.event_id,
                "source": wake.source,
                "wake_summary": wake.summary,
            },
        )
