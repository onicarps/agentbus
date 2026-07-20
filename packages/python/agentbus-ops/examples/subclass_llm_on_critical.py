#!/usr/bin/env python3
"""Example: subclass SREWatchdogAgent for optional LLM on critical transitions.

Default package behavior is publish-only (no LLM). This shows how operators
inject remediation without baking model cost into the edge engine.
"""

from __future__ import annotations

import os
import sys

from agentbus_ops import HealthSnapshot, SREWatchdogAgent
from agentbus_ops.policy import Decision


class LLMCriticalWatchdog(SREWatchdogAgent):
    """On critical edge, call a user-provided hook (stub here)."""

    def on_critical_alert(self, cur: HealthSnapshot, decision: Decision) -> None:
        # Replace with your LLM / pager. Never runs on silence or healthy ticks.
        print(
            f"[hook] critical alert level={cur.level} notes={cur.notes[:5]} "
            f"summary={decision.summary[:120]}",
            file=sys.stderr,
        )


def main() -> int:
    ws = os.environ.get("AGENTBUS_WORKSPACE", "/home/oni/okf_agent_workspace")
    agent = LLMCriticalWatchdog(workspace=ws)
    decision = agent.run_once(dry_run=True)
    print(f"action={decision.action} level={decision.level} should_publish={decision.should_publish}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
