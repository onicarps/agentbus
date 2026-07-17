"""Shared task-only prompts for CLI-based TurnAdapters."""

from __future__ import annotations

from agentbus.runner.types import WakeEnvelope


def build_cli_role_prompt(
    *,
    role_name: str,
    role_hint: str,
    wake: WakeEnvelope,
    budget_remaining: int,
) -> str:
    lines = [
        f"# AgentBus headless {role_name} turn",
        "",
        f"You are {role_name} running as an AgentBus headless runner turn ({role_hint}).",
        "This is an isolated turn — do not wait for a human in a TUI.",
        "Do not attach to or mutate an interactive session transcript.",
        "",
        f"- Wake event_id: {wake.event_id}",
        f"- Topic: {wake.topic}",
        f"- Source: {wake.source}",
        f"- from: {wake.from_agent}",
        f"- to: {wake.to}",
        f"- budget_remaining_turns_on_chain: {budget_remaining}",
        "",
        "## Workspace",
        "",
        "- Coordination workspace is AGENTBUS_WORKSPACE (OKF root).",
        "- Implementation code is only under projects/<name>/.",
        "",
        "## Bus publishing",
        "",
        "- The outer `agentbus run` process publishes RUNNER_ACK / RUNNER_ERROR",
        f"  with causation_id={wake.event_id}. Prefer finishing the task.",
        "- If you must publish yourself, use full okf/handoff fields",
        f"  (from, to, summary) and causation_id={wake.event_id}.",
        "",
        "## Task summary",
        "",
        wake.summary or "(empty summary)",
        "",
        "## Payload fields",
        "",
    ]
    for k, v in sorted((wake.payload or {}).items()):
        lines.append(f"- **{k}**: {v}")
    lines.append("")
    lines.append("Complete the task. End with a short operational summary.")
    return "\n".join(lines)
