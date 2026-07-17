"""Shared task-only prompts for CLI-based TurnAdapters."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from agentbus.runner.types import AWAIT_EXIT_CODE, TurnResult, WakeEnvelope


def runner_subprocess_env(
    workspace: Path,
    *,
    producer_id: str,
    wake: WakeEnvelope,
    extra_defaults: dict[str, str] | None = None,
) -> dict[str, str]:
    """Env for headless CLI adapters (workspace + wake id for ``agentbus await``)."""
    env = os.environ.copy()
    # Authoritative from the active runner; inherited ambient values must not
    # redirect an adapter's `agentbus await` drop to the wrong workspace/producer.
    env["AGENTBUS_WORKSPACE"] = str(workspace.resolve())
    env["AGENTBUS_PRODUCER_ID"] = producer_id
    env["AGENTBUS_WAKE_EVENT_ID"] = str(wake.event_id)
    # Matches ChainBudget.chain_key(event_id, causation_id)
    env["AGENTBUS_CHAIN_KEY"] = str(
        wake.causation_id if wake.causation_id is not None else wake.event_id
    )
    if extra_defaults:
        for key, value in extra_defaults.items():
            env.setdefault(key, value)
    return env


def turn_result_from_cli_exit(
    *,
    adapter: str,
    event_id: int,
    returncode: int,
    preview: str,
    detail: dict[str, Any] | None = None,
) -> TurnResult:
    """Map CLI exit codes to TurnResult; exit 75 → suspended (v0.16 await)."""
    body = dict(detail or {})
    body["adapter"] = adapter
    body["returncode"] = returncode
    preview_one = " ".join((preview or "").split())
    if returncode == AWAIT_EXIT_CODE:
        return TurnResult(
            status="suspended",
            summary=f"RUNNER_SUSPEND: event_id={event_id} adapter={adapter}",
            detail=body,
        )
    if returncode != 0:
        return TurnResult(
            ok=False,
            summary=(
                f"RUNNER_ERROR: {adapter} exit={returncode} "
                f"event_id={event_id} out={preview_one[:400]}"
            ),
            detail=body,
        )
    return TurnResult(
        ok=True,
        summary=(
            f"RUNNER_ACK: {adapter} completed event_id={event_id} "
            f"out={preview_one[:500]}"
        ),
        detail=body,
    )


def _resume_block(payload: dict[str, Any]) -> list[str]:
    resume = payload.get("resume")
    if not isinstance(resume, dict):
        return []
    lines = [
        "## Resume context (v0.16 async suspend)",
        "",
        "This turn is a **synthetic resume** after a prior cooperative await.",
        "Continue the original task; do not re-dispatch the same dependency unless",
        "the resume status is timeout or the verdict requires it (e.g. QA RED).",
        "",
        f"- wait_id: {resume.get('wait_id')}",
        f"- chain_key: {resume.get('chain_key')}",
        f"- origin_event_id: {resume.get('origin_event_id')}",
        f"- fulfilled_by: {resume.get('fulfilled_by')}",
        f"- resume_status: {resume.get('status')}",
        f"- reason: {resume.get('reason')}",
        "",
    ]
    return lines


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
        f"  / RUNNER_SUSPEND with causation_id={wake.event_id}. Prefer finishing the task.",
        "- If you must publish yourself, use full okf/handoff fields",
        f"  (from, to, summary) and causation_id={wake.event_id}.",
        "",
        "## Cooperative await (do not busy-wait)",
        "",
        "- If you need a dependency (e.g. Factory QA verdict), call:",
        "  `agentbus await --expect-from factory --causation-id <id> "
        "--match QA_VERDICT --timeout-hours 4`",
        "- That exits 75, registers a durable wait, and the runner resumes you later.",
        "- Do **not** poll the bus in a loop waiting for another agent.",
        "",
    ]
    lines.extend(_resume_block(wake.payload or {}))
    lines.extend(
        [
            "## Task summary",
            "",
            wake.summary or "(empty summary)",
            "",
            "## Payload fields",
            "",
        ]
    )
    for k, v in sorted((wake.payload or {}).items()):
        if k == "resume":
            continue  # already rendered
        lines.append(f"- **{k}**: {v}")
    lines.append("")
    lines.append("Complete the task. End with a short operational summary.")
    return "\n".join(lines)
