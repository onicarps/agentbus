"""Shared task-only prompts for CLI-based TurnAdapters."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

from agentbus.runner.types import AWAIT_EXIT_CODE, TurnResult, WakeEnvelope

# okf/handoff summary maxLength — keep companion ACK/ERROR under schema limit.
HANDOFF_SUMMARY_MAX = 2000

# v0.16.1 busy-wait circuit breaker: exact substrings in CLI stdout/stderr.
# When present, outer loop must not publish companion RUNNER_ACK/ERROR.
CIRCUIT_BREAK_MARKERS: tuple[str, ...] = (
    "CHAIN_BREAK",
    "TERMINAL_IDLE",
    "NO-OP",
)

# Inbound wake summaries that must never spawn an LLM turn or re-ACK.
# Companion RUNNER_ACK storms (hermes↔aider #1518–#1527) buried user-visible
# substance inside ops prefixes; Hermes standing orders correctly suppress
# Telegram for those prefixes — so the human never sees the answer unless
# agents publish a separate substance handoff. Structural skip stops the storm.
OPS_SUMMARY_PREFIXES: tuple[str, ...] = (
    "RUNNER_ACK",
    "RUNNER_ERROR",
    "RUNNER_SUSPEND",
    "NO-OP",
    "TERMINAL_IDLE",
    "CHAIN_BREAK",
    "SUPPRESS ACK",
)


def preview_suppresses_ack(preview: str | None) -> bool:
    """True if CLI output contains a reserved circuit-breaker keyword."""
    text = preview or ""
    return any(marker in text for marker in CIRCUIT_BREAK_MARKERS)


def is_ops_noise_summary(summary: str | None) -> bool:
    """True if handoff summary is ops/companion noise (skip LLM + no re-ACK)."""
    text = (summary or "").strip()
    if not text:
        return False
    upper = text.upper()
    for prefix in OPS_SUMMARY_PREFIXES:
        if upper.startswith(prefix.upper()):
            return True
    return False


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


def format_cli_out_preview(preview: str | None, max_len: int) -> str:
    """Format CLI stdout for companion ACK ``out=`` (Slack-readable).

    Historically collapsed *all* whitespace to a single 500-char line, which
    destroyed markdown/code and made the Slack ``out=`` safety-net unusable.
    Preserve newlines (soft-collapse horizontal runs only) and fill up to
    ``max_len`` (caller should reserve room so full summary ≤ schema max).
    """
    text = (preview or "").strip()
    if not text:
        return ""
    # Collapse horizontal whitespace; keep newlines for multi-line Slack posts.
    text = re.sub(r"[^\S\n]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    if max_len <= 0:
        return ""
    if len(text) <= max_len:
        return text
    if max_len <= 3:
        return text[:max_len]
    return text[: max_len - 3].rstrip() + "..."


def turn_result_from_cli_exit(
    *,
    adapter: str,
    event_id: int,
    returncode: int,
    preview: str,
    detail: dict[str, Any] | None = None,
) -> TurnResult:
    """Map CLI exit codes to TurnResult; exit 75 → suspended (v0.16 await).

    v0.16.1: if preview contains CHAIN_BREAK / TERMINAL_IDLE / NO-OP, set
    ``suppress_ack=True`` so the outer loop does not republish a companion ACK.
    """
    body = dict(detail or {})
    body["adapter"] = adapter
    body["returncode"] = returncode
    suppress = preview_suppresses_ack(preview)
    if suppress:
        body["suppress_ack"] = True
        body["circuit_break"] = True
    if returncode == AWAIT_EXIT_CODE:
        return TurnResult(
            status="suspended",
            summary=f"RUNNER_SUSPEND: event_id={event_id} adapter={adapter}",
            detail=body,
            # Suspend ACK is intentional coordination; do not suppress.
            suppress_ack=False,
        )
    if returncode != 0:
        prefix = (
            f"RUNNER_ERROR: {adapter} exit={returncode} event_id={event_id} out="
        )
        out = format_cli_out_preview(
            preview, HANDOFF_SUMMARY_MAX - len(prefix)
        )
        return TurnResult(
            ok=False,
            summary=prefix + out,
            detail=body,
            suppress_ack=suppress,
        )
    prefix = f"RUNNER_ACK: {adapter} completed event_id={event_id} out="
    out = format_cli_out_preview(preview, HANDOFF_SUMMARY_MAX - len(prefix))
    return TurnResult(
        ok=True,
        summary=prefix + out,
        detail=body,
        suppress_ack=suppress,
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
        "- **Human-visible answers (primary UI = Slack):** companion RUNNER_ACK is",
        "  ops-only. When the human should see your result, publish a separate",
        "  substance handoff to `slack` and **preserve** wake `links`",
        "  (`slack://{channel}/{ts}`) so the bridge can thread the reply.",
        "  Hermes is **legacy Telegram only** — do not default Slack-origin work",
        "  to `hermes`/`human`. Substance summaries must **not** start with",
        "  RUNNER_ACK/ERROR/SUSPEND/NO-OP/TERMINAL_IDLE/CHAIN_BREAK",
        "  (plain language answer or CLOSED/status).",
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
