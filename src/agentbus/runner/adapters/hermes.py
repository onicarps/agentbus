"""Hermes TurnAdapter — headless oneshot via `hermes chat -q` (Phase C)."""

from __future__ import annotations

import logging
import shutil
import subprocess
from pathlib import Path
from typing import Any, Callable, Sequence

from agentbus.runner.adapters.prompt_common import (
    runner_subprocess_env,
    turn_result_from_cli_exit,
)
from agentbus.runner.types import TurnResult, WakeEnvelope

log = logging.getLogger("agentbus.runner.hermes")

# Injectable for tests
RunFn = Callable[..., subprocess.CompletedProcess[str]]


def build_hermes_prompt(wake: WakeEnvelope, *, budget_remaining: int) -> str:
    """Task-only prompt for an isolated Hermes turn (no interactive transcript)."""
    lines = [
        "You are Hermes running as an AgentBus headless runner turn (bridge role).",
        "Scope: swarm↔human bridge, Telegram/webhooks, Linear/Notion external docs.",
        "Do not own DevOps/SRE/releases — escalate ops work to Aider (ops).",
        "This is an isolated turn — do not wait for a human in a TUI.",
        f"Wake event_id={wake.event_id} topic={wake.topic} source={wake.source}",
        f"from={wake.from_agent} to={wake.to}",
        f"budget_remaining_turns_on_chain={budget_remaining}",
        "When you finish, your final reply should be a short operational summary.",
        "If the task asks you to publish on the bus, use agentbus MCP/tools with",
        f"causation_id={wake.event_id} when acknowledging.",
        "",
        "Telegram Relay Standing Orders:",
        "- Suppress Telegram relay for inbound summaries starting with: RUNNER_ACK, RUNNER_ERROR, RUNNER_SUSPEND, NO-OP, TERMINAL_IDLE, CHAIN_BREAK.",
        "- Treat RESUME: as non-human by default.",
        "- Relay substance handoffs (addressed to hermes, bridge, or human) to Telegram, including explicit questions, closed status, or ship announcements.",
        "- Companion RUNNER_ACK is ops-only (runner may skip it before this turn). Never re-ACK an ACK to peer agents (prevents hermes↔peer storms).",
        "- When you originate a human question to the swarm, require agents to reply with a substance handoff (not only outer RUNNER_ACK) so you can relay it.",
        "",
        "Task summary:",
        wake.summary or "(empty summary)",
        "",
        "Full payload (JSON fields):",
    ]
    for k, v in sorted((wake.payload or {}).items()):
        lines.append(f"  {k}: {v}")
    return "\n".join(lines)


def build_hermes_command(
    *,
    hermes_bin: str,
    prompt: str,
    max_turns: int,
    model: str | None,
    provider: str | None,
    extra_args: Sequence[str],
    quiet: bool = True,
) -> list[str]:
    """
    Prefer: hermes chat -q PROMPT -Q --max-turns N --accept-hooks
    Falls back to hermes -z PROMPT if chat subcommand unavailable (runtime).
    """
    cmd: list[str] = [hermes_bin, "chat", "-q", prompt]
    if quiet:
        cmd.append("-Q")
    cmd.extend(["--max-turns", str(max_turns), "--accept-hooks"])
    if model:
        cmd.extend(["-m", model])
    if provider:
        cmd.extend(["--provider", provider])
    cmd.extend(list(extra_args))
    return cmd


class HermesAdapter:
    """
    Spawn an isolated Hermes oneshot process per wake.

    Config keys (adapter section in runner YAML):
      command: hermes binary (default: hermes on PATH)
      timeout_seconds: subprocess timeout (default 600)
      max_turns: hermes --max-turns (default 8)
      model / provider: optional
      extra_args: list of extra CLI args
      dry_run: if true, do not exec (tests / offline)
      cwd: optional override (default: runner workspace)
    """

    def __init__(
        self,
        *,
        workspace: Path,
        options: dict[str, Any] | None = None,
        run_fn: RunFn | None = None,
    ) -> None:
        self.workspace = workspace.resolve()
        self.options = dict(options or {})
        self._run_fn = run_fn or subprocess.run
        # Injected run_fn (unit tests) skips PATH binary preflight.
        self._skip_bin_check = run_fn is not None

    def start_turn(
        self, wake: WakeEnvelope, *, budget_remaining: int
    ) -> TurnResult:
        opts = self.options
        dry_run = bool(opts.get("dry_run", False))
        timeout = int(opts.get("timeout_seconds", 600))
        max_turns = int(opts.get("max_turns", 8))
        hermes_bin = str(opts.get("command") or opts.get("hermes_bin") or "hermes")
        model = opts.get("model")
        provider = opts.get("provider")
        extra = opts.get("extra_args") or []
        if not isinstance(extra, list):
            raise ValueError("adapter.extra_args must be a list")

        if not dry_run and not self._skip_bin_check:
            if hermes_bin != "hermes" and not Path(hermes_bin).is_file():
                # allow bare name on PATH
                if shutil.which(hermes_bin) is None:
                    return TurnResult(
                        ok=False,
                        summary=(
                            f"RUNNER_ERROR: hermes binary not found: {hermes_bin!r} "
                            f"event_id={wake.event_id}"
                        ),
                        detail={"hermes_bin": hermes_bin},
                    )
            elif hermes_bin == "hermes" and shutil.which("hermes") is None:
                return TurnResult(
                    ok=False,
                    summary=(
                        f"RUNNER_ERROR: hermes not on PATH event_id={wake.event_id}"
                    ),
                    detail={"hermes_bin": hermes_bin},
                )

        prompt = build_hermes_prompt(wake, budget_remaining=budget_remaining)
        cmd = build_hermes_command(
            hermes_bin=hermes_bin,
            prompt=prompt,
            max_turns=max_turns,
            model=str(model) if model else None,
            provider=str(provider) if provider else None,
            extra_args=[str(a) for a in extra],
        )

        cwd = opts.get("cwd")
        workdir = Path(cwd).resolve() if cwd else self.workspace

        env = runner_subprocess_env(
            self.workspace,
            producer_id="hermes",
            wake=wake,
            extra_defaults={"HERMES_ACCEPT_HOOKS": "1"},
        )

        if dry_run:
            return TurnResult(
                ok=True,
                summary=(
                    f"RUNNER_ACK: hermes dry_run event_id={wake.event_id} "
                    f"cmd_bin={hermes_bin} max_turns={max_turns}"
                ),
                detail={
                    "adapter": "hermes",
                    "dry_run": True,
                    "cmd": cmd[:4] + ["…"],  # omit full prompt in short detail
                    "prompt_preview": prompt[:500],
                    "cwd": str(workdir),
                },
            )

        log.info(
            "hermes turn start event_id=%s timeout=%s max_turns=%s",
            wake.event_id,
            timeout,
            max_turns,
        )
        try:
            proc = self._run_fn(
                cmd,
                cwd=str(workdir),
                env=env,
                capture_output=True,
                text=True,
                timeout=timeout,
            )
        except subprocess.TimeoutExpired as exc:
            return TurnResult(
                ok=False,
                summary=(
                    f"RUNNER_ERROR: hermes timeout event_id={wake.event_id} "
                    f"timeout_seconds={timeout}"
                ),
                detail={
                    "adapter": "hermes",
                    "timeout": True,
                    "stdout": (exc.stdout or "")[-4000:]
                    if isinstance(exc.stdout, str)
                    else "",
                    "stderr": (exc.stderr or "")[-4000:]
                    if isinstance(exc.stderr, str)
                    else "",
                },
            )
        except FileNotFoundError:
            return TurnResult(
                ok=False,
                summary=(
                    f"RUNNER_ERROR: hermes exec missing event_id={wake.event_id}"
                ),
                detail={"hermes_bin": hermes_bin},
            )

        stdout = (proc.stdout or "").strip()
        stderr = (proc.stderr or "").strip()
        preview = (stdout or stderr or "(no output)")[-800:]
        return turn_result_from_cli_exit(
            adapter="hermes",
            event_id=wake.event_id,
            returncode=proc.returncode,
            preview=preview,
            detail={
                "stdout": stdout[-8000:],
                "stderr": stderr[-8000:],
                "cmd0": cmd[:3],
            },
        )
