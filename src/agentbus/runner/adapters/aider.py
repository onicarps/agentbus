"""Aider TurnAdapter — headless SRE oneshot via `aider --message`."""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any, Callable, Sequence

from agentbus.runner.adapters.prompt_common import build_cli_role_prompt
from agentbus.runner.types import TurnResult, WakeEnvelope

log = logging.getLogger("agentbus.runner.aider")
RunFn = Callable[..., subprocess.CompletedProcess[str]]


def build_aider_prompt(wake: WakeEnvelope, *, budget_remaining: int) -> str:
    base = build_cli_role_prompt(
        role_name="Aider",
        role_hint="SRE / swarm health",
        wake=wake,
        budget_remaining=budget_remaining,
    )
    extra = """

## SRE standing orders

1. Prefer read-only checks: `./scripts/swarm_health_check.sh`, `agentbus status`, `agentbus ps`.
2. Grep `.agentbus/logs/*.stderr.log` for ERROR/panic before restarting.
3. Restart only with `agentbus down` / `agentbus up -d` after announcing SRE_ACTION.
4. Do not implement product features — escalate to Grok.
5. Final line of your reply should support an outer RUNNER_ACK summary.
"""
    return base + extra


def build_aider_command(
    *,
    aider_bin: str,
    message: str,
    cwd: Path,
    yes_always: bool,
    model: str | None,
    extra_args: Sequence[str],
) -> list[str]:
    """
    Headless-ish aider: --message runs one prompt.
    --yes-always / --yes avoid interactive confirms when supported.
    """
    cmd: list[str] = [aider_bin, "--message", message]
    if yes_always:
        # Prefer long form; older aider used --yes
        cmd.append("--yes-always")
    if model:
        cmd.extend(["--model", model])
    cmd.extend(list(extra_args))
    return cmd


class AiderAdapter:
    """
    Spawn isolated Aider turn for SRE/health wakes.

    Config: command, timeout_seconds, yes_always, model, extra_args,
            dry_run, cwd, runs_dir
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

    def start_turn(
        self, wake: WakeEnvelope, *, budget_remaining: int
    ) -> TurnResult:
        opts = self.options
        dry_run = bool(opts.get("dry_run", False))
        timeout = int(opts.get("timeout_seconds", 600))
        yes_always = bool(opts.get("yes_always", True))
        aider_bin = str(opts.get("command") or opts.get("aider_bin") or "aider")
        model = opts.get("model")
        extra = opts.get("extra_args") or []
        if not isinstance(extra, list):
            raise ValueError("adapter.extra_args must be a list")

        if not dry_run:
            if aider_bin == "aider" and shutil.which("aider") is None:
                return TurnResult(
                    ok=False,
                    summary=(
                        f"RUNNER_ERROR: aider not on PATH event_id={wake.event_id}"
                    ),
                    detail={"aider_bin": aider_bin},
                )
            if (
                aider_bin != "aider"
                and not Path(aider_bin).is_file()
                and shutil.which(aider_bin) is None
            ):
                return TurnResult(
                    ok=False,
                    summary=(
                        f"RUNNER_ERROR: aider binary not found: {aider_bin!r} "
                        f"event_id={wake.event_id}"
                    ),
                    detail={"aider_bin": aider_bin},
                )

        runs_rel = str(opts.get("runs_dir") or ".agentbus/runs")
        runs_dir = Path(runs_rel)
        if not runs_dir.is_absolute():
            runs_dir = self.workspace / runs_dir
        run_dir = runs_dir / str(wake.event_id)
        run_dir.mkdir(parents=True, exist_ok=True)
        prompt_path = run_dir / "prompt.md"
        prompt = build_aider_prompt(wake, budget_remaining=budget_remaining)
        prompt_path.write_text(prompt, encoding="utf-8")

        workdir = (
            Path(opts["cwd"]).resolve() if opts.get("cwd") else self.workspace
        )
        cmd = build_aider_command(
            aider_bin=aider_bin,
            message=prompt,
            cwd=workdir,
            yes_always=yes_always,
            model=str(model) if model else None,
            extra_args=[str(a) for a in extra],
        )

        env = os.environ.copy()
        env.setdefault("AGENTBUS_WORKSPACE", str(self.workspace))
        env.setdefault("AGENTBUS_PRODUCER_ID", "aider")

        if dry_run:
            return TurnResult(
                ok=True,
                summary=(
                    f"RUNNER_ACK: aider dry_run event_id={wake.event_id} "
                    f"sre_prompt_written"
                ),
                detail={
                    "adapter": "aider",
                    "dry_run": True,
                    "cmd0": cmd[:3] + ["…"],
                    "prompt_path": str(prompt_path),
                    "prompt_preview": prompt[:500],
                },
            )

        log.info("aider turn start event_id=%s timeout=%s", wake.event_id, timeout)
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
                    f"RUNNER_ERROR: aider timeout event_id={wake.event_id} "
                    f"timeout_seconds={timeout}"
                ),
                detail={
                    "adapter": "aider",
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
                    f"RUNNER_ERROR: aider exec missing event_id={wake.event_id}"
                ),
                detail={"aider_bin": aider_bin},
            )

        stdout = (proc.stdout or "").strip()
        stderr = (proc.stderr or "").strip()
        preview = " ".join((stdout or stderr or "(no output)")[-800:].split())
        if proc.returncode != 0:
            return TurnResult(
                ok=False,
                summary=(
                    f"RUNNER_ERROR: aider exit={proc.returncode} "
                    f"event_id={wake.event_id} out={preview[:400]}"
                ),
                detail={
                    "adapter": "aider",
                    "returncode": proc.returncode,
                    "stdout": stdout[-8000:],
                    "stderr": stderr[-8000:],
                    "prompt_path": str(prompt_path),
                },
            )
        return TurnResult(
            ok=True,
            summary=(
                f"RUNNER_ACK: aider completed event_id={wake.event_id} "
                f"out={preview[:500]}"
            ),
            detail={
                "adapter": "aider",
                "returncode": 0,
                "stdout": stdout[-8000:],
                "stderr": stderr[-4000:],
                "prompt_path": str(prompt_path),
            },
        )
