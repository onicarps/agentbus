"""Grok TurnAdapter — headless oneshot via `grok --prompt-file` (Phase E)."""

from __future__ import annotations

import logging
import shutil
import subprocess
from pathlib import Path
from typing import Any, Callable, Sequence

from agentbus.runner.adapters.prompt_common import (
    build_cli_role_prompt,
    runner_subprocess_env,
    turn_result_from_cli_exit,
)
from agentbus.runner.types import TurnResult, WakeEnvelope

log = logging.getLogger("agentbus.runner.grok")
RunFn = Callable[..., subprocess.CompletedProcess[str]]


def build_grok_prompt(wake: WakeEnvelope, *, budget_remaining: int) -> str:
    return build_cli_role_prompt(
        role_name="Grok",
        role_hint="engineer",
        wake=wake,
        budget_remaining=budget_remaining,
    )


def build_grok_command(
    *,
    grok_bin: str,
    prompt_path: Path,
    cwd: Path,
    max_turns: int,
    always_approve: bool,
    output_format: str,
    model: str | None,
    extra_args: Sequence[str],
) -> list[str]:
    cmd: list[str] = [
        grok_bin,
        "--cwd",
        str(cwd),
        "--max-turns",
        str(max_turns),
        "--output-format",
        output_format,
        "--prompt-file",
        str(prompt_path),
    ]
    if always_approve:
        cmd.append("--always-approve")
    if model:
        cmd.extend(["-m", model])
    cmd.extend(list(extra_args))
    return cmd


class GrokAdapter:
    """
    Spawn isolated Grok headless turn per wake.

    Config: command, timeout_seconds, max_turns, always_approve, output_format,
            model, extra_args, dry_run, cwd, runs_dir
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
        timeout = int(opts.get("timeout_seconds", 900))
        max_turns = int(opts.get("max_turns", 12))
        always_approve = bool(opts.get("always_approve", True))
        output_format = str(opts.get("output_format") or "plain")
        grok_bin = str(opts.get("command") or opts.get("grok_bin") or "grok")
        model = opts.get("model")
        extra = opts.get("extra_args") or []
        if not isinstance(extra, list):
            raise ValueError("adapter.extra_args must be a list")

        if not dry_run and not self._skip_bin_check:
            if grok_bin == "grok" and shutil.which("grok") is None:
                return TurnResult(
                    ok=False,
                    summary=(
                        f"RUNNER_ERROR: grok not on PATH event_id={wake.event_id}"
                    ),
                    detail={"grok_bin": grok_bin},
                )
            if (
                grok_bin != "grok"
                and not Path(grok_bin).is_file()
                and shutil.which(grok_bin) is None
            ):
                return TurnResult(
                    ok=False,
                    summary=(
                        f"RUNNER_ERROR: grok binary not found: {grok_bin!r} "
                        f"event_id={wake.event_id}"
                    ),
                    detail={"grok_bin": grok_bin},
                )

        runs_rel = str(opts.get("runs_dir") or ".agentbus/runs")
        runs_dir = Path(runs_rel)
        if not runs_dir.is_absolute():
            runs_dir = self.workspace / runs_dir
        run_dir = runs_dir / str(wake.event_id)
        run_dir.mkdir(parents=True, exist_ok=True)
        prompt_path = run_dir / "prompt.md"
        prompt = build_grok_prompt(wake, budget_remaining=budget_remaining)
        prompt_path.write_text(prompt, encoding="utf-8")

        workdir = (
            Path(opts["cwd"]).resolve() if opts.get("cwd") else self.workspace
        )
        cmd = build_grok_command(
            grok_bin=grok_bin,
            prompt_path=prompt_path,
            cwd=workdir,
            max_turns=max_turns,
            always_approve=always_approve,
            output_format=output_format,
            model=str(model) if model else None,
            extra_args=[str(a) for a in extra],
        )

        env = runner_subprocess_env(
            self.workspace, producer_id="grok", wake=wake
        )

        if dry_run:
            return TurnResult(
                ok=True,
                summary=(
                    f"RUNNER_ACK: grok dry_run event_id={wake.event_id} "
                    f"max_turns={max_turns}"
                ),
                detail={
                    "adapter": "grok",
                    "dry_run": True,
                    "cmd": cmd,
                    "prompt_path": str(prompt_path),
                    "prompt_preview": prompt[:500],
                },
            )

        log.info("grok turn start event_id=%s timeout=%s", wake.event_id, timeout)
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
                    f"RUNNER_ERROR: grok timeout event_id={wake.event_id} "
                    f"timeout_seconds={timeout}"
                ),
                detail={
                    "adapter": "grok",
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
                    f"RUNNER_ERROR: grok exec missing event_id={wake.event_id}"
                ),
                detail={"grok_bin": grok_bin},
            )

        stdout = (proc.stdout or "").strip()
        stderr = (proc.stderr or "").strip()
        preview = stdout or stderr or "(no output)"
        return turn_result_from_cli_exit(
            adapter="grok",
            event_id=wake.event_id,
            returncode=proc.returncode,
            preview=preview[-800:],
            detail={
                "stdout": stdout[-8000:],
                "stderr": stderr[-8000:],
                "prompt_path": str(prompt_path),
            },
        )
