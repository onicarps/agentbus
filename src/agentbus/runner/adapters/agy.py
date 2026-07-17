"""Agy TurnAdapter — headless oneshot via `agy --print` (Phase E)."""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any, Callable, Sequence

from agentbus.runner.adapters.prompt_common import build_cli_role_prompt
from agentbus.runner.types import TurnResult, WakeEnvelope

log = logging.getLogger("agentbus.runner.agy")
RunFn = Callable[..., subprocess.CompletedProcess[str]]


def build_agy_prompt(wake: WakeEnvelope, *, budget_remaining: int) -> str:
    return build_cli_role_prompt(
        role_name="Agy",
        role_hint="architect",
        wake=wake,
        budget_remaining=budget_remaining,
    )


def build_agy_command(
    *,
    agy_bin: str,
    prompt: str,
    workspace: Path,
    print_timeout: str,
    skip_permissions: bool,
    model: str | None,
    extra_args: Sequence[str],
) -> list[str]:
    """agy --print <prompt> --print-timeout … [--dangerously-skip-permissions]"""
    cmd: list[str] = [
        agy_bin,
        "--print",
        prompt,
        "--print-timeout",
        print_timeout,
        "--add-dir",
        str(workspace),
    ]
    if skip_permissions:
        cmd.append("--dangerously-skip-permissions")
    if model:
        cmd.extend(["--model", model])
    cmd.extend(list(extra_args))
    return cmd


class AgyAdapter:
    """
    Spawn isolated Agy headless turn per wake.

    Config: command, timeout_seconds, print_timeout, skip_permissions,
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

    def start_turn(
        self, wake: WakeEnvelope, *, budget_remaining: int
    ) -> TurnResult:
        opts = self.options
        dry_run = bool(opts.get("dry_run", False))
        timeout = int(opts.get("timeout_seconds", 900))
        print_timeout = str(opts.get("print_timeout") or "15m")
        skip_permissions = bool(opts.get("skip_permissions", True))
        agy_bin = str(opts.get("command") or opts.get("agy_bin") or "agy")
        model = opts.get("model")
        extra = opts.get("extra_args") or []
        if not isinstance(extra, list):
            raise ValueError("adapter.extra_args must be a list")

        if not dry_run:
            if agy_bin == "agy" and shutil.which("agy") is None:
                return TurnResult(
                    ok=False,
                    summary=(
                        f"RUNNER_ERROR: agy not on PATH event_id={wake.event_id}"
                    ),
                    detail={"agy_bin": agy_bin},
                )
            if (
                agy_bin != "agy"
                and not Path(agy_bin).is_file()
                and shutil.which(agy_bin) is None
            ):
                return TurnResult(
                    ok=False,
                    summary=(
                        f"RUNNER_ERROR: agy binary not found: {agy_bin!r} "
                        f"event_id={wake.event_id}"
                    ),
                    detail={"agy_bin": agy_bin},
                )

        runs_rel = str(opts.get("runs_dir") or ".agentbus/runs")
        runs_dir = Path(runs_rel)
        if not runs_dir.is_absolute():
            runs_dir = self.workspace / runs_dir
        run_dir = runs_dir / str(wake.event_id)
        run_dir.mkdir(parents=True, exist_ok=True)
        prompt_path = run_dir / "prompt.md"
        prompt = build_agy_prompt(wake, budget_remaining=budget_remaining)
        prompt_path.write_text(prompt, encoding="utf-8")

        workdir = (
            Path(opts["cwd"]).resolve() if opts.get("cwd") else self.workspace
        )
        cmd = build_agy_command(
            agy_bin=agy_bin,
            prompt=prompt,
            workspace=workdir,
            print_timeout=print_timeout,
            skip_permissions=skip_permissions,
            model=str(model) if model else None,
            extra_args=[str(a) for a in extra],
        )

        env = os.environ.copy()
        env.setdefault("AGENTBUS_WORKSPACE", str(self.workspace))
        env.setdefault("AGENTBUS_PRODUCER_ID", "agy")

        if dry_run:
            return TurnResult(
                ok=True,
                summary=(
                    f"RUNNER_ACK: agy dry_run event_id={wake.event_id} "
                    f"print_timeout={print_timeout}"
                ),
                detail={
                    "adapter": "agy",
                    "dry_run": True,
                    "cmd0": cmd[:4] + ["…"],
                    "prompt_path": str(prompt_path),
                    "prompt_preview": prompt[:500],
                },
            )

        log.info("agy turn start event_id=%s timeout=%s", wake.event_id, timeout)
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
                    f"RUNNER_ERROR: agy timeout event_id={wake.event_id} "
                    f"timeout_seconds={timeout}"
                ),
                detail={
                    "adapter": "agy",
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
                    f"RUNNER_ERROR: agy exec missing event_id={wake.event_id}"
                ),
                detail={"agy_bin": agy_bin},
            )

        stdout = (proc.stdout or "").strip()
        stderr = (proc.stderr or "").strip()
        preview = " ".join((stdout or stderr or "(no output)")[-800:].split())
        if proc.returncode != 0:
            return TurnResult(
                ok=False,
                summary=(
                    f"RUNNER_ERROR: agy exit={proc.returncode} "
                    f"event_id={wake.event_id} out={preview[:400]}"
                ),
                detail={
                    "adapter": "agy",
                    "returncode": proc.returncode,
                    "stdout": stdout[-8000:],
                    "stderr": stderr[-8000:],
                    "prompt_path": str(prompt_path),
                },
            )
        return TurnResult(
            ok=True,
            summary=(
                f"RUNNER_ACK: agy completed event_id={wake.event_id} "
                f"out={preview[:500]}"
            ),
            detail={
                "adapter": "agy",
                "returncode": 0,
                "stdout": stdout[-8000:],
                "stderr": stderr[-4000:],
                "prompt_path": str(prompt_path),
            },
        )
