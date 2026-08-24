"""Codex TurnAdapter — isolated headless execution via ``codex exec``."""

from __future__ import annotations

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

RunFn = Callable[..., subprocess.CompletedProcess[str]]


def build_codex_prompt(wake: WakeEnvelope, *, budget_remaining: int) -> str:
    return build_cli_role_prompt(
        role_name="Codex", role_hint="engineer", wake=wake,
        budget_remaining=budget_remaining,
    )


def build_codex_command(
    *, codex_bin: str, cwd: Path, model: str | None,
    output_format: str, extra_args: Sequence[str],
) -> list[str]:
    """Build only flags supported by the real Codex headless surface."""
    if output_format not in {"json", "text"}:
        raise ValueError("adapter.output_format must be json or text")
    cmd = [codex_bin, "exec", "-C", str(cwd), "--ephemeral"]
    if output_format == "json":
        cmd.append("--json")
    if model:
        cmd.extend(["-m", model])
    cmd.extend(str(arg) for arg in extra_args)
    cmd.append("-")  # prompt is supplied on stdin, not exposed in argv
    return cmd


class CodexAdapter:
    def __init__(
        self, *, workspace: Path, options: dict[str, Any] | None = None,
        run_fn: RunFn | None = None,
    ) -> None:
        self.workspace = workspace.resolve()
        self.options = dict(options or {})
        self._run_fn = run_fn or subprocess.run
        self._skip_bin_check = run_fn is not None

    def start_turn(self, wake: WakeEnvelope, *, budget_remaining: int) -> TurnResult:
        opts = self.options
        timeout = int(opts.get("timeout_seconds", 900))
        codex_bin = str(opts.get("command") or opts.get("codex_bin") or "codex")
        extra = opts.get("extra_args") or []
        if not isinstance(extra, list):
            raise ValueError("adapter.extra_args must be a list")
        if not bool(opts.get("dry_run")) and not self._skip_bin_check:
            if shutil.which(codex_bin) is None and not Path(codex_bin).is_file():
                return TurnResult(
                    ok=False,
                    summary=f"RUNNER_ERROR: codex binary not found event_id={wake.event_id}",
                    detail={"codex_bin": codex_bin},
                )

        run_dir = self.workspace / str(opts.get("runs_dir") or ".agentbus/runs") / str(wake.event_id)
        run_dir.mkdir(parents=True, exist_ok=True)
        prompt = build_codex_prompt(wake, budget_remaining=budget_remaining)
        prompt_path = run_dir / "prompt.md"
        prompt_path.write_text(prompt, encoding="utf-8")
        workdir = Path(opts["cwd"]).resolve() if opts.get("cwd") else self.workspace
        cmd = build_codex_command(
            codex_bin=codex_bin, cwd=workdir,
            model=str(opts["model"]) if opts.get("model") else None,
            output_format=str(opts.get("output_format") or "json"),
            extra_args=extra,
        )
        if bool(opts.get("dry_run")):
            return TurnResult(
                summary=f"RUNNER_ACK: codex dry_run event_id={wake.event_id}",
                detail={"adapter": "codex", "dry_run": True, "cmd": cmd, "prompt_path": str(prompt_path)},
            )
        try:
            proc = self._run_fn(
                cmd, cwd=str(workdir), env=runner_subprocess_env(
                    self.workspace, producer_id="codex", wake=wake,
                ),
                input=prompt, capture_output=True, text=True, timeout=timeout,
                close_fds=True,
            )
        except subprocess.TimeoutExpired as exc:
            return TurnResult(
                ok=False,
                summary=f"RUNNER_ERROR: codex timeout event_id={wake.event_id} timeout_seconds={timeout}",
                detail={"adapter": "codex", "timeout": True, "stdout": str(exc.stdout or "")[-4000:], "stderr": str(exc.stderr or "")[-4000:]},
            )
        except FileNotFoundError:
            return TurnResult(ok=False, summary=f"RUNNER_ERROR: codex exec missing event_id={wake.event_id}")
        stdout, stderr = (proc.stdout or "").strip(), (proc.stderr or "").strip()
        return turn_result_from_cli_exit(
            adapter="codex", event_id=wake.event_id, returncode=proc.returncode,
            preview=(stdout or stderr or "(no output)")[-800:],
            detail={"stdout": stdout[-8000:], "stderr": stderr[-8000:], "prompt_path": str(prompt_path)},
        )
