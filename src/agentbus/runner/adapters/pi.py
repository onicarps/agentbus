"""Pi TurnAdapter — isolated non-interactive operational execution."""

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


def build_pi_prompt(wake: WakeEnvelope, *, budget_remaining: int) -> str:
    return build_cli_role_prompt(
        role_name="Pi", role_hint="operations / swarm health", wake=wake,
        budget_remaining=budget_remaining,
    )


def build_pi_command(
    *, pi_bin: str, prompt_path: Path, model: str | None,
    approve_project_files: bool, load_extensions: bool,
    extra_args: Sequence[str],
) -> list[str]:
    if any(arg in {"--approve", "-a", "--no-approve", "-na"} for arg in extra_args):
        raise ValueError("set adapter.approve_project_files instead of approval flags in extra_args")
    cmd = [pi_bin, "--print", "--no-session"]
    cmd.append("--approve" if approve_project_files else "--no-approve")
    if not load_extensions:
        cmd.append("--no-extensions")
    if model:
        cmd.extend(["--model", model])
    cmd.extend(str(arg) for arg in extra_args)
    cmd.append("@" + str(prompt_path))
    return cmd


class PiAdapter:
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
        pi_bin = str(opts.get("command") or opts.get("pi_bin") or "pi")
        extra = opts.get("extra_args") or []
        if not isinstance(extra, list):
            raise ValueError("adapter.extra_args must be a list")
        if not bool(opts.get("dry_run")) and not self._skip_bin_check:
            if shutil.which(pi_bin) is None and not Path(pi_bin).is_file():
                return TurnResult(ok=False, summary=f"RUNNER_ERROR: pi binary not found event_id={wake.event_id}")
        run_dir = self.workspace / str(opts.get("runs_dir") or ".agentbus/runs") / str(wake.event_id)
        run_dir.mkdir(parents=True, exist_ok=True)
        prompt_path = run_dir / "prompt.md"
        prompt_path.write_text(build_pi_prompt(wake, budget_remaining=budget_remaining), encoding="utf-8")
        workdir = Path(opts["cwd"]).resolve() if opts.get("cwd") else self.workspace
        cmd = build_pi_command(
            pi_bin=pi_bin, prompt_path=prompt_path,
            model=str(opts["model"]) if opts.get("model") else None,
            approve_project_files=bool(opts.get("approve_project_files", False)),
            load_extensions=bool(opts.get("load_extensions", False)),
            extra_args=extra,
        )
        if bool(opts.get("dry_run")):
            return TurnResult(
                summary=f"RUNNER_ACK: pi dry_run event_id={wake.event_id}",
                detail={"adapter": "pi", "dry_run": True, "cmd": cmd, "prompt_path": str(prompt_path)},
            )
        try:
            proc = self._run_fn(
                cmd, cwd=str(workdir), env=runner_subprocess_env(
                    self.workspace, producer_id="pi", wake=wake,
                ), capture_output=True, text=True, timeout=timeout,
            )
        except subprocess.TimeoutExpired as exc:
            return TurnResult(
                ok=False,
                summary=f"RUNNER_ERROR: pi timeout event_id={wake.event_id} timeout_seconds={timeout}",
                detail={"adapter": "pi", "timeout": True, "stdout": str(exc.stdout or "")[-4000:], "stderr": str(exc.stderr or "")[-4000:]},
            )
        except FileNotFoundError:
            return TurnResult(ok=False, summary=f"RUNNER_ERROR: pi exec missing event_id={wake.event_id}")
        stdout, stderr = (proc.stdout or "").strip(), (proc.stderr or "").strip()
        return turn_result_from_cli_exit(
            adapter="pi", event_id=wake.event_id, returncode=proc.returncode,
            preview=(stdout or stderr or "(no output)")[-800:],
            detail={"stdout": stdout[-8000:], "stderr": stderr[-8000:], "prompt_path": str(prompt_path)},
        )
