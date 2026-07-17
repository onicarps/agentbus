"""Factory TurnAdapter — headless oneshot via `droid exec` (Phase D)."""

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

log = logging.getLogger("agentbus.runner.factory")

RunFn = Callable[..., subprocess.CompletedProcess[str]]


def build_factory_prompt(wake: WakeEnvelope, *, budget_remaining: int) -> str:
    """Task-only prompt for an isolated Factory droid turn."""
    lines = [
        "# AgentBus headless Factory turn (QA role)",
        "",
        "You are Factory running as an AgentBus headless runner turn.",
        "This is an isolated turn — do not wait for a human in a TUI.",
        "",
        f"- Wake event_id: {wake.event_id}",
        f"- Topic: {wake.topic}",
        f"- Source: {wake.source}",
        f"- from: {wake.from_agent}",
        f"- to: {wake.to}",
        f"- budget_remaining_turns_on_chain: {budget_remaining}",
        "",
        "## Workspace rules",
        "",
        "- Coordination workspace (`AGENTBUS_WORKSPACE`) is the OKF root.",
        "- Implementation code lives under `projects/<name>/` only.",
        "- Do not force-push or publish secrets.",
        "",
        "## Bus publishing",
        "",
        "- The outer `agentbus run` process will publish `RUNNER_ACK` / `RUNNER_ERROR`",
        f"  with causation_id={wake.event_id}. Prefer finishing the task over fighting bus schema.",
        "- If this wake is an explicit FACTORY_QA_MISSION / QA verdict request, you MAY",
        "  publish okf/handoff yourself with full required fields:",
        "  `from=factory`, `to=<requester>`, `summary` (use GREEN/RED verdict wording,",
        f"  not blocked substrings), and causation_id={wake.event_id}.",
        "  Include droid_proof when your role requires it.",
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
    lines.append("Complete the task with evidence (commands, paths under /tmp when useful).")
    lines.append("End with a short operational summary of what you did.")
    return "\n".join(lines)


def build_factory_command(
    *,
    droid_bin: str,
    prompt_path: Path,
    cwd: Path,
    auto: str,
    output_format: str,
    model: str | None,
    mission: bool,
    tag: str | None,
    extra_args: Sequence[str],
    skip_permissions: bool = False,
) -> list[str]:
    """droid exec -f prompt.md --cwd … [-o text] [--auto … | --skip-permissions-unsafe].

    ``--skip-permissions-unsafe`` cannot be combined with ``--auto`` (droid CLI).
    When skip_permissions is True, --auto is omitted.
    """
    cmd: list[str] = [
        droid_bin,
        "exec",
        "-f",
        str(prompt_path),
        "--cwd",
        str(cwd),
        "-o",
        output_format,
    ]
    if skip_permissions:
        cmd.append("--skip-permissions-unsafe")
    else:
        cmd.extend(["--auto", auto])
    if mission:
        cmd.append("--mission")
    if model:
        cmd.extend(["-m", model])
    if tag:
        cmd.extend(["--tag", tag])
    cmd.extend(list(extra_args))
    return cmd


class FactoryAdapter:
    """
    Spawn an isolated Factory `droid exec` process per wake.

    Config keys (adapter section in runner YAML):
      command: droid binary (default: droid)
      timeout_seconds: default 1800
      auto: low|medium|high (default high — used only when skip_permissions is false)
      skip_permissions: bool (default True for headless dogfood; maps to
        --skip-permissions-unsafe; mutually exclusive with --auto)
      output_format: text (default)
      model: optional
      mission: bool (default false)
      tag: optional session tag
      extra_args: list
      dry_run: bool
      cwd: optional override (default workspace)
      runs_dir: where to write prompt.md (default .agentbus/runs)
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
        timeout = int(opts.get("timeout_seconds", 1800))
        # Headless droid often hits permission walls even with --auto high;
        # default skip_permissions=True (same pattern as Agy). --auto is ignored
        # when skip_permissions is set (CLI mutual exclusion).
        skip_permissions = bool(opts.get("skip_permissions", True))
        auto = str(opts.get("auto") or "high").strip().lower()
        if not skip_permissions and auto not in {"low", "medium", "high"}:
            return TurnResult(
                ok=False,
                summary=(
                    f"RUNNER_ERROR: invalid auto level {auto!r} "
                    f"event_id={wake.event_id}"
                ),
                detail={"auto": auto},
            )
        output_format = str(opts.get("output_format") or "text")
        droid_bin = str(opts.get("command") or opts.get("droid_bin") or "droid")
        model = opts.get("model")
        mission = bool(opts.get("mission", False))
        tag = opts.get("tag")
        extra = opts.get("extra_args") or []
        if not isinstance(extra, list):
            raise ValueError("adapter.extra_args must be a list")

        if not dry_run and not self._skip_bin_check:
            if droid_bin == "droid":
                if shutil.which("droid") is None:
                    return TurnResult(
                        ok=False,
                        summary=(
                            f"RUNNER_ERROR: droid not on PATH "
                            f"event_id={wake.event_id}"
                        ),
                        detail={"droid_bin": droid_bin},
                    )
            elif not Path(droid_bin).is_file() and shutil.which(droid_bin) is None:
                return TurnResult(
                    ok=False,
                    summary=(
                        f"RUNNER_ERROR: droid binary not found: {droid_bin!r} "
                        f"event_id={wake.event_id}"
                    ),
                    detail={"droid_bin": droid_bin},
                )

        runs_rel = str(opts.get("runs_dir") or ".agentbus/runs")
        runs_dir = Path(runs_rel)
        if not runs_dir.is_absolute():
            runs_dir = self.workspace / runs_dir
        run_dir = runs_dir / str(wake.event_id)
        run_dir.mkdir(parents=True, exist_ok=True)
        prompt_path = run_dir / "prompt.md"
        prompt = build_factory_prompt(wake, budget_remaining=budget_remaining)
        prompt_path.write_text(prompt, encoding="utf-8")

        cwd_opt = opts.get("cwd")
        workdir = Path(cwd_opt).resolve() if cwd_opt else self.workspace

        cmd = build_factory_command(
            droid_bin=droid_bin,
            prompt_path=prompt_path,
            cwd=workdir,
            auto=auto,
            output_format=output_format,
            model=str(model) if model else None,
            mission=mission,
            tag=str(tag) if tag else None,
            extra_args=[str(a) for a in extra],
            skip_permissions=skip_permissions,
        )

        env = runner_subprocess_env(
            self.workspace, producer_id="factory", wake=wake
        )

        mode_label = (
            "skip_permissions"
            if skip_permissions
            else f"auto={auto}"
        )
        if dry_run:
            return TurnResult(
                ok=True,
                summary=(
                    f"RUNNER_ACK: factory dry_run event_id={wake.event_id} "
                    f"cmd_bin={droid_bin} {mode_label}"
                ),
                detail={
                    "adapter": "factory",
                    "dry_run": True,
                    "cmd": cmd,
                    "prompt_path": str(prompt_path),
                    "prompt_preview": prompt[:500],
                    "cwd": str(workdir),
                    "skip_permissions": skip_permissions,
                },
            )

        log.info(
            "factory turn start event_id=%s timeout=%s %s",
            wake.event_id,
            timeout,
            mode_label,
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
                    f"RUNNER_ERROR: factory timeout event_id={wake.event_id} "
                    f"timeout_seconds={timeout}"
                ),
                detail={
                    "adapter": "factory",
                    "timeout": True,
                    "stdout": (exc.stdout or "")[-4000:]
                    if isinstance(exc.stdout, str)
                    else "",
                    "stderr": (exc.stderr or "")[-4000:]
                    if isinstance(exc.stderr, str)
                    else "",
                    "prompt_path": str(prompt_path),
                },
            )
        except FileNotFoundError:
            return TurnResult(
                ok=False,
                summary=(
                    f"RUNNER_ERROR: factory exec missing event_id={wake.event_id}"
                ),
                detail={"droid_bin": droid_bin},
            )

        stdout = (proc.stdout or "").strip()
        stderr = (proc.stderr or "").strip()
        preview = (stdout or stderr or "(no output)")[-800:]
        return turn_result_from_cli_exit(
            adapter="factory",
            event_id=wake.event_id,
            returncode=proc.returncode,
            preview=preview,
            detail={
                "stdout": stdout[-8000:],
                "stderr": stderr[-8000:],
                "prompt_path": str(prompt_path),
                "cmd0": cmd[:6],
            },
        )
