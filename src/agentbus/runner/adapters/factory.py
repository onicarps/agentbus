"""Factory TurnAdapter — headless oneshot via `droid exec` (Phase D)."""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any, Callable, Sequence

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
) -> list[str]:
    """droid exec -f prompt.md --cwd … --auto … -o text"""
    cmd: list[str] = [
        droid_bin,
        "exec",
        "-f",
        str(prompt_path),
        "--cwd",
        str(cwd),
        "--auto",
        auto,
        "-o",
        output_format,
    ]
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
      auto: low|medium|high (default high — headless droid needs high for pytest/write)
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

    def start_turn(
        self, wake: WakeEnvelope, *, budget_remaining: int
    ) -> TurnResult:
        opts = self.options
        dry_run = bool(opts.get("dry_run", False))
        timeout = int(opts.get("timeout_seconds", 1800))
        # Headless QA routinely needs file writes + pytest; droid rejects medium.
        auto = str(opts.get("auto") or "high").strip().lower()
        if auto not in {"low", "medium", "high"}:
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

        if not dry_run:
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
        )

        env = os.environ.copy()
        env.setdefault("AGENTBUS_WORKSPACE", str(self.workspace))
        env.setdefault("AGENTBUS_PRODUCER_ID", "factory")

        if dry_run:
            return TurnResult(
                ok=True,
                summary=(
                    f"RUNNER_ACK: factory dry_run event_id={wake.event_id} "
                    f"cmd_bin={droid_bin} auto={auto}"
                ),
                detail={
                    "adapter": "factory",
                    "dry_run": True,
                    "cmd": cmd,
                    "prompt_path": str(prompt_path),
                    "prompt_preview": prompt[:500],
                    "cwd": str(workdir),
                },
            )

        log.info(
            "factory turn start event_id=%s timeout=%s auto=%s",
            wake.event_id,
            timeout,
            auto,
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
        final_text = stdout or stderr
        preview = final_text[-800:] if final_text else "(no output)"
        preview_one = " ".join(preview.split())

        if proc.returncode != 0:
            return TurnResult(
                ok=False,
                summary=(
                    f"RUNNER_ERROR: factory exit={proc.returncode} "
                    f"event_id={wake.event_id} out={preview_one[:400]}"
                ),
                detail={
                    "adapter": "factory",
                    "returncode": proc.returncode,
                    "stdout": stdout[-8000:],
                    "stderr": stderr[-8000:],
                    "prompt_path": str(prompt_path),
                    "cmd0": cmd[:6],
                },
            )

        return TurnResult(
            ok=True,
            summary=(
                f"RUNNER_ACK: factory completed event_id={wake.event_id} "
                f"out={preview_one[:500]}"
            ),
            detail={
                "adapter": "factory",
                "returncode": 0,
                "stdout": stdout[-8000:],
                "stderr": stderr[-4000:],
                "prompt_path": str(prompt_path),
            },
        )
