"""Freebuff TurnAdapter using a bounded pseudo-terminal interaction.

Freebuff is an interactive engineering UI, rather than a CLI with a supported
batch-prompt option. It can help with the assigned implementation work, but it
does not hold pre-push QA authority.
"""

from __future__ import annotations

import os
import pty
import select
import shutil
import signal
import subprocess
import time
from pathlib import Path
from typing import Any, Callable, Sequence

from agentbus.runner.artifacts import runner_artifact_dir
from agentbus.runner.adapters.prompt_common import build_cli_role_prompt, runner_subprocess_env
from agentbus.runner.types import TurnResult, WakeEnvelope

PtyRunFn = Callable[..., tuple[int, str, bool]]


def _premature_prompt_exit(
    returncode: int | None, output: list[bytes]
) -> tuple[int, str, bool]:
    """Return an honest failure when the child exits before task delivery."""
    observed_code = returncode if returncode not in (None, 0) else 125
    transcript = b"".join(output).decode("utf-8", "replace")
    detail = (
        "\nRUNNER_ERROR: freebuff process exited before task prompt delivery "
        f"(observed_exit={returncode!r}).\n"
    )
    return observed_code, transcript + detail, False


def build_freebuff_prompt(wake: WakeEnvelope, *, budget_remaining: int) -> str:
    prompt = build_cli_role_prompt(
        role_name="Freebuff", role_hint="interactive engineering assistant", wake=wake,
        budget_remaining=budget_remaining,
    )
    return prompt + "\n\n".join(
        [
            "",
            "## Engineering responsibilities",
            "",
            "Work only within the assigned implementation scope. Report useful "
            "engineering evidence and blockers to the requester, but do not claim "
            "or publish a pre-push QA verdict; that authority belongs to Pi/shared QA.",
        ]
    )


def build_freebuff_command(*, freebuff_bin: str, cwd: Path, trust_agents: bool,
                           extra_args: Sequence[str]) -> list[str]:
    cmd = [freebuff_bin, "--cwd", str(cwd)]
    if trust_agents:
        cmd.append("--trust-agents")
    cmd.extend(str(arg) for arg in extra_args)
    return cmd


def _run_pty(cmd: list[str], *, prompt: str, cwd: Path, env: dict[str, str],
             timeout: int, startup_seconds: float) -> tuple[int, str, bool]:
    """Deliver a task to the TUI and require both prompt delivery and exit."""
    master, slave = pty.openpty()
    proc: subprocess.Popen[bytes] | None = None
    output: list[bytes] = []
    try:
        proc = subprocess.Popen(cmd, cwd=str(cwd), env=env, stdin=slave,
            stdout=slave, stderr=slave, close_fds=True, start_new_session=True)
        os.close(slave)
        slave = -1
        started, prompt_sent = time.monotonic(), False
        while time.monotonic() - started < timeout:
            now = time.monotonic()
            if not prompt_sent and now - started >= startup_seconds:
                if proc.poll() is not None:
                    return _premature_prompt_exit(proc.returncode, output)
                try:
                    os.write(master, prompt.encode("utf-8", "replace") + b"\r")
                except OSError:
                    return _premature_prompt_exit(proc.returncode, output)
                prompt_sent = True
            ready, _, _ = select.select([master], [], [], 0.25)
            if ready:
                try:
                    chunk = os.read(master, 4096)
                except OSError:
                    chunk = b""
                if chunk:
                    output.append(chunk)
            if proc.poll() is not None:
                if not prompt_sent:
                    return _premature_prompt_exit(proc.returncode, output)
                return (
                    proc.returncode or 0,
                    b"".join(output).decode("utf-8", "replace"),
                    prompt_sent,
                )
        return 124, b"".join(output).decode("utf-8", "replace"), prompt_sent
    finally:
        if proc is not None and proc.poll() is None:
            try:
                os.killpg(proc.pid, signal.SIGTERM)
                proc.wait(timeout=5)
            except (OSError, subprocess.TimeoutExpired):
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except OSError:
                    pass
        if slave >= 0:
            os.close(slave)
        os.close(master)


class FreebuffAdapter:
    def __init__(self, *, workspace: Path, options: dict[str, Any] | None = None,
                 run_pty_fn: PtyRunFn | None = None) -> None:
        self.workspace = workspace.resolve()
        self.options = dict(options or {})
        self._run_pty_fn = run_pty_fn or _run_pty
        self._skip_bin_check = run_pty_fn is not None

    def start_turn(self, wake: WakeEnvelope, *, budget_remaining: int) -> TurnResult:
        opts = self.options
        binary = str(opts.get("command") or opts.get("freebuff_bin") or "freebuff")
        timeout = int(opts.get("timeout_seconds", 1200))
        startup = float(opts.get("startup_seconds", 3))
        extra = opts.get("extra_args") or []
        if not isinstance(extra, list):
            raise ValueError("adapter.extra_args must be a list")
        if startup < 0 or timeout <= startup:
            raise ValueError("invalid Freebuff PTY timing configuration")
        trust_agents = opts.get("trust_agents", False)
        if not isinstance(trust_agents, bool):
            raise ValueError("adapter.trust_agents must be a boolean")
        if not bool(opts.get("dry_run")) and not self._skip_bin_check:
            if shutil.which(binary) is None and not Path(binary).is_file():
                return TurnResult(ok=False, summary=f"RUNNER_ERROR: freebuff binary not found event_id={wake.event_id}")
        run_dir = runner_artifact_dir(
            self.workspace, opts, wake.event_id, fallback_runner_id="freebuff"
        )
        run_dir.mkdir(parents=True, exist_ok=True)
        prompt = build_freebuff_prompt(wake, budget_remaining=budget_remaining)
        prompt_path = run_dir / "freebuff-prompt.md"
        prompt_path.write_text(prompt, encoding="utf-8")
        cwd = Path(opts["cwd"]).resolve() if opts.get("cwd") else self.workspace
        cmd = build_freebuff_command(freebuff_bin=binary, cwd=cwd,
            trust_agents=trust_agents, extra_args=extra)
        if bool(opts.get("dry_run")):
            return TurnResult(summary=f"RUNNER_ACK: freebuff dry_run event_id={wake.event_id}", detail={"adapter": "freebuff", "cmd": cmd, "prompt_path": str(prompt_path)})
        code, transcript, prompt_sent = self._run_pty_fn(cmd, prompt=prompt, cwd=cwd,
            env=runner_subprocess_env(self.workspace, producer_id="freebuff", wake=wake),
            timeout=timeout, startup_seconds=startup)
        transcript_path = run_dir / "freebuff-transcript.txt"
        transcript_path.write_text(transcript[-12000:], encoding="utf-8")
        if not prompt_sent:
            return TurnResult(ok=False, summary=f"RUNNER_ERROR: freebuff process exited before prompt delivery code={code} event_id={wake.event_id}", detail={"adapter": "freebuff", "transcript_path": str(transcript_path)})
        if code != 0:
            return TurnResult(ok=False, summary=f"RUNNER_ERROR: freebuff exit={code} event_id={wake.event_id}", detail={"adapter": "freebuff", "transcript_path": str(transcript_path)})
        return TurnResult(summary=f"RUNNER_ACK: freebuff engineering turn delivered event_id={wake.event_id}", detail={"adapter": "freebuff", "prompt_path": str(prompt_path), "transcript_path": str(transcript_path)})
