from __future__ import annotations

import subprocess
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from agentbus.runner.adapters.pi import PiAdapter, build_pi_command
from agentbus.runner.types import WakeEnvelope


def wake() -> WakeEnvelope:
    summary = "inspect swarm health"
    return WakeEnvelope(
        event_id=9, topic="okf/handoff", from_agent="agy", to="pi",
        summary=summary,
        payload={"from": "agy", "to": "pi", "summary": summary},
        source="wake_file",
    )


def test_pi_command_defaults_to_no_trust_and_no_extensions(tmp_path: Path) -> None:
    cmd = build_pi_command(
        pi_bin="pi", prompt_path=tmp_path / "prompt.md", model=None,
        approve_project_files=False, load_extensions=False, extra_args=[],
    )
    assert cmd[:5] == ["pi", "--print", "--no-session", "--no-approve", "--no-extensions"]
    assert "--approve" not in cmd


def test_pi_approval_requires_explicit_config(tmp_path: Path) -> None:
    cmd = build_pi_command(
        pi_bin="pi", prompt_path=tmp_path / "prompt.md", model=None,
        approve_project_files=True, load_extensions=False, extra_args=[],
    )
    assert "--approve" in cmd
    with pytest.raises(ValueError, match="approve_project_files"):
        build_pi_command(
            pi_bin="pi", prompt_path=tmp_path / "prompt.md", model=None,
            approve_project_files=False, load_extensions=False,
            extra_args=["--approve"],
        )


def test_pi_adapter_success(tmp_path: Path) -> None:
    run = MagicMock(
        return_value=subprocess.CompletedProcess(
            args=["pi"], returncode=0, stdout="SRE_STATUS: healthy", stderr="",
        )
    )
    result = PiAdapter(workspace=tmp_path, run_fn=run).start_turn(
        wake(), budget_remaining=3,
    )
    assert result.ok
    assert "--no-approve" in run.call_args.args[0]
