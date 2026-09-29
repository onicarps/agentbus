from __future__ import annotations

import sys
from pathlib import Path

import pytest

from agentbus.runner.adapters import get_adapter
from agentbus.runner.adapters.freebuff import (
    FreebuffAdapter,
    _run_pty,
    build_freebuff_command,
    build_freebuff_prompt,
)
from agentbus.runner.config import SUPPORTED_ADAPTERS
from agentbus.runner.types import WakeEnvelope


def wake() -> WakeEnvelope:
    return WakeEnvelope(event_id=41, topic="okf/handoff", from_agent="codex", to="freebuff", summary="review the QA mission", payload={"from": "codex", "to": "freebuff", "summary": "review the QA mission"}, source="wake_file")


def test_freebuff_command_requires_explicit_trust_opt_in(tmp_path: Path) -> None:
    assert build_freebuff_command(
        freebuff_bin="freebuff", cwd=tmp_path, trust_agents=False, extra_args=[]
    ) == ["freebuff", "--cwd", str(tmp_path)]
    assert build_freebuff_command(
        freebuff_bin="freebuff", cwd=tmp_path, trust_agents=True, extra_args=[]
    ) == ["freebuff", "--cwd", str(tmp_path), "--trust-agents"]


def test_freebuff_prompt_excludes_qa_certification() -> None:
    prompt = build_freebuff_prompt(wake(), budget_remaining=3)
    assert "FREEBUFF_QA_VERDICT" not in prompt
    assert "do not claim or publish a pre-push QA verdict" in prompt


def test_freebuff_delivery_uses_no_idle_success_heuristic_or_trust(tmp_path: Path) -> None:
    captured: dict = {}

    def run_pty(*args, **kwargs):
        captured["cmd"] = args[0]
        captured.update(kwargs)
        return 0, "interactive review", True

    result = FreebuffAdapter(workspace=tmp_path, run_pty_fn=run_pty).start_turn(wake(), budget_remaining=2)
    assert result.ok
    assert "engineering turn delivered" in result.summary
    assert captured["env"]["AGENTBUS_PRODUCER_ID"] == "freebuff"
    assert "idle_completion_seconds" not in captured
    assert "--trust-agents" not in captured["cmd"]


def test_freebuff_timeout_is_an_error_not_a_clean_delivery(tmp_path: Path) -> None:
    result = FreebuffAdapter(
        workspace=tmp_path, run_pty_fn=lambda *_args, **_kwargs: (124, "silent", False)
    ).start_turn(wake(), budget_remaining=2)
    assert result.ok is False
    assert "before prompt delivery code=124" in result.summary


def test_freebuff_requires_boolean_trust_option(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="trust_agents"):
        FreebuffAdapter(
            workspace=tmp_path,
            options={"trust_agents": "false"},
            run_pty_fn=lambda *_args, **_kwargs: (0, "ignored", True),
        ).start_turn(wake(), budget_remaining=2)


def test_freebuff_adapter_is_registered(tmp_path: Path) -> None:
    assert "freebuff" in SUPPORTED_ADAPTERS
    assert type(get_adapter("freebuff", workspace=tmp_path)).__name__ == "FreebuffAdapter"


def test_freebuff_early_exit_before_prompt_is_not_delivery(tmp_path: Path) -> None:
    result = FreebuffAdapter(
        workspace=tmp_path,
        run_pty_fn=lambda *_args, **_kwargs: (0, "", False),
    ).start_turn(wake(), budget_remaining=2)
    assert result.ok is False
    assert "before prompt delivery code=0" in result.summary


def test_pty_early_clean_exit_is_converted_to_delivery_error(tmp_path: Path) -> None:
    code, transcript, prompt_sent = _run_pty(
        [sys.executable, "-c", "pass"],
        prompt="do the work",
        cwd=tmp_path,
        env={},
        timeout=2,
        startup_seconds=0.05,
    )
    assert code == 125
    assert prompt_sent is False
    assert "exited before task prompt delivery" in transcript
