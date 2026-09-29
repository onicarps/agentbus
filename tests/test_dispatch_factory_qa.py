"""Contract tests for Pi/shared-QA pre-push dispatch routing."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "dispatch_factory_qa.py"


def _template(workspace: Path) -> Path:
    path = workspace / "initiatives" / "agentbus" / "missions" / "template.md"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("Check {{initiative}} at {{git_head}}.", encoding="utf-8")
    return path


def _dispatch(workspace: Path, *extra: str) -> dict:
    repo = Path(__file__).resolve().parents[1]
    proc = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--workspace",
            str(workspace),
            "--repo",
            str(repo),
            "--title",
            "Pi dispatch contract",
            "--mission",
            str(_template(workspace)),
            "--dry-run",
            *extra,
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(proc.stdout)


def test_default_executor_targets_pi(tmp_path: Path) -> None:
    dispatched = _dispatch(tmp_path)
    assert dispatched["payload"]["to"] == "pi"
    assert dispatched["payload"]["summary"].startswith("PI_QA_MISSION:")
    assert "executor: pi" in Path(dispatched["mission_path"]).read_text(encoding="utf-8")


def test_executor_compatibility_selectors_all_target_pi(tmp_path: Path) -> None:
    for executor in ("auto", "pi", "qa"):
        dispatched = _dispatch(tmp_path, "--executor", executor)
        assert dispatched["payload"]["to"] == "pi"
        assert dispatched["payload"]["summary"].startswith("PI_QA_MISSION:")
        assert "executor: pi" in Path(dispatched["mission_path"]).read_text(encoding="utf-8")


def test_parked_and_non_qa_executors_are_rejected(tmp_path: Path) -> None:
    for executor in ("factory", "freebuff"):
        proc = subprocess.run(
            [
                sys.executable,
                str(SCRIPT),
                "--workspace",
                str(tmp_path),
                "--title",
                "invalid executor",
                "--executor",
                executor,
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        assert proc.returncode == 2
        assert "invalid choice" in proc.stderr


def test_traversal_initiative_is_rejected(tmp_path: Path) -> None:
    proc = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--workspace",
            str(tmp_path),
            "--title",
            "invalid initiative",
            "--initiative",
            "../outside",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 2
    assert "single initiative directory name" in proc.stderr


def test_explicit_pi_is_the_only_supported_executor(tmp_path: Path) -> None:
    dispatched = _dispatch(tmp_path, "--executor", "pi")
    assert dispatched["payload"]["to"] == "pi"
    assert dispatched["to"] == "pi"
    assert dispatched["published"] is False
