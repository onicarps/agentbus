"""OSS example scripts must exit 0 — PDD v0.8 AC3."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

EXAMPLES = sorted((Path(__file__).resolve().parents[1] / "examples").glob("[0-9]*.py"))


def test_all_examples_exit_zero():
    root = Path(__file__).resolve().parents[1]
    for script in EXAMPLES:
        proc = subprocess.run(
            [sys.executable, str(script)],
            cwd=root,
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert proc.returncode == 0, (
            f"{script.name} failed:\nstdout={proc.stdout}\nstderr={proc.stderr}"
        )