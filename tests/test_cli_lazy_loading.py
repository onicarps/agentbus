"""Three-tier CLI import and projected-status contracts."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

from agentbus.fast_cli import _fast_status
from agentbus.store import EventStore


def test_cli_import_does_not_load_tier_two_or_cockpit_dependencies():
    root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(root / "src")
    code = """
import json, sys
import agentbus.cli
blocked = ('mcp', 'cryptography', 'jsonschema', 'rich', 'textual', 'uvicorn')
print(json.dumps(sorted(name for name in sys.modules if name.split('.')[0] in blocked)))
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=root,
        env=env,
        text=True,
        capture_output=True,
        check=True,
    )
    assert json.loads(result.stdout) == []


def test_fast_status_reads_projection_without_click_or_event_scan(tmp_path, capsys):
    store = EventStore(tmp_path, auto_prune=False)
    store.publish(
        topic="okf/handoff",
        producer_id="codex",
        schema_version="1.0",
        payload={"from": "codex", "to": "agy", "summary": "fast"},
        skip_rbac=True,
    )
    store.close()

    assert _fast_status(["--workspace", str(tmp_path), "--producer-id", "codex"])
    output = json.loads(capsys.readouterr().out)
    assert output["event_count"] == 1
    assert output["latest_event_id"] == 1
    assert output["topics"] == ["okf/handoff"]
    assert output["producer_id"] == "codex"
