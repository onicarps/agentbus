"""Swarm orchestration DX — up/down/ps/logs (v0.10.0)."""

from __future__ import annotations

import shlex
import sys
import time
from pathlib import Path

import pytest
import yaml

from agentbus.swarm import (
    ServiceSpec,
    _pid_alive,
    list_processes,
    load_swarm_config,
    start_service,
    stop_all,
    stop_pid,
    swarm_up,
    write_example_swarm,
)


def _py_cmd(*code_parts: str) -> str:
    """Build a Windows-safe shell command running sys.executable -c ..."""
    code = "; ".join(code_parts)
    return f"{shlex.quote(sys.executable)} -c {shlex.quote(code)}"


def _write_swarm(ws: Path, services: dict) -> Path:
    ab = ws / ".agentbus"
    ab.mkdir(parents=True, exist_ok=True)
    path = ab / "swarm.yaml"
    path.write_text(
        yaml.safe_dump({"version": "1.0", "services": services}, sort_keys=False),
        encoding="utf-8",
    )
    return path


def test_load_swarm_config(tmp_path: Path):
    _write_swarm(
        tmp_path,
        {
            "watch": {"command": "agentbus watch"},
            "sleeper": _py_cmd("import time", "time.sleep(1)"),
        },
    )
    cfg = load_swarm_config(tmp_path)
    assert "watch" in cfg.services
    assert "python" in cfg.services["sleeper"].command or sys.executable in cfg.services[
        "sleeper"
    ].command


def test_load_missing_raises(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        load_swarm_config(tmp_path)


def test_write_example_swarm(tmp_path: Path):
    path = write_example_swarm(tmp_path)
    assert path.is_file()
    cfg = load_swarm_config(tmp_path)
    assert "watch" in cfg.services


def test_start_stop_service_and_ps(tmp_path: Path):
    cmd = _py_cmd("import time", "time.sleep(60)")
    _write_swarm(tmp_path, {"sleeper": {"command": cmd}})
    result = swarm_up(tmp_path, detach=True, run_monitor=False)
    assert len(result["started"]) == 1
    pid = result["started"][0]["pid"]
    assert _pid_alive(pid)

    rows = list_processes(tmp_path)
    assert any(r["name"] == "sleeper" and r["pid"] == pid for r in rows)

    stopped = stop_all(tmp_path)
    assert stopped[0]["status"] in {"terminated", "killed"}
    deadline = time.time() + 3.0
    while time.time() < deadline and _pid_alive(pid):
        time.sleep(0.05)
    assert not _pid_alive(pid), f"pid {pid} still alive after stop"
    assert list_processes(tmp_path) == []


def test_stop_pid_already_dead():
    status = stop_pid(999_999_999)
    assert status == "already_dead"


def test_service_logs_created(tmp_path: Path):
    # -u for unbuffered so log content is visible
    snippet = 'print("hello-swarm"); import time; time.sleep(30)'
    cmd = f"{shlex.quote(sys.executable)} -u -c {shlex.quote(snippet)}"
    spec = ServiceSpec(name="echoer", command=cmd)
    rec = start_service(tmp_path, spec)
    time.sleep(0.5)
    out_log = Path(rec["stdout_log"])
    assert out_log.is_file()
    stop_pid(int(rec["pid"]))
    text = out_log.read_text(encoding="utf-8", errors="replace")
    assert "hello-swarm" in text
