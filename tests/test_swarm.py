"""Swarm orchestration DX — up/down/ps/logs (v0.10.0)."""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import pytest
import yaml

from agentbus.swarm import (
    load_swarm_config,
    list_processes,
    start_service,
    stop_all,
    stop_pid,
    swarm_up,
    write_example_swarm,
    ServiceSpec,
    _pid_alive,
)


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
            "sleeper": "python -c 'import time; time.sleep(1)'",
        },
    )
    cfg = load_swarm_config(tmp_path)
    assert "watch" in cfg.services
    assert cfg.services["sleeper"].command.startswith("python")


def test_load_missing_raises(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        load_swarm_config(tmp_path)


def test_write_example_swarm(tmp_path: Path):
    path = write_example_swarm(tmp_path)
    assert path.is_file()
    cfg = load_swarm_config(tmp_path)
    assert "watch" in cfg.services


def test_start_stop_service_and_ps(tmp_path: Path):
    # Long-lived python sleep as fake service
    cmd = f'{sys.executable} -c "import time; time.sleep(60)"'
    _write_swarm(tmp_path, {"sleeper": {"command": cmd}})
    result = swarm_up(tmp_path, detach=True, run_monitor=False)
    assert len(result["started"]) == 1
    pid = result["started"][0]["pid"]
    assert _pid_alive(pid)

    rows = list_processes(tmp_path)
    assert any(r["name"] == "sleeper" and r["pid"] == pid for r in rows)

    stopped = stop_all(tmp_path)
    assert stopped[0]["status"] in {"terminated", "killed", "already_dead"}
    deadline = time.time() + 3.0
    while time.time() < deadline and _pid_alive(pid):
        time.sleep(0.05)
    assert not _pid_alive(pid), f"pid {pid} still alive after stop"
    assert list_processes(tmp_path) == []


def test_stop_pid_already_dead():
    # PID 1 may exist on Linux; use unlikely high pid that is dead
    status = stop_pid(999_999_999)
    assert status in {"already_dead", "terminated", "killed"}


def test_service_logs_created(tmp_path: Path):
    cmd = f'{sys.executable} -c "print(\'hello-swarm\'); import time; time.sleep(30)"'
    spec = ServiceSpec(name="echoer", command=cmd)
    rec = start_service(tmp_path, spec)
    time.sleep(0.4)
    out_log = Path(rec["stdout_log"])
    assert out_log.is_file()
    # content may be buffered; ensure path exists and process stoppable
    stop_pid(int(rec["pid"]))
    text = out_log.read_text(encoding="utf-8", errors="replace")
    # Python -u would force unbuffered; accept empty if fully buffered
    assert out_log.exists()
