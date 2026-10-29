"""MCP stdio handshake verification: real child processes, real reaping."""

from __future__ import annotations

import os
import sys
from pathlib import Path


from agentbus.discovery import HandshakeResult, verify_mcp_handshake
from agentbus.store import EventStore


def _pid_alive(pid: int) -> bool:
    if os.name == "nt":  # pragma: no cover - Windows has no os.kill(pid, 0)
        import pytest

        pytest.skip("PID liveness probe is POSIX-only")
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _write_server(tmp_path: Path, *, mode: str = "ok") -> tuple[Path, Path]:
    pid_file = tmp_path / "server.pid"
    script = tmp_path / "fake_mcp_server.py"
    if mode == "ok":
        body = '''
import json, os, sys
pid_file = sys.argv[1]
open(pid_file, "w").write(str(os.getpid()))
def send(message):
    sys.stdout.write(json.dumps(message) + "\\n")
    sys.stdout.flush()
for line in sys.stdin:
    message = json.loads(line)
    method = message.get("method")
    if method == "initialize":
        send({"jsonrpc": "2.0", "id": message["id"], "result": {
            "protocolVersion": "2025-06-18",
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "fake-agentbus", "version": "9.9"},
        }})
    elif method == "tools/list":
        send({"jsonrpc": "2.0", "id": message["id"], "result": {"tools": [
            {"name": "agentbus_publish"}, {"name": "agentbus_poll"},
        ]}})
'''
    elif mode == "silent":
        body = '''
import os, sys, time
pid_file = sys.argv[1]
open(pid_file, "w").write(str(os.getpid()))
time.sleep(60)
'''
    else:  # exits immediately
        body = '''
import os, sys
pid_file = sys.argv[1]
open(pid_file, "w").write(str(os.getpid()))
sys.exit(3)
'''
    script.write_text(body, encoding="utf-8")
    return script, pid_file


def test_handshake_succeeds_and_reaps_the_child(tmp_path: Path) -> None:
    script, pid_file = _write_server(tmp_path)

    result = verify_mcp_handshake(
        [sys.executable, str(script), str(pid_file)], cwd=tmp_path, timeout=20
    )

    assert isinstance(result, HandshakeResult)
    assert result.ok is True, result.error
    assert result.server_name == "fake-agentbus"
    assert result.server_version == "9.9"
    assert result.protocol_version == "2025-06-18"
    assert result.tool_names == ("agentbus_publish", "agentbus_poll")
    assert result.tool_count == 2
    pid = int(pid_file.read_text())
    assert _pid_alive(pid) is False, "verification leaked a child process"


def test_handshake_times_out_and_reaps_the_child(tmp_path: Path, monkeypatch) -> None:
    import agentbus.discovery as discovery

    script, pid_file = _write_server(tmp_path, mode="silent")
    real_popen = discovery.subprocess.Popen
    children = []

    def capture(*args, **kwargs):
        child = real_popen(*args, **kwargs)
        children.append(child)
        return child

    monkeypatch.setattr(discovery.subprocess, "Popen", capture)

    result = verify_mcp_handshake(
        [sys.executable, str(script), str(pid_file)], cwd=tmp_path, timeout=0.5
    )

    assert result.ok is False
    assert "TimeoutError" in (result.error or "")
    assert len(children) == 1
    assert children[0].poll() is not None, "timed out verification leaked a child"


def test_handshake_reports_early_exit(tmp_path: Path) -> None:
    script, pid_file = _write_server(tmp_path, mode="exit")

    result = verify_mcp_handshake(
        [sys.executable, str(script), str(pid_file)], cwd=tmp_path, timeout=5
    )

    assert result.ok is False
    assert "exited" in (result.error or "")


def test_handshake_validates_but_never_writes_client_config(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "ws"
    workspace.mkdir()
    before = sorted(str(p.relative_to(workspace)) for p in workspace.rglob("*"))
    script, pid_file = _write_server(tmp_path)

    result = verify_mcp_handshake(
        [sys.executable, str(script), str(pid_file)],
        workspace=workspace,
        timeout=20,
    )

    assert result.ok is True, result.error
    after = sorted(str(p.relative_to(workspace)) for p in workspace.rglob("*"))
    assert after == before
    assert not (workspace / ".agentbus").exists()


def test_handshake_against_real_agentbus_server(tmp_path: Path) -> None:
    """End-to-end proof the configured argv really speaks MCP."""
    workspace = tmp_path / "ws"
    workspace.mkdir()
    EventStore(workspace).close()

    result = verify_mcp_handshake(
        [
            sys.executable,
            "-m",
            "agentbus.cli",
            "mcp-serve",
            "--workspace",
            str(workspace),
        ],
        workspace=workspace,
        timeout=45,
    )

    assert result.ok is True, result.error
    assert "agentbus_publish" in result.tool_names
    assert "agentbus_poll" in result.tool_names
    assert result.tool_count == len(result.tool_names) >= 2


def test_handshake_rejects_empty_argv() -> None:
    result = verify_mcp_handshake([])
    assert result.ok is False
    assert result.error == "empty argv"
