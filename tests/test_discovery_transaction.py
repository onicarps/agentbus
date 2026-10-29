"""Serialized setup, staged writes, rollback, and connection semantics."""

from __future__ import annotations

import json
import os
import stat
import subprocess
import sys
import threading
import time
import tomllib
from pathlib import Path

import pytest

from agentbus import discovery as discovery_module
from agentbus.adapters.command import McpCommand
from agentbus.adapters.manifest import build_entry, install_target
from agentbus.adapters.writer import (
    ConfigConflictError,
    plan_json_patch,
    plan_toml_patch,
    plan_patch,
)
from agentbus.discovery import (
    ACTION_CREATE,
    ACTION_UNCHANGED,
    ACTION_UPDATE,
    EXIT_LOCK_TIMEOUT,
    EXIT_OK,
    STATE_AWAITING,
    STATE_CONFIGURED,
    STATE_ERROR,
    STATE_PLANNED,
    STATE_REFUSED,
    STATE_SKIPPED,
    ConcurrentEditError,
    FileTransaction,
    SetupWriteError,
    connect,
    setup_lock,
)

FAKE_COMMAND = McpCommand(("/usr/bin/agentbus", "mcp-serve"), "test")


@pytest.fixture(autouse=True)
def isolated_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("USERPROFILE", str(home))
    for name in (
        "AGENTBUS_WORKSPACE",
        "AGENTBUS_PRODUCER_ID",
        "AGENTBUS_MCP_COMMAND",
        "CODEX_HOME",
        "XDG_CONFIG_HOME",
    ):
        monkeypatch.delenv(name, raising=False)
    return home


def _no_which(_name: str) -> None:
    return None


def _run(workspace: Path, home: Path, **kwargs):
    kwargs.setdefault("command", FAKE_COMMAND)
    kwargs.setdefault("which", _no_which)
    kwargs.setdefault("home", home)
    kwargs.setdefault("platform", "linux")
    kwargs.setdefault("environ", {"HOME": str(home)})
    return connect(workspace=workspace, **kwargs)


def snapshot(root: Path) -> dict[str, bytes | None]:
    state: dict[str, bytes | None] = {}
    for path in sorted(root.rglob("*")):
        state[str(path.relative_to(root))] = (
            path.read_bytes() if path.is_file() else None
        )
    return state


@pytest.fixture
def spawn_process():
    """Spawn subprocesses and guarantee deterministic reaping in teardown."""
    procs: list[subprocess.Popen] = []

    def spawn(argv, **kwargs):
        proc = subprocess.Popen(argv, **kwargs)
        procs.append(proc)
        return proc

    yield spawn

    for proc in procs:
        if proc.poll() is None:
            proc.terminate()
        try:
            proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=2)
    leaked = [proc.pid for proc in procs if proc.poll() is None]
    assert not leaked, f"test leaked child processes: {leaked}"


def _cursor_workspace(tmp_path: Path, content: str = '{"mcpServers": {}}\n') -> Path:
    workspace = tmp_path / "proj"
    config = workspace / ".cursor" / "mcp.json"
    config.parent.mkdir(parents=True)
    config.write_text(content, encoding="utf-8")
    return workspace


def test_preview_writes_nothing_and_creates_no_lock(
    tmp_path: Path, isolated_home: Path
) -> None:
    workspace = _cursor_workspace(tmp_path)
    before = snapshot(workspace)

    report = _run(
        workspace, isolated_home, clients=["cursor-project"], apply=False
    )

    assert report.exit_code == EXIT_OK
    assert report.dry_run is True
    item = next(i for i in report.items if i.installation_id == "cursor-project")
    assert item.action == ACTION_UPDATE  # file exists, entry missing
    assert item.state == STATE_PLANNED
    assert snapshot(workspace) == before
    assert not (workspace / ".agentbus").exists()

    # A bare client directory is weak evidence: it must not authorize a write
    # (status unknown -> explicit request refuses).
    fresh = tmp_path / "fresh"
    (fresh / ".cursor").mkdir(parents=True)
    before_fresh = snapshot(fresh)
    weak = _run(fresh, isolated_home, clients=["cursor-project"], apply=False)
    assert weak.exit_code != EXIT_OK
    assert weak.items[0].state == STATE_REFUSED
    assert weak.items[0].reason.startswith("weak_evidence")
    assert snapshot(fresh) == before_fresh

    # A detected client binary with no config file previews a create.
    detected = tmp_path / "detected"
    detected.mkdir()
    created = _run(
        detected,
        isolated_home,
        clients=["claude-code-project"],
        apply=False,
        which=lambda name: "/usr/bin/claude" if name == "claude" else None,
    )
    created_item = created.items[0]
    assert created_item.action == ACTION_CREATE
    assert not (detected / ".mcp.json").exists()


def test_apply_preserves_unrelated_config_and_is_idempotent(
    tmp_path: Path, isolated_home: Path
) -> None:
    workspace = tmp_path / "proj"
    config = workspace / ".cursor" / "mcp.json"
    config.parent.mkdir(parents=True)
    config.write_text(
        json.dumps(
            {
                "editor.fontSize": 13,
                "mcpServers": {"team": {"command": "team-server"}},
            }
        )
        + "\n",
        encoding="utf-8",
    )

    first = _run(workspace, isolated_home, clients=["cursor-project"], apply=True)

    assert first.exit_code == EXIT_OK, first.errors
    item = next(i for i in first.items if i.installation_id == "cursor-project")
    assert item.state == STATE_AWAITING
    assert item.configured is True
    assert item.backup is not None
    assert Path(item.backup).is_file()

    data = json.loads(config.read_text(encoding="utf-8"))
    assert data["editor.fontSize"] == 13
    assert data["mcpServers"]["team"] == {"command": "team-server"}
    assert data["mcpServers"]["agentbus"]["command"] == "/usr/bin/agentbus"
    assert data["mcpServers"]["agentbus"]["args"][-2:] == [
        "--workspace",
        str(workspace.resolve()),
    ]

    before_second = config.read_bytes()
    second = _run(workspace, isolated_home, clients=["cursor-project"], apply=True)

    assert second.exit_code == EXIT_OK, second.errors
    again = next(i for i in second.items if i.installation_id == "cursor-project")
    assert again.action == ACTION_UNCHANGED
    assert again.state == STATE_CONFIGURED
    assert again.backup is None
    assert config.read_bytes() == before_second
    # The first backup is never overwritten or duplicated.
    backups = list(config.parent.glob("*.agentbus.bak.*"))
    assert len(backups) == 1
    original = (
        json.dumps(
            {
                "editor.fontSize": 13,
                "mcpServers": {"team": {"command": "team-server"}},
            }
        )
        + "\n"
    )
    assert backups[0].read_bytes() == original.encode()


def test_new_config_gets_restricted_permissions(
    tmp_path: Path, isolated_home: Path
) -> None:
    workspace = tmp_path / "proj"
    workspace.mkdir()

    report = _run(
        workspace,
        isolated_home,
        clients=["claude-code-project"],
        apply=True,
        which=lambda name: "/usr/bin/claude" if name == "claude" else None,
    )

    assert report.exit_code == EXIT_OK, report.errors
    config = workspace / ".mcp.json"
    assert config.is_file()
    assert stat.S_IMODE(config.stat().st_mode) == 0o600


def test_existing_permissions_are_preserved(
    tmp_path: Path, isolated_home: Path
) -> None:
    workspace = _cursor_workspace(tmp_path)
    config = workspace / ".cursor" / "mcp.json"
    config.chmod(0o640)

    _run(workspace, isolated_home, clients=["cursor-project"], apply=True)

    assert stat.S_IMODE(config.stat().st_mode) == 0o640


def test_conflicting_config_refuses_without_writing(
    tmp_path: Path, isolated_home: Path
) -> None:
    workspace = _cursor_workspace(tmp_path, '{"mcpServers": []}\n')
    config = workspace / ".cursor" / "mcp.json"
    before = config.read_bytes()

    report = _run(workspace, isolated_home, clients=["cursor-project"], apply=True)

    assert report.exit_code != EXIT_OK
    item = next(i for i in report.items if i.installation_id == "cursor-project")
    assert item.state == STATE_REFUSED
    assert "conflicting_agentbus_entry" in item.reason
    assert config.read_bytes() == before
    assert not (workspace / ".agentbus").exists()


def test_absent_client_is_skipped_unless_explicitly_requested(
    tmp_path: Path, isolated_home: Path
) -> None:
    workspace = tmp_path / "proj"
    workspace.mkdir()

    implicit = _run(workspace, isolated_home, apply=True)
    assert implicit.exit_code == EXIT_OK
    assert {i.state for i in implicit.items} == {STATE_SKIPPED}
    assert not (workspace / ".cursor").exists()
    assert not (workspace / ".agentbus").exists()

    explicit = _run(workspace, isolated_home, clients=["cursor-project"], apply=True)
    assert explicit.exit_code != EXIT_OK
    item = explicit.items[0]
    assert item.state == STATE_REFUSED
    assert item.reason == "client_not_detected"
    assert not (workspace / ".cursor").exists()


def test_user_scope_requires_explicit_authorization(
    tmp_path: Path, isolated_home: Path
) -> None:
    workspace = tmp_path / "proj"
    workspace.mkdir()
    global_config = isolated_home / ".cursor" / "mcp.json"
    global_config.parent.mkdir(parents=True)
    global_config.write_text('{"sentinel": true}\n', encoding="utf-8")
    before = global_config.read_bytes()

    refused = _run(workspace, isolated_home, clients=["cursor-user"], apply=True)
    assert refused.exit_code != EXIT_OK
    assert refused.items[0].reason == "user_scope_requires_authorization"
    assert global_config.read_bytes() == before

    allowed = _run(
        workspace,
        isolated_home,
        clients=["cursor-user"],
        scope="user",
        apply=True,
        allow_user_scope=True,
    )
    assert allowed.exit_code == EXIT_OK, allowed.errors
    data = json.loads(global_config.read_text(encoding="utf-8"))
    assert data["sentinel"] is True
    assert "agentbus" in data["mcpServers"]


def test_unresolvable_command_refuses_to_write(
    tmp_path: Path, isolated_home: Path
) -> None:
    workspace = _cursor_workspace(tmp_path)
    config = workspace / ".cursor" / "mcp.json"
    before = config.read_bytes()

    report = connect(
        workspace=workspace,
        clients=["cursor-project"],
        apply=True,
        home=isolated_home,
        platform="linux",
        environ={"HOME": str(isolated_home)},
        which=_no_which,
        python=str(tmp_path / "missing-python"),
    )

    assert report.exit_code != EXIT_OK
    assert report.command_error is not None
    assert report.items[0].reason == "no_durable_mcp_command"
    assert config.read_bytes() == before


def test_verify_requires_apply(tmp_path: Path, isolated_home: Path) -> None:
    workspace = _cursor_workspace(tmp_path)
    report = _run(
        workspace, isolated_home, clients=["cursor-project"], verify=True
    )
    assert report.verification["detail"] == "requires_apply"
    assert report.exit_code == EXIT_OK


def test_lock_timeout_is_reported_and_writes_nothing(
    tmp_path: Path, isolated_home: Path
) -> None:
    workspace = _cursor_workspace(tmp_path)
    config = workspace / ".cursor" / "mcp.json"
    before = config.read_bytes()
    flag = tmp_path / "locked.flag"
    script = (
        "import sys, time\n"
        "from pathlib import Path\n"
        "from agentbus.discovery import setup_lock\n"
        "ws = Path(sys.argv[1]); flag = Path(sys.argv[2])\n"
        "lock = setup_lock(ws, timeout=5)\n"
        "lock.acquire()\n"
        "flag.write_text('locked')\n"
        "time.sleep(float(sys.argv[3]))\n"
        "lock.release()\n"
    )
    holder = subprocess.Popen(
        [sys.executable, "-c", script, str(workspace), str(flag), "5"],
        cwd=str(Path(__file__).resolve().parents[1]),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        deadline = time.monotonic() + 10
        while not flag.exists() and time.monotonic() < deadline:
            time.sleep(0.02)
        assert flag.exists(), "lock holder never started"

        report = _run(
            workspace,
            isolated_home,
            clients=["cursor-project"],
            apply=True,
            lock_timeout=0.2,
        )

        assert report.exit_code == EXIT_LOCK_TIMEOUT
        assert report.items[0].reason == "setup_lock_timeout"
        assert config.read_bytes() == before
    finally:
        holder.terminate()
        holder.wait(timeout=5)


def test_setup_lock_is_reentrant_and_released_on_exception(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "ws"
    outer = setup_lock(workspace, timeout=1)
    outer.acquire()
    inner = setup_lock(workspace, timeout=0.01)
    inner.acquire()  # same process: reentrant, must not time out
    inner.release()

    other_process_can_acquire = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import sys\n"
                "from pathlib import Path\n"
                "from agentbus.discovery import setup_lock, SetupLockTimeout\n"
                "try:\n"
                "    setup_lock(Path(sys.argv[1]), timeout=0.2).acquire()\n"
                "except SetupLockTimeout:\n"
                "    sys.exit(7)\n"
                "sys.exit(0)\n"
            ),
            str(workspace),
        ],
        cwd=str(Path(__file__).resolve().parents[1]),
        capture_output=True,
        text=True,
        check=False,
    )
    assert other_process_can_acquire.returncode == 7  # still held

    outer.release()
    freed = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import sys\n"
                "from pathlib import Path\n"
                "from agentbus.discovery import setup_lock\n"
                "setup_lock(Path(sys.argv[1]), timeout=1).acquire()\n"
                "sys.exit(0)\n"
            ),
            str(workspace),
        ],
        cwd=str(Path(__file__).resolve().parents[1]),
        capture_output=True,
        text=True,
        check=False,
    )
    assert freed.returncode == 0
    with pytest.raises(RuntimeError, match="boom"):
        with setup_lock(workspace, timeout=1):
            raise RuntimeError("boom")
    after_exception = subprocess.run(
        freed.args, cwd=str(Path(__file__).resolve().parents[1]),
        capture_output=True, text=True, check=False,
    )
    assert after_exception.returncode == 0


def test_transaction_restores_files_when_a_later_write_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, isolated_home: Path
) -> None:
    workspace = tmp_path / "proj"
    cursor = workspace / ".cursor" / "mcp.json"
    vscode = workspace / ".vscode" / "mcp.json"
    cursor.parent.mkdir(parents=True)
    vscode.parent.mkdir(parents=True)
    cursor.write_text('{"mcpServers": {}}\n', encoding="utf-8")
    vscode.write_text('{"servers": {}}\n', encoding="utf-8")
    original_cursor = cursor.read_bytes()
    original_vscode = vscode.read_bytes()

    real_write = discovery_module._atomic_write

    def flaky(path: Path, data: bytes, mode: int) -> None:
        if path == vscode:
            raise OSError("simulated disk full")
        return real_write(path, data, mode)

    monkeypatch.setattr(discovery_module, "_atomic_write", flaky)

    report = _run(
        workspace,
        isolated_home,
        clients=["cursor-project", "vscode"],
        apply=True,
    )

    assert report.exit_code != EXIT_OK
    assert {item.state for item in report.items} == {STATE_ERROR}
    assert any("disk full" in error for error in report.errors)
    assert cursor.read_bytes() == original_cursor
    assert vscode.read_bytes() == original_vscode
    assert not list(workspace.rglob("*.agentbus-tmp"))
    # A backup of the first file may exist; it must not have replaced the file.
    assert json.loads(cursor.read_text(encoding="utf-8")) == {"mcpServers": {}}


def test_rollback_failure_is_surfaced(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    first = tmp_path / "a.json"
    second = tmp_path / "b.json"
    first.write_text('{"a": 1}\n', encoding="utf-8")
    second.write_text('{"b": 2}\n', encoding="utf-8")

    calls = {"n": 0}
    real_write = discovery_module._atomic_write

    def failing_everywhere(path: Path, data: bytes, mode: int) -> None:
        calls["n"] += 1
        if calls["n"] == 1:
            return real_write(path, data, mode)
        raise OSError("read-only filesystem")

    monkeypatch.setattr(discovery_module, "_atomic_write", failing_everywhere)

    transaction = FileTransaction()
    transaction.stage(first, b'{"a": 99}\n', original_bytes=first.read_bytes())
    transaction.stage(second, b'{"b": 99}\n', original_bytes=second.read_bytes())

    with pytest.raises(SetupWriteError) as excinfo:
        transaction.commit()

    assert excinfo.value.rollback_errors
    assert "rollback" in str(excinfo.value)
    # The rollback itself failed, so the first file keeps the new bytes and the
    # error names the file that could not be restored.
    assert first.read_text(encoding="utf-8") == '{"a": 99}\n'
    assert any(str(first) in error for error in excinfo.value.rollback_errors)
    # The second file never reached the replace step.
    assert second.read_text(encoding="utf-8") == '{"b": 2}\n'


def test_concurrent_edit_is_detected_before_overwrite(tmp_path: Path) -> None:
    path = tmp_path / "mcp.json"
    path.write_text('{"mcpServers": {}}\n', encoding="utf-8")
    planned_original = path.read_bytes()
    path.write_text('{"mcpServers": {"team": {}}}\n', encoding="utf-8")

    transaction = FileTransaction()
    transaction.stage(
        path, b'{"mcpServers": {"agentbus": {}}}\n', original_bytes=planned_original
    )

    with pytest.raises(SetupWriteError) as excinfo:
        transaction.commit()

    assert isinstance(excinfo.value.cause, ConcurrentEditError)
    assert json.loads(path.read_text(encoding="utf-8")) == {
        "mcpServers": {"team": {}}
    }


def test_connect_never_creates_a_lock_for_read_only_commands(
    tmp_path: Path, isolated_home: Path
) -> None:
    workspace = _cursor_workspace(tmp_path)

    from agentbus.discovery import discover

    discover(
        workspace=workspace,
        home=isolated_home,
        platform="linux",
        environ={"HOME": str(isolated_home)},
        clients=["cursor-project"],
        which=_no_which,
        python=sys.executable,
    )

    assert not (workspace / ".agentbus").exists()


def test_toml_patch_preserves_unrelated_tables_and_is_idempotent(
    tmp_path: Path,
) -> None:
    path = tmp_path / "config.toml"
    path.write_text(
        'model = "gpt-5"\n\n'
        "[mcp_servers.other]\n"
        'command = "other-server"\n'
        'args = ["--flag"]\n',
        encoding="utf-8",
    )
    installation = install_target("codex-user")
    entry = build_entry(
        installation,
        argv=("/usr/bin/agentbus", "mcp-serve"),
        workspace=tmp_path,
        producer_id="local-user",
    )

    first = plan_patch(installation, path=path, entry=entry)
    assert first.changed is True
    assert first.new_bytes is not None
    document = tomllib.loads(first.new_bytes.decode())
    assert document["model"] == "gpt-5"
    assert document["mcp_servers"]["other"] == {
        "command": "other-server",
        "args": ["--flag"],
    }
    assert document["mcp_servers"]["agentbus"]["command"] == "/usr/bin/agentbus"
    assert document["mcp_servers"]["agentbus"]["env"]["AGENTBUS_PRODUCER_ID"] == (
        "local-user"
    )

    path.write_bytes(first.new_bytes)
    second = plan_patch(installation, path=path, entry=entry)
    assert second.changed is False
    assert second.reason == "unchanged"


def test_toml_replaces_only_its_own_table(tmp_path: Path) -> None:
    path = tmp_path / "config.toml"
    path.write_text(
        'approval_policy = "never"\n\n'
        "[mcp_servers.agentbus]\n"
        'command = "/old/agentbus"\n'
        'args = ["mcp-serve"]\n'
        "\n"
        "[profiles.fast]\n"
        'model = "fast"\n',
        encoding="utf-8",
    )
    installation = install_target("codex-user")
    entry = build_entry(
        installation,
        argv=("/new/agentbus", "mcp-serve"),
        workspace=tmp_path,
        producer_id="local-user",
    )

    patch = plan_patch(installation, path=path, entry=entry)

    document = tomllib.loads(patch.new_bytes.decode())
    assert document["approval_policy"] == "never"
    assert document["profiles"]["fast"]["model"] == "fast"
    assert document["mcp_servers"]["agentbus"]["command"] == "/new/agentbus"
    assert document["mcp_servers"]["agentbus"]["args"][0] == "mcp-serve"


def test_toml_dotted_owned_key_is_a_conflict(tmp_path: Path) -> None:
    path = tmp_path / "config.toml"
    path.write_text(
        'mcp_servers.agentbus.command = "/old/agentbus"\n', encoding="utf-8"
    )
    with pytest.raises(ConfigConflictError) as excinfo:
        plan_toml_patch(
            installation_id="codex-user",
            path=path,
            table="mcp_servers.agentbus",
            entry={"command": "/new", "args": []},
        )
    assert excinfo.value.code == "dotted_key_conflict"


def test_json_patch_rejects_non_object_container(tmp_path: Path) -> None:
    path = tmp_path / "mcp.json"
    path.write_text('{"mcpServers": []}\n', encoding="utf-8")
    with pytest.raises(ConfigConflictError) as excinfo:
        plan_json_patch(
            installation_id="cursor-project",
            path=path,
            root="mcpServers",
            entry={"command": "/x"},
        )
    assert excinfo.value.code == "root_not_object"


def test_vscode_uses_servers_schema_not_mcp_servers(
    tmp_path: Path, isolated_home: Path
) -> None:
    workspace = tmp_path / "proj"
    config = workspace / ".vscode" / "mcp.json"
    config.parent.mkdir(parents=True)
    config.write_text('{"servers": {"team": {"type": "stdio"}}}\n', encoding="utf-8")

    report = _run(workspace, isolated_home, clients=["vscode"], apply=True)

    assert report.exit_code == EXIT_OK, report.errors
    data = json.loads(config.read_text(encoding="utf-8"))
    assert "mcpServers" not in data
    assert data["servers"]["team"] == {"type": "stdio"}
    assert data["servers"]["agentbus"]["type"] == "stdio"


def test_process_level_concurrent_first_run(
    tmp_path: Path, isolated_home: Path, spawn_process
) -> None:
    workspace = tmp_path / "proj"
    config = workspace / ".cursor" / "mcp.json"
    config.parent.mkdir(parents=True)
    config.write_text('{"mcpServers": {}}\n', encoding="utf-8")
    repo_root = Path(__file__).resolve().parents[1]
    env = {
        **os.environ,
        "HOME": str(isolated_home),
        "AGENTBUS_WORKSPACE": str(workspace),
    }

    procs = [
        spawn_process(
            [
                sys.executable,
                "-m",
                "agentbus.cli",
                "connect",
                "--apply",
                "--client",
                "cursor-project",
                "--json",
                "--lock-timeout",
                "20",
            ],
            cwd=str(repo_root),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for _ in range(4)
    ]
    results = [proc.communicate(timeout=120) for proc in procs]

    for (stdout, stderr), proc in zip(results, procs):
        assert proc.returncode == 0, stderr
        payload = json.loads(stdout)
        assert payload["exit_code"] == 0
    data = json.loads(config.read_text(encoding="utf-8"))
    assert data["mcpServers"]["agentbus"]["command"].endswith("agentbus") or (
        "agentbus.cli" in data["mcpServers"]["agentbus"]["args"]
    )
    assert len(list(config.parent.glob("*.agentbus.bak.*"))) == 1
    assert not list(workspace.rglob("*.agentbus-tmp"))


def test_failed_connect_reports_rollback_boundary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, isolated_home: Path
) -> None:
    workspace = _cursor_workspace(tmp_path)
    real_write = discovery_module._atomic_write

    def always_fails(path: Path, data: bytes, mode: int) -> None:
        raise OSError("simulated failure")

    monkeypatch.setattr(discovery_module, "_atomic_write", always_fails)
    report = _run(workspace, isolated_home, clients=["cursor-project"], apply=True)
    monkeypatch.setattr(discovery_module, "_atomic_write", real_write)

    assert report.exit_code != EXIT_OK
    assert report.items[0].state == STATE_ERROR
    assert report.items[0].reason == "simulated failure"
    assert report.errors
    assert "all files this operation changed were restored" in report.errors[0]
    assert "rollback incomplete" not in report.errors[0]
    assert json.loads((workspace / ".cursor" / "mcp.json").read_text()) == {
        "mcpServers": {}
    }


# ---------------------------------------------------------------------------
# Remediation regressions (review findings 2, 5, 6, 7)
# ---------------------------------------------------------------------------


def test_setup_lock_blocks_other_threads_in_same_process(tmp_path: Path) -> None:
    from agentbus.discovery import SetupLockTimeout

    workspace = tmp_path / "ws"
    holder = setup_lock(workspace, timeout=1)
    holder.acquire()
    outcome: dict[str, bool] = {}

    def contend() -> None:
        contender = setup_lock(workspace, timeout=0.3)
        try:
            contender.acquire()
        except SetupLockTimeout:
            outcome["blocked"] = True
        else:
            outcome["acquired"] = True
            contender.release()

    thread = threading.Thread(target=contend, name="setup-contender")
    thread.start()
    thread.join(timeout=10)
    assert not thread.is_alive()
    assert outcome == {"blocked": True}

    # The owning thread may still re-enter, and release frees the lock.
    reentrant = setup_lock(workspace, timeout=0.01)
    reentrant.acquire()
    reentrant.release()
    holder.release()

    winner: dict[str, bool] = {}

    def acquire_after_release() -> None:
        lock = setup_lock(workspace, timeout=2)
        lock.acquire()
        winner["acquired"] = True
        lock.release()

    thread = threading.Thread(target=acquire_after_release)
    thread.start()
    thread.join(timeout=10)
    assert winner == {"acquired": True}


def test_init_holds_setup_lock_across_scaffolding(
    tmp_path: Path, isolated_home: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import agentbus.cli as cli_module
    from click.testing import CliRunner

    from agentbus.discovery import SetupLockTimeout
    from agentbus.cli import main

    workspace = tmp_path / "proj"
    workspace.mkdir()
    real_scaffold = cli_module.scaffold_workspace

    def asserting_scaffold(*args, **kwargs):
        if kwargs.get("check_only") is False:
            observed: dict[str, bool] = {}

            def contend() -> None:
                try:
                    setup_lock(workspace, timeout=0.3).acquire()
                except SetupLockTimeout:
                    observed["blocked"] = True

            thread = threading.Thread(target=contend)
            thread.start()
            thread.join(timeout=5)
            assert observed.get("blocked") is True, (
                "okf/ scaffolding ran outside the setup lock"
            )
        return real_scaffold(*args, **kwargs)

    monkeypatch.setattr(cli_module, "scaffold_workspace", asserting_scaffold)
    result = CliRunner().invoke(
        main,
        [
            "init",
            "--workspace",
            str(workspace),
            "--apply",
            "--no-mcp",
            "--producer-id",
            "test-agent",
        ],
    )
    assert result.exit_code == 0, result.output


def test_auto_connect_false_skipped_in_bulk_unless_named(
    tmp_path: Path, isolated_home: Path
) -> None:
    workspace = tmp_path / "proj"
    config = workspace / ".codex" / "config.toml"
    config.parent.mkdir(parents=True)
    config.write_text('model = "gpt-5"\n', encoding="utf-8")
    before = config.read_bytes()

    bulk = _run(workspace, isolated_home, apply=True)
    assert bulk.exit_code == EXIT_OK, bulk.errors
    skipped = next(
        item for item in bulk.items if item.installation_id == "codex-project"
    )
    assert skipped.state == STATE_SKIPPED
    assert skipped.reason == "auto_connect_disabled"
    assert config.read_bytes() == before

    named = _run(
        workspace, isolated_home, clients=["codex-project"], apply=True
    )
    assert named.exit_code == EXIT_OK, named.errors
    item = next(
        i for i in named.items if i.installation_id == "codex-project"
    )
    assert item.state == STATE_AWAITING
    document = tomllib.loads(config.read_text(encoding="utf-8"))
    assert document["model"] == "gpt-5"
    assert document["mcp_servers"]["agentbus"]["command"] == "/usr/bin/agentbus"


def test_unowned_agentbus_entry_is_never_overwritten(
    tmp_path: Path, isolated_home: Path
) -> None:
    workspace = tmp_path / "proj"
    config = workspace / ".cursor" / "mcp.json"
    config.parent.mkdir(parents=True)
    config.write_text(
        json.dumps(
            {
                "mcpServers": {
                    "agentbus": {
                        "command": "another-tool",
                        "args": ["serve"],
                        "env": {"TEAM": "platform"},
                    }
                }
            }
        )
        + "\n",
        encoding="utf-8",
    )
    before = config.read_bytes()

    report = _run(workspace, isolated_home, clients=["cursor-project"], apply=True)

    assert report.exit_code != EXIT_OK
    item = report.items[0]
    assert item.state == STATE_REFUSED
    assert "entry_not_owned" in item.reason
    assert config.read_bytes() == before


def test_backups_are_collision_resistant_and_never_overwritten(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        discovery_module.time, "strftime", lambda *_a, **_k: "20260101T000000"
    )
    path = tmp_path / "mcp.json"
    path.write_text('{"mcpServers": {}}\n', encoding="utf-8")

    first = discovery_module._create_backup(
        path, b"first-original\n", 0o600
    )
    second = discovery_module._create_backup(
        path, b"second-original\n", 0o600
    )

    assert first != second
    assert first.is_file() and second.is_file()
    # O_EXCL allocation never replaces an existing backup.
    assert first.read_bytes() == b"first-original\n"
    assert second.read_bytes() == b"second-original\n"
    assert first.name.endswith(".0")
    assert second.name.endswith(".1")


def test_probe_error_is_unknown_with_actionable_diagnostics(
    tmp_path: Path, isolated_home: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from agentbus.adapters.probe import STATUS_UNKNOWN, probe_installations

    workspace = tmp_path / "proj"
    config = workspace / ".cursor" / "mcp.json"
    config.parent.mkdir(parents=True)
    config.write_text('{"mcpServers": {}}\n', encoding="utf-8")

    def denied(_path: Path) -> bytes:
        raise PermissionError(13, "permission denied", str(config))

    monkeypatch.setattr("agentbus.safe_file.read_regular_file", denied)
    probe = probe_installations(
        workspace=workspace,
        home=isolated_home,
        platform="linux",
        environ={"HOME": str(isolated_home)},
        clients=["cursor-project"],
        which=_no_which,
    )[0]

    assert probe.status == STATUS_UNKNOWN
    assert probe.reason.startswith("config_unreadable")
    assert "permission denied" in probe.reason


def test_apply_init_refuses_user_scope_without_authorization(
    tmp_path: Path, isolated_home: Path
) -> None:
    from agentbus.devex import apply_init

    workspace = tmp_path / "proj"
    project_config = workspace / ".cursor" / "mcp.json"
    project_config.parent.mkdir(parents=True)
    project_config.write_text('{"mcpServers": {}}\n', encoding="utf-8")
    global_config = isolated_home / ".cursor" / "mcp.json"
    global_config.parent.mkdir(parents=True)
    global_config.write_text('{"sentinel": true}\n', encoding="utf-8")
    before = global_config.read_bytes()

    result = apply_init(
        workspace,
        producer_id="local-user",
        dry_run=False,
        home=isolated_home,
        platform="linux",
        environ={"HOME": str(isolated_home)},
        command=FAKE_COMMAND,
        which=_no_which,
    )

    assert global_config.read_bytes() == before
    assert any(
        "user_scope_requires_authorization" in entry
        for entry in result.skipped
    )
    assert "agentbus" in json.loads(
        project_config.read_text(encoding="utf-8")
    )["mcpServers"]

    allowed = apply_init(
        workspace,
        producer_id="local-user",
        dry_run=False,
        home=isolated_home,
        platform="linux",
        environ={"HOME": str(isolated_home)},
        command=FAKE_COMMAND,
        which=_no_which,
        allow_user_scope=True,
    )
    assert allowed.updated
    data = json.loads(global_config.read_text(encoding="utf-8"))
    assert data["sentinel"] is True
    assert "agentbus" in data["mcpServers"]


def test_user_scope_writes_take_global_config_lock(
    tmp_path: Path, isolated_home: Path
) -> None:
    from agentbus.discovery import global_config_lock

    workspace = tmp_path / "proj"
    workspace.mkdir()
    global_config = isolated_home / ".cursor" / "mcp.json"
    global_config.parent.mkdir(parents=True)
    global_config.write_text('{"sentinel": true}\n', encoding="utf-8")
    before = global_config.read_bytes()

    holder_started = threading.Event()
    holder_release = threading.Event()

    def hold_global_lock() -> None:
        holder = global_config_lock(isolated_home, timeout=1)
        holder.acquire()
        holder_started.set()
        holder_release.wait(timeout=10)
        holder.release()

    holder_thread = threading.Thread(target=hold_global_lock)
    holder_thread.start()
    assert holder_started.wait(timeout=5)
    try:
        report = _run(
            workspace,
            isolated_home,
            clients=["cursor-user"],
            scope="user",
            apply=True,
            allow_user_scope=True,
            lock_timeout=0.2,
        )
    finally:
        holder_release.set()
        holder_thread.join(timeout=5)

    assert report.exit_code == EXIT_LOCK_TIMEOUT
    assert report.items[0].reason == "global_config_lock_timeout"
    assert global_config.read_bytes() == before


def test_apply_init_respects_auto_connect_false(
    tmp_path: Path, isolated_home: Path
) -> None:
    from agentbus.devex import apply_init

    workspace = tmp_path / "proj"
    config = workspace / ".codex" / "config.toml"
    config.parent.mkdir(parents=True)
    config.write_text('model = "gpt-5"\n', encoding="utf-8")
    before = config.read_bytes()

    result = apply_init(
        workspace,
        producer_id="local-user",
        dry_run=False,
        home=isolated_home,
        platform="linux",
        environ={"HOME": str(isolated_home)},
        command=FAKE_COMMAND,
        which=_no_which,
    )

    assert any(
        "codex-project: auto_connect disabled" in entry
        for entry in result.skipped
    )
    assert config.read_bytes() == before


# ---------------------------------------------------------------------------
# Round 2 remediation: data-loss, lock lifecycle, prompt, and command probes
# ---------------------------------------------------------------------------


def test_rollback_preserves_external_edit_to_existing_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An outside edit made mid-transaction must survive rollback intact."""
    from agentbus.discovery import (
        SetupWriteError,
        TransactionRollbackConflictError,
    )

    first = tmp_path / "a.json"
    second = tmp_path / "b.json"
    first.write_text('{"a": 1}\n', encoding="utf-8")
    second.write_text('{"b": 2}\n', encoding="utf-8")
    original_first = first.read_bytes()
    original_second = second.read_bytes()
    external = b'{"external": "operator edit"}\n'

    real_write = discovery_module._atomic_write

    def edit_first_then_fail(path: Path, data: bytes, mode: int) -> None:
        if path == second:
            # Someone saves a.json after AgentBus wrote it, then the second
            # write fails: rollback must not erase that save.
            first.write_bytes(external)
            raise OSError("simulated failure after external edit")
        return real_write(path, data, mode)

    monkeypatch.setattr(discovery_module, "_atomic_write", edit_first_then_fail)

    transaction = FileTransaction()
    transaction.stage(
        first, b'{"agentbus": "new"}\n', original_bytes=original_first
    )
    transaction.stage(
        second, b'{"agentbus": "new"}\n', original_bytes=original_second
    )

    with pytest.raises(SetupWriteError) as excinfo:
        transaction.commit()

    error = excinfo.value
    assert first.read_bytes() == external, "external edit was overwritten"
    assert second.read_bytes() == original_second
    assert len(error.rollback_conflicts) == 1
    conflict = error.rollback_conflicts[0]
    assert isinstance(conflict, TransactionRollbackConflictError)
    assert conflict.path == first
    assert conflict.backup is not None
    assert conflict.backup.read_bytes() == original_first
    assert "external modification detected" in str(conflict)
    assert "external modification detected" in " ".join(error.rollback_errors)
    assert "pre-transaction backup preserved" in str(conflict)


def test_rollback_preserves_external_edit_to_newly_created_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from agentbus.discovery import (
        SetupWriteError,
        TransactionRollbackConflictError,
    )

    created = tmp_path / "new.json"
    trigger = tmp_path / "b.json"
    trigger.write_text("{}\n", encoding="utf-8")
    external = b'{"external": "late writer"}\n'

    real_write = discovery_module._atomic_write

    def edit_then_fail(path: Path, data: bytes, mode: int) -> None:
        if path == trigger:
            created.write_bytes(external)
            raise OSError("simulated failure")
        return real_write(path, data, mode)

    monkeypatch.setattr(discovery_module, "_atomic_write", edit_then_fail)

    transaction = FileTransaction()
    transaction.stage(created, b'{"agentbus": 1}\n', original_bytes=None)
    transaction.stage(trigger, b"{}\n", original_bytes=b"{}\n")

    with pytest.raises(SetupWriteError) as excinfo:
        transaction.commit()

    assert created.read_bytes() == external
    conflict = excinfo.value.rollback_conflicts[0]
    assert isinstance(conflict, TransactionRollbackConflictError)
    assert conflict.backup is None
    assert "nothing was deleted" in str(conflict)


def test_rollback_removes_untouched_new_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No conflict: a new file nobody else touched is still rolled back."""
    from agentbus.discovery import SetupWriteError

    created = tmp_path / "new.json"
    trigger = tmp_path / "b.json"
    trigger.write_text("{}\n", encoding="utf-8")

    real_write = discovery_module._atomic_write

    def fail_trigger(path: Path, data: bytes, mode: int) -> None:
        if path == trigger:
            raise OSError("simulated failure")
        return real_write(path, data, mode)

    monkeypatch.setattr(discovery_module, "_atomic_write", fail_trigger)

    transaction = FileTransaction()
    transaction.stage(created, b'{"agentbus": 1}\n', original_bytes=None)
    transaction.stage(trigger, b"{}\n", original_bytes=b"{}\n")

    with pytest.raises(SetupWriteError) as excinfo:
        transaction.commit()

    assert not created.exists()
    assert excinfo.value.rollback_conflicts == ()


def test_setup_lock_nested_release_order_is_safe(tmp_path: Path) -> None:
    """Nested handles may be released in any order without freeing early."""
    from agentbus.discovery import SetupLockTimeout

    workspace = tmp_path / "ws"
    outer = setup_lock(workspace, timeout=1)
    outer.acquire()
    inner = setup_lock(workspace, timeout=1)
    inner.acquire()
    assert outer.depth == 2
    assert inner.depth == 2
    assert outer.owner_thread_id == inner.owner_thread_id

    outer.release()  # out of order: the lock must remain held
    assert inner.depth == 1

    outcome: dict[str, bool] = {}
    contended = setup_lock(workspace, timeout=0.2)

    def contend() -> None:
        try:
            contended.acquire()
        except SetupLockTimeout:
            outcome["blocked"] = True
        else:
            outcome["acquired"] = True
            contended.release()

    thread = threading.Thread(target=contend, name="nested-contender")
    thread.start()
    thread.join(timeout=10)
    assert not thread.is_alive()
    assert outcome == {"blocked": True}
    contended.release()  # this thread never acquired: a no-op

    inner.release()
    assert inner.depth == 0

    again = setup_lock(workspace, timeout=1)
    again.acquire()
    again.release()


def test_setup_lock_timeout_closes_its_descriptor_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, spawn_process
) -> None:
    from agentbus.discovery import SetupLockTimeout

    workspace = tmp_path / "ws"
    flag = tmp_path / "locked.flag"
    script = (
        "import sys, time\n"
        "from pathlib import Path\n"
        "from agentbus.discovery import setup_lock\n"
        "lock = setup_lock(Path(sys.argv[1]), timeout=5)\n"
        "lock.acquire()\n"
        "Path(sys.argv[2]).write_text('locked')\n"
        "time.sleep(10)\n"
    )
    spawn_process(
        [sys.executable, "-c", script, str(workspace), str(flag)],
        cwd=str(Path(__file__).resolve().parents[1]),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    deadline = time.monotonic() + 10
    while not flag.exists() and time.monotonic() < deadline:
        time.sleep(0.02)
    assert flag.exists(), "lock holder never started"

    opened: list[int] = []
    closed: list[int] = []
    real_open = os.open
    real_close = os.close

    def tracking_open(path, flags, *args, **kwargs) -> int:
        fd = real_open(path, flags, *args, **kwargs)
        opened.append(fd)
        return fd

    def tracking_close(fd: int) -> None:
        closed.append(fd)
        return real_close(fd)

    monkeypatch.setattr(discovery_module.os, "open", tracking_open)
    monkeypatch.setattr(discovery_module.os, "close", tracking_close)

    contender = setup_lock(workspace, timeout=0.1)
    with pytest.raises(SetupLockTimeout):
        contender.acquire()
    contender.release()

    assert opened, "the contender never opened the lock file"
    for fd in opened:
        assert closed.count(fd) == 1, (
            f"descriptor {fd} closed {closed.count(fd)} times"
        )


def test_connect_json_output_never_calls_confirm(
    tmp_path: Path, isolated_home: Path
) -> None:
    workspace = _cursor_workspace(tmp_path)
    user_config = isolated_home / ".cursor" / "mcp.json"
    user_config.parent.mkdir(parents=True)
    user_config.write_text('{"sentinel": true}\n', encoding="utf-8")
    before = user_config.read_bytes()
    prompts: list[str] = []

    report = _run(
        workspace,
        isolated_home,
        clients=["cursor-user"],
        scope="user",
        apply=True,
        interactive=True,
        json_output=True,
        confirm=lambda message: prompts.append(message) or True,
    )

    assert prompts == []
    assert report.exit_code != EXIT_OK
    assert report.items[0].reason == "user_scope_requires_authorization"
    assert user_config.read_bytes() == before


def test_apply_init_ignores_broken_unauthorized_user_config(
    tmp_path: Path, isolated_home: Path
) -> None:
    """A global file this run cannot write must not abort project wiring."""
    from agentbus.devex import apply_init

    workspace = _cursor_workspace(tmp_path)
    broken_user = isolated_home / ".codex" / "config.toml"
    broken_user.parent.mkdir(parents=True)
    broken_user.write_text("this is not = valid toml\n", encoding="utf-8")
    before = broken_user.read_bytes()
    prompts = []

    result = apply_init(
        workspace,
        producer_id="local-user",
        dry_run=False,
        home=isolated_home,
        platform="linux",
        environ={"HOME": str(isolated_home)},
        command=FAKE_COMMAND,
        which=_no_which,
        interactive=True,
        confirm=lambda prompt: prompts.append(prompt) or False,
    )

    assert any(
        "user scope not authorized in this run" in entry
        for entry in result.skipped
    ), result.skipped
    assert broken_user.read_bytes() == before
    assert prompts == []
    document = json.loads(
        (workspace / ".cursor" / "mcp.json").read_text(encoding="utf-8")
    )
    assert document["mcpServers"]["agentbus"]["command"] == "/usr/bin/agentbus"


def test_init_bootstraps_when_command_unavailable(tmp_path, monkeypatch):
    import agentbus.adapters.command as command_module
    from agentbus.devex import apply_init

    def unavailable(**kwargs):
        raise command_module.McpCommandError("unavailable", "no command")

    monkeypatch.setattr(command_module, "resolve_mcp_command", unavailable)
    result = apply_init(tmp_path, producer_id="local-user", dry_run=False)
    assert result.runtime_initialized
    assert any("no durable command" in entry for entry in result.skipped)
    assert (tmp_path / ".agentbus" / "workspace").exists()


def test_apply_init_malformed_authorized_user_config_still_aborts(
    tmp_path: Path, isolated_home: Path
) -> None:
    from agentbus.devex import InvalidMcpConfigError, apply_init

    workspace = _cursor_workspace(tmp_path)
    broken_user = isolated_home / ".codex" / "config.toml"
    broken_user.parent.mkdir(parents=True)
    broken_user.write_text("this is not = valid toml\n", encoding="utf-8")

    with pytest.raises(InvalidMcpConfigError):
        apply_init(
            workspace,
            producer_id="local-user",
            dry_run=False,
            allow_user_scope=True,
            home=isolated_home,
            platform="linux",
            environ={"HOME": str(isolated_home)},
            command=FAKE_COMMAND,
            which=_no_which,
        )


def test_apply_init_replan_conflict_is_typed_and_releases_locks(
    tmp_path: Path, isolated_home: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import agentbus.adapters.writer as writer_module
    from agentbus.devex import InvalidMcpConfigError, apply_init

    workspace = _cursor_workspace(tmp_path)
    real_plan = writer_module.plan_patch
    calls = {"count": 0}

    def flaky_plan(*args, **kwargs):
        calls["count"] += 1
        if calls["count"] == 2:
            # A concurrent writer rewrote the file between planning and commit.
            raise writer_module.ConfigConflictError(
                "entry_not_owned", "changed under the setup lock"
            )
        return real_plan(*args, **kwargs)

    monkeypatch.setattr(writer_module, "plan_patch", flaky_plan)

    with pytest.raises(InvalidMcpConfigError) as excinfo:
        apply_init(
            workspace,
            producer_id="local-user",
            dry_run=False,
            home=isolated_home,
            platform="linux",
            environ={"HOME": str(isolated_home)},
            command=FAKE_COMMAND,
            which=_no_which,
        )
    assert "entry_not_owned" in str(excinfo.value)
    assert calls["count"] == 2

    # The setup lock must be free again immediately after the typed error.
    lock = setup_lock(workspace, timeout=0.5)
    lock.acquire()
    lock.release()


def test_verification_env_drops_pythonpath(
    tmp_path: Path, isolated_home: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from agentbus.discovery import HandshakeResult
    from agentbus.store import EventStore

    workspace = _cursor_workspace(tmp_path)
    EventStore(workspace).close()  # --verify requires an initialized workspace
    captured: dict[str, dict[str, str]] = {}

    def fake_handshake(argv, *, env=None, cwd=None, **kwargs):
        captured["env"] = dict(env or {})
        return HandshakeResult(ok=True, server_name="agentbus", tool_count=2)

    monkeypatch.setattr(discovery_module, "verify_mcp_handshake", fake_handshake)

    report = _run(
        workspace,
        isolated_home,
        clients=["cursor-project"],
        apply=True,
        verify=True,
        environ={"HOME": str(isolated_home), "PYTHONPATH": "/tmp/transient-src"},
    )

    assert report.verification["performed"] is True
    assert report.verification["ok"] is True
    assert "PYTHONPATH" not in captured["env"]
    assert captured["env"]["AGENTBUS_WORKSPACE"] == str(workspace.resolve())


def test_atomic_write_preserves_symlink_and_updates_target(tmp_path):
    from agentbus.discovery import _atomic_write

    target = tmp_path / 'target.json'
    target.write_bytes(b'old')
    link = tmp_path / 'config.json'
    link.symlink_to(target.name)
    _atomic_write(link, b'new', 0o600)
    assert link.is_symlink()
    assert target.read_bytes() == b'new'


def test_redact_encoded_query_and_fragment_credentials():
    from agentbus.adapters.writer import redact_url

    url = 'https://example.test/mcp?%74oken=secret&X-Amz-Signature=signed&ok=yes#/route?access_token=hidden&view=full'
    result = redact_url(url)
    assert result == 'https://example.test/mcp?%74oken=[REDACTED]&X-Amz-Signature=[REDACTED]&ok=yes#/route?access_token=[REDACTED]&view=full'


def test_verify_without_initialized_workspace_fails(tmp_path, isolated_home):
    workspace = _cursor_workspace(tmp_path)
    report = _run(workspace, isolated_home, clients=['cursor-project'], apply=True, verify=True)
    assert report.verification['performed'] is False
    assert report.exit_code == discovery_module.EXIT_VERIFY_FAILED


def test_init_without_discovery_does_not_load_manifests(tmp_path, monkeypatch):
    import agentbus.adapters.manifest as manifest_module
    from agentbus.devex import apply_init

    def broken():
        raise manifest_module.ManifestError('broken')

    monkeypatch.setattr(manifest_module, 'load_manifests', broken)
    result = apply_init(tmp_path, producer_id='local-user', dry_run=False, discover_clients=False)
    assert (tmp_path / '.agentbus' / 'workspace').exists()
    assert not result.updated


def test_init_preview_does_not_prompt_for_user_scope(tmp_path, isolated_home):
    from agentbus.devex import apply_init

    config = isolated_home / '.codex' / 'config.toml'
    config.parent.mkdir()
    config.write_text('model = "test"\n')
    prompts = []
    result = apply_init(tmp_path, producer_id='local-user', dry_run=True,
                        home=isolated_home, platform='linux', environ={'HOME': str(isolated_home)},
                        command=FAKE_COMMAND, which=_no_which, interactive=True,
                        confirm=lambda prompt: prompts.append(prompt) or False)
    assert prompts == []
    assert any('user_scope_requires_authorization' in entry for entry in result.skipped)
    assert 'agentbus' not in config.read_text()


def test_project_symlink_outside_workspace_is_refused(tmp_path, isolated_home):
    from agentbus.devex import InvalidMcpConfigError, apply_init

    workspace = tmp_path / 'project'
    workspace.mkdir()
    outside = tmp_path / 'outside'
    outside.mkdir()
    config = outside / 'mcp.json'
    config.write_text('{"mcpServers": {}}\n')
    (workspace / '.cursor').symlink_to(outside, target_is_directory=True)
    before = config.read_bytes()
    report = _run(workspace, isolated_home, clients=['cursor-project'], apply=True)
    assert report.exit_code != EXIT_OK
    assert any(item.reason == 'project_path_escapes_workspace' for item in report.items)
    with pytest.raises(InvalidMcpConfigError, match='project_path_escapes_workspace'):
        apply_init(workspace, producer_id='local-user', dry_run=False,
                   home=isolated_home, platform='linux', environ={'HOME': str(isolated_home)},
                   command=FAKE_COMMAND, which=_no_which)
    assert config.read_bytes() == before


@pytest.mark.parametrize('format_name', ['json', 'toml'])
def test_patch_preserves_unmanaged_owned_settings(tmp_path, format_name):
    from agentbus.adapters.writer import entry_matches

    entry = {'command': '/usr/bin/agentbus', 'args': ['mcp-serve'], 'env': {'AGENTBUS_WORKSPACE': '/new'}}
    if format_name == 'json':
        path = tmp_path / 'config.json'
        path.write_text(json.dumps({'mcpServers': {'agentbus': {'command': '/usr/bin/agentbus', 'args': ['mcp-serve'], 'disabled': True, 'env': {'AGENTBUS_WORKSPACE': '/old', 'CUSTOM': 'keep'}}}}))
        patch = plan_json_patch(installation_id='test', path=path, root='mcpServers', entry=entry)
        updated = json.loads(patch.new_bytes)['mcpServers']['agentbus']
    else:
        path = tmp_path / 'config.toml'
        path.write_text('[mcp_servers.agentbus]\ncommand = "/usr/bin/agentbus"\nargs = ["mcp-serve"]\ndisabled = true\n[mcp_servers.agentbus.env]\nAGENTBUS_WORKSPACE = "/old"\nCUSTOM = "keep"\n')
        patch = plan_toml_patch(installation_id='test', path=path, table='mcp_servers.agentbus', entry=entry)
        updated = tomllib.loads(patch.new_bytes.decode())['mcp_servers']['agentbus']
    assert updated['disabled'] is True
    assert updated['env'] == {'AGENTBUS_WORKSPACE': '/new', 'CUSTOM': 'keep'}
    assert entry_matches(updated, entry)


def test_explicit_installation_excluded_by_scope_is_refused(tmp_path, isolated_home):
    report = _run(tmp_path, isolated_home, clients=['cursor-user'], scope='project', apply=True)
    assert report.exit_code != EXIT_OK
    assert any(item.reason == 'excluded_by_scope:project' for item in report.items)


def test_redaction_preserves_paths_without_hiding_tokens():
    from agentbus.adapters.writer import redact_entry

    result = redact_entry({'args': ['--workspace', '/home/me/task-tracker-app', 'sk-abcdefghij']})
    assert result['args'] == ['--workspace', '/home/me/task-tracker-app', '[REDACTED]']


def test_setup_rejects_symlink_lock_and_marker(tmp_path: Path) -> None:
    from agentbus.devex import apply_init
    from agentbus.discovery import SetupLock, SetupLockSecurityError

    workspace = tmp_path / "project"
    runtime = workspace / ".agentbus"
    runtime.mkdir(parents=True)
    target = tmp_path / "victim"
    target.write_text("unchanged")
    lock = runtime / "init.lock"
    lock.symlink_to(target)
    with pytest.raises(SetupLockSecurityError):
        SetupLock(workspace).acquire()
    assert target.read_text() == "unchanged"
    lock.unlink()
    marker = runtime / "workspace"
    marker.symlink_to(target)
    with pytest.raises(OSError, match="symlinked workspace marker"):
        apply_init(workspace, producer_id="local-user", dry_run=False, discover_clients=False)
    assert target.read_text() == "unchanged"


def test_family_selector_skips_opted_out_and_absent_siblings(tmp_path: Path, isolated_home: Path) -> None:
    from agentbus.devex import apply_init

    workspace = tmp_path / "project"
    config = workspace / ".codex" / "config.toml"
    config.parent.mkdir(parents=True)
    config.write_text('model = "gpt-5"\n')
    report = _run(workspace, isolated_home, clients=["codex"], apply=True)
    items = {item.installation_id: item for item in report.items}
    assert items["codex-project"].reason == "auto_connect_disabled"
    assert items["codex-user"].state == STATE_SKIPPED
    assert report.exit_code == EXIT_OK
    result = apply_init(workspace, producer_id="local-user", dry_run=False,
                        home=isolated_home, platform="linux", environ={"HOME": str(isolated_home)},
                        command=FAKE_COMMAND, which=_no_which, clients=["codex"])
    assert any("codex-project: auto_connect disabled" in item for item in result.skipped)
    assert config.read_text() == 'model = "gpt-5"\n'


@pytest.mark.parametrize("runtime_name", ["token", "roles.yaml"])
def test_init_rejects_symlinked_runtime_files(tmp_path: Path, runtime_name: str) -> None:
    from agentbus.devex import apply_init

    workspace = tmp_path / "project"
    runtime = workspace / ".agentbus"
    runtime.mkdir(parents=True)
    victim = tmp_path / "victim"
    victim.write_text("")
    (runtime / runtime_name).symlink_to(victim)
    with pytest.raises(OSError, match="symlinked runtime file"):
        apply_init(workspace, producer_id="local-user", dry_run=False, discover_clients=False)
    assert victim.read_text() == ""


def test_non_regular_client_config_is_unreadable(tmp_path: Path, isolated_home: Path) -> None:
    from agentbus.adapters.probe import CONFIG_UNREADABLE, _load_document
    from agentbus.adapters.writer import ConfigConflictError, read_bytes

    if not hasattr(os, "mkfifo"):
        pytest.skip("FIFO unavailable")
    fifo = tmp_path / "config"
    os.mkfifo(fifo)
    assert _load_document(fifo, "json")[0] == CONFIG_UNREADABLE
    with pytest.raises(ConfigConflictError, match="regular file"):
        read_bytes(fifo)


def test_secret_assignment_argument_is_redacted() -> None:
    from agentbus.adapters.writer import redact_entry

    result = redact_entry({"args": ["-e", "GITHUB_TOKEN=abc123", "https://u:p@example.com/?token=secret"]})
    assert result is not None
    assert "abc123" not in str(result)
    assert "u:p" not in str(result)
    assert "secret" not in str(result)


def test_family_selector_skips_unauthorized_user_sibling(tmp_path: Path, isolated_home: Path) -> None:
    workspace = tmp_path / "project"
    project_config = workspace / ".cursor" / "mcp.json"
    user_config = isolated_home / ".cursor" / "mcp.json"
    for config in (project_config, user_config):
        config.parent.mkdir(parents=True)
        config.write_text('{}\n')
    report = _run(workspace, isolated_home, clients=["cursor"], apply=True)
    items = {item.installation_id: item for item in report.items}
    assert report.exit_code == EXIT_OK
    assert items["cursor-project"].state == STATE_AWAITING
    assert items["cursor-user"].state == STATE_SKIPPED
    assert user_config.read_text() == '{}\n'


def test_secret_header_arguments_are_redacted() -> None:
    from agentbus.adapters.writer import redact_entry

    result = redact_entry({"args": ["--header", "X-API-Key: abc123", "--header", "Authorization: Basic xyz"]})
    assert result is not None
    assert "abc123" not in str(result)
    assert "Basic xyz" not in str(result)


def test_setup_lock_reports_directory_creation_error(tmp_path: Path) -> None:
    from agentbus.discovery import SetupLock, SetupLockSecurityError

    workspace = tmp_path / "project"
    workspace.mkdir()
    (workspace / ".agentbus").write_text("not a directory")
    with pytest.raises(SetupLockSecurityError, match="cannot create lock directory"):
        SetupLock(workspace).acquire()


def test_init_preview_skips_unauthorized_conflicting_user_config(tmp_path: Path, isolated_home: Path) -> None:
    from agentbus.devex import apply_init

    workspace = tmp_path / "project"
    workspace.mkdir()
    config = isolated_home / ".cursor" / "mcp.json"
    config.parent.mkdir()
    config.write_text('{"mcpServers": {"agentbus": {"command": "other"}}}')
    result = apply_init(workspace, producer_id="local-user", dry_run=True,
                        home=isolated_home, platform="linux", environ={"HOME": str(isolated_home)},
                        command=FAKE_COMMAND, which=_no_which, clients=["cursor-user"])
    assert any("user_scope_requires_authorization" in entry for entry in result.skipped)


def test_init_cli_reports_symlinked_marker(tmp_path: Path) -> None:
    from click.testing import CliRunner
    from agentbus.cli import main

    workspace = tmp_path / "project"
    runtime = workspace / ".agentbus"
    runtime.mkdir(parents=True)
    victim = tmp_path / "victim"
    victim.write_text("unchanged")
    (runtime / "workspace").symlink_to(victim)
    result = CliRunner().invoke(main, ["init", "--workspace", str(workspace), "--apply", "--no-mcp", "--producer-id", "local-user"])
    assert result.exit_code != 0
    assert "runtime_bootstrap_failed" in result.output
    assert victim.read_text() == "unchanged"


def test_toml_owned_table_preserves_following_section_comments() -> None:
    from agentbus.adapters.writer import _find_table_block

    lines = ['[mcp_servers.agentbus]\n', 'command = "old"\n', '\n',
             '# CI profile\n', '[profiles.fast]\n', 'model = "fast"\n']
    assert _find_table_block(lines, 'mcp_servers.agentbus') == (0, 2)


def test_connect_reports_unsafe_lock_as_security_error(tmp_path: Path, isolated_home: Path) -> None:
    workspace = _cursor_workspace(tmp_path)
    runtime = workspace / ".agentbus"
    runtime.mkdir()
    target = tmp_path / "lock-target"
    target.write_text("keep")
    (runtime / "init.lock").symlink_to(target)
    report = _run(workspace, isolated_home, clients=["cursor-project"], apply=True)
    assert report.exit_code == 1
    assert report.items[0].reason == "setup_lock_security"
    assert target.read_text() == "keep"


def test_redact_list_and_scalar_environment_containers() -> None:
    from agentbus.adapters.writer import redact_entry

    redacted = redact_entry({"env": ["GITHUB_TOKEN=abc123"], "headers": ["X-API-Key: abc123"], "environment": "secret"})
    assert redacted == {"env": ["[REDACTED]"], "headers": ["[REDACTED]"], "environment": "[REDACTED]"}


def test_unsafe_extra_environment_is_refused_without_overwriting(tmp_path: Path) -> None:
    from agentbus.adapters.writer import ConfigConflictError, entry_matches, plan_json_patch

    path = tmp_path / "mcp.json"
    expected = {"command": "agentbus", "args": ["mcp-serve"], "env": {"AGENTBUS_WORKSPACE": "/tmp/ws"}}
    existing = {**expected, "env": {**expected["env"], "PYTHONPATH": "./evil"}}
    original = json.dumps({"mcpServers": {"agentbus": existing}})
    path.write_text(original)
    assert not entry_matches(existing, expected)
    with pytest.raises(ConfigConflictError, match="unsafe keys"):
        plan_json_patch(installation_id="cursor-project", path=path, root="mcpServers", entry=expected)
    assert path.read_text() == original


def test_toml_patch_preserves_following_array_table(tmp_path: Path) -> None:
    import tomllib
    from agentbus.adapters.writer import plan_toml_patch

    path = tmp_path / "config.toml"
    path.write_text('[mcp_servers.agentbus]\ncommand = "agentbus"\n\n[[profiles]]\nname = "keep"\n')
    patch = plan_toml_patch(installation_id="codex", path=path, table="mcp_servers.agentbus", entry={"command": "agentbus", "args": ["mcp-serve"]})
    assert tomllib.loads(patch.new_bytes.decode())["profiles"] == [{"name": "keep"}]


def test_unmanaged_launch_settings_are_refused(tmp_path: Path) -> None:
    from agentbus.adapters.writer import ConfigConflictError, entry_matches, plan_json_patch

    path = tmp_path / "mcp.json"
    expected = {"command": "agentbus", "env": {"AGENTBUS_WORKSPACE": "/tmp/ws"}}
    existing = {**expected, "envFile": ".evil.env"}
    path.write_text(json.dumps({"servers": {"agentbus": existing}}))
    assert not entry_matches(existing, expected)
    with pytest.raises(ConfigConflictError, match="unrecognized launch settings"):
        plan_json_patch(installation_id="vscode", path=path, root="servers", entry=expected)


def test_unmanaged_agentbus_environment_is_refused(tmp_path: Path) -> None:
    from agentbus.adapters.writer import ConfigConflictError, entry_matches, plan_json_patch

    path = tmp_path / "mcp.json"
    expected = {"command": "agentbus", "env": {"AGENTBUS_WORKSPACE": "/tmp/ws"}}
    existing = {**expected, "env": {**expected["env"], "AGENTBUS_SQLITE_JOURNAL": "OFF"}}
    path.write_text(json.dumps({"servers": {"agentbus": existing}}))
    assert not entry_matches(existing, expected)
    with pytest.raises(ConfigConflictError, match="unsafe keys"):
        plan_json_patch(installation_id="vscode", path=path, root="servers", entry=expected)


def test_stdio_transport_and_inert_settings_are_preserved(tmp_path: Path) -> None:
    from agentbus.adapters.writer import entry_matches, plan_json_patch

    path = tmp_path / "mcp.json"
    expected = {"command": "agentbus", "env": {"AGENTBUS_WORKSPACE": "/tmp/ws"}}
    existing = {**expected, "type": "stdio", "enabled": True, "tool_timeout_sec": 30}
    path.write_text(json.dumps({"mcpServers": {"agentbus": existing}}))
    assert entry_matches(existing, expected)
    patch = plan_json_patch(installation_id="cursor", path=path, root="mcpServers", entry=expected)
    assert not patch.changed
    assert not entry_matches({**existing, "type": "http"}, expected)


def test_redacts_bare_credential_flags() -> None:
    from agentbus.adapters.writer import redact_entry

    entry = redact_entry({"args": ["--key", "value1", "--pat=value2", "-k", "value3"]})
    assert entry["args"] == ["--key", "[REDACTED]", "--pat=[REDACTED]", "-k", "[REDACTED]"]


def test_first_handoff_payload_escapes_identifiers(tmp_path: Path) -> None:
    from agentbus.discovery import _first_handoff_prompt

    prompt = _first_handoff_prompt(workspace=tmp_path, producer_id='a"b', client='c\\d')
    payload = prompt.split("and payload ", 1)[1].split(". Record the", 1)[0]
    assert json.loads(payload) == {"from": 'a"b', "to": "codex", "summary": 'First handoff from c\\d via MCP'}


def test_codex_header_containers_are_redacted() -> None:
    from agentbus.adapters.writer import redact_entry

    result = redact_entry({"http_headers": {"X-Tenant": "secret"}, "env_http_headers": {"X-Other": "secret"}})
    assert result == {"http_headers": {"X-Tenant": "[REDACTED]"}, "env_http_headers": {"X-Other": "[REDACTED]"}}
