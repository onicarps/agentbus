"""Durable MCP command resolution and validation (no persistent servers)."""

from __future__ import annotations

import json
import os
import stat
import subprocess
import sys
from pathlib import Path

import pytest

from agentbus.adapters.command import (
    ENV_OVERRIDE,
    CommandProbe,
    McpCommand,
    McpCommandError,
    resolve_mcp_command,
    validate_mcp_command,
)
from agentbus.adapters.manifest import build_entry, install_target


def _write_executable(path: Path, body: str = "#!/bin/sh\nexit 0\n") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)
    return path


def test_env_override_accepts_json_and_shell_forms(tmp_path: Path) -> None:
    json_cmd = resolve_mcp_command(
        environ={ENV_OVERRIDE: json.dumps(["/opt/ab", "mcp-serve"])},
        which=lambda _name: None,
        validate=False,
    )
    assert json_cmd.argv == ("/opt/ab", "mcp-serve")
    assert json_cmd.source == "env-override"

    shell_cmd = resolve_mcp_command(
        environ={ENV_OVERRIDE: "'/opt/agent bus/bin/agentbus' mcp-serve"},
        which=lambda _name: None,
        validate=False,
    )
    # Quoting survives: one argv element even with a space in the path.
    assert shell_cmd.argv == ("/opt/agent bus/bin/agentbus", "mcp-serve")


@pytest.mark.parametrize("raw", ["[]", '{"command": "x"}', "['unterminated"])
def test_env_override_rejects_bad_values(raw: str) -> None:
    with pytest.raises(McpCommandError) as excinfo:
        resolve_mcp_command(
            environ={ENV_OVERRIDE: raw}, which=lambda _name: None, validate=False
        )
    assert excinfo.value.code == "invalid_override"


def test_env_override_is_validated(tmp_path: Path) -> None:
    bogus = tmp_path / "agentbus"
    with pytest.raises(McpCommandError) as excinfo:
        resolve_mcp_command(
            environ={ENV_OVERRIDE: f"{bogus} mcp-serve"},
            which=lambda _name: None,
        )
    assert excinfo.value.code == "invalid_override"
    assert excinfo.value.hints


def test_prefers_console_script_next_to_interpreter(tmp_path: Path) -> None:
    bin_dir = tmp_path / "venv" / "bin"
    python = _write_executable(bin_dir / "python", "#!/bin/sh\nexit 0\n")
    shim = _write_executable(bin_dir / "agentbus", "#!/bin/sh\nexit 0\n")

    cmd = resolve_mcp_command(
        environ={},
        which=lambda _name: None,
        python=str(python),
        probe_runner=lambda path: CommandProbe(True, f"ok:{path}"),
    )

    assert cmd.argv == (str(shim), "mcp-serve")
    assert cmd.source == "installed-console-script"
    assert cmd.argv[0].startswith("/")


def test_path_shim_is_made_absolute(tmp_path: Path) -> None:
    shim = _write_executable(tmp_path / "shell-only" / "agentbus")

    cmd = resolve_mcp_command(
        environ={},
        which=lambda name: str(shim) if name == "agentbus" else None,
        python=str(tmp_path / "no-python"),
        probe_runner=lambda path: CommandProbe(True, "ok"),
    )

    assert Path(cmd.argv[0]).is_absolute()
    assert cmd.source == "path-console-script"


def test_falls_back_to_module_when_shim_is_broken(tmp_path: Path) -> None:
    # A console script whose shebang interpreter vanished must not be written
    # into a client config: fall through to the interpreter module form.
    shim = _write_executable(
        tmp_path / "bin" / "agentbus", "#!/nonexistent/python\n"
    )
    python = _write_executable(tmp_path / "venv" / "bin" / "python")

    cmd = resolve_mcp_command(
        environ={},
        which=lambda name: str(shim) if name == "agentbus" else None,
        python=str(python),
        probe_runner=lambda _p: CommandProbe(True, "import ok"),
    )

    assert cmd.argv == (str(python), "-P", "-m", "agentbus.cli", "mcp-serve")
    assert any("interpreter is missing" in warning for warning in cmd.warnings)


def test_hostile_path_without_python_reports_actionable_error(
    tmp_path: Path,
) -> None:
    with pytest.raises(McpCommandError) as excinfo:
        resolve_mcp_command(
            environ={"PATH": ""},
            which=lambda _name: None,
            python=str(tmp_path / "missing" / "python"),
        )
    assert excinfo.value.code == "no_durable_command"
    assert any(ENV_OVERRIDE in hint for hint in excinfo.value.hints)


def test_module_unavailable_is_reported_with_hints(tmp_path: Path) -> None:
    python = _write_executable(tmp_path / "bin" / "python")
    with pytest.raises(McpCommandError) as excinfo:
        resolve_mcp_command(
            environ={},
            which=lambda _name: None,
            python=str(python),
            probe_runner=lambda _p: CommandProbe(False, "cannot import"),
        )
    assert excinfo.value.code == "module_unavailable"
    assert "install okf-agentbus" in str(excinfo.value)


def test_validation_never_starts_the_server(tmp_path: Path) -> None:
    shim = _write_executable(tmp_path / "bin" / "agentbus")
    seen: list[str] = []

    def runner(path: str) -> CommandProbe:
        seen.append(path)
        return CommandProbe(True, "stub --version")

    probe = validate_mcp_command(
        McpCommand((str(shim), "mcp-serve"), "installed-console-script"),
        runner=runner,
    )
    assert probe.ok is True
    # A console script is actively probed (--version), never started as a
    # long-lived MCP server.
    assert seen == [str(shim)]
    assert "executed" in probe.detail


def test_broken_console_script_is_rejected(tmp_path: Path) -> None:
    # Executable bit and a shebang are not enough: the script must actually run.
    shim = _write_executable(
        tmp_path / "bin" / "agentbus", "#!/bin/sh\nexit 42\n"
    )
    probe = validate_mcp_command(McpCommand((str(shim), "mcp-serve"), "test"))
    assert probe.ok is False
    assert "exit 42" in probe.detail


def test_unrelated_executable_is_rejected(tmp_path: Path) -> None:
    shim = _write_executable(
        tmp_path / "bin" / "agentbus", "#!/bin/sh\necho other-tool\n"
    )
    probe = validate_mcp_command(McpCommand((str(shim), "mcp-serve"), "test"))
    assert probe.ok is False
    assert "does not identify as AgentBus" in probe.detail


def test_missing_shebang_interpreter_is_rejected(tmp_path: Path) -> None:
    shim = _write_executable(
        tmp_path / "bin" / "agentbus", "#!/nonexistent/python\n"
    )
    probe = validate_mcp_command(McpCommand((str(shim), "mcp-serve"), "test"))
    assert probe.ok is False
    assert "interpreter is missing" in probe.detail


def test_relative_command_is_rejected() -> None:
    probe = validate_mcp_command(McpCommand(("agentbus", "mcp-serve"), "test"))
    assert probe.ok is False
    assert "PATH" in probe.detail


def test_entry_quotes_workspace_with_spaces(tmp_path: Path) -> None:
    workspace = tmp_path / "dir with spaces"
    workspace.mkdir()
    installation = install_target("cursor-project")
    entry = build_entry(
        installation,
        argv=("/opt/agent bus/bin/agentbus", "mcp-serve"),
        workspace=workspace,
        producer_id="local-user",
    )
    # Args stay a list: no shell quoting is baked into the value.
    assert str(workspace) in entry["args"]
    assert entry["command"] == "/opt/agent bus/bin/agentbus"
    round_trip = json.loads(json.dumps(entry))
    assert round_trip == entry


def test_vscode_entry_uses_stdio_type_and_not_mcp_servers(tmp_path: Path) -> None:
    installation = install_target("vscode")
    entry = build_entry(
        installation,
        argv=("/usr/bin/agentbus", "mcp-serve"),
        workspace=tmp_path,
        producer_id="local-user",
    )
    assert entry["type"] == "stdio"
    assert "mcpServers" not in entry


def test_real_interpreter_resolves_to_durable_command() -> None:
    import sys

    cmd = resolve_mcp_command(
        environ={ENV_OVERRIDE: ""},
        which=lambda _name: None,
        python=sys.executable,
    )
    assert Path(cmd.argv[0]).is_absolute()
    assert "mcp-serve" in cmd.argv
    assert cmd.source in {"installed-console-script", "python-module"}
    assert validate_mcp_command(cmd).ok is True


def test_module_fallback_uses_absolute_interpreter(tmp_path: Path) -> None:
    python = _write_executable(tmp_path / "venv" / "bin" / "python")
    cmd = resolve_mcp_command(
        environ={ENV_OVERRIDE: ""},
        which=lambda _name: None,
        python=str(python),
        validate=False,
    )
    assert cmd.argv == (str(python), "-P", "-m", "agentbus.cli", "mcp-serve")
    assert cmd.source == "python-module"


def test_env_override_writes_resolved_absolute_executable(tmp_path: Path) -> None:
    shim = _write_executable(tmp_path / "bin" / "agentbus")

    cmd = resolve_mcp_command(
        environ={ENV_OVERRIDE: "agentbus mcp-serve"},
        which=lambda name: str(shim) if name == "agentbus" else None,
        python=str(tmp_path / "no-python"),
        probe_runner=lambda _path: CommandProbe(True, "ok"),
    )

    assert cmd.argv == (str(shim.absolute()), "mcp-serve")
    assert Path(cmd.argv[0]).is_absolute()


@pytest.fixture(scope="module")
def dependency_free_python(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """An interpreter that can import neither AgentBus nor its dependencies."""
    base = tmp_path_factory.mktemp("dependency-free")
    venv_dir = base / "venv"
    subprocess.run(
        [sys.executable, "-m", "venv", "--without-pip", str(venv_dir)],
        check=True,
        capture_output=True,
        timeout=120,
    )
    relative = (
        Path("Scripts") / "python.exe" if os.name == "nt" else Path("bin") / "python"
    )
    python = venv_dir / relative
    assert python.exists(), "venv interpreter was not created"
    return python


def test_probe_rejects_interpreter_missing_runtime_dependencies(
    dependency_free_python: Path,
) -> None:
    """Locating the module is not enough: the probe must import it for real."""
    cmd = McpCommand(
        (str(dependency_free_python), "-m", "agentbus.cli", "mcp-serve"),
        "python-module",
    )

    probe = validate_mcp_command(cmd)

    assert probe.ok is False
    assert "missing runtime dependencies" in probe.detail
    assert "cannot import agentbus.cli" in probe.detail


def test_probe_ignores_transient_pythonpath(
    dependency_free_python: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A source path in the shell must not make a broken command look valid.

    This mirrors the review reproduction: with ``src`` on PYTHONPATH the old
    ``find_spec`` check reported the module as importable, while the real
    import failed on the missing ``click`` dependency.
    """
    src = str(Path(__file__).resolve().parents[1] / "src")
    developer_env = {**os.environ, "PYTHONPATH": src}

    located = subprocess.run(
        [
            str(dependency_free_python),
            "-c",
            "import importlib.util; "
            "raise SystemExit(0 if importlib.util.find_spec('agentbus.cli') else 3)",
        ],
        env=developer_env,
        capture_output=True,
        timeout=30,
    )
    assert located.returncode == 0, "find_spec should locate the source module"

    imported = subprocess.run(
        [str(dependency_free_python), "-c", "import agentbus.cli"],
        env=developer_env,
        capture_output=True,
        timeout=30,
    )
    assert imported.returncode != 0
    assert b"ModuleNotFoundError" in imported.stderr

    monkeypatch.setenv("PYTHONPATH", src)
    probe = validate_mcp_command(
        McpCommand(
            (str(dependency_free_python), "-m", "agentbus.cli", "mcp-serve"),
            "python-module",
        )
    )
    assert probe.ok is False


def test_resolution_refuses_dependency_free_interpreter(
    dependency_free_python: Path,
) -> None:
    with pytest.raises(McpCommandError) as excinfo:
        resolve_mcp_command(
            environ={ENV_OVERRIDE: ""},
            which=lambda _name: None,
            python=str(dependency_free_python),
        )
    assert excinfo.value.code == "module_unavailable"
    assert "missing runtime dependencies" in str(excinfo.value)


def test_probe_accepts_the_running_interpreter() -> None:
    probe = validate_mcp_command(
        McpCommand((sys.executable, "-m", "agentbus.cli", "mcp-serve"), "test")
    )
    assert probe.ok is True
    assert "imports agentbus.cli and its dependencies" in probe.detail


def test_path_console_script_preserves_symlink(tmp_path):
    target = _write_executable(tmp_path / 'versioned' / 'agentbus')
    shim = tmp_path / 'agentbus'
    shim.symlink_to(target)
    cmd = resolve_mcp_command(environ={}, which=lambda name: str(shim) if name == 'agentbus' else None,
                              python=str(tmp_path / 'missing-python'), validate=False)
    assert cmd.argv[0] == str(shim)


def test_windows_shell_override_preserves_backslashes(monkeypatch: pytest.MonkeyPatch) -> None:
    from agentbus.adapters import command

    monkeypatch.setattr(command.os, "name", "nt")
    assert command._parse_override(r'"C:\Program Files\AgentBus\agentbus.exe" mcp-serve') == [
        r"C:\Program Files\AgentBus\agentbus.exe", "mcp-serve"
    ]


def test_resolution_does_not_probe_same_executable_twice(tmp_path, monkeypatch):
    from agentbus.adapters.command import CommandProbe, McpCommandError, resolve_mcp_command
    import agentbus.adapters.command as command_module

    shim = tmp_path / "agentbus"
    shim.write_text("#!/bin/sh\nexit 1\n")
    shim.chmod(0o755)
    monkeypatch.setattr(command_module, "_same_venv_shim", lambda interpreter: shim)
    calls = []
    def runner(path):
        calls.append(path)
        return CommandProbe(False, "bad")
    with pytest.raises(McpCommandError):
        resolve_mcp_command(environ={}, which=lambda name: str(shim), python=str(tmp_path / "missing"), probe_runner=runner)
    assert calls == [str(shim)]

@pytest.mark.parametrize("directory", ["root", "cwd", "cwd/bin", "bin", ".venv/bin", ".venv/Scripts"])
def test_path_command_trust_boundary(tmp_path, monkeypatch, directory):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    cwd = tmp_path / "cwd"
    cwd.mkdir()
    monkeypatch.chdir(cwd)
    target = cwd if directory == "cwd" else cwd / "bin" if directory == "cwd/bin" else workspace if directory == "root" else workspace / directory
    binary = _write_executable(target / "agentbus")
    interpreter = _write_executable(tmp_path / "runtime" / "python")
    calls = []

    def runner(path):
        calls.append(path)
        return CommandProbe(True, "ok")

    cmd = resolve_mcp_command(environ={}, which=lambda _: str(binary),
                              python=str(interpreter), workspace=workspace,
                              probe_runner=runner)
    assert cmd.command == str(interpreter)
    assert any("ignored untrusted workspace executable" in warning for warning in cmd.warnings)
    assert calls == [str(interpreter)]
    with pytest.raises(McpCommandError) as excinfo:
        resolve_mcp_command(environ={ENV_OVERRIDE: "agentbus"},
                            which=lambda _: str(binary), workspace=workspace,
                            probe_runner=runner)
    assert excinfo.value.code == "invalid_override"
    assert calls == [str(interpreter)]


def test_workspace_interpreter_sibling_is_trusted(tmp_path, monkeypatch):
    workspace = tmp_path / "workspace"
    interpreter = _write_executable(workspace / ".venv" / "bin" / "python")
    shim = _write_executable(interpreter.parent / "agentbus")
    monkeypatch.chdir(workspace)
    calls = []

    def runner(path):
        calls.append(path)
        return CommandProbe(True, "ok")

    cmd = resolve_mcp_command(environ={}, which=lambda _: str(shim),
                              python=str(interpreter), workspace=workspace,
                              probe_runner=runner)
    assert cmd.source == "installed-console-script"
    assert cmd.command == str(shim)
    assert calls == [str(shim)]


def test_override_checks_the_path_result_it_uses(tmp_path):
    trusted = _write_executable(tmp_path / "trusted" / "agentbus")
    workspace = tmp_path / "workspace"
    untrusted = _write_executable(workspace / "bin" / "agentbus")
    results = iter([str(trusted), str(untrusted)])
    cmd = resolve_mcp_command(environ={ENV_OVERRIDE: "agentbus"},
                              which=lambda _: next(results), workspace=workspace,
                              validate=False)
    assert cmd.command == str(trusted)
