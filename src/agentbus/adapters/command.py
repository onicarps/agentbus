"""Resolve a durable, validated MCP server command for this installation.

The command written into a GUI client config must keep working when the client
runs outside the shell that installed AgentBus: no inherited ``PATH``, no
activated virtualenv, no ``python3`` from a different interpreter.  Resolution
therefore prefers absolute executables and always validates what it returns
without starting a long-lived server process.
"""

from __future__ import annotations

import json
import os
import shlex
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Mapping, Sequence

from agentbus.adapters.opencode import is_safe_client_executable

MODULE = "agentbus.cli"
SERVER_SUBCOMMAND = "mcp-serve"
ENV_OVERRIDE = "AGENTBUS_MCP_COMMAND"
#: Tight bound: an executable probe must never hang a first-run CLI.
_PROBE_TIMEOUT = 10.0
#: A real import, not ``find_spec``: locating a module proves nothing about
#: whether its dependencies (``click``, ``mcp``, ...) are installed.
_IMPORT_CHECK = "import agentbus.cli"


class McpCommandError(RuntimeError):
    """No dependable MCP command could be resolved or validated."""

    def __init__(self, code: str, message: str, hints: Sequence[str] = ()):
        self.code = code
        self.hints = tuple(hints)
        detail = message
        if self.hints:
            detail += "\n" + "\n".join(f"  - {hint}" for hint in self.hints)
        super().__init__(detail)


@dataclass(frozen=True)
class McpCommand:
    argv: tuple[str, ...]
    source: str
    warnings: tuple[str, ...] = ()

    @property
    def command(self) -> str:
        return self.argv[0]

    @property
    def args(self) -> tuple[str, ...]:
        return self.argv[1:]

    def with_workspace(self, workspace: Path) -> tuple[str, ...]:
        return (*self.argv, "--workspace", str(workspace))

    def to_dict(self) -> dict[str, object]:
        return {
            "command": self.command,
            "args": list(self.args),
            "source": self.source,
            "warnings": list(self.warnings),
        }


@dataclass(frozen=True)
class CommandProbe:
    ok: bool
    detail: str

    def to_dict(self) -> dict[str, object]:
        return {"ok": self.ok, "detail": self.detail}


def _is_executable_file(path: Path) -> bool:
    try:
        return path.is_file() and os.access(path, os.X_OK)
    except OSError:
        return False


def _script_interpreter(path: Path) -> str | None:
    """Return the shebang interpreter for a console script, if it has one."""
    try:
        with path.open("rb") as handle:
            first = handle.readline(512)
    except OSError:
        return None
    if not first.startswith(b"#!"):
        return None
    try:
        line = first[2:].decode("utf-8").strip()
    except UnicodeDecodeError:
        return None
    if not line:
        return None
    return line.split()[0]


def _same_venv_shim(python: Path) -> Path | None:
    """Console script installed next to the running interpreter, if any."""
    names = ("agentbus.exe", "agentbus") if os.name == "nt" else ("agentbus",)
    candidates = [python.parent / name for name in names]
    if os.name == "nt":
        candidates.append(python.parent / "Scripts" / "agentbus.exe")
    for candidate in candidates:
        if _is_executable_file(candidate) and candidate.resolve() != python.resolve():
            return candidate
    return None


def probe_environ() -> dict[str, str]:
    """Environment for an active probe: the target's own, not this shell's.

    ``PYTHONPATH`` is deliberately dropped. A GUI client launches the command
    without the shell that ran ``agentbus``, so a command that only resolves
    because of a transient source path is a broken client entry. The probe
    must reproduce the client's environment, not the developer's.
    """
    return {
        key: value
        for key, value in os.environ.items()
        if key.upper() != "PYTHONPATH"
    }


def _importable(python: str, *, timeout: float = _PROBE_TIMEOUT) -> CommandProbe:
    """Actively import the CLI under ``python`` so dependencies are proven.

    ``importlib.util.find_spec`` only locates source; it happily reports a
    module as importable inside an interpreter that has none of its runtime
    dependencies. Actually importing ``agentbus.cli`` fails loudly in that
    environment, which is exactly the configuration we must refuse to write.
    """
    try:
        completed = subprocess.run(
            [python, "-P", "-c", _IMPORT_CHECK],
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
            env=probe_environ(),
        )
    except subprocess.TimeoutExpired:
        return CommandProbe(
            False,
            f"{python} did not finish importing agentbus.cli within "
            f"{timeout:.1f}s",
        )
    except OSError as exc:
        return CommandProbe(False, f"import check failed: {exc}")
    if completed.returncode == 0:
        return CommandProbe(
            True, f"{python} imports agentbus.cli and its dependencies"
        )
    detail = (completed.stderr or completed.stdout or "").strip().splitlines()
    tail = detail[-1] if detail else f"exit {completed.returncode}"
    return CommandProbe(
        False,
        f"{python} cannot import agentbus.cli "
        f"(missing runtime dependencies? exit {completed.returncode}): {tail}",
    )


def _executable_responds(
    executable: str, *, timeout: float = _PROBE_TIMEOUT
) -> CommandProbe:
    """Actively execute ``<cmd> --version`` to prove the script can run.

    This catches broken shebangs, missing interpreters, and unrelated files
    with the executable bit set. The probe is bounded to ten seconds and
    never starts the MCP server.
    """
    try:
        completed = subprocess.run(
            [executable, "--version"],
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
            env=probe_environ(),
        )
    except subprocess.TimeoutExpired:
        return CommandProbe(False, f"{executable} did not answer --version in time")
    except OSError as exc:
        return CommandProbe(False, f"{executable} cannot be executed: {exc}")
    output = (completed.stdout or "") + (completed.stderr or "")
    if completed.returncode != 0:
        tail = output.strip().splitlines()
        detail = tail[-1] if tail else f"exit {completed.returncode}"
        return CommandProbe(
            False,
            f"{executable} --version failed (exit {completed.returncode}): {detail}",
        )
    if Path(executable).name.startswith("agentbus") and "agentbus" not in (
        output.lower()
    ):
        return CommandProbe(
            False,
            f"{executable} does not identify as AgentBus: {output.strip()[:120]}",
        )
    return CommandProbe(True, f"{executable} responded to --version")


def _absolutize_argv(
    argv: Sequence[str], *, which: Callable[[str], str | None]
) -> tuple[str, ...]:
    """Replace a PATH-relative executable with an absolute path.

    Symlinks are intentionally **not** resolved: a virtualenv interpreter is
    usually a symlink to the base interpreter, and resolving it would run the
    probe (and later the client) outside the virtualenv that holds AgentBus.
    """
    if not argv:
        return tuple(argv)
    executable = argv[0]
    candidate = Path(executable).expanduser()
    if candidate.is_absolute():
        return (str(candidate), *argv[1:])
    if any(sep in executable for sep in (os.sep, "/", "\\")):
        return (str(candidate.absolute()), *argv[1:])
    found = which(executable)
    if found:
        return (str(Path(found).expanduser().absolute()), *argv[1:])
    return tuple(argv)


def _untrusted_path_binary(binary: str | Path, workspace: Path | None = None) -> bool:
    return not is_safe_client_executable(binary, workspace=workspace)


def validate_mcp_command(
    cmd: McpCommand,
    *,
    timeout: float = _PROBE_TIMEOUT,
    runner: Callable[[str], CommandProbe] | None = None,
) -> CommandProbe:
    """Validate a resolved command without starting the server."""
    executable = cmd.command
    if executable == "agentbus" and not Path(executable).is_absolute():
        return CommandProbe(
            False,
            "relative command 'agentbus' depends on an inherited PATH",
        )
    path = Path(executable)
    if path.is_absolute():
        if not _is_executable_file(path):
            return CommandProbe(False, f"not an executable file: {path}")
        if path.name.startswith("python") or path.name.startswith("pypy"):
            if runner is not None:
                return runner(str(path))
            return _importable(str(path), timeout=timeout)
        interpreter = _script_interpreter(path)
        if interpreter is not None:
            interpreted = Path(interpreter)
            if interpreted.is_absolute() and not _is_executable_file(interpreted):
                return CommandProbe(
                    False,
                    f"script interpreter is missing: {interpreter}",
                )
        # Actively prove the executable can start; never start the server.
        probe = (
            runner(str(path))
            if runner is not None
            else _executable_responds(str(path), timeout=timeout)
        )
        if not probe.ok:
            return probe
        return CommandProbe(True, f"console script executed: {probe.detail}")
    resolved = shutil.which(executable)
    if resolved is None:
        return CommandProbe(False, f"command not found on PATH: {executable}")
    return CommandProbe(True, f"resolved on PATH: {resolved}")


def _parse_override(raw: str) -> list[str]:
    text = raw.strip()
    if not text:
        raise McpCommandError(
            "invalid_override",
            f"{ENV_OVERRIDE} is empty",
            hints=[f"unset {ENV_OVERRIDE} or provide a JSON array of arguments"],
        )
    if text.startswith("{"):
        raise McpCommandError(
            "invalid_override",
            f"{ENV_OVERRIDE} must be a JSON array or argv string, not an object",
        )
    if text.startswith("["):
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError as exc:
            raise McpCommandError(
                "invalid_override",
                f"{ENV_OVERRIDE} is not valid JSON: {exc}",
            ) from exc
        if not isinstance(parsed, list) or not all(
            isinstance(item, str) for item in parsed
        ):
            raise McpCommandError(
                "invalid_override",
                f"{ENV_OVERRIDE} JSON must be an array of strings",
            )
        argv = list(parsed)
    else:
        try:
            argv = shlex.split(text, posix=os.name != "nt")
            if os.name == "nt":
                argv = [
                    a[1:-1] if len(a) >= 2 and a[0] == a[-1] and a[0] in "\"'" else a
                    for a in argv
                ]
        except ValueError as exc:
            raise McpCommandError(
                "invalid_override",
                f"{ENV_OVERRIDE} cannot be parsed: {exc}",
            ) from exc
    if not argv:
        raise McpCommandError("invalid_override", f"{ENV_OVERRIDE} produced no argv")
    return argv


def resolve_mcp_command(
    *,
    environ: Mapping[str, str] | None = None,
    which: Callable[[str], str | None] = shutil.which,
    python: str | None = None,
    validate: bool = True,
    probe_runner: Callable[[str], CommandProbe] | None = None,
    workspace: Path | None = None,
) -> McpCommand:
    """Resolve argv for the MCP server, preferring durable absolute paths."""
    env = dict(os.environ if environ is None else environ)
    interpreter = Path(python or sys.executable)
    if not interpreter.is_absolute():
        interpreter = interpreter.resolve()
    warnings: list[str] = []
    deadline = time.monotonic() + _PROBE_TIMEOUT
    attempted: set[str] = set()

    def probe_command(cmd: McpCommand) -> CommandProbe:
        key = os.path.normcase(os.path.abspath(cmd.command))
        if key in attempted:
            return CommandProbe(False, "executable already probed")
        attempted.add(key)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return CommandProbe(False, "command discovery timed out")
        return validate_mcp_command(cmd, timeout=remaining, runner=probe_runner)

    override = env.get(ENV_OVERRIDE, "").strip()
    if override:
        argv = _parse_override(override)
        # A GUI client may launch with a minimal PATH: always write the
        # resolved absolute executable into the client configuration.
        if argv[0] == "agentbus":
            found = which("agentbus")
            if found and _untrusted_path_binary(found, workspace):
                raise McpCommandError("invalid_override", "untrusted workspace executable on PATH")
            argv = list(_absolutize_argv(argv, which=lambda _name: found))
        else:
            argv = list(_absolutize_argv(argv, which=which))
        cmd = McpCommand(tuple(argv), "env-override", ())
        if validate:
            probe = probe_command(cmd)
            if not probe.ok:
                raise McpCommandError(
                    "invalid_override",
                    f"{ENV_OVERRIDE} failed validation: {probe.detail}",
                    hints=[f"unset {ENV_OVERRIDE} to use auto-detection"],
                )
        return cmd

    shim = _same_venv_shim(interpreter)
    if shim is not None:
        cmd = McpCommand((str(shim), SERVER_SUBCOMMAND), "installed-console-script")
        if not validate:
            return cmd
        probe = probe_command(cmd)
        if probe.ok:
            return cmd
        warnings.append(f"ignored {shim}: {probe.detail}")

    on_path = which("agentbus")
    if on_path and _untrusted_path_binary(on_path, workspace):
        warnings.append(f"ignored untrusted workspace executable: {on_path}")
        on_path = None
    if on_path:
        resolved = Path(on_path).expanduser().absolute()
        if resolved.is_file() and os.access(resolved, os.X_OK):
            cmd = McpCommand(
                (str(resolved), SERVER_SUBCOMMAND),
                "path-console-script",
                tuple(warnings),
            )
            if not validate:
                return cmd
            probe = probe_command(cmd)
            if probe.ok:
                extra: list[str] = []
                if ".venv" in resolved.parts:
                    extra.append(
                        "command points into a virtualenv; that environment must "
                        "stay installed for GUI clients"
                    )
                return McpCommand(cmd.argv, cmd.source, (*cmd.warnings, *extra))
            warnings.append(f"ignored {resolved}: {probe.detail}")

    if _is_executable_file(interpreter):
        cmd = McpCommand(
            (str(interpreter), "-P", "-m", MODULE, SERVER_SUBCOMMAND),
            "python-module",
            tuple(warnings),
        )
        if not validate:
            return cmd
        probe = probe_command(cmd)
        if probe.ok:
            return cmd
        raise McpCommandError(
            "module_unavailable",
            f"no usable MCP command: {probe.detail}",
            hints=[
                "install okf-agentbus into the interpreter your client will use"
                f" ({interpreter})",
                f"or set {ENV_OVERRIDE} to an explicit argv",
                "run 'agentbus doctor' for installation diagnostics",
            ],
        )

    raise McpCommandError(
        "no_durable_command",
        "no durable MCP command found and the current interpreter is not executable",
        hints=[
            f"set {ENV_OVERRIDE} to an explicit argv",
            "install okf-agentbus with pipx/uv so a console script exists",
        ],
    )
