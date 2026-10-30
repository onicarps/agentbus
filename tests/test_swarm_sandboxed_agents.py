"""Sandboxed multi-agent swarm coverage (Mission Part B).

This module exercises the swarm integration surfaces that AgentBus actually
owns, inside a throwaway HOME + workspace, without intentionally using host agent profiles, the live coordination bus,
or an external network/API:

1. **Declarative MCP clients** — Gemini/Agy, Codex and OpenCode v2 are wired
   with ``connect(apply=True, verify=True)`` and then verified with a *real*
   MCP stdio ``initialize`` + ``tools/list`` handshake against a real
   ``agentbus mcp-serve`` child process in the sandbox workspace.
2. **Wake plane** — the real Go worker (``agentbus-go-worker once``) drains
   targeted ``okf/handoff`` events and writes ``WAKE.<agent>.json`` for the
   addressed agent only.
3. **Runner dispatch** — the real Python runner loop consumes each agent's
   wake file, applies its own ``accept_to`` filter and publishes one
   ``RUNNER_ACK`` with ``causation_id`` equal to its own event.
4. **Agent adapters** — Factory, Hermes, Pi and Freebuff command construction,
   subprocess execution, environment scrubbing and ACK publication are driven
   through sandbox *stub* binaries. This proves the adapter/runner plumbing,
   not an LLM turn: no model is called and the real client binaries are never
   executed.
5. **Aider (parked)** — compatibility coverage only: the runner adapter argv
   and prompt are verified, and the parked ``.aider.conf.yml`` is left
   untouched. AgentBus has no Aider config-file parser or Aider MCP manifest,
   so no "config parsing" capability is claimed here.

``LEDGER`` records what each row of evidence actually proves. Tests that need
a capability this environment does not provide (the prebuilt Go worker
binary) skip with an explicit reason instead of substituting a fake. In CI,
or when ``AGENTBUS_REQUIRE_GO_WORKER`` is set, a missing worker fails the test.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import tomllib
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
import yaml

from agentbus.adapters.command import McpCommand
from agentbus.adapters.manifest import entry_launch, load_manifests
from agentbus.discovery import connect
from agentbus.runner import load_runner_config, run_once
from agentbus.store import EventStore

REPO_ROOT = Path(__file__).resolve().parents[1]
GO_WORKER = REPO_ROOT / "go-core" / "bin" / "agentbus-go-worker"

#: Explicit source injection lets the child import this checkout even when
#: verification scrubs PYTHONPATH. The child still runs the real MCP server.
MCP_COMMAND = McpCommand(
    (
        sys.executable,
        "-c",
        f"import runpy, sys; sys.path.insert(0, {str(REPO_ROOT / 'src')!r}); runpy.run_module('agentbus.cli', run_name='__main__')",
        "mcp-serve",
    ),
    "sandbox-test",
)

#: Swarm roster from the mission brief.
SWARM = (
    "agy",
    "codex",
    "opencode",
    "factory",
    "hermes",
    "pi",
    "freebuff",
    "aider",
)

#: What each evidence row proves. ``real_agent_process`` is True only when the
#: genuine vendor binary is executed (it is False everywhere: this suite must
#: stay offline and must not spend tokens).
LEDGER: dict[str, dict[str, Any]] = {
    "agy": {
        "evidence": "gemini MCP config + real agentbus stdio handshake",
        "real_agent_process": False,
        "note": "gemini host client itself is not launched or observed",
    },
    "codex": {
        "evidence": "codex MCP config + real agentbus stdio handshake",
        "real_agent_process": False,
        "note": "codex host client itself is not launched or observed",
    },
    "opencode": {
        "evidence": "opencode v2 config + entry_launch + real stdio handshake",
        "real_agent_process": False,
        "note": "opencode host client itself is not launched or observed",
    },
    "factory": {
        "evidence": "go worker wake file + runner loop + stub droid adapter",
        "real_agent_process": False,
        "note": "droid exec argv/prompt built and executed against a stub",
    },
    "hermes": {
        "evidence": "go worker wake file + runner loop + stub hermes adapter",
        "real_agent_process": False,
        "note": "hermes chat argv/prompt built and executed against a stub",
    },
    "pi": {
        "evidence": "go worker wake file + runner loop + stub pi adapter",
        "real_agent_process": False,
        "note": "pi argv/prompt built and executed against a stub",
    },
    "freebuff": {
        "evidence": "go worker wake file + runner loop + stub pty adapter",
        "real_agent_process": False,
        "note": "freebuff PTY prompt delivery driven against a stub",
    },
    "aider": {
        "evidence": "runner adapter argv/prompt + parked config untouched",
        "real_agent_process": False,
        "note": "parked identity; no aider MCP manifest or config parser exists",
    },
}

#: Host agent profiles that must never be created or rewritten by this suite.
_HOST_PROFILE_PATHS = (
    ".gemini/config/mcp_config.json",
    ".gemini/settings.json",
    ".codex/config.toml",
    ".config/opencode/opencode.json",
    ".aider.conf.yml",
    ".aider.tags.cache.v4",
    ".factory/settings.json",
    ".hermes/config.yaml",
    ".agentbus",
)

#: Ambient authority / credentials that must not leak into sandboxed children.
_ENV_KEYS_TO_CLEAR = (
    "AGENTBUS_TOKEN",
    "AGENTBUS_WORKSPACE",
    "AGENTBUS_PRODUCER_ID",
    "AGENTBUS_MCP_COMMAND",
    "AGENTBUS_IDENTITY_PRIVATE_KEY",
    "AGENTBUS_WEBHOOK_TOKEN",
    "AGENTBUS_ALLOW_DRVFS",
    "XDG_CONFIG_HOME",
    "CODEX_HOME",
    "VIBE_HOME",
    "APPDATA",
    "LOCALAPPDATA",
    "OPENAI_API_KEY",
    "ANTHROPIC_API_KEY",
    "GEMINI_API_KEY",
)


class ExternalNetworkBlocked(RuntimeError):
    """Raised when in-process code attempts an outbound socket connection."""


def _real_home() -> Path:
    """Host home directory, independent of the ``HOME`` env var."""
    try:
        import pwd

        return Path(pwd.getpwuid(os.getuid()).pw_dir)
    except (ImportError, KeyError):  # pragma: no cover - non-POSIX
        return Path(os.path.expanduser("~"))


def _stat_signature(path: Path) -> tuple[bool, int, int]:
    """(exists, size or child count, mtime_ns) — cheap mutation detector."""
    try:
        stat = path.stat()
    except (FileNotFoundError, NotADirectoryError):
        return (False, 0, 0)
    if path.is_dir():
        try:
            children = len(list(path.iterdir()))
        except OSError:
            children = -1
        return (True, children, stat.st_mtime_ns)
    return (True, stat.st_size, stat.st_mtime_ns)


def _host_profiles_state() -> dict[str, tuple[bool, int, int]]:
    home = _real_home()
    return {rel: _stat_signature(home / rel) for rel in _HOST_PROFILE_PATHS}


@dataclass(frozen=True)
class Sandbox:
    root: Path
    home: Path
    workspace: Path
    bin_dir: Path
    tmp_dir: Path
    environ: Mapping[str, str]


@pytest.fixture(autouse=True)
def hermetic_environment(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Shadow HOME/workspace, block in-process outbound sockets, and check host profiles.

    The host-profile snapshot is captured *before* ``HOME`` is redirected and
    compared after the test, so every test in this module proves it left the
    operator's agent configuration alone.
    """
    before = _host_profiles_state()

    home = tmp_path / "sandbox-home"
    workspace = tmp_path / "sandbox-workspace"
    bindir = tmp_path / "sandbox-bin"
    tmpdir = tmp_path / "sandbox-tmp"
    for path in (home, workspace, bindir, tmpdir):
        path.mkdir(parents=True, exist_ok=True)

    for key in _ENV_KEYS_TO_CLEAR:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("USERPROFILE", str(home))
    monkeypatch.setenv("XDG_CONFIG_HOME", str(home / ".config"))
    monkeypatch.setenv("AGENTBUS_WORKSPACE", str(workspace))
    monkeypatch.setenv("TMPDIR", str(tmpdir))
    monkeypatch.setenv("PATH", str(bindir) + os.pathsep + os.environ.get("PATH", ""))

    # Never launch a host ``opencode --version`` probe from inside a test.
    import agentbus.adapters.opencode as opencode_module

    monkeypatch.setattr(
        opencode_module, "_default_version_probe", lambda _binary, _env: None
    )

    def _blocked(*_args: Any, **_kwargs: Any) -> Any:
        raise ExternalNetworkBlocked("outbound network is blocked in the sandbox")

    monkeypatch.setattr(socket.socket, "connect", _blocked)
    monkeypatch.setattr(socket.socket, "connect_ex", _blocked)
    monkeypatch.setattr(socket, "create_connection", _blocked)

    yield

    assert _host_profiles_state() == before, (
        "sandboxed swarm test mutated a host agent profile"
    )
    assert str(Path(os.environ["AGENTBUS_WORKSPACE"])).startswith(str(tmp_path))
    assert Path(os.environ["HOME"]) == home


@pytest.fixture
def sandbox(tmp_path: Path) -> Sandbox:
    home = tmp_path / "sandbox-home"
    workspace = tmp_path / "sandbox-workspace"
    bindir = tmp_path / "sandbox-bin"
    tmpdir = tmp_path / "sandbox-tmp"
    for path in (home, workspace, bindir, tmpdir):
        path.mkdir(parents=True, exist_ok=True)
    return Sandbox(
        root=tmp_path,
        home=home,
        workspace=workspace,
        bin_dir=bindir,
        tmp_dir=tmpdir,
        environ={
            "HOME": str(home),
            "USERPROFILE": str(home),
            "XDG_CONFIG_HOME": str(home / ".config"),
            "PATH": os.environ.get("PATH", ""),
            "AGENTBUS_WORKSPACE": str(workspace),
        },
    )


# ---------------------------------------------------------------------------
# Helpers: events, wake files, runner configs, stub CLIs
# ---------------------------------------------------------------------------


def _publish_handoffs(workspace: Path, agents: tuple[str, ...]) -> dict[str, int]:
    store = EventStore(workspace)
    try:
        published: dict[str, int] = {}
        for agent in agents:
            event, duplicate = store.publish(
                topic="okf/handoff",
                producer_id="agy",
                schema_version="1.0",
                payload={
                    "from": "agy",
                    "to": agent,
                    "summary": f"sandboxed task for {agent}",
                },
            )
            assert duplicate is False
            published[agent] = event.event_id
        return published
    finally:
        store.close()


def _write_wake_file(workspace: Path, agent: str, event_id: int) -> Path:
    """Write the documented ``WAKE.<agent>.json`` shape for a stored event.

    Field-for-field identical to the Go worker's ``makeWakeFile`` payload.
    The runner rehydrates payload from ``events.db``, so the store stays the
    source of truth even for this hand-written envelope.
    """
    store = EventStore(workspace)
    try:
        event = store.get_event(event_id)
    finally:
        store.close()
    assert event is not None
    document = {
        "schema_version": "1.0",
        "woken_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "worker_id": f"{agent}-1",
        "event_id": event.event_id,
        "topic": event.topic,
        "trace_id": event.trace_id,
        "causation_id": event.causation_id,
        "payload": event.payload,
        "hint": {
            "suggested_prompt_tokens_budget": "task-only",
            "do_not": "full-system-prompt-replay-for-idle-check",
            "ack_with_causation_id": str(event.event_id),
        },
    }
    path = workspace / ".agentbus" / f"WAKE.{agent}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
    return path


def _write_runner_config(
    workspace: Path,
    agent: str,
    *,
    adapter: dict[str, Any] | None = None,
    accept_to: tuple[str, ...] | None = None,
    wake_file: str | None = None,
) -> Path:
    path = workspace / ".agentbus" / f"runner.{agent}.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    document = {
        "version": "1.0",
        "runner_id": f"{agent}-sandbox",
        "producer_id": agent,
        "intake": {
            "mode": "wake_file",
            "wake_file": wake_file or f".agentbus/WAKE.{agent}.json",
        },
        "adapter": adapter or {"type": "echo"},
        "accept_to": list(accept_to or (agent,)),
        "allow_broadcast": False,
        "budget": {"max_turns_per_chain": 4, "max_event_age_hours": 24},
        "runs_dir": f".agentbus/runs/{agent}",
    }
    path.write_text(yaml.safe_dump(document), encoding="utf-8")
    return path


def _stub_cli(bin_dir: Path, name: str) -> Path:
    """Sandbox stand-in for a vendor CLI.

    Records its argv/environment (path in ``STUB_DUMP``) and prints a marker so
    the runner ACK summarises a real child process. Freebuff is driven through
    a PTY, so that variant consumes one stdin line first.
    """
    path = bin_dir / name
    path.write_text(
        """
#!/usr/bin/env python3
import json
import os
import sys

dump = os.environ.get("STUB_DUMP", "")
line = ""
if os.path.basename(sys.argv[0]) == "freebuff":
    line = sys.stdin.readline()
record = {
    "argv": sys.argv,
    "producer": os.environ.get("AGENTBUS_PRODUCER_ID"),
    "workspace": os.environ.get("AGENTBUS_WORKSPACE"),
    "wake_event": os.environ.get("AGENTBUS_WAKE_EVENT_ID"),
    "credentials": {
        key: os.environ.get(key)
        for key in (
            "AGENTBUS_TOKEN", "AGENTBUS_WEBHOOK_TOKEN", "AGENTBUS_IDENTITY_PRIVATE_KEY",
            "OPENAI_API_KEY", "ANTHROPIC_API_KEY", "GEMINI_API_KEY",
        )
    },
    "cwd": os.getcwd(),
    "pty_line": line,
}
if dump:
    with open(dump, "w", encoding="utf-8") as handle:
        json.dump(record, handle)
print(
    "STUB_TURN_COMPLETE producer=%s wake=%s"
    % (record["producer"], record["wake_event"])
)
""".lstrip(),
        encoding="utf-8",
    )
    path.chmod(0o755)
    return path


def _go_worker() -> Path:
    """Use a prebuilt worker; building inside the sandbox may access the network."""
    if GO_WORKER.is_file() and os.access(GO_WORKER, os.X_OK):
        return GO_WORKER
    message = f"prebuild agentbus-go-worker before running this test: {GO_WORKER}"
    if os.environ.get("CI") or os.environ.get("AGENTBUS_REQUIRE_GO_WORKER"):
        pytest.fail(message)
    pytest.skip(message)


def _write_worker_config(workspace: Path, agent: str) -> Path:
    path = workspace / ".agentbus" / f"worker.{agent}.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "version": "1.0",
                "worker_id": f"{agent}-1",
                "role": agent,
                "producer_id": agent,
                "cursor_path": f".agentbus/worker.{agent}.cursor",
                "state_path": f".agentbus/worker.{agent}.state.json",
                "subscribe": [{"topic": "okf/handoff", "to": [agent]}],
                "watch": {
                    "mode": "poll",
                    "poll_fallback_ms": 50,
                    "paths": [".agentbus/events.db"],
                },
                "on_task": [{"write": {"path": f".agentbus/WAKE.{agent}.json"}}],
                "budget": {"max_dispatches_per_hour": 60, "max_concurrent_exec": 1},
                "dispatch": {"max_event_age": "1h"},
                "wake_mode": "file",
            }
        ),
        encoding="utf-8",
    )
    return path


def _run_worker_once(binary: Path, sandbox: Sandbox, config: Path) -> dict[str, Any]:
    env = {
        "PATH": sandbox.environ["PATH"],
        "HOME": str(sandbox.home),
        "TMPDIR": str(sandbox.tmp_dir),
        "AGENTBUS_WORKSPACE": str(sandbox.workspace),
    }
    completed = subprocess.run(
        [
            str(binary),
            "--cmd",
            "once",
            "--config",
            str(config),
            "--workspace",
            str(sandbox.workspace),
        ],
        cwd=str(sandbox.workspace),
        check=False,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert completed.returncode == 0, completed.stderr
    return json.loads(completed.stdout.strip())


# ---------------------------------------------------------------------------
# 1. Declarative MCP clients: sandboxed config + real stdio handshake
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ClientCase:
    agent: str
    installation: str
    seed: Callable[[Sandbox], None]
    config_path: Callable[[Sandbox], Path]
    read_entry: Callable[[Path], Mapping[str, Any]]
    extra_assert: Callable[[Path], None]


def _seed_gemini(sandbox: Sandbox) -> None:
    path = sandbox.home / ".gemini" / "config" / "mcp_config.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"theme": "dark", "mcpServers": {}}, indent=2) + "\n",
        encoding="utf-8",
    )


def _seed_codex_project(sandbox: Sandbox) -> None:
    path = sandbox.workspace / ".codex" / "config.toml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('model = "gpt-6-sol"\n', encoding="utf-8")
    (sandbox.workspace / ".opencode").mkdir(exist_ok=True)


def _seed_opencode_v2(sandbox: Sandbox) -> None:
    (sandbox.workspace / ".opencode").mkdir(exist_ok=True)
    path = sandbox.workspace / "opencode.json"
    path.write_text(
        json.dumps(
            {
                "$schema": "https://opencode.ai/config.json",
                "theme": "opencode",
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def _gemini_entry(path: Path) -> Mapping[str, Any]:
    document = json.loads(path.read_text(encoding="utf-8"))
    assert document["theme"] == "dark", "unrelated Gemini settings were clobbered"
    return document["mcpServers"]["agentbus"]


def _codex_entry(path: Path) -> Mapping[str, Any]:
    document = tomllib.loads(path.read_text(encoding="utf-8"))
    assert document["model"] == "gpt-6-sol", "unrelated Codex settings were clobbered"
    return document["mcp_servers"]["agentbus"]


def _opencode_entry(path: Path) -> Mapping[str, Any]:
    document = json.loads(path.read_text(encoding="utf-8"))
    assert document["$schema"].endswith("opencode.ai/config.json")
    assert document["theme"] == "opencode"
    return document["mcp"]["servers"]["agentbus"]


def _assert_opencode_shape(path: Path) -> None:
    document = json.loads(path.read_text(encoding="utf-8"))
    entry = document["mcp"]["servers"]["agentbus"]
    # v2 keeps the whole argv in one list and env under "environment".
    assert isinstance(entry["command"], list)
    assert isinstance(entry["environment"], dict)
    assert entry["disabled"] is False
    assert "agentbus" not in document["mcp"], "v1 layout was written by mistake"


def _assert_codex_shape(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    assert "[mcp_servers.agentbus]" in text
    assert "[mcp_servers.agentbus.env]" in text


def _assert_gemini_shape(path: Path) -> None:
    document = json.loads(path.read_text(encoding="utf-8"))
    assert set(document) >= {"mcpServers", "theme"}


CLIENT_CASES = (
    ClientCase(
        agent="agy",
        installation="gemini",
        seed=_seed_gemini,
        config_path=lambda s: s.home / ".gemini" / "config" / "mcp_config.json",
        read_entry=_gemini_entry,
        extra_assert=_assert_gemini_shape,
    ),
    ClientCase(
        agent="codex",
        installation="codex-project",
        seed=_seed_codex_project,
        config_path=lambda s: s.workspace / ".codex" / "config.toml",
        read_entry=_codex_entry,
        extra_assert=_assert_codex_shape,
    ),
    ClientCase(
        agent="opencode",
        installation="opencode-v2-project",
        seed=_seed_opencode_v2,
        config_path=lambda s: s.workspace / "opencode.json",
        read_entry=_opencode_entry,
        extra_assert=_assert_opencode_shape,
    ),
)


@pytest.mark.parametrize("case", CLIENT_CASES, ids=lambda c: c.agent)
def test_client_config_and_real_stdio_handshake(
    case: ClientCase, sandbox: Sandbox
) -> None:
    """Wire one real client layout, then prove the written command speaks MCP."""
    case.seed(sandbox)
    EventStore(sandbox.workspace).close()

    report = connect(
        workspace=sandbox.workspace,
        clients=[case.installation],
        apply=True,
        verify=True,
        allow_user_scope=True,
        home=sandbox.home,
        platform="linux",
        environ=sandbox.environ,
        command=MCP_COMMAND,
        which=lambda _name: None,
    )

    assert report.exit_code == 0, report.errors
    items = {item.installation_id: item for item in report.items}
    item = items[case.installation]
    assert item.state == "awaiting_approval", item.reason
    assert item.command_verified is True, item.verification
    # A stdio handshake proves the command; it does not prove the host client
    # loaded or approved anything. The report must keep saying so.
    assert item.host_client_observed is False

    config_path = case.config_path(sandbox)
    assert config_path.is_file()
    case.extra_assert(config_path)

    entry = case.read_entry(config_path)
    argv, entry_env = entry_launch(entry)
    assert argv[:4] == list(MCP_COMMAND.argv)
    assert argv[-2:] == ["--workspace", str(sandbox.workspace)]
    assert entry_env["AGENTBUS_WORKSPACE"] == str(sandbox.workspace)
    assert entry_env["AGENTBUS_PRODUCER_ID"] == "local-user"

    verification = report.verification
    assert verification["performed"] is True
    assert verification["ok"] is True, verification
    assert verification["detail"] == "mcp_stdio_handshake"
    assert verification["host_client_observed"] is False
    assert "unobserved" in verification["limitations"]

    result = next(
        r for r in verification["results"] if r["installation"] == case.installation
    )
    assert result["ok"] is True, result["error"]
    assert result["server_name"] == "agentbus"
    assert result["protocol_version"]
    assert {"agentbus_publish", "agentbus_poll"} <= set(result["tool_names"])


def test_connect_verification_never_writes_outside_the_sandbox(
    sandbox: Sandbox,
) -> None:
    """Config artifacts stay under the sandbox HOME/workspace; nothing global."""
    _seed_gemini(sandbox)
    EventStore(sandbox.workspace).close()

    report = connect(
        workspace=sandbox.workspace,
        clients=["gemini"],
        apply=True,
        verify=True,
        allow_user_scope=True,
        home=sandbox.home,
        platform="linux",
        environ=sandbox.environ,
        command=MCP_COMMAND,
        which=lambda _name: None,
    )
    assert report.exit_code == 0, report.errors
    targets = [Path(item.path) for item in report.items if item.path]
    assert targets, "expected the Gemini config artifact"
    for path in targets:
        assert sandbox.root in path.parents, path

    # The handshake launched the configured agentbus command, not a host client.
    assert report.verification["results"][0]["server_name"] == "agentbus"
    assert not any(sandbox.bin_dir.iterdir())


def _which_from_bin(sandbox: Sandbox, *names: str) -> Callable[[str], str | None]:
    """Resolve only the sandbox stub binaries, never the operator's PATH."""

    def resolve(name: str) -> str | None:
        if name not in names:
            return None
        candidate = sandbox.bin_dir / name
        return str(candidate) if candidate.is_file() else None

    return resolve


def test_codex_user_config_is_created_from_binary_evidence(sandbox: Sandbox) -> None:
    """A detected Codex binary with no config: create ``~/.codex/config.toml``."""
    stub = _stub_cli(sandbox.bin_dir, "codex")

    report = connect(
        workspace=sandbox.workspace,
        clients=["codex-user"],
        apply=True,
        allow_user_scope=True,
        home=sandbox.home,
        platform="linux",
        environ=sandbox.environ,
        command=MCP_COMMAND,
        which=_which_from_bin(sandbox, "codex"),
    )

    assert report.exit_code == 0, report.errors
    item = next(i for i in report.items if i.installation_id == "codex-user")
    assert item.state == "awaiting_approval", item.reason
    assert item.action == "create"
    assert item.changed is True

    path = sandbox.home / ".codex" / "config.toml"
    assert path.is_file()
    document = tomllib.loads(path.read_text(encoding="utf-8"))
    assert document["mcp_servers"]["agentbus"]["command"] == sys.executable
    # Detection never executes the detected client binary.
    assert stub.is_file()


def test_opencode_layout_is_not_guessed_without_a_version_signal(
    sandbox: Sandbox,
) -> None:
    """No config, no ``$schema``, no version output: fail closed, write nothing."""
    _stub_cli(sandbox.bin_dir, "opencode")
    (sandbox.workspace / ".opencode").mkdir()

    report = connect(
        workspace=sandbox.workspace,
        clients=["opencode"],
        apply=True,
        home=sandbox.home,
        platform="linux",
        environ=sandbox.environ,
        command=MCP_COMMAND,
        which=_which_from_bin(sandbox, "opencode"),
    )

    assert report.items
    for item in report.items:
        assert item.state == "refused"
        assert item.reason.startswith("opencode_version_ambiguous"), item.reason
    assert not (sandbox.workspace / "opencode.json").exists()


def test_gemini_app_dir_without_binary_evidence_is_refused(sandbox: Sandbox) -> None:
    """``~/.gemini`` alone is weak evidence; AgentBus must not invent a config."""
    (sandbox.home / ".gemini").mkdir()

    report = connect(
        workspace=sandbox.workspace,
        clients=["gemini"],
        apply=True,
        allow_user_scope=True,
        home=sandbox.home,
        platform="linux",
        environ=sandbox.environ,
        command=MCP_COMMAND,
        which=lambda _name: None,
    )

    item = next(i for i in report.items if i.installation_id == "gemini")
    assert item.state == "refused"
    assert item.reason == "weak_evidence:app_dir_only"
    assert report.exit_code != 0
    assert not (sandbox.home / ".gemini" / "config" / "mcp_config.json").exists()


# ---------------------------------------------------------------------------
# 2. Wake plane: real Go worker, targeted wakes only
# ---------------------------------------------------------------------------


TARGETED_DAEMONS = ("factory", "hermes", "pi", "freebuff")


def test_go_worker_writes_only_the_addressed_agents_wake(sandbox: Sandbox) -> None:
    binary = _go_worker()
    published = _publish_handoffs(sandbox.workspace, (*TARGETED_DAEMONS, "codex"))

    results = {}
    for agent in TARGETED_DAEMONS:
        config = _write_worker_config(sandbox.workspace, agent)
        first = _run_worker_once(binary, sandbox, config)
        assert first["matched"] == 1, (agent, first)

        wake_path = sandbox.workspace / ".agentbus" / f"WAKE.{agent}.json"
        assert wake_path.is_file()
        wake = json.loads(wake_path.read_text(encoding="utf-8"))
        assert wake["schema_version"] == "1.0"
        assert wake["worker_id"] == f"{agent}-1"
        assert wake["event_id"] == published[agent]
        assert wake["topic"] == "okf/handoff"
        assert wake["payload"]["to"] == agent
        assert wake["payload"]["summary"] == f"sandboxed task for {agent}"
        assert wake["hint"]["ack_with_causation_id"] == str(published[agent])

        # Cursor advanced: a second drain re-dispatches nothing and does not
        # rewrite the wake file.
        before = wake_path.read_bytes()
        second = _run_worker_once(binary, sandbox, config)
        assert second["matched"] == 0
        assert wake_path.read_bytes() == before
        results[agent] = first

    # Codex was addressed but has no worker config here: no wake artifact.
    wake_files = sorted(
        p.name for p in (sandbox.workspace / ".agentbus").glob("WAKE.*.json")
    )
    assert wake_files == sorted(f"WAKE.{agent}.json" for agent in TARGETED_DAEMONS)
    for agent in TARGETED_DAEMONS:
        wake = json.loads(
            (sandbox.workspace / ".agentbus" / f"WAKE.{agent}.json").read_text()
        )
        assert wake["event_id"] != published["codex"]


# ---------------------------------------------------------------------------
# 3. Runner dispatch: confined to the addressed agent
# ---------------------------------------------------------------------------


def test_worker_to_runner_handoff_stays_within_one_agent(sandbox: Sandbox) -> None:
    """Full wake plane: Go worker writes the file, the runner consumes it.

    Each of the four daemons gets its own worker config and its own runner
    config in one shared sandbox workspace. Every runner must process exactly
    its own event and ACK only that event.
    """
    binary = _go_worker()
    published = _publish_handoffs(sandbox.workspace, TARGETED_DAEMONS)

    consumed: dict[str, int] = {}
    for agent in TARGETED_DAEMONS:
        worker_config = _write_worker_config(sandbox.workspace, agent)
        drained = _run_worker_once(binary, sandbox, worker_config)
        assert drained["matched"] == 1

        runner_config = load_runner_config(
            _write_runner_config(sandbox.workspace, agent)
        )
        results = run_once(sandbox.workspace, runner_config)
        assert len(results) == 1, (agent, results)
        assert results[0]["status"] == "processed", results[0]
        assert results[0]["event_id"] == published[agent]
        consumed[agent] = results[0]["event_id"]

        # Only workers drained so far may have created wake files.
        remaining = {
            p.name for p in (sandbox.workspace / ".agentbus").glob("WAKE.*.json")
        }
        drained_so_far = TARGETED_DAEMONS[: TARGETED_DAEMONS.index(agent) + 1]
        assert remaining == {f"WAKE.{name}.json" for name in drained_so_far}

    assert consumed == published

    store = EventStore(sandbox.workspace)
    try:
        events = store.poll("okf/handoff", since_id=0)["events"]
    finally:
        store.close()
    acks = [e for e in events if e["payload"]["summary"].startswith("RUNNER_ACK")]
    assert len(acks) == len(TARGETED_DAEMONS)
    assert sorted(ack["causation_id"] for ack in acks) == sorted(published.values())
    producer_by_causation = {
        ack["causation_id"]: ack["payload"]["from"] for ack in acks
    }
    for agent, event_id in published.items():
        assert producer_by_causation[event_id] == agent


def test_runner_dispatch_is_confined_to_the_addressed_agent(sandbox: Sandbox) -> None:
    published = _publish_handoffs(sandbox.workspace, TARGETED_DAEMONS)
    for agent in TARGETED_DAEMONS:
        _write_wake_file(sandbox.workspace, agent, published[agent])

    processed: dict[str, int] = {}
    for agent in TARGETED_DAEMONS:
        config = load_runner_config(_write_runner_config(sandbox.workspace, agent))
        results = run_once(sandbox.workspace, config)
        assert len(results) == 1, (agent, results)
        outcome = results[0]
        assert outcome["status"] == "processed", outcome
        assert outcome["ok"] is True
        assert outcome["event_id"] == published[agent]
        processed[agent] = outcome["event_id"]

        run_log = (
            sandbox.workspace
            / ".agentbus"
            / "runs"
            / agent
            / f"{agent}-sandbox"
            / str(published[agent])
            / "result.json"
        )
        assert run_log.is_file(), run_log

    store = EventStore(sandbox.workspace)
    try:
        events = store.poll("okf/handoff", since_id=0)["events"]
    finally:
        store.close()

    acks = [
        e for e in events if e["payload"].get("summary", "").startswith("RUNNER_ACK")
    ]
    assert len(acks) == len(TARGETED_DAEMONS)
    by_causation = {ack["causation_id"]: ack for ack in acks}
    assert set(by_causation) == set(processed.values())
    for agent, event_id in processed.items():
        ack = by_causation[event_id]
        assert ack["payload"]["from"] == agent
        assert ack["payload"]["to"] == "agy"
        assert ack["payload"]["action"]["status"] == "ops_only"


def test_runner_skips_a_wake_addressed_to_another_agent(sandbox: Sandbox) -> None:
    """A wake file is not authority: ``accept_to`` still gates the turn."""
    published = _publish_handoffs(sandbox.workspace, ("hermes",))
    # Factory's intake path now contains hermes' event (misrouted wake).
    _write_wake_file(sandbox.workspace, "factory", published["hermes"])
    config = load_runner_config(_write_runner_config(sandbox.workspace, "factory"))

    results = run_once(sandbox.workspace, config)

    assert len(results) == 1
    assert results[0]["status"] == "skipped"
    assert results[0]["reason"] == "to_not_accepted:hermes"
    store = EventStore(sandbox.workspace)
    try:
        events = store.poll("okf/handoff", since_id=0)["events"]
    finally:
        store.close()
    assert len(events) == 1, "the misrouted event must not be acknowledged"


# ---------------------------------------------------------------------------
# 4. Agent adapters: real subprocess execution against sandbox stubs
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class AdapterCase:
    agent: str
    binary: str
    expected_argv_head: tuple[str, ...]
    expected_argv_tokens: tuple[str, ...]


ADAPTER_CASES = (
    AdapterCase(
        agent="factory",
        binary="droid",
        expected_argv_head=("exec", "-f"),
        expected_argv_tokens=("--cwd", "--skip-permissions-unsafe"),
    ),
    AdapterCase(
        agent="hermes",
        binary="hermes",
        expected_argv_head=("chat", "-q"),
        expected_argv_tokens=("--max-turns", "--accept-hooks"),
    ),
    AdapterCase(
        agent="pi",
        binary="pi",
        expected_argv_head=("--print", "--no-session"),
        expected_argv_tokens=("--no-approve", "--no-extensions"),
    ),
    AdapterCase(
        agent="freebuff",
        binary="freebuff",
        expected_argv_head=("--cwd",),
        expected_argv_tokens=(),
    ),
)


@pytest.mark.skipif(
    os.name == "nt", reason="sandbox stubs rely on POSIX shebang execution"
)
@pytest.mark.parametrize("case", ADAPTER_CASES, ids=lambda c: c.agent)
def test_runner_executes_agent_adapter_against_sandbox_stub(
    case: AdapterCase, sandbox: Sandbox, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Adapter argv + env + runner ACK, executed as a real child process.

    The vendor binary is replaced by a sandbox stub: this validates the
    adapter contract, not an LLM turn.
    """
    stub = _stub_cli(sandbox.bin_dir, case.binary)
    dump = sandbox.root / f"dump-{case.agent}.json"
    monkeypatch.setenv("STUB_DUMP", str(dump))
    # Ambient bus authority must never reach the child.
    for key in (
        "AGENTBUS_TOKEN",
        "AGENTBUS_WEBHOOK_TOKEN",
        "AGENTBUS_IDENTITY_PRIVATE_KEY",
        "OPENAI_API_KEY",
        "ANTHROPIC_API_KEY",
        "GEMINI_API_KEY",
    ):
        monkeypatch.setenv(key, "test-value")

    published = _publish_handoffs(sandbox.workspace, (case.agent,))
    _write_wake_file(sandbox.workspace, case.agent, published[case.agent])

    adapter: dict[str, Any] = {
        "type": case.agent,
        "command": str(stub),
        "timeout_seconds": 30,
    }
    if case.agent == "freebuff":
        adapter["startup_seconds"] = 0.2
    config = load_runner_config(
        _write_runner_config(sandbox.workspace, case.agent, adapter=adapter)
    )

    results = run_once(sandbox.workspace, config)

    assert len(results) == 1, results
    outcome = results[0]
    assert outcome["status"] == "processed", outcome
    assert outcome["ok"] is True, outcome
    assert outcome["event_id"] == published[case.agent]
    assert "RUNNER_ACK" in outcome["summary"]

    record = json.loads(dump.read_text(encoding="utf-8"))
    argv = record["argv"]
    assert argv[0] == str(stub)
    assert tuple(argv[1 : 1 + len(case.expected_argv_head)]) == case.expected_argv_head
    for token in case.expected_argv_tokens:
        assert token in argv, (token, argv)

    assert record["producer"] == case.agent
    assert record["workspace"] == str(sandbox.workspace)
    assert record["wake_event"] == str(published[case.agent])
    assert record["cwd"] == str(sandbox.workspace)
    for key in (
        "AGENTBUS_TOKEN",
        "AGENTBUS_WEBHOOK_TOKEN",
        "AGENTBUS_IDENTITY_PRIVATE_KEY",
    ):
        assert record["credentials"][key] is None
    for key in ("OPENAI_API_KEY", "ANTHROPIC_API_KEY", "GEMINI_API_KEY"):
        assert record["credentials"][key] == "test-value"

    store = EventStore(sandbox.workspace)
    try:
        events = store.poll("okf/handoff", since_id=0)["events"]
    finally:
        store.close()
    acks = [e for e in events if e["causation_id"] == published[case.agent]]
    assert len(acks) == 1
    assert acks[0]["payload"]["from"] == case.agent

    if case.agent == "freebuff":
        # Freebuff is prompt-driven over a PTY; delivery is the contract.
        assert record["pty_line"].startswith("# AgentBus headless Freebuff turn")
    else:
        assert "STUB_TURN_COMPLETE" in outcome["summary"]


# ---------------------------------------------------------------------------
# 5. Aider: parked compatibility bridge
# ---------------------------------------------------------------------------


def test_aider_bridge_runs_via_stub_and_leaves_parked_config_alone(
    sandbox: Sandbox, monkeypatch: pytest.MonkeyPatch
) -> None:
    parked = sandbox.home / ".aider.conf.yml"
    parked.write_text(
        "read: [CONVENTIONS.md]\nmodel: parked-host-model\n", encoding="utf-8"
    )
    parked_before = parked.read_bytes()

    stub = _stub_cli(sandbox.bin_dir, "aider")
    dump = sandbox.root / "dump-aider.json"
    monkeypatch.setenv("STUB_DUMP", str(dump))

    published = _publish_handoffs(sandbox.workspace, ("aider",))
    _write_wake_file(sandbox.workspace, "aider", published["aider"])
    config = load_runner_config(
        _write_runner_config(
            sandbox.workspace,
            "aider",
            adapter={
                "type": "aider",
                "command": str(stub),
                "model": "sandbox-model",
                "yes_always": True,
                "timeout_seconds": 30,
            },
        )
    )

    results = run_once(sandbox.workspace, config)

    assert len(results) == 1
    assert results[0]["status"] == "processed"
    assert results[0]["ok"] is True, results[0]

    argv = json.loads(dump.read_text(encoding="utf-8"))["argv"]
    assert argv[0] == str(stub)
    assert argv[1] == "--message"
    prompt = argv[2]
    assert "Aider" in prompt
    assert f"event_id: {published['aider']}" in prompt
    assert "--yes-always" in argv
    assert argv[argv.index("--model") + 1] == "sandbox-model"

    # Aider is parked: AgentBus neither parses nor rewrites its config file.
    assert parked.read_bytes() == parked_before
    manifests = load_manifests()
    assert "aider" not in {manifest.manifest_id for manifest in manifests}
    assert all(manifest.client != "aider" for manifest in manifests)
    assert all(manifest.installation("aider") is None for manifest in manifests)


def test_aider_yes_flag_is_opt_in() -> None:
    from agentbus.runner.adapters.aider import build_aider_command

    without = build_aider_command(
        aider_bin="aider",
        message="prompt",
        cwd=Path("/tmp"),
        yes_always=False,
        model=None,
        extra_args=[],
    )
    with_flag = build_aider_command(
        aider_bin="aider",
        message="prompt",
        cwd=Path("/tmp"),
        yes_always=True,
        model=None,
        extra_args=[],
    )
    assert without == ["aider", "--message", "prompt"]
    assert with_flag == ["aider", "--message", "prompt", "--yes-always"]


# ---------------------------------------------------------------------------
# 6. Evidence ledger: keep genuine vs simulated claims honest
# ---------------------------------------------------------------------------


def test_coverage_ledger_covers_every_swarm_agent() -> None:
    assert set(LEDGER) == set(SWARM)
    for agent, row in LEDGER.items():
        assert row["evidence"], agent
        assert row["note"], agent
        # No row may claim the vendor agent process actually ran: this suite
        # is offline and token-free by design.
        assert row["real_agent_process"] is False, agent


def test_factory_runner_never_requests_mission_mode(
    sandbox: Sandbox, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``--mission`` spawns workers; the swarm runner must keep it off."""
    stub = _stub_cli(sandbox.bin_dir, "droid")
    dump = sandbox.root / "dump-mission.json"
    monkeypatch.setenv("STUB_DUMP", str(dump))
    published = _publish_handoffs(sandbox.workspace, ("factory",))
    _write_wake_file(sandbox.workspace, "factory", published["factory"])
    config = load_runner_config(
        _write_runner_config(
            sandbox.workspace,
            "factory",
            adapter={
                "type": "factory",
                "command": str(stub),
                "timeout_seconds": 30,
            },
        )
    )
    results = run_once(sandbox.workspace, config)

    assert results[0]["ok"] is True, results[0]
    argv = json.loads(dump.read_text(encoding="utf-8"))["argv"]
    assert "--mission" not in argv
    assert "--auto" not in argv  # skip_permissions default maps to the unsafe flag


def test_sandbox_is_hermetic_and_blocks_outbound_network(sandbox: Sandbox) -> None:
    """Guard fixture sanity: env is re-rooted and sockets fail closed."""
    assert Path(os.environ["HOME"]) == sandbox.home
    assert Path(os.environ["AGENTBUS_WORKSPACE"]) == sandbox.workspace
    assert _real_home() != sandbox.home
    assert not list(sandbox.bin_dir.glob("*"))
    with pytest.raises(ExternalNetworkBlocked):
        socket.create_connection(("example.com", 443), timeout=0.1)
