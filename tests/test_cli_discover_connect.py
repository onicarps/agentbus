"""``agentbus discover`` / ``agentbus connect`` and handoff truthfulness."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest
from click.testing import CliRunner

from agentbus.cli import main
from agentbus.store import EventStore


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


def _workspace(tmp_path: Path, config: str = '{"mcpServers": {}}\n') -> Path:
    workspace = tmp_path / "proj"
    path = workspace / ".cursor" / "mcp.json"
    path.parent.mkdir(parents=True)
    path.write_text(config, encoding="utf-8")
    return workspace


def _invoke(args: list[str]) -> "object":
    return CliRunner().invoke(main, args)


def test_discover_json_is_read_only_and_exit_zero(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    before = sorted(str(p.relative_to(workspace)) for p in workspace.rglob("*"))

    result = _invoke(["discover", "--workspace", str(workspace), "--json"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["workspace"] == str(workspace.resolve())
    assert payload["platform"]
    assert isinstance(payload["clients"], list)
    cursor = next(
        item for item in payload["clients"] if item["installation"] == "cursor-project"
    )
    assert cursor["status"] == "present"
    assert cursor["managed"] is False
    assert isinstance(payload["warnings"], list)
    assert all(isinstance(warning, str) for warning in payload["warnings"])
    after = sorted(str(p.relative_to(workspace)) for p in workspace.rglob("*"))
    assert after == before
    assert not (workspace / ".agentbus").exists()


def test_discover_human_output_lists_three_states(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    broken = workspace / ".vscode" / "mcp.json"
    broken.parent.mkdir()
    broken.write_text("{broken\n", encoding="utf-8")
    result = _invoke(["discover", "--workspace", str(workspace)])
    assert result.exit_code == 0, result.output
    assert "Workspace:" in result.output
    cursor_line = next(line for line in result.output.splitlines() if "cursor-project" in line)
    assert cursor_line.split()[0] == "present"
    assert "Clients: 0 present" not in result.output
    assert any(line.split() and line.split()[0] == "absent" for line in result.output.splitlines() if line.startswith("  "))
    vscode_line = next(line for line in result.output.splitlines() if " vscode " in line)
    assert vscode_line.split()[0] == "unknown"


def test_discover_unknown_client_fails_closed(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    before = sorted(str(p) for p in workspace.rglob("*"))

    result = _invoke(["discover", "--workspace", str(workspace), "--client", "nope"])

    assert result.exit_code == 2, result.output
    message = result.output
    assert (
        "Error: Unrecognized client 'nope'. "
        "Supported clients: claude, codex, cursor, gemini, vscode." in message
    )
    # Usage errors happen before discovery: nothing is written or reported.
    assert "Clients:" not in result.output
    assert sorted(str(p) for p in workspace.rglob("*")) == before


def test_connect_unknown_client_fails_closed(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    config = workspace / ".cursor" / "mcp.json"
    before = config.read_bytes()

    result = _invoke(
        [
            "connect",
            "--workspace",
            str(workspace),
            "--client",
            "currsor",
            "--apply",
            "--json",
        ]
    )

    assert result.exit_code == 2, result.output
    assert "Unrecognized client 'currsor'" in (result.output)
    assert config.read_bytes() == before
    assert not (workspace / ".agentbus").exists()


def test_connect_preview_and_apply(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    config = workspace / ".cursor" / "mcp.json"
    original = config.read_bytes()

    preview = _invoke(
        [
            "connect",
            "--workspace",
            str(workspace),
            "--client",
            "cursor-project",
            "--json",
        ]
    )
    assert preview.exit_code == 0, preview.output
    preview_payload = json.loads(preview.output)
    assert preview_payload["dry_run"] is True
    assert preview_payload["items"][0]["action"] == "update"
    assert config.read_bytes() == original
    assert not (workspace / ".agentbus").exists()

    applied = _invoke(
        [
            "connect",
            "--workspace",
            str(workspace),
            "--client",
            "cursor-project",
            "--apply",
            "--json",
        ]
    )
    assert applied.exit_code == 0, applied.output
    applied_payload = json.loads(applied.output)
    item = applied_payload["items"][0]
    assert item["state"] == "awaiting_approval"
    assert item["configured"] is True
    assert item["backup"]
    data = json.loads(config.read_text(encoding="utf-8"))
    assert "agentbus" in data["mcpServers"]
    # Lock file remains as an empty coordination artifact, never as a write lock.
    assert (workspace / ".agentbus" / "init.lock").is_file()

    again = _invoke(
        [
            "connect",
            "--workspace",
            str(workspace),
            "--client",
            "cursor-project",
            "--apply",
            "--json",
        ]
    )
    assert again.exit_code == 0, again.output
    again_payload = json.loads(again.output)
    assert again_payload["items"][0]["action"] == "unchanged"
    assert again_payload["items"][0]["state"] == "configured"

    human = _invoke(
        [
            "connect",
            "--workspace",
            str(workspace),
            "--client",
            "cursor-project",
            "--apply",
        ]
    )
    assert human.exit_code == 0, human.output
    assert "Connect finished (no changes)" in human.output


def test_connect_refuses_conflicting_config_with_nonzero_exit(
    tmp_path: Path,
) -> None:
    workspace = _workspace(tmp_path, '{"mcpServers": "nope"}\n')
    config = workspace / ".cursor" / "mcp.json"
    before = config.read_bytes()

    result = _invoke(
        [
            "connect",
            "--workspace",
            str(workspace),
            "--client",
            "cursor-project",
            "--apply",
        ]
    )

    assert result.exit_code == 1, result.output
    assert "Connect finished with errors" in result.output
    assert "refused" in result.output
    assert config.read_bytes() == before
    assert not (workspace / ".agentbus").exists()


def test_connect_user_scope_needs_authorization_flag(
    tmp_path: Path, isolated_home: Path
) -> None:
    workspace = tmp_path / "proj"
    workspace.mkdir()
    global_config = isolated_home / ".cursor" / "mcp.json"
    global_config.parent.mkdir(parents=True)
    global_config.write_text('{"sentinel": true}\n', encoding="utf-8")
    before = global_config.read_bytes()

    refused = _invoke(
        [
            "connect",
            "--workspace",
            str(workspace),
            "--client",
            "cursor-user",
            "--apply",
        ]
    )
    assert refused.exit_code == 1, refused.output
    assert "user_scope_requires_authorization" in refused.output
    assert global_config.read_bytes() == before

    allowed = _invoke(
        [
            "connect",
            "--workspace",
            str(workspace),
            "--client",
            "cursor-user",
            "--scope",
            "user",
            "--apply",
            "--allow-user-scope",
            "--json",
        ]
    )
    assert allowed.exit_code == 0, allowed.output
    data = json.loads(global_config.read_text(encoding="utf-8"))
    assert data["sentinel"] is True
    assert "agentbus" in data["mcpServers"]


def _prepare_user_cursor(home: Path) -> Path:
    config = home / ".cursor" / "mcp.json"
    config.parent.mkdir(parents=True)
    config.write_text('{"sentinel": true}\n', encoding="utf-8")
    return config


def test_connect_user_scope_interactive_consent_accept(
    tmp_path: Path, isolated_home: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import agentbus.cli as cli_module

    monkeypatch.setattr(cli_module, "_is_interactive_terminal", lambda: True)
    workspace = tmp_path / "proj"
    workspace.mkdir()
    config = _prepare_user_cursor(isolated_home)

    result = CliRunner().invoke(
        main,
        [
            "connect",
            "--workspace",
            str(workspace),
            "--client",
            "cursor-user",
            "--scope",
            "user",
            "--apply",
        ],
        input="y\n",
    )

    assert result.exit_code == 0, result.output
    assert "cursor-user" in result.output
    data = json.loads(config.read_text(encoding="utf-8"))
    assert "agentbus" in data["mcpServers"]


def test_connect_user_scope_interactive_consent_reject(
    tmp_path: Path, isolated_home: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import agentbus.cli as cli_module

    monkeypatch.setattr(cli_module, "_is_interactive_terminal", lambda: True)
    workspace = tmp_path / "proj"
    workspace.mkdir()
    config = _prepare_user_cursor(isolated_home)
    before = config.read_bytes()

    result = CliRunner().invoke(
        main,
        [
            "connect",
            "--workspace",
            str(workspace),
            "--client",
            "cursor-user",
            "--scope",
            "user",
            "--apply",
        ],
        input="n\n",
    )

    assert result.exit_code == 1, result.output
    assert "user_scope_requires_authorization" in result.output
    assert config.read_bytes() == before


def test_connect_verify_requires_apply(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    result = _invoke(
        [
            "connect",
            "--workspace",
            str(workspace),
            "--client",
            "cursor-project",
            "--verify",
            "--json",
        ]
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["verification"]["detail"] == "requires_apply"
    assert payload["verification"]["performed"] is False


def test_connect_apply_verify_completes_real_handshake(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    EventStore(workspace).close()  # verification requires an initialized workspace

    result = _invoke(
        [
            "connect",
            "--workspace",
            str(workspace),
            "--client",
            "cursor-project",
            "--apply",
            "--verify",
            "--json",
        ]
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["verification"]["performed"] is True
    assert payload["verification"]["ok"] is True
    assert payload["verification"]["command_handshake_verified"] is True
    assert payload["verification"]["host_client_observed"] is False
    item = payload["items"][0]
    # The command speaks MCP, but the host client has not been observed:
    # approval remains pending and no client-level "verified" state exists.
    assert item["state"] == "awaiting_approval"
    assert item["command_verified"] is True
    assert item["host_client_observed"] is False
    assert item["verification"]["tool_count"] >= 2
    # Even a verified handshake must not claim the host approved the server.
    assert "host" in payload["verification"]["limitations"].lower()
    # A project-scope write must have produced a first-handoff prompt, and the
    # prompt is never executed by AgentBus itself.
    assert payload["first_handoffs"], "expected a project-scope handoff prompt"
    assert all(
        handoff["executed"] is False for handoff in payload["first_handoffs"]
    )
    assert all(
        handoff["installation"] == "cursor-project"
        for handoff in payload["first_handoffs"]
    )
    for item in payload["items"]:
        assert item["state"] != "verified"


def test_cli_output_never_contains_workspace_secrets(tmp_path: Path) -> None:
    from agentbus.auth import generate_token

    secret = generate_token()
    workspace = _workspace(tmp_path, json.dumps({"mcpServers": {"agentbus": {
        "command": "/old/agentbus", "args": ["mcp-serve", "--token", secret],
        "env": {"EXAMPLE_API_KEY": secret},
    }}}))
    token = workspace / ".agentbus" / "token"
    token.parent.mkdir(parents=True, exist_ok=True)
    token.write_text(secret + "\n", encoding="utf-8")
    token.chmod(0o600)

    for args in (
        ["discover", "--workspace", str(workspace), "--json"],
        ["discover", "--workspace", str(workspace)],
        ["connect", "--workspace", str(workspace), "--client", "cursor-project", "--json"],
        ["connect", "--workspace", str(workspace), "--client", "cursor-project", "--apply"],
    ):
        result = _invoke(args)
        assert result.exit_code == 0, (args, result.output)
        assert secret not in result.output, args


def test_connect_redacts_existing_client_credentials(tmp_path: Path) -> None:
    """Existing env credentials must never appear in preview or apply output."""
    secret = "synthetic-secret-value-12345"
    workspace = tmp_path / "proj"
    config = workspace / ".cursor" / "mcp.json"
    config.parent.mkdir(parents=True)
    config.write_text(
        json.dumps(
            {
                "mcpServers": {
                    "agentbus": {
                        "command": "/old/agentbus",
                        "args": ["mcp-serve", "--token", secret],
                        "env": {
                            "ANTHROPIC_API_KEY": secret,
                            "AGENTBUS_WORKSPACE": str(workspace),
                        },
                    }
                }
            }
        )
        + "\n",
        encoding="utf-8",
    )

    for extra in ([], ["--apply"]):
        result = _invoke(
            [
                "connect",
                "--workspace",
                str(workspace),
                "--client",
                "cursor-project",
                "--json",
                *extra,
            ]
        )
        assert result.exit_code == 0, result.output
        assert secret not in result.output
        payload = json.loads(result.output)
        item = payload["items"][0]
        previous = item["previous_entry"]
        assert previous["env"]["ANTHROPIC_API_KEY"] == "[REDACTED]"
        assert previous["env"]["AGENTBUS_WORKSPACE"] == "[REDACTED]"
        assert item["previous_env_keys"] == [
            "AGENTBUS_WORKSPACE",
            "ANTHROPIC_API_KEY",
        ]
        assert previous["args"][-1] == "[REDACTED]"

    human = _invoke(
        [
            "connect",
            "--workspace",
            str(workspace),
            "--client",
            "cursor-project",
            "--apply",
        ]
    )
    assert human.exit_code == 0, human.output
    assert secret not in human.output


def test_connect_preview_marks_divergent_entry_as_differs(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    config = workspace / ".cursor" / "mcp.json"
    config.write_text(
        json.dumps(
            {
                "mcpServers": {
                    "agentbus": {
                        "command": "/old/agentbus",
                        "args": ["mcp-serve"],
                        "env": {"AGENTBUS_WORKSPACE": str(workspace)},
                    }
                }
            }
        )
        + "\n",
        encoding="utf-8",
    )

    result = _invoke(
        [
            "connect",
            "--workspace",
            str(workspace),
            "--client",
            "cursor-project",
            "--json",
        ]
    )

    assert result.exit_code == 0, result.output
    item = json.loads(result.output)["items"][0]
    assert item["state"] == "differs"
    assert item["configured"] is False
    assert item["changed"] is True
    assert item["previous_entry"]["command"] == "/old/agentbus"


def test_first_handoff_prompt_workflow_against_event_store(tmp_path: Path) -> None:
    """Execute the four prompt hops with the real since_id semantics."""
    from agentbus.discovery import _first_handoff_prompt

    prompt = _first_handoff_prompt(
        workspace=tmp_path, producer_id="factory", client="cursor-project"
    )
    assert "N - 1" in prompt
    assert "since_id set to N" in prompt
    assert "causation_id set to N" in prompt
    assert "event_id > since_id" in prompt

    store = EventStore(tmp_path)
    try:
        message, _ = store.publish(
            topic="okf/handoff",
            producer_id="factory",
            schema_version="1.0",
            payload={
                "from": "factory",
                "to": "codex",
                "summary": "First handoff from cursor-project via MCP",
            },
        )
        n = message.event_id

        # Hop 2: since_id = N - 1 includes event N.
        seen = store.poll("okf/handoff", since_id=n - 1)
        assert [event["event_id"] for event in seen["events"]] == [n]
        # The old instruction (since_id = N) would exclude event N.
        assert store.poll("okf/handoff", since_id=n)["events"] == []

        ack, _ = store.publish(
            topic="okf/handoff",
            producer_id="codex",
            schema_version="1.0",
            payload={
                "from": "codex",
                "to": "factory",
                "summary": f"Ack {n}",
            },
            causation_id=n,
        )
        # Hop 4: since_id = N observes the acknowledgement.
        seen_ack = store.poll("okf/handoff", since_id=n)
        assert [event["event_id"] for event in seen_ack["events"]] == [
            ack.event_id
        ]
        assert seen_ack["events"][0]["causation_id"] == n
    finally:
        store.close()


def test_init_non_interactive_refuses_user_scope_cleanly(
    tmp_path: Path, isolated_home: Path
) -> None:
    workspace = tmp_path / "proj"
    project_config = workspace / ".cursor" / "mcp.json"
    project_config.parent.mkdir(parents=True)
    project_config.write_text('{"mcpServers": {}}\n', encoding="utf-8")
    global_config = isolated_home / ".cursor" / "mcp.json"
    global_config.parent.mkdir(parents=True)
    global_config.write_text('{"sentinel": true}\n', encoding="utf-8")
    before = global_config.read_bytes()

    result = _invoke(
        [
            "init",
            "--workspace",
            str(workspace),
            "--apply",
            "--no-okf",
            "--producer-id",
            "local-user",
        ]
    )

    assert result.exit_code == 0, result.output
    assert "user_scope_requires_authorization" in result.output
    assert "--allow-user-scope" in result.output
    assert global_config.read_bytes() == before
    assert "agentbus" in json.loads(
        project_config.read_text(encoding="utf-8")
    )["mcpServers"]


def test_discover_redacts_secret_override_arguments(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    secret = "override-secret-67890"
    monkeypatch.setenv(
        "AGENTBUS_MCP_COMMAND",
        f"{sys.executable} -m agentbus.cli mcp-serve --token {secret}",
    )
    workspace = _workspace(tmp_path)

    result = _invoke(["discover", "--workspace", str(workspace), "--json"])

    assert result.exit_code == 0, result.output
    assert secret not in result.output
    command = json.loads(result.output)["command"]
    assert command["args"][-2:] == ["--token", "[REDACTED]"]


URL_WITH_CREDENTIALS = (
    "https://example.test/mcp?auth=secret-auth-token&api_key=secret-key"
)
URL_REDACTED = "https://example.test/mcp?auth=[REDACTED]&api_key=[REDACTED]"


def test_discover_redacts_url_query_credentials(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Credentials carried in a URL are redacted by position, not spelling."""
    monkeypatch.setenv(
        "AGENTBUS_MCP_COMMAND",
        f"{sys.executable} -m agentbus.cli mcp-serve --url {URL_WITH_CREDENTIALS}",
    )
    workspace = _workspace(tmp_path)

    result = _invoke(["discover", "--workspace", str(workspace), "--json"])

    assert result.exit_code == 0, result.output
    assert "secret-auth-token" not in result.output
    assert "secret-key" not in result.output
    payload = json.loads(result.output)
    assert payload["command"]["args"][-2:] == ["--url", URL_REDACTED]

    human = _invoke(["discover", "--workspace", str(workspace)])
    assert human.exit_code == 0, human.output
    assert "secret-auth-token" not in human.output
    assert "secret-key" not in human.output
    assert "auth=[REDACTED]" in human.output
    assert "api_key=[REDACTED]" in human.output


def test_connect_redacts_url_credentials_in_existing_entries(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "proj"
    config = workspace / ".cursor" / "mcp.json"
    config.parent.mkdir(parents=True)
    config.write_text(
        json.dumps(
            {
                "mcpServers": {
                    "agentbus": {
                        "command": "/old/agentbus",
                        "args": ["mcp-serve", "--url", URL_WITH_CREDENTIALS],
                        "env": {"AGENTBUS_WORKSPACE": str(workspace)},
                    }
                }
            }
        )
        + "\n",
        encoding="utf-8",
    )

    result = _invoke(
        [
            "connect",
            "--workspace",
            str(workspace),
            "--client",
            "cursor-project",
            "--json",
        ]
    )

    assert result.exit_code == 0, result.output
    assert "secret-auth-token" not in result.output
    assert "secret-key" not in result.output
    item = json.loads(result.output)["items"][0]
    assert item["previous_entry"]["args"][-1] == URL_REDACTED
    assert item["state"] == "differs"


def test_discover_reports_missing_config_in_writable_dir_as_writable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = tmp_path / "proj"
    workspace.mkdir()
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    cursor_binary = bin_dir / "cursor"
    cursor_binary.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    cursor_binary.chmod(0o755)
    monkeypatch.setenv(
        "PATH", f"{bin_dir}{os.pathsep}{os.environ.get('PATH', '')}"
    )

    result = _invoke(["discover", "--workspace", str(workspace), "--json"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    cursor = next(
        item
        for item in payload["clients"]
        if item["installation"] == "cursor-user"
    )
    # Installed binary, no config yet, writable home: a real first run.
    assert cursor["status"] == "present"
    assert cursor["reason"] == "installed_not_configured"
    assert cursor["config_state"] == "missing"
    assert cursor["writable"] is True


def test_nearest_writable_handles_existing_and_missing_ancestors(
    tmp_path: Path,
) -> None:
    from agentbus.adapters.probe import _nearest_writable

    existing = tmp_path / "mcp.json"
    existing.write_text("{}\n", encoding="utf-8")
    assert _nearest_writable(existing) is True
    assert _nearest_writable(tmp_path / "deep" / "nested" / "mcp.json") is True

    readonly = tmp_path / "readonly"
    readonly.mkdir()
    readonly.chmod(0o500)
    try:
        if hasattr(os, "geteuid") and os.geteuid() == 0:
            # Root bypasses directory permission bits, so the assertion below
            # would be meaningless (and CI containers often run as root).
            pytest.skip("root bypasses directory permission bits")
        assert _nearest_writable(readonly / "sub" / "mcp.json") is False
    finally:
        readonly.chmod(0o700)


def test_connect_json_never_prompts_for_user_scope(
    tmp_path: Path, isolated_home: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import agentbus.cli as cli_module

    # Even on a TTY, --json must not offer a prompt.
    monkeypatch.setattr(cli_module, "_is_interactive_terminal", lambda: True)
    workspace = tmp_path / "proj"
    workspace.mkdir()
    config = _prepare_user_cursor(isolated_home)
    before = config.read_bytes()

    result = CliRunner().invoke(
        main,
        [
            "connect",
            "--workspace",
            str(workspace),
            "--client",
            "cursor-user",
            "--scope",
            "user",
            "--apply",
            "--json",
        ],
    )

    assert result.exit_code == 1, result.output
    assert "Write AgentBus server config to user-scope file" not in result.output
    payload = json.loads(result.output)
    assert payload["items"][0]["reason"] == "user_scope_requires_authorization"
    assert payload["items"][0]["state"] == "refused"
    assert config.read_bytes() == before
