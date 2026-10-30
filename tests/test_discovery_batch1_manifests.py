"""Batch 1 discovery: six client manifests, probe states, preview, and apply."""

from __future__ import annotations

import json
import tomllib
from pathlib import Path

import pytest
import yaml

from agentbus.adapters.command import McpCommand
from agentbus.adapters.manifest import (
    build_entry,
    install_target,
    load_manifests,
)
from agentbus.adapters.probe import (
    ENTRY_ABSENT,
    ENTRY_DIFFERS,
    ENTRY_MATCHES,
    STATUS_ABSENT,
    STATUS_PRESENT,
    probe_installations,
)
from agentbus.adapters.writer import plan_patch
from agentbus.discovery import (
    ACTION_CREATE,
    ACTION_UPDATE,
    STATE_AWAITING,
    STATE_CONFIGURED,
    STATE_DIFFERS,
    STATE_PLANNED,
    STATE_REFUSED,
    connect,
)

FAKE_COMMAND = McpCommand(("/usr/bin/agentbus", "mcp-serve"), "test")

BATCH1 = {"goose", "cline", "vibe", "copilot", "opencode", "qwen"}


@pytest.fixture(autouse=True)
def _no_real_version_probe(monkeypatch: pytest.MonkeyPatch) -> None:
    import agentbus.adapters.opencode as opencode_module

    monkeypatch.setattr(
        opencode_module, "_default_version_probe", lambda _binary, _env: None
    )


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
        "XDG_CONFIG_HOME",
        "VIBE_HOME",
        "CODEX_HOME",
        "APPDATA",
        "LOCALAPPDATA",
    ):
        monkeypatch.delenv(name, raising=False)
    return home


def _no_which(_name: str) -> None:
    return None


def _which(*names: str):
    def resolve(name: str) -> str | None:
        if name in names:
            return f"/usr/bin/{name}"
        return None

    return resolve


def _workspace(tmp_path: Path) -> Path:
    workspace = tmp_path / "project"
    workspace.mkdir()
    return workspace


def _entry(installation_id: str, workspace: Path) -> dict:
    return build_entry(
        install_target(installation_id),
        argv=FAKE_COMMAND.argv,
        workspace=workspace,
        producer_id="local-user",
    )


def _snapshot(root: Path) -> dict[str, bytes]:
    return {
        str(path.relative_to(root)): path.read_bytes()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


# ---------------------------------------------------------------------------
# Manifests
# ---------------------------------------------------------------------------


def test_batch1_manifests_are_packaged() -> None:
    manifests = load_manifests()
    ids = {manifest.manifest_id for manifest in manifests}
    assert BATCH1 <= ids
    # The release-base integrations remain packaged.
    assert {"claude", "codex", "cursor", "gemini", "vscode"} <= ids


@pytest.mark.parametrize(
    "installation_id, attributes",
    [
        ("goose-user", {"format": "yaml", "root": "extensions", "scope": "user"}),
        ("cline-user", {"format": "json", "root": "mcpServers", "scope": "user"}),
        (
            "vibe-project",
            {"format": "toml", "root": "mcp_servers", "root_kind": "array"},
        ),
        ("copilot-user", {"format": "json", "root": "mcpServers", "scope": "user"}),
        (
            "opencode-v1-project",
            {
                "format": "json",
                "root": "mcp",
                "variant_group": "opencode",
                "variant": "v1",
            },
        ),
        (
            "opencode-v2-user",
            {
                "format": "json",
                "root": "mcp.servers",
                "variant_group": "opencode",
                "variant": "v2",
            },
        ),
        ("qwen-user", {"format": "json", "root": "mcpServers", "scope": "user"}),
        ("qwen-project", {"format": "json", "root": "mcpServers", "scope": "project"}),
    ],
)
def test_batch1_manifest_layout(installation_id: str, attributes: dict) -> None:
    installation = install_target(installation_id)
    for attribute, value in attributes.items():
        assert getattr(installation, attribute) == value


@pytest.mark.parametrize(
    "installation_id, keys",
    [
        ("goose-user", {"type", "name", "enabled", "cmd", "args", "envs"}),
        ("cline-user", {"command", "args", "env", "disabled"}),
        ("vibe-project", {"name", "transport", "command", "args", "env"}),
        ("copilot-user", {"type", "command", "args", "env", "tools"}),
        ("opencode-v1-project", {"type", "command", "environment", "enabled"}),
        ("opencode-v2-project", {"type", "command", "environment", "disabled"}),
        ("qwen-project", {"command", "args", "env"}),
    ],
)
def test_batch1_entry_shapes(installation_id: str, keys: set[str]) -> None:
    entry = _entry(installation_id, Path("/workspace"))
    assert set(entry) == keys
    # Every entry must point at this workspace with the standard producer.
    env = entry.get("envs") or entry.get("environment") or entry.get("env")
    assert env["AGENTBUS_WORKSPACE"] == "/workspace"
    assert env["AGENTBUS_PRODUCER_ID"] == "local-user"


def test_batch1_documented_values() -> None:
    goose = _entry("goose-user", Path("/workspace"))
    assert goose["type"] == "stdio"
    assert goose["name"] == "agentbus"
    assert goose["enabled"] is True
    assert goose["cmd"] == "/usr/bin/agentbus"

    vibe = _entry("vibe-project", Path("/workspace"))
    assert vibe["name"] == "agentbus"
    assert vibe["transport"] == "stdio"

    copilot = _entry("copilot-user", Path("/workspace"))
    assert copilot["type"] == "local"
    assert copilot["tools"] == ["*"]

    cline = _entry("cline-user", Path("/workspace"))
    assert cline["disabled"] is False

    v1 = _entry("opencode-v1-project", Path("/workspace"))
    assert v1["enabled"] is True
    assert v1["command"] == [
        "/usr/bin/agentbus",
        "mcp-serve",
        "--workspace",
        "/workspace",
    ]

    v2 = _entry("opencode-v2-project", Path("/workspace"))
    assert v2["disabled"] is False
    assert v2["type"] == "local"


def test_opencode_variants_share_paths_and_selectors() -> None:
    v1 = install_target("opencode-v1-project")
    v2 = install_target("opencode-v2-project")
    assert v1.paths == v2.paths
    assert v1.variant_selector == "opencode-v1"
    assert v2.variant_selector == "opencode-v2"
    assert v1.selected_by("opencode-v1")
    assert not v1.selected_by("opencode-v2")
    assert v1.selected_by("opencode")


# ---------------------------------------------------------------------------
# Probe states for every batch 1 client
# ---------------------------------------------------------------------------


def _write_configured(
    installation_id: str, workspace: Path, home: Path
) -> tuple[Path, dict]:
    installation = install_target(installation_id)
    entry = _entry(installation_id, workspace)
    path = installation.path_for(
        platform="linux",
        workspace=workspace,
        home=home,
        environ={"HOME": str(home)},
    )
    patch = plan_patch(installation, path=path, entry=entry)
    assert patch.new_bytes is not None
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(patch.new_bytes)
    return path, entry


@pytest.mark.parametrize(
    "installation_id",
    [
        "goose-user",
        "cline-user",
        "vibe-project",
        "copilot-user",
        "opencode-v1-project",
        "opencode-v2-project",
        "qwen-user",
        "qwen-project",
    ],
)
def test_probe_reports_configured_differing_and_absent(
    installation_id: str, tmp_path: Path, isolated_home: Path
) -> None:
    workspace = _workspace(tmp_path)
    installation = install_target(installation_id)

    absent = probe_installations(
        workspace=workspace,
        home=isolated_home,
        platform="linux",
        environ={"HOME": str(isolated_home)},
        clients=[installation_id],
        expected_entries={installation_id: _entry(installation_id, workspace)},
        which=_no_which,
    )
    assert len(absent) == 1
    assert absent[0].status == STATUS_ABSENT
    assert absent[0].entry_state == ENTRY_ABSENT

    _path, entry = _write_configured(installation_id, workspace, isolated_home)

    configured = probe_installations(
        workspace=workspace,
        home=isolated_home,
        platform="linux",
        environ={"HOME": str(isolated_home)},
        clients=[installation_id],
        expected_entries={installation_id: entry},
        which=_no_which,
    )
    assert len(configured) == 1
    assert configured[0].status == STATUS_PRESENT
    assert configured[0].entry_state == ENTRY_MATCHES
    assert configured[0].managed is True

    different = dict(entry)
    if installation.format == "yaml":
        different["cmd"] = "/different/agentbus"
    elif installation.entry_shape == "opencode_v1":
        different["disabled"] = False
    elif installation.entry_shape == "opencode_v2":
        different["disabled"] = True
    elif installation.entry_shape == "vibe_stdio":
        different["args"] = ["other"]
    else:
        different["args"] = ["other"]

    differs = probe_installations(
        workspace=workspace,
        home=isolated_home,
        platform="linux",
        environ={"HOME": str(isolated_home)},
        clients=[installation_id],
        expected_entries={installation_id: different},
        which=_no_which,
    )
    assert len(differs) == 1
    assert differs[0].entry_state == ENTRY_DIFFERS


def test_probe_detects_binary_without_config(tmp_path: Path, isolated_home: Path) -> None:
    workspace = _workspace(tmp_path)
    probes = probe_installations(
        workspace=workspace,
        home=isolated_home,
        platform="linux",
        environ={"HOME": str(isolated_home)},
        clients=["qwen-user"],
        expected_entries={"qwen-user": _entry("qwen-user", workspace)},
        which=_which("qwen"),
    )
    assert probes[0].status == STATUS_PRESENT
    assert probes[0].reason == "installed_not_configured"
    assert probes[0].config_state == "missing"


# ---------------------------------------------------------------------------
# Dry-run preview and idempotent apply for all six clients
# ---------------------------------------------------------------------------


def _prepare_all_six(workspace: Path, home: Path) -> dict[str, Path]:
    paths = {
        "goose": home / ".config" / "goose" / "config.yaml",
        "cline": home / ".cline" / "data" / "settings" / "cline_mcp_settings.json",
        "copilot": home / ".copilot" / "mcp-config.json",
        "vibe": workspace / ".vibe" / "config.toml",
        "opencode": workspace / "opencode.json",
        "qwen": workspace / ".qwen" / "settings.json",
    }
    paths["goose"].parent.mkdir(parents=True, exist_ok=True)
    paths["goose"].write_text(
        "# operator comment\nactive_provider: anthropic\n", encoding="utf-8"
    )
    paths["cline"].parent.mkdir(parents=True, exist_ok=True)
    paths["cline"].write_text('{\n  "mcpServers": {}\n}\n', encoding="utf-8")
    paths["copilot"].parent.mkdir(parents=True, exist_ok=True)
    paths["copilot"].write_text('{\n  "mcpServers": {}\n}\n', encoding="utf-8")
    paths["vibe"].parent.mkdir(parents=True, exist_ok=True)
    paths["vibe"].write_text(
        '# vibe comment\nactive_model = "mistral-medium-3.5"\n', encoding="utf-8"
    )
    paths["opencode"].write_text(
        '{\n  "$schema": "https://opencode.ai/config.json",\n'
        '  "mcp": {"servers": {"other": {"type": "local", "command": ["x"]}}}\n}\n',
        encoding="utf-8",
    )
    paths["qwen"].parent.mkdir(parents=True, exist_ok=True)
    paths["qwen"].write_text('{\n  "mcpServers": {}\n}\n', encoding="utf-8")
    return paths


def test_preview_is_read_only_and_plans_all_six(
    tmp_path: Path, isolated_home: Path
) -> None:
    workspace = _workspace(tmp_path)
    paths = _prepare_all_six(workspace, isolated_home)
    before = {name: path.read_bytes() for name, path in paths.items()}

    report = connect(
        workspace=workspace,
        home=isolated_home,
        platform="linux",
        environ={"HOME": str(isolated_home)},
        command=FAKE_COMMAND,
        which=_no_which,
        apply=False,
    )

    assert report.exit_code == 0, report.errors
    items = {item.installation_id: item for item in report.items}
    for installation_id in (
        "goose-user",
        "cline-user",
        "vibe-project",
        "copilot-user",
        "opencode-v2-project",
        "qwen-project",
    ):
        assert items[installation_id].state == STATE_PLANNED
        assert items[installation_id].action in (ACTION_CREATE, ACTION_UPDATE)
    # The losing OpenCode variant is not a candidate at all.
    assert "opencode-v1-project" not in items
    # Nothing touched.
    assert {name: path.read_bytes() for name, path in paths.items()} == before


def test_apply_all_six_then_idempotent(
    tmp_path: Path, isolated_home: Path
) -> None:
    workspace = _workspace(tmp_path)
    paths = _prepare_all_six(workspace, isolated_home)

    first = connect(
        workspace=workspace,
        home=isolated_home,
        platform="linux",
        environ={"HOME": str(isolated_home)},
        command=FAKE_COMMAND,
        which=_no_which,
        apply=True,
        allow_user_scope=True,
    )
    assert first.exit_code == 0, first.errors
    states = {item.installation_id: item.state for item in first.items}
    for installation_id in (
        "goose-user",
        "cline-user",
        "vibe-project",
        "copilot-user",
        "opencode-v2-project",
        "qwen-project",
    ):
        assert states[installation_id] == STATE_AWAITING

    # Goose: comments preserved, canonical envs shape, stdio type.
    goose_document = yaml.safe_load(paths["goose"].read_text(encoding="utf-8"))
    goose_entry = goose_document["extensions"]["agentbus"]
    assert goose_entry["type"] == "stdio"
    assert goose_entry["cmd"] == "/usr/bin/agentbus"
    assert goose_entry["envs"]["AGENTBUS_WORKSPACE"] == str(workspace)
    assert "# operator comment" in paths["goose"].read_text(encoding="utf-8")

    # Cline and Copilot are plain JSON objects with their documented flags.
    cline = json.loads(paths["cline"].read_text(encoding="utf-8"))
    assert cline["mcpServers"]["agentbus"]["disabled"] is False
    copilot = json.loads(paths["copilot"].read_text(encoding="utf-8"))
    assert copilot["mcpServers"]["agentbus"]["type"] == "local"
    assert copilot["mcpServers"]["agentbus"]["tools"] == ["*"]

    # Vibe is an array of tables with a named stdio server.
    vibe = tomllib.loads(paths["vibe"].read_text(encoding="utf-8"))
    servers = {item["name"]: item for item in vibe["mcp_servers"]}
    assert servers["agentbus"]["transport"] == "stdio"
    assert servers["agentbus"]["env"]["AGENTBUS_WORKSPACE"] == str(workspace)

    # OpenCode v2 layout only.
    opencode = json.loads(paths["opencode"].read_text(encoding="utf-8"))
    assert "agentbus" in opencode["mcp"]["servers"]
    assert "agentbus" not in opencode["mcp"]
    assert opencode["mcp"]["servers"]["other"]["command"] == ["x"]

    # Qwen.
    qwen = json.loads(paths["qwen"].read_text(encoding="utf-8"))
    assert qwen["mcpServers"]["agentbus"]["command"] == "/usr/bin/agentbus"

    before_second = {name: path.read_bytes() for name, path in paths.items()}
    second = connect(
        workspace=workspace,
        home=isolated_home,
        platform="linux",
        environ={"HOME": str(isolated_home)},
        command=FAKE_COMMAND,
        which=_no_which,
        apply=True,
        allow_user_scope=True,
    )
    assert second.exit_code == 0, second.errors
    second_states = {item.installation_id: item.state for item in second.items}
    for installation_id in (
        "goose-user",
        "cline-user",
        "vibe-project",
        "copilot-user",
        "opencode-v2-project",
        "qwen-project",
    ):
        assert second_states[installation_id] == STATE_CONFIGURED
        assert next(
            item for item in second.items if item.installation_id == installation_id
        ).changed is False
    assert {name: path.read_bytes() for name, path in paths.items()} == before_second


def test_opencode_v1_server_named_servers_is_not_treated_as_v2(
    tmp_path: Path, isolated_home: Path
) -> None:
    workspace = _workspace(tmp_path)
    config = workspace / "opencode.json"
    original = {
        "$schema": "https://opencode.ai/config.json",
        "mcp": {
            "servers": {"type": "local", "command": ["npx", "example"]},
        },
    }
    config.write_text(json.dumps(original, indent=2) + "\n", encoding="utf-8")

    report = connect(
        workspace=workspace,
        home=isolated_home,
        platform="linux",
        environ={"HOME": str(isolated_home)},
        command=FAKE_COMMAND,
        which=_which("opencode"),
        apply=True,
        allow_user_scope=True,
    )
    assert report.exit_code == 0, report.errors
    patched = json.loads(config.read_text(encoding="utf-8"))
    # The v1 server named "servers" is preserved verbatim.
    assert patched["mcp"]["servers"] == {"type": "local", "command": ["npx", "example"]}
    assert patched["mcp"]["agentbus"]["enabled"] is True


def test_opencode_empty_servers_fails_closed_with_actionable_error(
    tmp_path: Path, isolated_home: Path
) -> None:
    workspace = _workspace(tmp_path)
    (workspace / "opencode.json").write_text(
        '{"mcp": {"servers": {}}}\n', encoding="utf-8"
    )

    report = connect(
        workspace=workspace,
        home=isolated_home,
        platform="linux",
        environ={"HOME": str(isolated_home)},
        command=FAKE_COMMAND,
        which=_which("opencode"),
        apply=False,
    )

    assert report.exit_code == 1
    refused = [item for item in report.items if item.state == STATE_REFUSED]
    assert refused, report.items
    assert any("opencode_version_ambiguous" in item.reason for item in refused)
    assert any(
        "--client opencode-v2 or --client opencode-v1" in item.reason
        for item in refused
    )


def test_opencode_explicit_selector_bypasses_ambiguity(
    tmp_path: Path, isolated_home: Path
) -> None:
    workspace = _workspace(tmp_path)
    (workspace / "opencode.json").write_text(
        '{"mcp": {"servers": {}}}\n', encoding="utf-8"
    )

    report = connect(
        workspace=workspace,
        clients=["opencode-v2"],
        home=isolated_home,
        platform="linux",
        environ={"HOME": str(isolated_home)},
        command=FAKE_COMMAND,
        which=_which("opencode"),
        apply=False,
    )

    assert report.exit_code == 0, report.errors
    planned = {
        item.installation_id: item
        for item in report.items
        if item.state == STATE_PLANNED
    }
    assert "opencode-v2-project" in planned
    assert "opencode-v1-project" not in planned


def test_opencode_conflicting_selectors_fail_closed(
    tmp_path: Path, isolated_home: Path
) -> None:
    workspace = _workspace(tmp_path)
    (workspace / "opencode.json").write_text("{}\n", encoding="utf-8")

    report = connect(
        workspace=workspace,
        clients=["opencode-v1", "opencode-v2"],
        home=isolated_home,
        platform="linux",
        environ={"HOME": str(isolated_home)},
        command=FAKE_COMMAND,
        which=_no_which,
        apply=False,
    )

    assert report.exit_code == 1
    assert any(
        item.state == STATE_REFUSED and "conflicting variant selectors" in item.reason
        for item in report.items
    )


def test_opencode_selector_is_a_valid_client_name(tmp_path: Path) -> None:
    from agentbus.discovery import validate_client_selection

    validate_client_selection(["opencode-v1"])
    validate_client_selection(["opencode-v2"])
    with pytest.raises(Exception):
        validate_client_selection(["opencode-v3"])


def test_secrets_are_redacted_in_connect_reports(
    tmp_path: Path, isolated_home: Path
) -> None:
    workspace = _workspace(tmp_path)
    config = workspace / ".qwen" / "settings.json"
    config.parent.mkdir(parents=True)
    config.write_text(
        json.dumps(
            {
                "mcpServers": {
                    "agentbus": {
                        "command": "/usr/bin/agentbus",
                        "args": ["mcp-serve", "--token", "sk-live-1234567890"],
                        "env": {
                            "AGENTBUS_WORKSPACE": str(workspace),
                            "AGENTBUS_PRODUCER_ID": "old-producer",
                            "SERVICE_TOKEN": "ghp_supersecretvalue",
                        },
                    }
                }
            }
        )
        + "\n",
        encoding="utf-8",
    )

    report = connect(
        workspace=workspace,
        clients=["qwen-project"],
        home=isolated_home,
        platform="linux",
        environ={"HOME": str(isolated_home)},
        command=FAKE_COMMAND,
        which=_no_which,
        apply=False,
    )
    payload = json.dumps(report.to_dict())
    assert "ghp_supersecretvalue" not in payload
    assert "sk-live-1234567890" not in payload
    item = next(item for item in report.items if item.installation_id == "qwen-project")
    assert item.state == STATE_DIFFERS
    assert all(
        value == "[REDACTED]"
        for value in (item.previous_entry or {}).get("env", {}).values()
    )


def test_connect_never_creates_or_rewrites_roles(
    tmp_path: Path, isolated_home: Path
) -> None:
    workspace = _workspace(tmp_path)
    paths = _prepare_all_six(workspace, isolated_home)

    connect(
        workspace=workspace,
        home=isolated_home,
        platform="linux",
        environ={"HOME": str(isolated_home)},
        command=FAKE_COMMAND,
        which=_no_which,
        apply=True,
        allow_user_scope=True,
    )
    roles = workspace / ".agentbus" / "roles.yaml"
    assert not roles.exists()

    roles.write_text(
        "roles:\n  operator:\n    can_publish_topics: ['okf/*']\n"
        "producers:\n  operator: operator\n",
        encoding="utf-8",
    )
    before = roles.read_bytes()
    connect(
        workspace=workspace,
        home=isolated_home,
        platform="linux",
        environ={"HOME": str(isolated_home)},
        command=FAKE_COMMAND,
        which=_no_which,
        apply=True,
        allow_user_scope=True,
    )
    assert roles.read_bytes() == before
    assert paths["qwen"].is_file()
