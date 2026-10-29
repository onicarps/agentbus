"""Packaged manifests, platform paths, and three-state discovery probes."""

from __future__ import annotations

import json
import os
from importlib import resources
from pathlib import Path

import pytest

from agentbus.adapters.manifest import (
    ManifestError,
    build_entry,
    expand_template,
    install_target,
    load_manifests,
    manifest_root,
)
from agentbus.adapters.probe import (
    CONFIG_MALFORMED,
    CONFIG_VALID,
    ENTRY_ABSENT,
    ENTRY_DIFFERS,
    ENTRY_MATCHES,
    STATUS_ABSENT,
    STATUS_PRESENT,
    STATUS_UNKNOWN,
    probe_installations,
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
        "CODEX_HOME",
        "XDG_CONFIG_HOME",
        "APPDATA",
        "LOCALAPPDATA",
    ):
        monkeypatch.delenv(name, raising=False)
    return home


def snapshot(root: Path) -> dict[str, tuple[bool, int, int, int]]:
    state: dict[str, tuple[bool, int, int, int]] = {}
    if not root.exists():
        return state
    for path in sorted(root.rglob("*")):
        info = path.stat()
        state[str(path.relative_to(root))] = (
            path.is_dir(),
            info.st_mode,
            info.st_size,
            info.st_mtime_ns,
        )
    return state


def _empty_which(_name: str) -> None:
    return None


def test_manifests_are_packaged_and_cover_required_clients() -> None:
    manifests = load_manifests()
    ids = {manifest.manifest_id for manifest in manifests}
    assert {"claude", "codex", "cursor"} <= ids
    # Gemini and VS Code are preserved from v0.27 behaviour.
    assert {"gemini", "vscode"} <= ids

    installations = {
        installation.installation_id
        for manifest in manifests
        for installation in manifest.installations
    }
    assert {
        "claude-desktop",
        "claude-code-project",
        "codex-user",
        "cursor-project",
        "cursor-user",
        "gemini",
        "vscode",
    } <= installations
    for manifest in manifests:
        assert manifest.version, manifest.manifest_id
        assert manifest.generated_by, manifest.manifest_id


def test_manifests_are_readable_through_importlib_resources() -> None:
    package = resources.files("agentbus.adapters").joinpath("manifests")
    names = {entry.name for entry in package.iterdir()}
    assert {"claude.yaml", "codex.yaml", "cursor.yaml"} <= names
    with resources.as_file(package.joinpath("cursor.yaml")) as path:
        assert "mcpServers" in Path(path).read_text(encoding="utf-8")
    assert manifest_root().is_dir()


def test_expand_template_platform_paths(isolated_home: Path) -> None:
    workspace = isolated_home / "proj"
    env = {"HOME": str(isolated_home), "APPDATA": r"C:\Users\me\AppData\Roaming"}

    claude = install_target("claude-desktop")
    linux = claude.path_for(
        platform="linux", workspace=workspace, home=isolated_home, environ=env
    )
    assert str(linux).endswith("/.config/Claude/claude_desktop_config.json")
    assert linux.is_absolute()

    darwin = claude.path_for(
        platform="darwin", workspace=workspace, home=isolated_home, environ=env
    )
    assert str(darwin).startswith(str(isolated_home / "Library/Application Support"))

    win = claude.path_for(
        platform="win32", workspace=workspace, home=isolated_home, environ=env
    )
    assert str(win).startswith("C:/Users/me/AppData/Roaming/Claude")

    cursor = install_target("cursor-user").path_for(
        platform="linux", workspace=workspace, home=isolated_home, environ=env
    )
    assert cursor == isolated_home / ".cursor/mcp.json"

    # CODEX_HOME wins over ~/.codex without touching the real home.
    codex_env = {**env, "CODEX_HOME": str(isolated_home / "codex-home")}
    codex = install_target("codex-user").path_for(
        platform="linux", workspace=workspace, home=isolated_home, environ=codex_env
    )
    assert codex == isolated_home / "codex-home/config.toml"


def test_expand_template_reports_missing_variable(isolated_home: Path) -> None:
    with pytest.raises(ManifestError, match="APPDATA"):
        expand_template(
            "%APPDATA%/Claude/x.json",
            platform="win32",
            workspace=isolated_home,
            home=isolated_home,
            environ={},
        )


def test_unknown_installation_id_is_rejected() -> None:
    with pytest.raises(ManifestError, match="unknown client installation"):
        install_target("no-such-client")


def test_probe_present_from_existing_config(isolated_home: Path) -> None:
    workspace = isolated_home / "proj"
    config = workspace / ".cursor" / "mcp.json"
    config.parent.mkdir(parents=True)
    config.write_text('{"mcpServers": {}}\n', encoding="utf-8")

    probes = probe_installations(
        workspace=workspace,
        home=isolated_home,
        platform="linux",
        environ={"HOME": str(isolated_home)},
        clients=["cursor-project"],
        which=_empty_which,
    )

    assert len(probes) == 1
    probe = probes[0]
    assert probe.status == STATUS_PRESENT
    assert probe.config_state == CONFIG_VALID
    assert probe.entry_state == ENTRY_ABSENT
    assert probe.managed is False
    assert probe.writable is True


def test_probe_absent_only_when_no_evidence(isolated_home: Path) -> None:
    workspace = isolated_home / "proj"
    workspace.mkdir()

    probes = probe_installations(
        workspace=workspace,
        home=isolated_home,
        platform="linux",
        environ={"HOME": str(isolated_home)},
        clients=["claude-desktop", "codex-user"],
        which=_empty_which,
    )

    assert {probe.status for probe in probes} == {STATUS_ABSENT}
    assert {probe.reason for probe in probes} == {"no_evidence"}


def test_probe_present_when_client_binary_exists_without_config(
    isolated_home: Path,
) -> None:
    workspace = isolated_home / "proj"
    workspace.mkdir()

    probes = probe_installations(
        workspace=workspace,
        home=isolated_home,
        platform="linux",
        environ={"HOME": str(isolated_home)},
        clients=["codex-user"],
        which=lambda name: "/usr/local/bin/codex" if name == "codex" else None,
    )

    probe = probes[0]
    assert probe.status == STATUS_PRESENT
    assert probe.reason == "installed_not_configured"
    assert probe.entry_state == ENTRY_ABSENT
    assert any(item.startswith("binary:codex=") for item in probe.evidence)


def test_probe_malformed_config_is_unknown_not_present(isolated_home: Path) -> None:
    workspace = isolated_home / "proj"
    config = workspace / ".cursor" / "mcp.json"
    config.parent.mkdir(parents=True)
    config.write_text("{broken\n", encoding="utf-8")

    probe = probe_installations(
        workspace=workspace,
        home=isolated_home,
        platform="linux",
        environ={"HOME": str(isolated_home)},
        clients=["cursor-project"],
        which=_empty_which,
    )[0]

    assert probe.status == STATUS_UNKNOWN
    assert probe.config_state == CONFIG_MALFORMED
    assert probe.reason.startswith("config_malformed")
    assert "invalid json" in probe.reason


def test_probe_unreadable_path_is_unknown(isolated_home: Path) -> None:
    workspace = isolated_home / "proj"
    config = workspace / ".cursor" / "mcp.json"
    # A directory where a file is expected cannot be read as config.
    config.mkdir(parents=True)

    probe = probe_installations(
        workspace=workspace,
        home=isolated_home,
        platform="linux",
        environ={"HOME": str(isolated_home)},
        clients=["cursor-project"],
        which=_empty_which,
    )[0]

    assert probe.status == STATUS_UNKNOWN
    assert probe.reason.startswith("config_")
    assert "unreadable" in probe.reason


def test_probe_unsupported_platform_is_unknown(isolated_home: Path) -> None:
    workspace = isolated_home / "proj"
    workspace.mkdir()
    probe = probe_installations(
        workspace=workspace,
        home=isolated_home,
        platform="plan9",
        environ={"HOME": str(isolated_home)},
        clients=["claude-desktop"],
        which=_empty_which,
    )[0]
    assert probe.status == STATUS_UNKNOWN
    assert probe.reason == "unsupported_platform:plan9"


def test_probe_compares_expected_entry(isolated_home: Path) -> None:
    workspace = isolated_home / "proj"
    config = workspace / ".cursor" / "mcp.json"
    config.parent.mkdir(parents=True)
    installation = install_target("cursor-project")
    entry = build_entry(
        installation,
        argv=("/usr/bin/agentbus", "mcp-serve"),
        workspace=workspace,
        producer_id="local-user",
    )
    config.write_text(
        json.dumps({"mcpServers": {"agentbus": entry}}) + "\n",
        encoding="utf-8",
    )

    matching = probe_installations(
        workspace=workspace,
        home=isolated_home,
        platform="linux",
        environ={"HOME": str(isolated_home)},
        clients=["cursor-project"],
        expected_entries={"cursor-project": entry},
        which=_empty_which,
    )[0]
    assert matching.entry_state == ENTRY_MATCHES
    assert matching.managed is True

    stale = dict(entry)
    stale["command"] = "/old/agentbus"
    differing = probe_installations(
        workspace=workspace,
        home=isolated_home,
        platform="linux",
        environ={"HOME": str(isolated_home)},
        clients=["cursor-project"],
        expected_entries={"cursor-project": stale},
        which=_empty_which,
    )[0]
    assert differing.entry_state == ENTRY_DIFFERS


def test_discovery_is_read_only_for_absent_and_present_state(
    isolated_home: Path, tmp_path: Path
) -> None:
    from agentbus.discovery import discover

    empty_workspace = tmp_path / "empty"
    empty_workspace.mkdir()
    before = snapshot(tmp_path)

    report = discover(
        workspace=empty_workspace,
        home=isolated_home,
        platform="linux",
        environ={"HOME": str(isolated_home), "PATH": ""},
        clients=["cursor-project", "claude-code-project"],
        which=_empty_which,
        python=str(tmp_path / "missing-python"),
    )

    assert report.command_error is not None  # no interpreter command resolvable
    assert snapshot(tmp_path) == before
    assert not (empty_workspace / ".agentbus").exists()

    present_workspace = tmp_path / "present"
    config = present_workspace / ".cursor" / "mcp.json"
    config.parent.mkdir(parents=True)
    config.write_text('{"mcpServers": {}}\n', encoding="utf-8")
    before_present = snapshot(tmp_path)

    discover(
        workspace=present_workspace,
        home=isolated_home,
        platform="linux",
        environ={"HOME": str(isolated_home), "PATH": ""},
        clients=["cursor-project"],
        which=_empty_which,
    )

    assert snapshot(tmp_path) == before_present


def test_discovery_never_touches_unrelated_paths(
    isolated_home: Path, tmp_path: Path
) -> None:
    from agentbus.discovery import discover

    workspace = tmp_path / "proj"
    workspace.mkdir()
    unrelated = tmp_path / "unrelated"
    unrelated.mkdir()
    (unrelated / "keep.txt").write_text("keep\n", encoding="utf-8")
    before = snapshot(tmp_path)

    discover(
        workspace=workspace,
        home=isolated_home,
        platform="linux",
        environ={"HOME": str(isolated_home), "PATH": os.environ.get("PATH", "")},
        clients=None,
    )

    assert snapshot(tmp_path) == before


def test_codex_toml_root_resolves_existing_agentbus_entry(
    isolated_home: Path,
) -> None:
    """``mcp_servers.agentbus`` is the entry table, not a container."""
    workspace = isolated_home / "proj"
    config = workspace / ".codex" / "config.toml"
    config.parent.mkdir(parents=True)
    installation = install_target("codex-project")
    entry = build_entry(
        installation,
        argv=("/usr/bin/agentbus", "mcp-serve"),
        workspace=workspace,
        producer_id="local-user",
    )
    args = ", ".join(json.dumps(value) for value in entry["args"])
    env = "\n".join(
        f'{key} = {json.dumps(value)}'
        for key, value in entry["env"].items()
    )
    config.write_text(
        'model = "gpt-5"\n\n'
        "[mcp_servers.agentbus]\n"
        f'command = "/usr/bin/agentbus"\n'
        f"args = [{args}]\n\n"
        "[mcp_servers.agentbus.env]\n"
        f"{env}\n",
        encoding="utf-8",
    )

    probe = probe_installations(
        workspace=workspace,
        home=isolated_home,
        platform="linux",
        environ={"HOME": str(isolated_home)},
        clients=["codex-project"],
        expected_entries={"codex-project": entry},
        which=_empty_which,
    )[0]

    assert probe.status == STATUS_PRESENT
    assert probe.config_state == CONFIG_VALID
    assert probe.entry_state == ENTRY_MATCHES
    assert probe.managed is True


def test_probe_reports_auto_connect_policy(isolated_home: Path) -> None:
    workspace = isolated_home / "proj"
    config = workspace / ".codex" / "config.toml"
    config.parent.mkdir(parents=True)
    config.write_text('model = "gpt-5"\n', encoding="utf-8")

    probes = probe_installations(
        workspace=workspace,
        home=isolated_home,
        platform="linux",
        environ={"HOME": str(isolated_home)},
        clients=["codex-project", "cursor-project"],
        which=_empty_which,
    )
    by_id = {probe.installation_id: probe for probe in probes}
    assert by_id["codex-project"].auto_connect is False
    assert by_id["codex-project"].to_dict()["auto_connect"] is False


def test_template_does_not_expand_variables_in_inserted_paths(tmp_path, monkeypatch):
    monkeypatch.setenv('build', 'redirected')
    workspace = tmp_path / '$build' / 'project'
    home = tmp_path / '$build' / 'home'
    assert expand_template('{workspace}/config', platform='linux', workspace=workspace, home=home) == workspace / 'config'
    assert expand_template('{home}/config', platform='linux', workspace=workspace, home=home) == home / 'config'
