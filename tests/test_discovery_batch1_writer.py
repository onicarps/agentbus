"""Batch 1 writers (YAML/TOML arrays/JSONC) and OpenCode discrimination."""

from __future__ import annotations

import json
import os
import tomllib
from pathlib import Path

import pytest
import yaml
from ruamel.yaml import YAML

from agentbus.adapters.manifest import SERVER_NAME, build_entry, install_target
from agentbus.adapters.opencode import (
    AMBIGUITY_ACTION,
    VARIANT_V1,
    VARIANT_V2,
    OpenCodeVersionAmbiguityError,
    detect_variant,
    inspect_config_structure,
    inspect_schema,
    variant_from_version_text,
)
from agentbus.adapters.writer import (
    ConfigConflictError,
    entry_matches,
    entry_env_keys,
    plan_json_patch,
    plan_patch,
    plan_toml_array_patch,
    plan_yaml_patch,
    redact_entry,
)

ARGV = ("/usr/bin/agentbus", "mcp-serve")


def _entry(installation_id: str, workspace: Path) -> dict:
    return build_entry(
        install_target(installation_id),
        argv=ARGV,
        workspace=workspace,
        producer_id="local-user",
    )


# ---------------------------------------------------------------------------
# YAML round-trip
# ---------------------------------------------------------------------------


def test_yaml_patch_preserves_yaml12_plain_scalars(tmp_path: Path) -> None:
    path = tmp_path / "config.yaml"
    raw = b"GOOSE_TOOLSHIM: off\nextensions:\n  other:\n    flag: on\n"
    installation = install_target("goose-user")
    entry = _entry("goose-user", tmp_path)

    patch = plan_patch(installation, path=path, entry=entry, raw=raw)
    assert patch.changed and patch.new_bytes is not None
    document = YAML(typ="safe").load(patch.new_bytes.decode("utf-8"))
    assert document["GOOSE_TOOLSHIM"] == "off"
    assert document["extensions"]["other"]["flag"] == "on"
    assert document["extensions"]["agentbus"]["cmd"] == "/usr/bin/agentbus"
    assert not plan_patch(installation, path=path, entry=entry, raw=patch.new_bytes).changed


def test_yaml_patch_preserves_comments_order_and_anchors(tmp_path: Path) -> None:
    path = tmp_path / "config.yaml"
    path.write_text(
        "# top of file\n"
        "active_provider: anthropic\n"
        "defaults: &base\n"
        "  type: builtin\n"
        "extensions:\n"
        "  # keep developer first\n"
        "  developer: *base\n"
        "  time:\n"
        "    type: builtin\n"
        "    name: time\n",
        encoding="utf-8",
    )
    installation = install_target("goose-user")
    entry = _entry("goose-user", tmp_path)

    patch = plan_patch(installation, path=path, entry=entry)
    assert patch.changed
    text = (patch.new_bytes or b"").decode()
    assert "# top of file" in text
    assert "&base" in text and "*base" in text
    assert text.index("active_provider") < text.index("extensions")
    assert text.index("developer") < text.index("agentbus")
    document = yaml.safe_load(text)
    assert document["extensions"]["agentbus"]["cmd"] == "/usr/bin/agentbus"
    assert document["extensions"]["developer"] == {"type": "builtin"}

    again = plan_yaml_patch(
        installation_id="goose-user",
        path=path,
        root="extensions",
        entry=entry,
        raw=patch.new_bytes,
    )
    assert not again.changed


def test_yaml_single_document_marker_is_allowed(tmp_path: Path) -> None:
    path = tmp_path / "config.yaml"
    raw = b"---\nactive_provider: anthropic\n"
    patch = plan_yaml_patch(
        installation_id="goose-user",
        path=path,
        root="extensions",
        entry=_entry("goose-user", tmp_path),
        raw=raw,
    )
    assert patch.changed
    assert yaml.safe_load(patch.new_bytes)["extensions"]["agentbus"]["type"] == "stdio"


@pytest.mark.parametrize(
    "raw, detail",
    [
        (b"a: 1\n---\nb: 2\n", "multi-document"),
        (b"a: !custom value\n", "custom YAML tag"),
        (b"a: &x\n  b: *x\n", "recursive anchors"),
        (b"a: 1\na: 2\n", "duplicate mapping key"),
        (b"a: !!python/object:os.system x\n", "custom YAML tag"),
    ],
)
def test_yaml_unsafe_constructs_fail_closed(
    tmp_path: Path, raw: bytes, detail: str
) -> None:
    path = tmp_path / "config.yaml"
    with pytest.raises(ConfigConflictError) as excinfo:
        plan_yaml_patch(
            installation_id="goose-user",
            path=path,
            root="extensions",
            entry=_entry("goose-user", tmp_path),
            raw=raw,
        )
    assert excinfo.value.code == "unsafe_yaml_construct"
    assert detail in str(excinfo.value)


def test_yaml_malformed_is_reported(tmp_path: Path) -> None:
    with pytest.raises(ConfigConflictError) as excinfo:
        plan_yaml_patch(
            installation_id="goose-user",
            path=tmp_path / "config.yaml",
            root="extensions",
            entry=_entry("goose-user", tmp_path),
            raw=b"extensions: [unclosed\n",
        )
    assert excinfo.value.code in ("malformed_yaml", "unsafe_yaml_construct")


def test_yaml_env_alias_is_folded_into_the_canonical_envs_key(
    tmp_path: Path,
) -> None:
    raw = (
        b"extensions:\n"
        b"  agentbus:\n"
        b"    type: stdio\n"
        b"    name: agentbus\n"
        b"    enabled: true\n"
        b"    cmd: /usr/bin/agentbus\n"
        b"    args: [mcp-serve]\n"
        b"    env:\n"
        b"      AGENTBUS_WORKSPACE: /old\n"
        b"      SERVICE_TOKEN: keep-me\n"
    )
    patch = plan_yaml_patch(
        installation_id="goose-user",
        path=tmp_path / "config.yaml",
        root="extensions",
        entry=_entry("goose-user", tmp_path),
        raw=raw,
    )
    goose = yaml.safe_load(patch.new_bytes)["extensions"]["agentbus"]
    # Goose treats ``env`` only as an alias of ``envs``; both at once can fail
    # deserialization, so the alias is folded into the canonical key.
    assert "env" not in goose
    assert goose["envs"]["AGENTBUS_WORKSPACE"] == str(tmp_path)
    assert goose["envs"]["SERVICE_TOKEN"] == "keep-me"


@pytest.mark.parametrize("alias", ["env", "environment"])
def test_matching_yaml_entry_rejects_unsafe_alias(tmp_path: Path, alias: str) -> None:
    entry = _entry("goose-user", tmp_path)
    actual = {**entry, alias: {"LD_PRELOAD": "/tmp/evil.so"}}
    assert not entry_matches(actual, entry)
    raw = yaml.safe_dump({"extensions": {"agentbus": actual}}).encode()
    with pytest.raises(ConfigConflictError) as excinfo:
        plan_yaml_patch(installation_id="goose-user", path=tmp_path / "config.yaml",
                        root="extensions", entry=entry, raw=raw)
    assert excinfo.value.code == "unsafe_env"


@pytest.mark.parametrize("alias", ["env", "environment"])
def test_matching_yaml_entry_folds_safe_alias(tmp_path: Path, alias: str) -> None:
    entry = _entry("goose-user", tmp_path)
    actual = {**entry, alias: {"SERVICE_TOKEN": "keep-me"}}
    assert not entry_matches(actual, entry)
    raw = yaml.safe_dump({"extensions": {"agentbus": actual}}).encode()
    kwargs = dict(installation_id="goose-user", path=tmp_path / "config.yaml",
                  root="extensions", entry=entry)
    patch = plan_yaml_patch(**kwargs, raw=raw)
    assert patch.changed
    result = yaml.safe_load(patch.new_bytes)["extensions"]["agentbus"]
    assert alias not in result
    assert result["envs"]["SERVICE_TOKEN"] == "keep-me"
    assert entry_matches(result, entry)
    assert not plan_yaml_patch(**kwargs, raw=patch.new_bytes).changed


@pytest.mark.parametrize("value", [None, [], "invalid", {"NODE_OPTIONS": "--require evil"}])
def test_matching_entry_rejects_invalid_expected_environment(tmp_path: Path, value: object) -> None:
    entry = _entry("goose-user", tmp_path)
    assert not entry_matches({**entry, "envs": value}, entry)


def test_yaml_refuses_to_overwrite_an_unowned_entry(tmp_path: Path) -> None:
    path = tmp_path / "config.yaml"
    raw = b"extensions:\n  agentbus:\n    cmd: other-server\n    args: []\n"
    with pytest.raises(ConfigConflictError) as excinfo:
        plan_yaml_patch(
            installation_id="goose-user",
            path=path,
            root="extensions",
            entry=_entry("goose-user", tmp_path),
            raw=raw,
        )
    assert excinfo.value.code == "entry_not_owned"


def test_yaml_root_conflict_is_reported(tmp_path: Path) -> None:
    with pytest.raises(ConfigConflictError) as excinfo:
        plan_yaml_patch(
            installation_id="goose-user",
            path=tmp_path / "config.yaml",
            root="extensions",
            entry=_entry("goose-user", tmp_path),
            raw=b"extensions:\n  - a list\n",
        )
    assert excinfo.value.code == "root_not_object"


# ---------------------------------------------------------------------------
# TOML array-of-tables
# ---------------------------------------------------------------------------


def test_toml_array_appends_preserving_comments_and_tables() -> None:
    raw = (
        b"# vibe config\n"
        b'active_model = "mistral"\n\n'
        b"[[mcp_servers]]\n"
        b'name = "fetch"\n'
        b'transport = "stdio"\n'
        b'command = "uvx"\n'
        b'args = ["mcp-server-fetch"]\n'
    )
    entry = {
        "name": SERVER_NAME,
        "transport": "stdio",
        "command": "/usr/bin/agentbus",
        "args": ["mcp-serve"],
        "env": {"AGENTBUS_WORKSPACE": "/ws", "AGENTBUS_PRODUCER_ID": "local-user"},
    }
    patch = plan_toml_array_patch(
        installation_id="vibe-project",
        path=Path("/tmp/vibe.toml"),
        table="mcp_servers",
        entry=entry,
        raw=raw,
    )
    text = (patch.new_bytes or b"").decode()
    assert "# vibe config" in text
    document = tomllib.loads(text)
    assert [item["name"] for item in document["mcp_servers"]] == ["fetch", "agentbus"]
    assert document["mcp_servers"][1]["env"]["AGENTBUS_WORKSPACE"] == "/ws"

    again = plan_toml_array_patch(
        installation_id="vibe-project",
        path=Path("/tmp/vibe.toml"),
        table="mcp_servers",
        entry=entry,
        raw=patch.new_bytes,
    )
    assert not again.changed


def test_toml_array_updates_named_entry_in_place() -> None:
    raw = (
        b"[[mcp_servers]]\n"
        b'name = "agentbus"\n'
        b'transport = "stdio"\n'
        b'command = "/usr/bin/agentbus"\n'
        b'args = ["mcp-serve"]\n'
        b"[mcp_servers.env]\n"
        b'AGENTBUS_WORKSPACE = "/old"\n\n'
        b"[[mcp_servers]]\n"
        b'name = "other"\n'
        b'transport = "stdio"\n'
        b'command = "x"\n'
    )
    entry = {
        "name": SERVER_NAME,
        "transport": "stdio",
        "command": "/usr/bin/agentbus",
        "args": ["mcp-serve"],
        "env": {"AGENTBUS_WORKSPACE": "/new", "AGENTBUS_PRODUCER_ID": "local-user"},
    }
    patch = plan_toml_array_patch(
        installation_id="vibe-project",
        path=Path("/tmp/vibe.toml"),
        table="mcp_servers",
        entry=entry,
        raw=raw,
    )
    document = tomllib.loads((patch.new_bytes or b"").decode())
    assert [item["name"] for item in document["mcp_servers"]] == ["agentbus", "other"]
    assert document["mcp_servers"][0]["env"]["AGENTBUS_WORKSPACE"] == "/new"
    assert document["mcp_servers"][1]["command"] == "x"


def test_toml_array_preserves_comments_before_following_element() -> None:
    raw = (
        b"[[mcp_servers]]\n"
        b'name = "agentbus"\n'
        b'transport = "stdio"\n'
        b'command = "/usr/bin/agentbus"\n'
        b'args = ["mcp-serve"]\n'
        b"[mcp_servers.env]\n"
        b'AGENTBUS_WORKSPACE = "/old"\n\n'
        b"# Important fetch server comment\n"
        b"[[mcp_servers]]\n"
        b'name = "fetch"\n'
        b'transport = "stdio"\n'
        b'command = "fetch-mcp"\n'
    )
    entry = {
        "name": SERVER_NAME,
        "transport": "stdio",
        "command": "/usr/bin/agentbus",
        "args": ["mcp-serve"],
        "env": {"AGENTBUS_WORKSPACE": "/new", "AGENTBUS_PRODUCER_ID": "local-user"},
    }
    patch = plan_toml_array_patch(
        installation_id="vibe-project",
        path=Path("/tmp/vibe.toml"),
        table="mcp_servers",
        entry=entry,
        raw=raw,
    )
    text = (patch.new_bytes or b"").decode("utf-8")
    assert "# Important fetch server comment" in text
    document = tomllib.loads(text)
    assert [item["name"] for item in document["mcp_servers"]] == ["agentbus", "fetch"]


def test_toml_array_refuses_unowned_entry() -> None:
    raw = (
        b"[[mcp_servers]]\n"
        b'name = "agentbus"\n'
        b'transport = "stdio"\n'
        b'command = "somebody-else"\n'
    )
    with pytest.raises(ConfigConflictError) as excinfo:
        plan_toml_array_patch(
            installation_id="vibe-project",
            path=Path("/tmp/vibe.toml"),
            table="mcp_servers",
            entry={
                "name": SERVER_NAME,
                "transport": "stdio",
                "command": "/usr/bin/agentbus",
                "args": [],
                "env": {},
            },
            raw=raw,
        )
    assert excinfo.value.code == "entry_not_owned"


def test_toml_inline_array_structure_fails_closed() -> None:
    raw = b'mcp_servers = [{ name = "other", transport = "stdio" }]\n'
    with pytest.raises(ConfigConflictError) as excinfo:
        plan_toml_array_patch(
            installation_id="vibe-project",
            path=Path("/tmp/vibe.toml"),
            table="mcp_servers",
            entry={
                "name": SERVER_NAME,
                "transport": "stdio",
                "command": "/usr/bin/agentbus",
                "args": [],
                "env": {},
            },
            raw=raw,
        )
    assert excinfo.value.code == "toml_structure_conflict"


# ---------------------------------------------------------------------------
# JSONC dotted roots (OpenCode v2)
# ---------------------------------------------------------------------------


def test_json_dotted_root_creates_nested_and_preserves_comments() -> None:
    raw = (
        b"{\n"
        b"  // operator comment\n"
        b'  "$schema": "https://opencode.ai/config.json"\n'
        b"}\n"
    )
    entry = {
        "type": "local",
        "command": ["/usr/bin/agentbus", "mcp-serve"],
        "environment": {"AGENTBUS_WORKSPACE": "/ws", "AGENTBUS_PRODUCER_ID": "local-user"},
        "disabled": False,
    }
    patch = plan_json_patch(
        installation_id="opencode-v2-user",
        path=Path("/tmp/opencode.json"),
        root="mcp.servers",
        entry=entry,
        raw=raw,
    )
    text = (patch.new_bytes or b"").decode()
    assert "// operator comment" in text
    from agentbus.jsonc import sanitize

    document = json.loads(sanitize(text))
    assert document["mcp"]["servers"]["agentbus"]["disabled"] is False
    assert document["$schema"].startswith("https://opencode.ai")


def test_json_dotted_root_updates_existing_entry_only() -> None:
    raw = (
        b'{\n  "mcp": {\n    "servers": {\n'
        b'      "other": {"type": "local", "command": ["x"]}\n'
        b"    }\n  }\n}\n"
    )
    entry = {
        "type": "local",
        "command": ["/usr/bin/agentbus", "mcp-serve"],
        "environment": {"AGENTBUS_WORKSPACE": "/ws", "AGENTBUS_PRODUCER_ID": "local-user"},
        "disabled": False,
    }
    patch = plan_json_patch(
        installation_id="opencode-v2-user",
        path=Path("/tmp/opencode.json"),
        root="mcp.servers",
        entry=entry,
        raw=raw,
    )
    from agentbus.jsonc import sanitize

    document = json.loads(sanitize((patch.new_bytes or b"").decode()))
    assert document["mcp"]["servers"]["other"]["command"] == ["x"]
    assert document["mcp"]["servers"]["agentbus"]["command"][0] == "/usr/bin/agentbus"


def test_json_dotted_root_conflict_when_parent_is_not_an_object() -> None:
    with pytest.raises(ConfigConflictError) as excinfo:
        plan_json_patch(
            installation_id="opencode-v2-user",
            path=Path("/tmp/opencode.json"),
            root="mcp.servers",
            entry={"type": "local", "command": ["x"], "environment": {}},
            raw=b'{"mcp": "not-an-object"}\n',
        )
    assert excinfo.value.code == "root_not_object"


# ---------------------------------------------------------------------------
# Redaction
# ---------------------------------------------------------------------------


def test_redaction_masks_every_environment_value() -> None:
    entry = {
        "type": "local",
        "command": ["/usr/bin/agentbus", "mcp-serve"],
        "environment": {
            "AGENTBUS_WORKSPACE": "/secret/workspace",
            "SERVICE_TOKEN": "ghp_abcdefghijklmno",
        },
        "envs": {"API_KEY": "sk-live-0123456789"},
        "disabled": False,
    }
    redacted = redact_entry(entry) or {}
    assert redacted["environment"] == {
        "AGENTBUS_WORKSPACE": "[REDACTED]",
        "SERVICE_TOKEN": "[REDACTED]",
    }
    assert redacted["envs"] == {"API_KEY": "[REDACTED]"}
    assert entry_env_keys(entry) == [
        "AGENTBUS_WORKSPACE",
        "API_KEY",
        "SERVICE_TOKEN",
    ]


# ---------------------------------------------------------------------------
# OpenCode version discrimination
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "text, expected",
    [
        ("opencode v2.0.18\n", VARIANT_V2),
        ("opencode v1.15.3", VARIANT_V1),
        ("opencode 2.0.18", VARIANT_V2),
        ("2.0.18\n", VARIANT_V2),
        ("v1.0.0", VARIANT_V1),
        ("opencode v3.0.0", None),
        ("not a version", None),
        ("", None),
    ],
)
def test_variant_from_version_text(text: str, expected: str | None) -> None:
    assert variant_from_version_text(text) == expected


@pytest.mark.parametrize(
    "document, expected",
    [
        (
            {"mcp": {"servers": {"type": "local", "command": ["npx", "example"]}}},
            VARIANT_V1,
        ),
        (
            {"mcp": {"servers": {"agentbus": {"type": "local", "command": ["x"]}}}},
            VARIANT_V2,
        ),
        (
            {"mcp": {"servers": {"remote": {"type": "remote", "url": "https://x"}}}},
            VARIANT_V2,
        ),
        ({"mcp": {"agentbus": {"type": "local", "command": ["x"]}}}, VARIANT_V1),
        ({"mcp": {"servers": {}}}, None),
        ({"mcp": {"servers": {"child": {"type": "local"}}}}, None),
        ({"mcp": {}}, None),
        ({}, None),
        ({"mcp": "nope"}, None),
    ],
)
def test_inspect_config_structure(document: dict, expected: str | None) -> None:
    variant, _detail = inspect_config_structure(document)
    assert variant == expected


def test_schema_markers_are_honored() -> None:
    assert inspect_schema({"$schema": "https://opencode.ai/v2/config.json"}) == VARIANT_V2
    assert inspect_schema({"$schema": "https://example.com/v1/config.json"}) == VARIANT_V1
    assert inspect_schema({"$schema": "https://opencode.ai/config.json"}) is None


def test_detect_variant_refuses_selector_contradicting_structure(tmp_path: Path) -> None:
    config = tmp_path / "opencode.json"
    config.write_text('{"mcp": {"agentbus": {"command": ["x"]}}}\n', encoding="utf-8")
    with pytest.raises(OpenCodeVersionAmbiguityError):
        detect_variant(
            forced="opencode-v2",
            config_paths=[config],
            which=lambda _name: None,
        )


def test_detect_variant_accepts_selector_when_no_structural_conflict(tmp_path: Path) -> None:
    config = tmp_path / "opencode.json"
    config.write_text("{}\n", encoding="utf-8")
    decision = detect_variant(
        forced="opencode-v2",
        config_paths=[config],
        which=lambda _name: None,
    )
    assert decision.variant == VARIANT_V2
    assert decision.evidence == ("explicit_selector:v2",)


def test_detect_variant_uses_structural_evidence(tmp_path: Path) -> None:
    config = tmp_path / "opencode.json"
    config.write_text(
        '{"mcp": {"servers": {"agentbus": {"command": ["x"]}}}}\n', encoding="utf-8"
    )
    decision = detect_variant(
        config_paths=[config], which=lambda _name: None, probe_versions=False
    )
    assert decision.variant == VARIANT_V2


def test_detect_variant_parses_cli_output(tmp_path: Path) -> None:
    calls: list[str] = []

    def probe(binary: str, _environ) -> str:
        calls.append(binary)
        return "opencode v1.2.3\n"

    decision = detect_variant(
        config_paths=[],
        which=lambda name: "/usr/bin/opencode" if name == "opencode" else None,
        version_probe=probe,
        probe_versions=True,
    )
    assert decision.variant == VARIANT_V1
    assert calls == ["/usr/bin/opencode"]


def test_detect_variant_conflict_fails_closed(tmp_path: Path) -> None:
    config = tmp_path / "opencode.json"
    config.write_text('{"mcp": {"agentbus": {"command": ["x"]}}}\n', encoding="utf-8")
    with pytest.raises(OpenCodeVersionAmbiguityError) as excinfo:
        detect_variant(
            config_paths=[config],
            which=lambda name: "/usr/bin/opencode",
            version_probe=lambda _b, _e: "opencode v2.0.18\n",
            probe_versions=True,
        )
    assert AMBIGUITY_ACTION in str(excinfo.value)
    assert "conflicting version evidence" in str(excinfo.value)


def test_detect_variant_without_evidence_fails_closed(tmp_path: Path) -> None:
    config = tmp_path / "opencode.json"
    config.write_text('{"mcp": {"servers": {}}}\n', encoding="utf-8")
    with pytest.raises(OpenCodeVersionAmbiguityError) as excinfo:
        detect_variant(
            config_paths=[config], which=lambda name: "/usr/bin/opencode",
            probe_versions=False,
        )
    assert AMBIGUITY_ACTION in str(excinfo.value)
    assert "--client opencode-v2 or --client opencode-v1" in str(excinfo.value)


def test_read_only_detection_never_launches_a_process(tmp_path: Path) -> None:
    config = tmp_path / "opencode.json"
    config.write_text(
        '{"mcp": {"servers": {"agentbus": {"command": ["x"]}}}}\n', encoding="utf-8"
    )

    def forbidden(_binary, _environ):
        raise AssertionError("read-only detection must not launch a process")

    decision = detect_variant(
        config_paths=[config],
        which=lambda name: "/usr/bin/opencode",
        version_probe=forbidden,
        probe_versions=False,
    )
    assert decision.variant == VARIANT_V2


def test_detect_variant_rejects_unknown_forced_selector() -> None:
    with pytest.raises(ValueError):
        detect_variant(forced="opencode-v9", which=lambda _n: None)


@pytest.mark.skipif(os.name == "nt", reason="POSIX shell stub")
def test_version_probe_is_bounded_and_uses_a_throwaway_home(tmp_path: Path) -> None:
    from agentbus.adapters.opencode import _default_version_probe

    real_home = tmp_path / "real-home"
    real_home.mkdir()
    stub = tmp_path / "fake-opencode"
    stub.write_text(
        "#!/bin/sh\n"
        'echo touched > "$HOME/probe-wrote-here"\n'
        "echo opencode v2.0.18\n",
        encoding="utf-8",
    )
    stub.chmod(0o755)

    output = _default_version_probe(
        str(stub), {"HOME": str(real_home), "PATH": os.environ.get("PATH", "")}
    )

    assert output is not None
    assert variant_from_version_text(output) == VARIANT_V2
    # The client's $HOME write landed in the scratch dir, not the real one.
    assert not (real_home / "probe-wrote-here").exists()
    assert list(real_home.iterdir()) == []

@pytest.mark.parametrize("text", ["opencode v0.15.3", "0.15.3", "opencode v0.1.0"])
def test_opencode_zero_versions(text: str) -> None:
    assert variant_from_version_text(text) == VARIANT_V1


def test_untrusted_opencode_is_never_executed(tmp_path: Path, monkeypatch) -> None:
    cwd = tmp_path / "cwd"
    cwd.mkdir()
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    monkeypatch.chdir(cwd)
    for binary in ("opencode", "./opencode", workspace / "nested" / "opencode"):
        calls = []
        with pytest.raises(OpenCodeVersionAmbiguityError, match="untrusted relative or workspace binary"):
            detect_variant(workspace=workspace, which=lambda _: str(binary),
                           version_probe=lambda *args: calls.append(args))
        assert not calls


def test_zero_version_conflicts_with_v2_structure(tmp_path: Path) -> None:
    config = tmp_path / "opencode.json"
    config.write_text('{"mcp":{"servers":{"agentbus":{"command":["x"]}}}}')
    with pytest.raises(OpenCodeVersionAmbiguityError, match="conflicting version evidence"):
        detect_variant(config_paths=[config], which=lambda _: "/usr/bin/opencode",
                       version_probe=lambda *_: "opencode v0.15.3")


def test_client_discovery_filters_relative_and_workspace_binaries(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from agentbus.adapters.probe import _evidence_binaries

    cwd = tmp_path / "cwd"
    workspace = tmp_path / "workspace"
    cwd.mkdir()
    workspace.mkdir()
    monkeypatch.chdir(cwd)
    installation = SimpleNamespace(binaries=("client",))
    for binary in ("client", "./client", workspace / "nested" / "client"):
        assert not _evidence_binaries(installation, workspace=workspace, which=lambda _: str(binary))
    for binary in ("/usr/bin/client", str(cwd / "client"), str(Path.home() / ".local/bin/client")):
        assert _evidence_binaries(installation, workspace=workspace, which=lambda _: binary)


def test_opencode_trusted_absolute_binary_is_probed(tmp_path, monkeypatch):
    cwd = tmp_path / "cwd"
    cwd.mkdir()
    monkeypatch.chdir(cwd)
    workspace = tmp_path / "workspace"
    for binary in ("/usr/bin/opencode", str(cwd / "opencode"), str(Path.home() / ".local/bin/opencode")):
        calls = []
        assert detect_variant(workspace=workspace, which=lambda _: binary,
                              version_probe=lambda *args: calls.append(args) or "opencode v0.15.3").variant == VARIANT_V1
        assert calls[0][0] == binary
