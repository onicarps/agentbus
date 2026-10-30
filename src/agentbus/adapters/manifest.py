"""Load packaged MCP client manifests and resolve their paths.

Manifests are read-only YAML data. Nothing in this module touches the
filesystem beyond reading the packaged resources themselves.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from importlib import resources
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import yaml

MANIFEST_VERSION = 1
MANIFEST_SUFFIX = ".yaml"
ENTRY_SHAPES = (
    "command_args_env",
    "vscode_stdio",
    "goose_stdio",
    "copilot_local",
    "cline_stdio",
    "vibe_stdio",
    "opencode_v1",
    "opencode_v2",
)
FORMATS = ("json", "toml", "yaml")
ROOT_KINDS = ("map", "array")
SCOPES = ("user", "project")
APPROVAL_MODES = ("restart", "reload", "approve", "none")
SERVER_NAME = "agentbus"

_VAR = re.compile(
    r"\$\{(?P<braced>[A-Za-z_][A-Za-z0-9_]*)(?::-?(?P<default>[^}]*))?\}"
    r"|\$(?P<plain>[A-Za-z_][A-Za-z0-9_]*)"
    r"|%(?P<win>[A-Za-z_][A-Za-z0-9_]*)%"
)
_WINDOWS_ABSOLUTE = re.compile(r"^[A-Za-z]:/")


class ManifestError(ValueError):
    """A packaged manifest is missing, unreadable, or violates the schema."""


@dataclass(frozen=True)
class Approval:
    mode: str
    guidance: str


@dataclass(frozen=True)
class Installation:
    manifest_id: str
    client: str
    title: str
    installation_id: str
    scope: str
    format: str
    root: str
    entry_shape: str
    auto_connect: bool
    paths: Mapping[str, str]
    binaries: tuple[str, ...]
    app_dirs: Mapping[str, tuple[str, ...]]
    approval: Approval
    manifest_version: str
    #: ``map`` (a server container object) or ``array`` (TOML array-of-tables
    #: matched by a ``name`` key, as used by Mistral Vibe).
    root_kind: str = "map"
    #: Installations sharing a variant group describe incompatible config
    #: layouts of one client (OpenCode v1/v2). At most one may be connected.
    variant_group: str = ""
    variant: str = ""

    @property
    def identifier(self) -> str:
        return self.installation_id

    @property
    def variant_selector(self) -> str:
        """``opencode-v1``-style selector, empty for ordinary installs."""
        return f"{self.client}-{self.variant}" if self.variant else ""

    def selected_by(self, name: str) -> bool:
        """Whether a ``--client`` selector names this installation."""
        if name in (self.installation_id, self.client):
            return True
        selector = self.variant_selector
        return bool(selector) and name == selector

    def supports(self, platform: str) -> bool:
        return "all" in self.paths or platform in self.paths

    def path_for(
        self,
        *,
        platform: str,
        workspace: Path,
        home: Path,
        environ: Mapping[str, str] | None = None,
    ) -> Path:
        template = self.paths.get(platform) or self.paths.get("all")
        if template is None:
            raise ManifestError(
                f"{self.installation_id}: no path template for platform {platform!r}"
            )
        return expand_template(
            template,
            platform=platform,
            workspace=workspace,
            home=home,
            environ=environ,
        )

    def app_dir_templates(self, platform: str) -> tuple[str, ...]:
        if platform in self.app_dirs:
            return self.app_dirs[platform]
        return self.app_dirs.get("all", ())


@dataclass(frozen=True)
class Manifest:
    manifest_id: str
    client: str
    title: str
    version: str
    generated_by: str
    generated_at: str
    notes: str
    default_approval: Approval
    installations: tuple[Installation, ...]

    def installation(self, installation_id: str) -> Installation | None:
        for item in self.installations:
            if item.installation_id == installation_id:
                return item
        return None


def manifest_root() -> Path:
    """Filesystem path of the packaged manifests (works for wheels too)."""
    return Path(resources.files("agentbus.adapters").joinpath("manifests"))


def _require(mapping: Mapping[str, Any], key: str, *, where: str) -> Any:
    if key not in mapping:
        raise ManifestError(f"{where}: missing required field {key!r}")
    return mapping[key]


def _str_list(value: Any, *, where: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
        raise ManifestError(f"{where}: expected a list of strings")
    return tuple(value)


def _platform_map(
    value: Any, *, where: str
) -> dict[str, tuple[str, ...]]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ManifestError(f"{where}: expected a mapping of platform -> paths")
    out: dict[str, tuple[str, ...]] = {}
    for platform, items in value.items():
        if not isinstance(platform, str):
            raise ManifestError(f"{where}: platform keys must be strings")
        out[platform] = _str_list(items, where=f"{where}.{platform}")
    return out


def _approval(value: Any, *, where: str, fallback: Approval) -> Approval:
    if value is None:
        return fallback
    if not isinstance(value, dict):
        raise ManifestError(f"{where}: approval must be a mapping")
    mode = value.get("mode", fallback.mode)
    if mode not in APPROVAL_MODES:
        raise ManifestError(
            f"{where}: approval.mode must be one of {APPROVAL_MODES}, got {mode!r}"
        )
    guidance = value.get("guidance", fallback.guidance)
    if not isinstance(guidance, str):
        raise ManifestError(f"{where}: approval.guidance must be a string")
    return Approval(mode=mode, guidance=guidance)


def _parse_manifest(document: Any, *, source: str) -> Manifest:
    if not isinstance(document, dict):
        raise ManifestError(f"{source}: manifest root must be a mapping")
    version = document.get("manifest_version")
    if version != MANIFEST_VERSION:
        raise ManifestError(
            f"{source}: unsupported manifest_version {version!r} "
            f"(expected {MANIFEST_VERSION})"
        )
    generated = document.get("generated") or {}
    if not isinstance(generated, dict):
        raise ManifestError(f"{source}: generated must be a mapping")
    default_approval = _approval(
        document.get("approval"),
        where=f"{source}.approval",
        fallback=Approval(mode="restart", guidance=""),
    )
    installations_raw = _require(document, "installations", where=source)
    if not isinstance(installations_raw, list) or not installations_raw:
        raise ManifestError(f"{source}: installations must be a non-empty list")

    manifest_id = str(_require(document, "manifest_id", where=source))
    client = str(_require(document, "client", where=source))
    title = str(_require(document, "title", where=source))
    version_label = str(document.get("version", ""))

    installations: list[Installation] = []
    for index, raw in enumerate(installations_raw):
        where = f"{source}.installations[{index}]"
        if not isinstance(raw, dict):
            raise ManifestError(f"{where}: expected a mapping")
        installation_id = str(_require(raw, "id", where=where))
        fmt = str(_require(raw, "format", where=where))
        if fmt not in FORMATS:
            raise ManifestError(f"{where}: format must be one of {FORMATS}")
        scope = str(_require(raw, "scope", where=where))
        if scope not in SCOPES:
            raise ManifestError(f"{where}: scope must be one of {SCOPES}")
        shape = str(_require(raw, "entry_shape", where=where))
        if shape not in ENTRY_SHAPES:
            raise ManifestError(f"{where}: entry_shape must be one of {ENTRY_SHAPES}")
        root = str(_require(raw, "root", where=where))
        if not root.strip():
            raise ManifestError(f"{where}: root must be a non-empty string")
        root_kind = str(raw.get("root_kind", "map"))
        if root_kind not in ROOT_KINDS:
            raise ManifestError(f"{where}: root_kind must be one of {ROOT_KINDS}")
        variant_group = str(raw.get("variant_group", "") or "")
        variant = str(raw.get("variant", "") or "")
        if variant and not variant_group:
            raise ManifestError(f"{where}: variant requires variant_group")
        if variant_group and not variant:
            raise ManifestError(f"{where}: variant_group requires variant")
        if fmt == "yaml" and root_kind != "map":
            raise ManifestError(f"{where}: yaml configs only support root_kind 'map'")
        if shape == "vibe_stdio" and (fmt != "toml" or root_kind != "array"):
            raise ManifestError(
                f"{where}: vibe_stdio requires format 'toml' and root_kind 'array'"
            )
        paths_raw = _require(raw, "paths", where=where)
        if not isinstance(paths_raw, dict) or not paths_raw:
            raise ManifestError(f"{where}: paths must be a non-empty mapping")
        paths: dict[str, str] = {}
        for platform, template in paths_raw.items():
            if not isinstance(template, str) or not template.strip():
                raise ManifestError(f"{where}.paths.{platform}: expected a path string")
            if platform not in ("all", "linux", "darwin", "win32"):
                raise ManifestError(
                    f"{where}.paths: unknown platform key {platform!r}"
                )
            paths[str(platform)] = template
        detection = raw.get("detection") or {}
        if not isinstance(detection, dict):
            raise ManifestError(f"{where}.detection: expected a mapping")
        auto_connect = raw.get("auto_connect", True)
        if not isinstance(auto_connect, bool):
            raise ManifestError(f"{where}: auto_connect must be a boolean")
        installations.append(
            Installation(
                manifest_id=manifest_id,
                client=client,
                title=str(raw.get("title", installation_id)),
                installation_id=installation_id,
                scope=scope,
                format=fmt,
                root=root,
                entry_shape=shape,
                auto_connect=auto_connect,
                paths=paths,
                binaries=_str_list(
                    detection.get("binaries"), where=f"{where}.detection.binaries"
                ),
                app_dirs=_platform_map(
                    detection.get("app_dirs"), where=f"{where}.detection.app_dirs"
                ),
                approval=_approval(
                    raw.get("approval"),
                    where=f"{where}.approval",
                    fallback=default_approval,
                ),
                manifest_version=version_label,
                root_kind=root_kind,
                variant_group=variant_group,
                variant=variant,
            )
        )

    ids = [item.installation_id for item in installations]
    if len(set(ids)) != len(ids):
        raise ManifestError(f"{source}: duplicate installation ids {ids}")

    return Manifest(
        manifest_id=manifest_id,
        client=client,
        title=title,
        version=version_label,
        generated_by=str(generated.get("by", "")),
        generated_at=str(generated.get("at", "")),
        notes=str(document.get("notes", "")),
        default_approval=default_approval,
        installations=tuple(installations),
    )


def load_manifests() -> tuple[Manifest, ...]:
    """Load every packaged manifest in deterministic order."""
    # ``importlib.resources`` keeps this working for zipped/wheel installs.
    root = resources.files("agentbus.adapters").joinpath("manifests")
    if not root.is_dir():
        raise ManifestError(f"manifest directory not packaged: {root}")
    manifests: list[Manifest] = []
    for entry in sorted(root.iterdir(), key=lambda p: p.name):
        if not entry.name.endswith(MANIFEST_SUFFIX):
            continue
        try:
            text = entry.read_text(encoding="utf-8")
        except OSError as exc:  # pragma: no cover - packaging failure
            raise ManifestError(f"{entry.name}: unreadable: {exc}") from exc
        try:
            document = yaml.safe_load(text)
        except yaml.YAMLError as exc:
            raise ManifestError(f"{entry.name}: invalid YAML: {exc}") from exc
        manifest = _parse_manifest(document, source=entry.name)
        manifests.append(manifest)
    if not manifests:
        raise ManifestError(f"no manifests found in {root}")
    return tuple(manifests)


def install_target(
    installation_id: str, manifests: Iterable[Manifest] | None = None
) -> Installation:
    """Look up one installation by its stable id (e.g. ``cursor-project``)."""
    for manifest in manifests if manifests is not None else load_manifests():
        found = manifest.installation(installation_id)
        if found is not None:
            return found
    raise ManifestError(f"unknown client installation {installation_id!r}")


def expand_template(
    template: str,
    *,
    platform: str,
    workspace: Path,
    home: Path,
    environ: Mapping[str, str] | None = None,
) -> Path:
    """Expand ``{workspace}``, ``${VAR}``/``${VAR:-default}``, ``%VAR%`` and ``~``."""
    env = dict(os.environ if environ is None else environ)
    env.setdefault("HOME", str(home))
    env.setdefault("USERPROFILE", str(home))
    def _substitute(match: re.Match[str]) -> str:
        name = match.group("braced") or match.group("plain") or match.group("win")
        default = match.group("default")
        value = env.get(name or "")
        if value:
            return value
        if default is not None:
            return default
        raise ManifestError(
            f"path template {template!r} needs environment variable {name!r}"
        )

    text = _VAR.sub(_substitute, template)
    text = text.replace("{workspace}", str(workspace))
    text = text.replace("{home}", str(home))
    if text.startswith("~"):
        text = str(home) + text[1:]
    if platform == "win32":
        text = text.replace("\\", "/")
    path = Path(text).expanduser()
    if not path.is_absolute() and not _WINDOWS_ABSOLUTE.match(text):
        raise ManifestError(
            f"path template {template!r} did not expand to an absolute path: {path}"
        )
    return path


def build_command_entry(
    argv: Sequence[str],
    *,
    workspace: Path,
    producer_id: str,
) -> dict[str, Any]:
    """Client-neutral stdio entry (command/args/env)."""
    if not argv:
        raise ValueError("argv must not be empty")
    return {
        "command": argv[0],
        "args": [
            *argv[1:],
            "--workspace",
            str(workspace),
        ],
        "env": {
            "AGENTBUS_WORKSPACE": str(workspace),
            "AGENTBUS_PRODUCER_ID": producer_id,
        },
    }


def build_entry(
    installation: Installation,
    *,
    argv: tuple[str, ...],
    workspace: Path,
    producer_id: str,
) -> dict[str, Any]:
    """Build the client-specific server entry for the resolved command."""
    base = build_command_entry(
        argv, workspace=workspace, producer_id=producer_id
    )
    shape = installation.entry_shape
    if shape == "command_args_env":
        return base
    if shape == "vscode_stdio":
        return {"type": "stdio", **base}
    if shape == "goose_stdio":
        # Goose calls the executable "cmd" and names the environment map
        # "envs" (``env`` is only a deserialization alias).
        return {
            "type": "stdio",
            "name": SERVER_NAME,
            "enabled": True,
            "cmd": base["command"],
            "args": list(base["args"]),
            "envs": dict(base["env"]),
        }
    if shape == "copilot_local":
        return {"type": "local", **base, "tools": ["*"]}
    if shape == "cline_stdio":
        return {**base, "disabled": False}
    if shape == "vibe_stdio":
        # Vibe stores servers as an array of tables matched by "name".
        return {
            "name": SERVER_NAME,
            "transport": "stdio",
            "command": base["command"],
            "args": list(base["args"]),
            "env": dict(base["env"]),
        }
    if shape in ("opencode_v1", "opencode_v2"):
        # OpenCode keeps the whole argv (including --workspace) in one array
        # and calls the environment mapping "environment".
        command = [base["command"], *base["args"]]
        entry: dict[str, Any] = {
            "type": "local",
            "command": command,
            "environment": dict(base["env"]),
        }
        if shape == "opencode_v1":
            entry["enabled"] = True
        else:
            entry["disabled"] = False
        return entry
    raise ManifestError(
        f"{installation.installation_id}: unknown entry_shape {shape!r}"
    )
