"""Read-only client discovery probes driven by packaged manifests.

Guarantees:

* never creates, modifies, or deletes anything;
* never opens a socket;
* never reads a value it reports as a secret (only structure is inspected);
* reports ``unknown`` whenever evidence is ambiguous or unreadable instead of
  guessing ``present`` or ``absent``.

The one process boundary is a bounded, read-only ``<binary> --version`` probe
used only to disambiguate client *variants* that share a config filename and
format (OpenCode v1/v2). It runs only when the client is already evidenced as
installed, never for ordinary clients.
"""

from __future__ import annotations

import json
import os
import platform as platform_module
import shutil
import stat
import tomllib
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

import yaml

from agentbus.adapters.manifest import (
    SERVER_NAME,
    Installation,
    Manifest,
    expand_template,
    load_manifests,
)
from agentbus.adapters.opencode import (
    AMBIGUITY_ACTION,
    OpenCodeVersionAmbiguityError,
    detect_variant,
    is_safe_client_executable,
)

STATUS_PRESENT = "present"
STATUS_ABSENT = "absent"
STATUS_UNKNOWN = "unknown"

CONFIG_MISSING = "missing"
CONFIG_VALID = "valid"
CONFIG_MALFORMED = "malformed"
CONFIG_UNREADABLE = "unreadable"

ENTRY_ABSENT = "absent"
ENTRY_MATCHES = "matches"
ENTRY_DIFFERS = "differs"
ENTRY_CONFLICT = "conflict"


@dataclass(frozen=True)
class ClientProbe:
    manifest_id: str
    client: str
    installation_id: str
    title: str
    scope: str
    format: str
    root: str
    status: str
    reason: str
    config_path: str | None
    config_state: str
    entry_state: str
    managed: bool
    writable: bool
    evidence: tuple[str, ...] = field(default_factory=tuple)
    approval_mode: str = "restart"
    approval_guidance: str = ""
    auto_connect: bool = True
    #: Variant label (``v1``/``v2``) for clients with incompatible layouts.
    variant: str = ""

    @property
    def present(self) -> bool:
        return self.status == STATUS_PRESENT

    @property
    def variant_selector(self) -> str:
        """``opencode-v1``-style selector, empty for ordinary installs."""
        return f"{self.client}-{self.variant}" if self.variant else ""

    def selected_by(self, name: str) -> bool:
        """Whether a ``--client`` selector names this probe."""
        if name in (self.installation_id, self.client):
            return True
        selector = self.variant_selector
        return bool(selector) and name == selector

    def to_dict(self) -> dict[str, Any]:
        return {
            "client": self.client,
            "installation": self.installation_id,
            "title": self.title,
            "scope": self.scope,
            "format": self.format,
            "status": self.status,
            "reason": self.reason,
            "config_path": self.config_path,
            "config_state": self.config_state,
            "entry_state": self.entry_state,
            "managed": self.managed,
            "writable": self.writable,
            "auto_connect": self.auto_connect,
            "evidence": list(self.evidence),
            "approval": {
                "mode": self.approval_mode,
                "guidance": self.approval_guidance,
            },
        }


def current_platform() -> str:
    if os.name == "nt":
        return "win32"
    return platform_module.system().lower() or "linux"


def _stat_state(path: Path) -> str:
    """``missing`` | ``denied`` | ``error`` | ``file`` | ``other``."""
    try:
        st = path.stat()
    except FileNotFoundError:
        return "missing"
    except PermissionError:
        return "denied"
    except OSError:
        return "error"
    if stat.S_ISREG(st.st_mode):
        return "file"
    return "other"


def _load_document(path: Path, fmt: str) -> tuple[str, Any, str]:
    """Return ``(state, document, detail)`` without ever writing.

    ``detail`` carries the offending exception so unreadable or malformed
    configuration never degrades silently to ``absent``.
    """
    try:
        from agentbus.safe_file import read_regular_file

        raw = read_regular_file(path)
    except PermissionError as exc:
        return CONFIG_UNREADABLE, None, f"permission denied: {exc}"
    except OSError as exc:
        return CONFIG_UNREADABLE, None, f"unreadable: {exc}"
    if not raw.strip():
        return CONFIG_VALID, {}, ""
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        return CONFIG_MALFORMED, None, f"invalid utf-8: {exc}"
    if fmt == "json":
        try:
            from agentbus.jsonc import loads

            document = loads(text)
        except json.JSONDecodeError as exc:
            return CONFIG_MALFORMED, None, f"invalid json: {exc}"
    elif fmt == "toml":
        try:
            document = tomllib.loads(text)
        except tomllib.TOMLDecodeError as exc:
            return CONFIG_MALFORMED, None, f"invalid toml: {exc}"
    elif fmt == "yaml":
        try:
            document = yaml.safe_load(text)
        except yaml.YAMLError as exc:
            return CONFIG_MALFORMED, None, f"invalid yaml: {exc}"
        if document is None:
            document = {}
    else:  # pragma: no cover - guarded by manifest validation
        return CONFIG_MALFORMED, None, f"unsupported format {fmt!r}"
    if not isinstance(document, dict):
        return CONFIG_MALFORMED, None, "expected a mapping at document root"
    return CONFIG_VALID, document, ""


def _root_table(document: Mapping[str, Any], root: str) -> tuple[str, Any]:
    """Resolve a dotted root path (``mcpServers`` or ``mcp_servers.agentbus``)."""
    node: Any = document
    for part in root.split("."):
        if not isinstance(node, dict):
            return ENTRY_CONFLICT, None
        node = node.get(part)
        if node is None:
            return ENTRY_ABSENT, None
    if not isinstance(node, dict):
        return ENTRY_CONFLICT, None
    return ENTRY_MATCHES, node


def _server_entry(
    document: Mapping[str, Any], installation: Installation
) -> tuple[str, Any]:
    """Return ``(entry_state, entry)`` for the agentbus server.

    ``installation.root`` is the full path to the managed table. JSON clients
    use a server container (``mcpServers``) and need one more ``agentbus``
    lookup; TOML clients already address ``mcp_servers.agentbus`` directly.
    The final path segment decides, so no client-specific branching is needed.
    ``root_kind: array`` addresses an array of tables matched by ``name``
    (Mistral Vibe) instead of a mapping key.
    """
    parts = installation.root.split(".")
    node: Any = document
    for part in parts:
        if not isinstance(node, dict):
            return ENTRY_CONFLICT, None
        if part not in node:
            return ENTRY_ABSENT, None
        node = node[part]

    if installation.root_kind == "array":
        if not isinstance(node, list):
            return ENTRY_CONFLICT, None
        for item in node:
            if isinstance(item, dict) and item.get("name") == SERVER_NAME:
                return ENTRY_MATCHES, item
        return ENTRY_ABSENT, None

    if not isinstance(node, dict):
        return ENTRY_CONFLICT, None
    if parts and parts[-1] == SERVER_NAME:
        return ENTRY_MATCHES, node
    entry = node.get(SERVER_NAME)
    if entry is None:
        return ENTRY_ABSENT, None
    if not isinstance(entry, dict):
        return ENTRY_CONFLICT, None
    return ENTRY_MATCHES, entry


def _entry_equal(actual: Any, expected: Mapping[str, Any] | None) -> bool:
    if expected is None:
        return False
    from agentbus.adapters.writer import entry_matches

    return entry_matches(actual, expected)


def _nearest_writable(path: Path) -> bool:
    """Whether the closest existing ancestor directory is writable by us.

    A prospective config file (``~/.cursor/mcp.json`` on a fresh machine) does
    not exist yet, so writability has to be judged from the deepest existing
    parent. Directories are writable-or-not like anything else: classifying
    them as an opaque ``other`` made every missing config look unwritable,
    which misreported discovery for exactly the first-run case this code
    exists to serve.
    """
    try:
        if path.exists():
            return os.access(path, os.W_OK)
        current = path.parent
        while not current.exists() and current != current.parent:
            current = current.parent
        if not current.exists():
            return False
        return os.access(current, os.W_OK)
    except OSError:
        return False


def _evidence_app_dirs(
    installation: Installation,
    *,
    platform: str,
    workspace: Path,
    home: Path,
    environ: Mapping[str, str],
) -> list[str]:
    found: list[str] = []
    for template in installation.app_dir_templates(platform):
        try:
            candidate = expand_template(
                template,
                platform=platform,
                workspace=workspace,
                home=home,
                environ=environ,
            )
        except Exception:  # ambiguous evidence must not abort discovery
            continue
        state = _stat_state(candidate)
        if state in ("file", "other"):
            found.append(f"app_dir:{candidate}")
    return found


def _evidence_binaries(
    installation: Installation,
    *,
    which: Callable[[str], str | None],
    workspace: Path,
) -> list[str]:
    found: list[str] = []
    for name in installation.binaries:
        resolved = which(name)
        if resolved and is_safe_client_executable(resolved, workspace=workspace):
            found.append(f"binary:{name}={resolved}")
    return found


def probe_installation(
    installation: Installation,
    *,
    workspace: Path,
    home: Path,
    platform: str,
    environ: Mapping[str, str],
    expected_entry: Mapping[str, Any] | None,
    which: Callable[[str], str | None] = shutil.which,
) -> ClientProbe:
    guidance = installation.approval.guidance
    if not installation.supports(platform):
        return ClientProbe(
            manifest_id=installation.manifest_id,
            client=installation.client,
            installation_id=installation.installation_id,
            title=installation.title,
            scope=installation.scope,
            format=installation.format,
            root=installation.root,
            status=STATUS_UNKNOWN,
            reason=f"unsupported_platform:{platform}",
            config_path=None,
            config_state=CONFIG_MISSING,
            entry_state=ENTRY_ABSENT,
            managed=False,
            writable=False,
            evidence=(),
            approval_mode=installation.approval.mode,
            approval_guidance=guidance,
            auto_connect=installation.auto_connect,
            variant=installation.variant,
        )

    try:
        config_path = installation.path_for(
            platform=platform, workspace=workspace, home=home, environ=environ
        )
    except Exception as exc:
        return ClientProbe(
            manifest_id=installation.manifest_id,
            client=installation.client,
            installation_id=installation.installation_id,
            title=installation.title,
            scope=installation.scope,
            format=installation.format,
            root=installation.root,
            status=STATUS_UNKNOWN,
            reason=f"path_unresolved:{exc}",
            config_path=None,
            config_state=CONFIG_MISSING,
            entry_state=ENTRY_ABSENT,
            managed=False,
            writable=False,
            evidence=(),
            approval_mode=installation.approval.mode,
            approval_guidance=guidance,
            auto_connect=installation.auto_connect,
            variant=installation.variant,
        )

    evidence = _evidence_binaries(installation, which=which, workspace=workspace)
    evidence.extend(
        _evidence_app_dirs(
            installation,
            platform=platform,
            workspace=workspace,
            home=home,
            environ=environ,
        )
    )

    state = _stat_state(config_path)
    if state in ("denied", "error"):
        detail = (
            "permission_denied" if state == "denied" else "stat_failed"
        )
        return ClientProbe(
            manifest_id=installation.manifest_id,
            client=installation.client,
            installation_id=installation.installation_id,
            title=installation.title,
            scope=installation.scope,
            format=installation.format,
            root=installation.root,
            status=STATUS_UNKNOWN,
            reason=detail,
            config_path=str(config_path),
            config_state=CONFIG_UNREADABLE,
            entry_state=ENTRY_ABSENT,
            managed=False,
            writable=False,
            evidence=(*evidence, f"config:{config_path}"),
            approval_mode=installation.approval.mode,
            approval_guidance=guidance,
            auto_connect=installation.auto_connect,
            variant=installation.variant,
        )

    if state in ("file", "other"):
        document_state, document, detail = _load_document(
            config_path, installation.format
        )
        if document_state != CONFIG_VALID or document is None:
            reason = f"config_{document_state}"
            if detail:
                reason += f": {detail}"
            return ClientProbe(
                manifest_id=installation.manifest_id,
                client=installation.client,
                installation_id=installation.installation_id,
                title=installation.title,
                scope=installation.scope,
                format=installation.format,
                root=installation.root,
                status=STATUS_UNKNOWN,
                reason=reason,
                config_path=str(config_path),
                config_state=document_state,
                entry_state=ENTRY_ABSENT,
                managed=False,
                writable=False,
                evidence=(*evidence, f"config:{config_path}"),
                approval_mode=installation.approval.mode,
                approval_guidance=guidance,
                auto_connect=installation.auto_connect,
                variant=installation.variant,
            )
        entry_state, entry = _server_entry(document, installation)
        managed = entry is not None
        if entry_state == ENTRY_CONFLICT:
            # The agentbus key exists but is not an object we understand.
            return ClientProbe(
                manifest_id=installation.manifest_id,
                client=installation.client,
                installation_id=installation.installation_id,
                title=installation.title,
                scope=installation.scope,
                format=installation.format,
                root=installation.root,
                status=STATUS_UNKNOWN,
                reason="conflicting_agentbus_entry",
                config_path=str(config_path),
                config_state=CONFIG_VALID,
                entry_state=ENTRY_CONFLICT,
                managed=True,
                writable=False,
                evidence=(*evidence, f"config:{config_path}"),
                approval_mode=installation.approval.mode,
                approval_guidance=guidance,
                auto_connect=installation.auto_connect,
                variant=installation.variant,
            )
        if entry_state == ENTRY_MATCHES:
            entry_state = (
                ENTRY_MATCHES
                if _entry_equal(entry, expected_entry)
                else ENTRY_DIFFERS
            )
        return ClientProbe(
            manifest_id=installation.manifest_id,
            client=installation.client,
            installation_id=installation.installation_id,
            title=installation.title,
            scope=installation.scope,
            format=installation.format,
            root=installation.root,
            status=STATUS_PRESENT,
            reason="config_found",
            config_path=str(config_path),
            config_state=CONFIG_VALID,
            entry_state=entry_state,
            managed=managed,
            writable=os.access(config_path, os.W_OK),
            evidence=(*evidence, f"config:{config_path}"),
            approval_mode=installation.approval.mode,
            approval_guidance=guidance,
            auto_connect=installation.auto_connect,
            variant=installation.variant,
        )

    strong_evidence = tuple(
        item for item in evidence if item.startswith("binary:")
    )
    if strong_evidence:
        return ClientProbe(
            manifest_id=installation.manifest_id,
            client=installation.client,
            installation_id=installation.installation_id,
            title=installation.title,
            scope=installation.scope,
            format=installation.format,
            root=installation.root,
            status=STATUS_PRESENT,
            reason="installed_not_configured",
            config_path=str(config_path),
            config_state=CONFIG_MISSING,
            entry_state=ENTRY_ABSENT,
            managed=False,
            writable=_nearest_writable(config_path),
            evidence=tuple(evidence),
            approval_mode=installation.approval.mode,
            approval_guidance=guidance,
            auto_connect=installation.auto_connect,
            variant=installation.variant,
        )

    if evidence:
        # A directory alone is not proof the client is installed and usable:
        # stale or unrelated directories must not authorize a config write.
        return ClientProbe(
            manifest_id=installation.manifest_id,
            client=installation.client,
            installation_id=installation.installation_id,
            title=installation.title,
            scope=installation.scope,
            format=installation.format,
            root=installation.root,
            status=STATUS_UNKNOWN,
            reason="weak_evidence:app_dir_only",
            config_path=str(config_path),
            config_state=CONFIG_MISSING,
            entry_state=ENTRY_ABSENT,
            managed=False,
            writable=False,
            evidence=tuple(evidence),
            approval_mode=installation.approval.mode,
            approval_guidance=guidance,
            auto_connect=installation.auto_connect,
            variant=installation.variant,
        )

    return ClientProbe(
        manifest_id=installation.manifest_id,
        client=installation.client,
        installation_id=installation.installation_id,
        title=installation.title,
        scope=installation.scope,
        format=installation.format,
        root=installation.root,
        status=STATUS_ABSENT,
        reason="no_evidence",
        config_path=str(config_path),
        config_state=CONFIG_MISSING,
        entry_state=ENTRY_ABSENT,
        managed=False,
        writable=False,
        evidence=(),
        approval_mode=installation.approval.mode,
        approval_guidance=guidance,
        auto_connect=installation.auto_connect,
        variant=installation.variant,
    )


def _selected(
    installation: Installation, clients: Iterable[str] | None
) -> bool:
    if clients is None:
        return True
    return any(installation.selected_by(name) for name in clients)


def _variant_groups(
    catalogue: Iterable[Manifest],
) -> dict[tuple[str, str], list[Installation]]:
    groups: dict[tuple[str, str], list[Installation]] = {}
    for manifest in catalogue:
        for installation in manifest.installations:
            if installation.variant_group:
                groups.setdefault(
                    (installation.client, installation.variant_group), []
                ).append(installation)
    return groups


def _resolve_variant_groups(
    groups: Mapping[tuple[str, str], Sequence[Installation]],
    *,
    workspace: Path,
    home: Path,
    platform: str,
    environ: Mapping[str, str],
    clients: Iterable[str] | None,
    which: Callable[[str], str | None],
    probe_versions: bool,
) -> tuple[dict[str, str], dict[str, str]]:
    """Pick at most one installation per variant group.

    Returns ``(suppressed, ambiguous)``. ``suppressed`` maps the losing
    installation id to a reason (it is not probed at all); ``ambiguous`` maps
    every member to the fail-closed reason that replaces its probe status.
    """
    suppressed: dict[str, str] = {}
    ambiguous: dict[str, str] = {}
    wanted = set(clients) if clients is not None else None

    for members in groups.values():
        if wanted is not None and not any(
            member.selected_by(name)
            for member in members
            for name in wanted
        ):
            continue
        # An explicit variant selector (``opencode-v1``) or an explicit
        # installation id pins the variant. Naming the whole family
        # (``opencode``) leaves detection in charge.
        requested_variants = {
            member.variant
            for member in members
            for name in wanted or ()
            if name == member.variant_selector or name == member.installation_id
        }
        if len(requested_variants) > 1:
            detail = (
                "conflicting variant selectors "
                f"{sorted(requested_variants)} were requested explicitly"
            )
            for member in members:
                ambiguous[member.installation_id] = (
                    f"opencode_version_ambiguous: {detail}. {AMBIGUITY_ACTION}"
                )
            continue
        forced = next(iter(requested_variants), None)

        config_paths: list[Path] = []
        for member in members:
            try:
                path = member.path_for(
                    platform=platform,
                    workspace=workspace,
                    home=home,
                    environ=environ,
                )
            except Exception:  # unresolved paths are not evidence
                continue
            if path not in config_paths:
                config_paths.append(path)
        existing = [path for path in config_paths if path.exists()]

        binary: str | None = None
        for member in members:
            for name in member.binaries:
                binary = which(name)
                if binary and not is_safe_client_executable(binary, workspace=workspace):
                    binary = None
                if binary:
                    break
            if binary:
                break

        if forced is None and binary is None and not existing:
            # Nothing installed: ordinary ``absent`` probes are accurate and
            # an ambiguity error would be noise.
            continue
        try:
            decision = detect_variant(
                forced=forced,
                workspace=workspace,
                config_paths=existing,
                environ=environ,
                which=which,
                probe_versions=probe_versions,
            )
        except OpenCodeVersionAmbiguityError as exc:
            for member in members:
                ambiguous[member.installation_id] = (
                    f"opencode_version_ambiguous: {exc}"
                )
            continue
        for member in members:
            if member.variant != decision.variant:
                suppressed[member.installation_id] = (
                    f"variant_not_selected:{decision.variant}"
                )
    return suppressed, ambiguous


def probe_installations(
    *,
    workspace: Path,
    home: Path | None = None,
    platform: str | None = None,
    environ: Mapping[str, str] | None = None,
    clients: Iterable[str] | None = None,
    expected_entries: Mapping[str, Mapping[str, Any]] | None = None,
    manifests: Iterable[Manifest] | None = None,
    which: Callable[[str], str | None] = shutil.which,
    probe_versions: bool = False,
) -> tuple[ClientProbe, ...]:
    """Probe every manifest installation (or only ``clients``).

    ``probe_versions`` defaults to ``False`` so discovery stays free of
    process launches; mutating callers opt in when they are already about to
    write configuration.
    """
    resolved_home = Path.home() if home is None else home
    resolved_platform = current_platform() if platform is None else platform
    resolved_env = dict(os.environ if environ is None else environ)
    catalogue = tuple(load_manifests() if manifests is None else manifests)

    suppressed, ambiguous = _resolve_variant_groups(
        _variant_groups(catalogue),
        workspace=workspace,
        home=resolved_home,
        platform=resolved_platform,
        environ=resolved_env,
        clients=clients,
        which=which,
        probe_versions=probe_versions,
    )

    probes: list[ClientProbe] = []
    for manifest in catalogue:
        for installation in manifest.installations:
            if not _selected(installation, clients):
                continue
            if installation.installation_id in suppressed:
                continue
            expected = None
            if expected_entries is not None:
                expected = expected_entries.get(installation.installation_id)
            probe = probe_installation(
                installation,
                workspace=workspace,
                home=resolved_home,
                platform=resolved_platform,
                environ=resolved_env,
                expected_entry=expected,
                which=which,
            )
            reason = ambiguous.get(installation.installation_id)
            if reason is not None:
                probe = replace(probe, status=STATUS_UNKNOWN, reason=reason)
            probes.append(probe)
    return tuple(probes)
