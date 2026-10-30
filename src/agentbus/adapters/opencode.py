"""OpenCode v1/v2 configuration discrimination.

OpenCode shipped two incompatible MCP layouts that share the same config
filename:

* v1: ``{"mcp": {"<server>": {...}}}`` with an ``enabled`` flag.
* v2: ``{"mcp": {"servers": {"<server>": {...}}}}`` with a ``disabled`` flag.

The ambiguous case is a *v1 server literally named* ``servers``::

    {"mcp": {"servers": {"type": "local", "command": ["npx", "example"]}}}

so the layout cannot be guessed from the presence of ``mcp.servers`` alone.
This module collects independent signals (explicit selector, ``$schema`` URL,
bounded ``--version`` probe, structural analysis) and **fails closed** when
they disagree or when no signal is conclusive. It never guesses a variant
from absence.

Everything here is read-only: at most one bounded ``<binary> --version``
process is started, and only when a caller already found evidence that the
client is installed.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable, Mapping, Sequence

VARIANT_V1 = "v1"
VARIANT_V2 = "v2"
VARIANTS = (VARIANT_V1, VARIANT_V2)

#: ``opencode v2.0.18`` (the observed output) and tolerant variants.
_VERSION_OUTPUT = re.compile(r"opencode\s+v?(\d+)(?:\.\d+)*", re.IGNORECASE)
_BARE_VERSION = re.compile(r"^v?(\d+)(?:\.\d+)*$")
_V2_MARKER = re.compile(r"(?:^|[/._-])v2(?:[/._-]|$)", re.IGNORECASE)
_V1_MARKER = re.compile(r"(?:^|[/._-])v1(?:[/._-]|$)", re.IGNORECASE)

_VERSION_TIMEOUT_SECONDS = 5.0

#: Guidance every ambiguity error repeats so the fix is always visible.
AMBIGUITY_ACTION = (
    "Specify --client opencode-v2 or --client opencode-v1 to connect."
)


class OpenCodeVersionAmbiguityError(RuntimeError):
    """OpenCode's configuration version cannot be determined safely."""

    def __init__(self, detail: str = "", *, evidence: Sequence[str] = ()):
        self.detail = detail
        self.evidence = tuple(evidence)
        message = f"Ambiguous OpenCode configuration detected. {AMBIGUITY_ACTION}"
        if detail:
            message = f"{message} ({detail})"
        super().__init__(message)


@dataclass(frozen=True)
class VariantDecision:
    variant: str
    evidence: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {"variant": self.variant, "evidence": list(self.evidence)}


def variant_from_version_text(text: str) -> str | None:
    """Map ``opencode --version`` output to a variant, or ``None``.

    The documented output shape is ``opencode v<major>.<minor>.<patch>``.
    A bare ``2.0.18`` is also accepted because some wrappers strip the name.
    """
    if not isinstance(text, str):
        return None
    match = _VERSION_OUTPUT.search(text)
    if match is None:
        candidate = text.strip().splitlines()[0].strip() if text.strip() else ""
        match = _BARE_VERSION.match(candidate)
    if match is None:
        return None
    major = int(match.group(1))
    if major in (0, 1):
        return VARIANT_V1
    if major == 2:
        return VARIANT_V2
    return None


def _is_server_object(node: object) -> bool:
    return isinstance(node, dict) and ("command" in node or "url" in node)


def inspect_schema(document: object) -> str | None:
    """Explicit ``$schema`` version marker, if the URL names one."""
    if not isinstance(document, dict):
        return None
    schema = document.get("$schema")
    if not isinstance(schema, str):
        return None
    if _V2_MARKER.search(schema):
        return VARIANT_V2
    if _V1_MARKER.search(schema):
        return VARIANT_V1
    return None


def inspect_config_structure(document: object) -> tuple[str | None, str]:
    """Classify an OpenCode config by structure alone.

    Returns ``(variant | None, explanation)``. ``None`` means the document
    carries no decisive structural signal; the explanation says why, so an
    ambiguity error can be actionable instead of a bare "unknown".
    """
    if not isinstance(document, dict):
        return None, "config root is not an object"
    mcp = document.get("mcp")
    if mcp is None:
        return None, "no mcp section"
    if not isinstance(mcp, dict):
        return None, "mcp is not an object"

    servers = mcp.get("servers")
    if servers is not None:
        if not isinstance(servers, dict):
            return None, "mcp.servers is not an object"
        if "command" in servers or "url" in servers:
            # v1 server literally named "servers": a server object, not a
            # v2 container.
            return VARIANT_V1, "mcp.servers is itself a v1 server object"
        if not servers:
            return None, "mcp.servers is an empty object"
        if any(_is_server_object(child) for child in servers.values()):
            return VARIANT_V2, "mcp.servers is a container of server objects"
        return None, "mcp.servers children have no command or url"

    direct = [name for name, child in mcp.items() if _is_server_object(child)]
    if direct:
        return VARIANT_V1, "mcp maps server names directly"
    return None, "no server definition found"


def _default_version_probe(
    binary: str, environ: Mapping[str, str] | None
) -> str | None:
    """Run one bounded, hermetic ``--version`` probe.

    HOME and the XDG roots point at a throwaway directory so a client that
    writes a version cache cannot pollute the operator's real profile.
    """
    import tempfile

    env = dict(environ or {})
    if not env.get("PATH"):
        env["PATH"] = os.environ.get("PATH", "")
    try:
        with tempfile.TemporaryDirectory(
            prefix="agentbus-opencode-probe-", ignore_cleanup_errors=True
        ) as scratch:
            root = Path(scratch)
            env.update(
                {
                    "HOME": str(root),
                    "XDG_CACHE_HOME": str(root / "cache"),
                    "XDG_CONFIG_HOME": str(root / "config"),
                    "XDG_DATA_HOME": str(root / "data"),
                    "XDG_STATE_HOME": str(root / "state"),
                }
            )
            completed = subprocess.run(
                [binary, "--version"],
                capture_output=True,
                text=True,
                timeout=_VERSION_TIMEOUT_SECONDS,
                env=env,
                check=False,
            )
    except (OSError, subprocess.SubprocessError):
        return None
    output = (completed.stdout or "") + "\n" + (completed.stderr or "")
    return output if output.strip() else None


def _read_json_document(path: Path) -> tuple[object | None, str | None]:
    try:
        raw = path.read_bytes()
    except FileNotFoundError:
        return None, None
    except OSError as exc:
        return None, f"{path}: unreadable ({exc})"
    if not raw.strip():
        return None, f"{path}: empty config"
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        return None, f"{path}: invalid utf-8 ({exc})"
    try:
        from agentbus.jsonc import loads

        return loads(text), None
    except json.JSONDecodeError as exc:
        return None, f"{path}: unparsable config ({exc})"


def is_safe_client_executable(binary: str | Path, *, workspace: Path | None = None) -> bool:
    """Reject relative search results and executables within the workspace."""
    try:
        raw = Path(binary).expanduser()
        if not raw.is_absolute():
            return False
        resolved = raw.resolve()
        if workspace is not None:
            ws = workspace.expanduser().resolve()
            if resolved == ws or ws in resolved.parents:
                return False
        return True
    except (OSError, RuntimeError, ValueError):
        return False


def detect_variant(
    *,
    forced: str | None = None,
    workspace: Path | None = None,
    config_paths: Iterable[Path] = (),
    environ: Mapping[str, str] | None = None,
    which: Callable[[str], str | None] = shutil.which,
    version_probe: Callable[[str, Mapping[str, str] | None], str | None] | None = None,
    probe_versions: bool = True,
) -> VariantDecision:
    """Resolve the OpenCode variant or raise :class:`OpenCodeVersionAmbiguityError`.

    Signal order: explicit selector, ``$schema`` URL, ``--version`` probe,
    structural analysis. Every signal is collected and any disagreement is
    treated as ambiguity — the function never prefers "the newer one".

    ``probe_versions=False`` keeps read-only callers (``agentbus discover``
    and a dry-run ``agentbus init``) strictly free of process launches; only
    structural and schema evidence is used in that mode.
    """
    if forced is not None:
        normalized = str(forced).strip().lower()
        if normalized.startswith("opencode-"):
            normalized = normalized.partition("opencode-")[2]
        if normalized not in VARIANTS:
            raise ValueError(
                f"unknown OpenCode variant selector {forced!r}; "
                f"expected one of {VARIANTS}"
            )
        for path in config_paths:
            document, _problem = _read_json_document(Path(path))
            if document is None:
                continue
            structure, explanation = inspect_config_structure(document)
            if structure is not None and structure != normalized:
                raise OpenCodeVersionAmbiguityError(
                    f"--client opencode-{normalized} contradicts {path}: {explanation}",
                    evidence=(f"{path}: {explanation}",),
                )
        return VariantDecision(normalized, (f"explicit_selector:{normalized}",))

    evidence: dict[str, list[str]] = {VARIANT_V1: [], VARIANT_V2: []}
    notes: list[str] = []

    for path in config_paths:
        document, problem = _read_json_document(Path(path))
        if problem is not None:
            notes.append(problem)
            continue
        if document is None:
            continue
        schema = inspect_schema(document)
        if schema is not None:
            evidence[schema].append(f"{path}: $schema names {schema}")
        structure, explanation = inspect_config_structure(document)
        if structure is not None:
            evidence[structure].append(f"{path}: {explanation}")
        else:
            notes.append(f"{path}: {explanation}")

    binary = which("opencode")
    if binary and not is_safe_client_executable(binary, workspace=workspace):
        notes.append(f"untrusted relative or workspace binary ignored: {binary}")
        binary = None
    if not probe_versions:
        notes.append("version probe suppressed in read-only mode")
    elif binary:
        probe = version_probe or _default_version_probe
        output = probe(binary, environ)
        if output is None:
            notes.append("opencode --version produced no output")
        else:
            from_version = variant_from_version_text(output)
            if from_version is None:
                first_line = output.strip().splitlines()[0]
                notes.append(f"opencode --version unrecognized: {first_line[:120]}")
            else:
                evidence[from_version].append(
                    f"opencode --version reports {from_version}"
                )
    else:
        notes.append("opencode CLI not found")

    conclusive = {
        variant: reasons for variant, reasons in evidence.items() if reasons
    }
    if len(conclusive) == 1:
        variant, reasons = next(iter(conclusive.items()))
        return VariantDecision(variant, tuple(reasons))
    if len(conclusive) > 1:
        conflicts = sorted(
            reason for reasons in conclusive.values() for reason in reasons
        )
        raise OpenCodeVersionAmbiguityError(
            "conflicting version evidence: " + "; ".join(conflicts),
            evidence=conflicts,
        )
    raise OpenCodeVersionAmbiguityError(
        "no conclusive version evidence: " + ("; ".join(notes) or "none"),
        evidence=tuple(notes),
    )
