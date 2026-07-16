"""Workspace path hard constraints (wake-plane / SQLite safety).

Canonical bus must live on a native Linux (or native macOS/Windows) filesystem.
WSL DrvFS mounts (``/mnt/c``, ``/mnt/d``, …) are unsupported: weak/missing
inotify and unreliable SQLite WAL under concurrent writers.

Decision: initiatives/agentbus/decisions/wake-session-bridge-tech-discussion-2026-07-16.md
Audit: initiatives/agentbus/decisions/grok-spec-audit-2026-07-16.md
"""

from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path

# WSL interop mounts (DrvFS / 9p)
_MNT_DRIVE = re.compile(r"^/mnt/[a-zA-Z](/|$)")
_DRVFS_FSTYPES = frozenset({"9p", "drvfs", "virtiofs", "fuse.drvfs"})


class UnsupportedWorkspaceError(RuntimeError):
    """Workspace path is on an unsupported filesystem for AgentBus."""

    def __init__(self, workspace: Path, reason: str) -> None:
        self.workspace = workspace
        self.reason = reason
        super().__init__(
            f"unsupported AgentBus workspace {workspace}: {reason}. "
            "Use a native Linux path under /home (or non-DrvFS). "
            "Set AGENTBUS_ALLOW_DRVFS=1 only for emergency break-glass "
            "(wake/fsnotify and SQLite are not supported)."
        )


def _allow_drvfs() -> bool:
    return os.environ.get("AGENTBUS_ALLOW_DRVFS", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _path_looks_like_drvfs(path: Path) -> bool:
    try:
        resolved = path.resolve()
    except OSError:
        resolved = path
    s = resolved.as_posix()
    if _MNT_DRIVE.match(s):
        return True
    # Windows path leaked into WSL tooling
    if re.match(r"^[A-Za-z]:[/\\]", s) or s.startswith("/cygdrive/"):
        return True
    return False


def _fstype(path: Path) -> str | None:
    """Best-effort filesystem type (Linux findmnt / df)."""
    try:
        resolved = path.resolve()
    except OSError:
        return None
    # Prefer findmnt (util-linux)
    try:
        out = subprocess.run(
            ["findmnt", "-n", "-o", "FSTYPE", "-T", str(resolved)],
            capture_output=True,
            text=True,
            timeout=2,
            check=False,
        )
        if out.returncode == 0 and out.stdout.strip():
            return out.stdout.strip().split()[0].lower()
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        pass
    try:
        out = subprocess.run(
            ["df", "-T", str(resolved)],
            capture_output=True,
            text=True,
            timeout=2,
            check=False,
        )
        if out.returncode == 0:
            lines = out.stdout.strip().splitlines()
            if len(lines) >= 2:
                # Filesystem Type 1K-blocks ...
                parts = lines[1].split()
                if len(parts) >= 2:
                    return parts[1].lower()
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        pass
    return None


def diagnose_workspace(workspace: Path | str) -> tuple[bool, str]:
    """Return (ok, reason). ok=False means unsupported."""
    path = Path(workspace)
    if _path_looks_like_drvfs(path):
        return False, "path is on WSL DrvFS (/mnt/<drive>) or Windows path"
    fstype = _fstype(path if path.exists() else path.parent if path.parent.exists() else path)
    if fstype and fstype in _DRVFS_FSTYPES:
        return False, f"filesystem type {fstype!r} is unsupported for wake/SQLite"
    return True, "ok"


def assert_workspace_supported(workspace: Path | str) -> Path:
    """Resolve and enforce workspace constraints. Raises UnsupportedWorkspaceError."""
    path = Path(workspace).expanduser()
    try:
        path = path.resolve()
    except OSError:
        path = path.absolute()

    if _allow_drvfs():
        return path

    ok, reason = diagnose_workspace(path)
    if not ok:
        raise UnsupportedWorkspaceError(path, reason)
    return path
