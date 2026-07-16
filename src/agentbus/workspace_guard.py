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
import sys
from pathlib import Path, PosixPath, PurePath, WindowsPath

# WSL interop mounts (DrvFS / 9p)
_MNT_DRIVE = re.compile(r"^/mnt/[a-zA-Z](/|$)")
_WIN_DRIVE = re.compile(r"^[A-Za-z]:[/\\]")
_DRVFS_FSTYPES = frozenset({"9p", "drvfs", "virtiofs", "fuse.drvfs"})


def _host_path(raw: str) -> Path:
    """Construct a Path for the real host OS.

    Uses ``sys.platform`` (not ``os.name``) so unit tests that mock ``os.name``
    to exercise Windows SQLite PRAGMAs do not force ``WindowsPath`` on Linux CI.
    """
    if sys.platform == "win32":
        return WindowsPath(raw)
    return PosixPath(raw)


class UnsupportedWorkspaceError(RuntimeError):
    """Workspace path is on an unsupported filesystem for AgentBus."""

    def __init__(self, workspace: str | Path, reason: str) -> None:
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


def _path_to_str(workspace: Path | str | PurePath) -> str:
    """String form without re-instantiating platform Path subclasses."""
    if isinstance(workspace, str):
        return workspace
    # Path / PurePath — fspath is safe on all platforms
    return os.fspath(workspace)


def _string_looks_like_drvfs(raw: str) -> bool:
    # Normalize slashes for matching only
    s = raw.replace("\\", "/")
    if _MNT_DRIVE.match(s):
        return True
    if _WIN_DRIVE.match(raw) or _WIN_DRIVE.match(s):
        return True
    if s.startswith("/cygdrive/"):
        return True
    return False


def _fstype(path_str: str) -> str | None:
    """Best-effort filesystem type (Linux findmnt / df). Skip non-existent hosts."""
    if _string_looks_like_drvfs(path_str):
        return None
    try:
        path = _host_path(path_str)
    except (TypeError, ValueError, NotImplementedError, OSError):
        return None
    try:
        probe = path if path.exists() else (path.parent if path.parent.exists() else None)
    except (OSError, NotImplementedError):
        return None
    if probe is None:
        return None
    try:
        out = subprocess.run(
            ["findmnt", "-n", "-o", "FSTYPE", "-T", str(probe)],
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
            ["df", "-T", str(probe)],
            capture_output=True,
            text=True,
            timeout=2,
            check=False,
        )
        if out.returncode == 0:
            lines = out.stdout.strip().splitlines()
            if len(lines) >= 2:
                parts = lines[1].split()
                if len(parts) >= 2:
                    return parts[1].lower()
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        pass
    return None


def diagnose_workspace(workspace: Path | str) -> tuple[bool, str]:
    """Return (ok, reason). ok=False means unsupported."""
    raw = os.path.expanduser(_path_to_str(workspace))
    if _string_looks_like_drvfs(raw):
        return False, "path is on WSL DrvFS (/mnt/<drive>) or Windows path"
    fstype = _fstype(raw)
    if fstype and fstype in _DRVFS_FSTYPES:
        return False, f"filesystem type {fstype!r} is unsupported for wake/SQLite"
    return True, "ok"


def assert_workspace_supported(workspace: Path | str) -> Path:
    """Resolve and enforce workspace constraints. Raises UnsupportedWorkspaceError.

    Uses string-level checks first so Windows-style paths never force construction
    of ``WindowsPath`` on Linux (CI / unit tests).
    """
    raw = os.path.expanduser(_path_to_str(workspace))

    if _allow_drvfs():
        if _string_looks_like_drvfs(raw):
            return _host_path(raw.replace("\\", "/"))
        return _host_path(raw).resolve()

    ok, reason = diagnose_workspace(raw)
    if not ok:
        raise UnsupportedWorkspaceError(raw, reason)

    path = _host_path(raw)
    try:
        return path.resolve()
    except OSError:
        return path.absolute()
