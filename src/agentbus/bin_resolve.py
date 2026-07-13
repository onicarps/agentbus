"""Locate packaged Go binaries (platform wheels) — no runtime download."""

from __future__ import annotations

import os
import platform
import sys
from pathlib import Path

# Map (sys.platform, machine) → directory name used in wheels / optional npm packages.
_PLATFORM_DIRS: dict[tuple[str, str], str] = {
    ("linux", "x86_64"): "linux-x64",
    ("linux", "amd64"): "linux-x64",
    ("linux", "aarch64"): "linux-arm64",
    ("linux", "arm64"): "linux-arm64",
    ("darwin", "x86_64"): "darwin-x64",
    ("darwin", "arm64"): "darwin-arm64",
    ("win32", "amd64"): "win32-x64",
    ("win32", "x86_64"): "win32-x64",
}


def platform_dir() -> str:
    """Return platform key for current host (e.g. linux-x64)."""
    sys_plat = sys.platform  # linux, darwin, win32
    machine = platform.machine().lower()
    key = (sys_plat, machine)
    if key in _PLATFORM_DIRS:
        return _PLATFORM_DIRS[key]
    # normalize amd64/x86_64
    if machine in ("amd64", "x86_64"):
        key2 = (sys_plat, "x86_64")
        if key2 in _PLATFORM_DIRS:
            return _PLATFORM_DIRS[key2]
    if machine in ("aarch64", "arm64"):
        key2 = (sys_plat, "arm64")
        if key2 in _PLATFORM_DIRS:
            return _PLATFORM_DIRS[key2]
    raise RuntimeError(
        f"unsupported platform for bundled Go binaries: {sys_plat}/{machine}"
    )


def _exe_name(name: str) -> str:
    if sys.platform == "win32" and not name.endswith(".exe"):
        return name + ".exe"
    return name


def package_bin_root() -> Path:
    """Directory that may hold platform subdirs or flat binaries (dev)."""
    return Path(__file__).resolve().parent / "bin"


def resolve_bundled_binary(name: str) -> Path | None:
    """Find a Go binary shipped inside the wheel, if present.

    Layout (release wheels)::

        agentbus/bin/<platform-dir>/agentbus-go-worker
        agentbus/bin/<platform-dir>/agentbus-go-serve

    Dev layout (optional flat)::

        agentbus/bin/agentbus-go-worker
    """
    root = package_bin_root()
    exe = _exe_name(name)
    # Platform-specific (Ruff-style single-platform wheel content)
    try:
        plat = platform_dir()
    except RuntimeError:
        plat = None
    candidates: list[Path] = []
    if plat:
        candidates.append(root / plat / exe)
    candidates.append(root / exe)
    for c in candidates:
        if c.is_file() and os.access(c, os.X_OK):
            return c
    return None


def resolve_go_binary(
    name: str,
    *,
    env_var: str,
    dev_candidates: list[Path] | None = None,
) -> Path:
    """Resolve Go helper binary: env override → wheel bundle → dev paths → PATH."""
    env = os.environ.get(env_var)
    if env:
        p = Path(env).expanduser()
        if p.is_file():
            return p
        raise FileNotFoundError(f"{env_var} not found: {env}")

    bundled = resolve_bundled_binary(name)
    if bundled is not None:
        return bundled

    for c in dev_candidates or []:
        if c.is_file() and os.access(c, os.X_OK):
            return c

    # PATH
    path_env = os.environ.get("PATH", "")
    for part in path_env.split(os.pathsep):
        if not part:
            continue
        c = Path(part) / _exe_name(name)
        if c.is_file() and os.access(c, os.X_OK):
            return c

    raise FileNotFoundError(
        f"{name} not found. Install a platform wheel of okf-agentbus "
        f"(includes Go binaries), set {env_var}, or build go-core "
        f"(cd go-core && make build)."
    )
