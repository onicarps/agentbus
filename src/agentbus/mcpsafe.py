"""Decoupled mcpsafe policy middleware — .mcpsafe.lock allow/block lists."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

LOCKFILE_NAME = ".mcpsafe.lock"
ENV_ENABLE = "AGENTBUS_ENABLE_MCPSAFE"
ENV_LOCK = "AGENTBUS_MCPSAFE_LOCK"


class AccessDeniedError(Exception):
    """Policy denial — maps to 403 for MCP/CLI."""

    def __init__(self, message: str, *, code: int = 403) -> None:
        super().__init__(message)
        self.code = code


class PolicyEnforcer:
    """O(1) tool allow/block checks from a JSON lockfile."""

    def __init__(self, lockfile_path: str | Path) -> None:
        self.lockfile_path = Path(lockfile_path)
        self.allowed_tools: set[str] = set()
        self.blocked_tools: set[str] = set()
        self.load_policy()

    def load_policy(self) -> None:
        self.allowed_tools = set()
        self.blocked_tools = set()
        if not self.lockfile_path.exists():
            return
        with open(self.lockfile_path, encoding="utf-8") as f:
            policy = json.load(f)
        if not isinstance(policy, dict):
            return
        self.allowed_tools = {str(t) for t in (policy.get("allowed_tools") or [])}
        self.blocked_tools = {str(t) for t in (policy.get("blocked_tools") or [])}

    def evaluate(self, tool_name: str) -> bool:
        """Return True if the tool is permitted.

        Rules:
        - blocked_tools always deny
        - empty allowed_tools → allow all non-blocked
        - non-empty allowed_tools → tool must be listed
        """
        name = (tool_name or "").strip()
        if not name:
            return True
        if name in self.blocked_tools:
            return False
        if not self.allowed_tools:
            return True
        return name in self.allowed_tools

    def require(self, tool_name: str) -> None:
        if not self.evaluate(tool_name):
            raise AccessDeniedError(
                f"AccessDenied: tool {tool_name!r} blocked by mcpsafe policy "
                f"({self.lockfile_path})"
            )

    def evaluate_payload(self, payload: dict[str, Any] | None) -> bool:
        """If payload names a tool, evaluate it; otherwise allow."""
        if not payload or not isinstance(payload, dict):
            return True
        for key in ("tool", "tool_name", "mcp_tool"):
            val = payload.get(key)
            if isinstance(val, str) and val.strip():
                if not self.evaluate(val):
                    return False
        return True

    def require_payload(self, payload: dict[str, Any] | None) -> None:
        if not self.evaluate_payload(payload):
            tool = None
            if isinstance(payload, dict):
                for key in ("tool", "tool_name", "mcp_tool"):
                    if payload.get(key):
                        tool = payload.get(key)
                        break
            raise AccessDeniedError(
                f"AccessDenied: payload tool {tool!r} blocked by mcpsafe policy "
                f"({self.lockfile_path})"
            )


def resolve_lockfile(workspace: Path | None, lockfile: str | Path | None = None) -> Path:
    if lockfile:
        return Path(lockfile).expanduser().resolve()
    env = os.environ.get(ENV_LOCK)
    if env:
        return Path(env).expanduser().resolve()
    if workspace is not None:
        return (Path(workspace) / LOCKFILE_NAME).resolve()
    return Path(LOCKFILE_NAME).resolve()


def mcpsafe_enabled_from_env() -> bool:
    raw = (os.environ.get(ENV_ENABLE) or "").strip().lower()
    return raw in ("1", "true", "yes", "on")


def load_enforcer(
    workspace: Path | None = None,
    *,
    enabled: bool | None = None,
    lockfile: str | Path | None = None,
) -> PolicyEnforcer | None:
    """Return a PolicyEnforcer when enabled; None when off."""
    if enabled is None:
        enabled = mcpsafe_enabled_from_env()
    if not enabled:
        return None
    path = resolve_lockfile(workspace, lockfile)
    return PolicyEnforcer(path)
