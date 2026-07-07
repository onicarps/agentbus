"""Workspace-scoped ephemeral token authentication for publish."""

from __future__ import annotations

import os
import secrets
from pathlib import Path

TOKEN_FILENAME = "token"


def token_path(workspace: Path) -> Path:
    return workspace.resolve() / ".agentbus" / TOKEN_FILENAME


def generate_token() -> str:
    return secrets.token_urlsafe(32)


def read_workspace_token(workspace: Path) -> str | None:
    path = token_path(workspace)
    if not path.is_file():
        return None
    return path.read_text(encoding="utf-8").strip()


def write_workspace_token(workspace: Path, token: str) -> Path:
    path = token_path(workspace)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(token + "\n", encoding="utf-8")
    os.chmod(path, 0o600)
    return path


def ensure_ephemeral_token(workspace: Path, *, rotate: bool = False) -> str:
    """Create or reuse workspace token. Set rotate=True to regenerate."""
    existing = read_workspace_token(workspace)
    if existing and not rotate:
        return existing
    token = generate_token()
    write_workspace_token(workspace, token)
    return token


def expected_publish_token(workspace: Path | None) -> str | None:
    """Resolve the token required for publish operations."""
    if os.environ.get("AGENTBUS_AUTH", "auto").lower() == "off":
        return None
    if workspace is not None:
        file_token = read_workspace_token(workspace)
        if file_token:
            return file_token
    return os.environ.get("AGENTBUS_EXPECTED_TOKEN") or None


def provided_publish_token(
    auth_token: str | None = None,
    workspace: Path | None = None,
) -> str:
    if auth_token:
        return auth_token
    env_token = os.environ.get("AGENTBUS_TOKEN", "")
    if env_token:
        return env_token
    if workspace is not None:
        file_token = read_workspace_token(workspace)
        if file_token:
            return file_token
    return ""


def check_publish_token(
    workspace: Path | None = None,
    auth_token: str | None = None,
) -> None:
    """Require a valid token for publish when auth is configured."""
    expected = expected_publish_token(workspace)
    if not expected:
        return
    if provided_publish_token(auth_token, workspace) != expected:
        raise ValueError("unauthorized")