"""Persisted workspace settings under .agentbus/config.json."""

from __future__ import annotations

import json
from pathlib import Path

CONFIG_FILENAME = "config.json"
DEFAULT_RETENTION_DAYS = 7


def config_path(workspace: Path) -> Path:
    return workspace.resolve() / ".agentbus" / CONFIG_FILENAME


def load_config(workspace: Path) -> dict:
    path = config_path(workspace)
    if not path.is_file():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def save_config(workspace: Path, data: dict) -> Path:
    path = config_path(workspace)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
    return path


def load_retention_days(workspace: Path) -> int | None:
    value = load_config(workspace).get("retention_days")
    if value is None:
        return None
    return int(value)


def save_retention_days(workspace: Path, days: int) -> None:
    data = load_config(workspace)
    data["retention_days"] = days
    save_config(workspace, data)


def resolve_retention_days(workspace: Path, cli_value: int) -> int:
    """CLI flag wins when non-default; otherwise use persisted workspace value."""
    if cli_value != DEFAULT_RETENTION_DAYS:
        save_retention_days(workspace, cli_value)
        return cli_value
    stored = load_retention_days(workspace)
    if stored is not None:
        return stored
    return DEFAULT_RETENTION_DAYS