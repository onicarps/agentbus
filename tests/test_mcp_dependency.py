"""Packaging guard for the MCP Python SDK v2 compatibility line."""

from __future__ import annotations

import tomllib
from importlib.metadata import version
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_project_metadata_pins_supported_mcp_major() -> None:
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))[
        "project"
    ]

    assert project["version"] == "0.18.0"
    assert "mcp>=2,<3" in project["dependencies"]


def test_test_environment_uses_supported_mcp_major() -> None:
    major, *_ = (int(part) for part in version("mcp").split(".") if part.isdigit())

    assert major == 2
