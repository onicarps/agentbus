"""Packaging guard for the temporary MCP Python SDK v1 compatibility line."""

from __future__ import annotations

import tomllib
from importlib.metadata import version
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_project_metadata_pins_supported_mcp_major() -> None:
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))[
        "project"
    ]

    assert project["version"] == "0.16.4"
    assert "mcp>=1.29,<2" in project["dependencies"]


def test_test_environment_uses_supported_mcp_major() -> None:
    major, minor, *_ = (int(part) for part in version("mcp").split(".") if part.isdigit())

    assert (major, minor) >= (1, 29)
    assert major < 2
