"""Pytest wrapper for factory MCP integration validation."""

from __future__ import annotations

import pytest

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from factory_validate import run_validation  # noqa: E402


@pytest.mark.integration
def test_factory_mcp_validation_green():
    results = run_validation()
    assert results.failed == 0, results.results