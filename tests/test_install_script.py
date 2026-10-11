"""Regression checks for the public fresh-machine installer."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_universal_installer_bootstraps_an_isolated_managed_runtime():
    script = (ROOT / "install.sh").read_text(encoding="utf-8")

    assert "uv/install.sh" in script
    assert "tool install --upgrade" in script
    assert "--managed-python" in script
    assert "UV_TOOL_BIN_DIR" in script
    assert "pip install --user" not in script


def test_installer_never_initializes_the_directory_it_was_run_from():
    script = (ROOT / "install.sh").read_text(encoding="utf-8")

    # The usage hint may teach init; the installer itself must never invoke it.
    assert '"$BIN_DIR/agentbus" init --apply' not in script
    assert "AGENTBUS_CMD=" not in script
    assert "AGENTS.md" not in script
