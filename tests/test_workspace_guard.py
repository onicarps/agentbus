"""DrvFS / workspace path hard-ban."""

from pathlib import Path

import pytest

from agentbus.workspace_guard import (
    UnsupportedWorkspaceError,
    assert_workspace_supported,
    diagnose_workspace,
)


def test_home_path_ok(tmp_path, monkeypatch):
    monkeypatch.delenv("AGENTBUS_ALLOW_DRVFS", raising=False)
    # tmp is native FS on CI/Linux
    p = assert_workspace_supported(tmp_path)
    assert p == tmp_path.resolve()


def test_mnt_c_rejected(monkeypatch):
    monkeypatch.delenv("AGENTBUS_ALLOW_DRVFS", raising=False)
    ok, reason = diagnose_workspace("/mnt/c/Users/foo/project")
    assert ok is False
    assert "DrvFS" in reason or "mnt" in reason.lower()
    with pytest.raises(UnsupportedWorkspaceError):
        assert_workspace_supported("/mnt/c/Users/foo/project")


def test_break_glass(monkeypatch):
    monkeypatch.setenv("AGENTBUS_ALLOW_DRVFS", "1")
    p = assert_workspace_supported("/mnt/c/Users/foo/project")
    assert "mnt" in p.as_posix() or p.as_posix().startswith("/mnt")


def test_event_store_rejects_drvfs(monkeypatch):
    monkeypatch.delenv("AGENTBUS_ALLOW_DRVFS", raising=False)
    from agentbus.store import EventStore

    with pytest.raises(UnsupportedWorkspaceError):
        EventStore(Path("/mnt/c/Users/foo/ws"))
