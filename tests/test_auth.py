"""Publish token auth tests."""

from __future__ import annotations

import os
import stat

import pytest

from agentbus.auth import (
    check_publish_token,
    ensure_ephemeral_token,
    read_workspace_token,
    token_path,
    write_workspace_token,
)


def test_no_token_required_when_unconfigured(monkeypatch, tmp_path):
    monkeypatch.setenv("AGENTBUS_AUTH", "off")
    monkeypatch.delenv("AGENTBUS_EXPECTED_TOKEN", raising=False)
    monkeypatch.delenv("AGENTBUS_TOKEN", raising=False)
    check_publish_token(tmp_path)


def test_matching_env_token_passes(monkeypatch, tmp_path):
    monkeypatch.setenv("AGENTBUS_EXPECTED_TOKEN", "dev-local-only")
    monkeypatch.setenv("AGENTBUS_TOKEN", "dev-local-only")
    check_publish_token(tmp_path)


def test_wrong_token_raises(monkeypatch, tmp_path):
    monkeypatch.setenv("AGENTBUS_EXPECTED_TOKEN", "dev-local-only")
    monkeypatch.setenv("AGENTBUS_TOKEN", "wrong")
    with pytest.raises(ValueError, match="unauthorized"):
        check_publish_token(tmp_path)


def test_missing_token_raises(monkeypatch, tmp_path):
    monkeypatch.setenv("AGENTBUS_EXPECTED_TOKEN", "dev-local-only")
    monkeypatch.delenv("AGENTBUS_TOKEN", raising=False)
    with pytest.raises(ValueError, match="unauthorized"):
        check_publish_token(tmp_path)


def test_workspace_token_file_rejects_wrong_token(monkeypatch, tmp_path):
    monkeypatch.delenv("AGENTBUS_EXPECTED_TOKEN", raising=False)
    write_workspace_token(tmp_path, "ws-secret")
    monkeypatch.setenv("AGENTBUS_TOKEN", "wrong")
    with pytest.raises(ValueError, match="unauthorized"):
        check_publish_token(tmp_path)


def test_workspace_token_file_auto_provided(monkeypatch, tmp_path):
    monkeypatch.delenv("AGENTBUS_EXPECTED_TOKEN", raising=False)
    monkeypatch.delenv("AGENTBUS_TOKEN", raising=False)
    write_workspace_token(tmp_path, "ws-secret")
    check_publish_token(tmp_path)


def test_auth_token_arg_overrides_env(monkeypatch, tmp_path):
    write_workspace_token(tmp_path, "ws-secret")
    monkeypatch.setenv("AGENTBUS_TOKEN", "wrong")
    check_publish_token(tmp_path, auth_token="ws-secret")


def test_ensure_ephemeral_token_reuses_existing(tmp_path):
    first = ensure_ephemeral_token(tmp_path, rotate=False)
    second = ensure_ephemeral_token(tmp_path, rotate=False)
    assert first == second


def test_ensure_ephemeral_token_rotate(tmp_path):
    first = ensure_ephemeral_token(tmp_path, rotate=False)
    second = ensure_ephemeral_token(tmp_path, rotate=True)
    assert first != second


def test_token_file_permissions(tmp_path):
    ensure_ephemeral_token(tmp_path, rotate=False)
    mode = stat.S_IMODE(os.stat(token_path(tmp_path)).st_mode)
    assert mode == 0o600


def test_read_workspace_token_missing(tmp_path):
    assert read_workspace_token(tmp_path) is None