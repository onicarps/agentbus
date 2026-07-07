"""Phase 5 advisory lease lock tests."""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from agentbus.auth import check_publish_token, write_workspace_token
from agentbus.leases import (
    DEFAULT_TTL_SECONDS,
    LeaseStore,
    MAX_TTL_SECONDS,
    normalize_resource,
)


@pytest.fixture
def ws(tmp_path):
    return tmp_path


@pytest.fixture
def leases(ws):
    store = LeaseStore(ws)
    yield store
    store.close()


def test_acquire_and_release(ws, leases):
    resource = str(ws / "log.md")
    (ws / "log.md").write_text("# log\n")
    got = leases.lock_acquire(resource, "hermes", ttl_seconds=300)
    assert got["acquired"] is True
    assert "lease_id" in got

    released = leases.lock_release(resource, got["lease_id"], "hermes")
    assert released["released"] is True

    status = leases.lock_status(resource)
    assert status["locked"] is False


def test_acquire_conflict(ws, leases):
    resource = str(ws / "a.txt")
    (ws / "a.txt").write_text("x")
    first = leases.lock_acquire(resource, "hermes", 300)
    second = leases.lock_acquire(resource, "grok", 300)
    assert first["acquired"] is True
    assert second["acquired"] is False
    assert second["current_owner"] == "hermes"


def test_renew_extends_lease(ws, leases):
    resource = str(ws / "b.txt")
    (ws / "b.txt").write_text("x")
    got = leases.lock_acquire(resource, "grok", ttl_seconds=60)
    before = leases.lock_status(resource)["expires_at"]
    renewed = leases.lock_renew(resource, got["lease_id"], "grok", ttl_seconds=120)
    assert renewed["renewed"] is True
    assert renewed["expires_at"] > before


def test_renew_wrong_owner_fails(ws, leases):
    resource = str(ws / "c.txt")
    (ws / "c.txt").write_text("x")
    got = leases.lock_acquire(resource, "hermes", 300)
    result = leases.lock_renew(resource, got["lease_id"], "grok", 300)
    assert result["renewed"] is False


def test_release_idempotent_when_missing(ws, leases):
    resource = str(ws / "d.txt")
    (ws / "d.txt").write_text("x")
    result = leases.lock_release(resource, "00000000-0000-0000-0000-000000000000", "hermes")
    assert result["released"] is True


def test_ttl_expiry(ws, leases):
    resource = str(ws / "e.txt")
    (ws / "e.txt").write_text("x")
    leases.lock_acquire(resource, "hermes", ttl_seconds=1)
    assert leases.lock_status(resource)["locked"] is True
    time.sleep(1.1)
    assert leases.lock_status(resource)["locked"] is False


def test_resource_outside_workspace(ws, leases):
    with pytest.raises(ValueError, match="resource_outside_workspace"):
        leases.lock_acquire("/etc/passwd", "hermes", 300)


def test_invalid_owner_id(ws, leases):
    resource = str(ws / "f.txt")
    (ws / "f.txt").write_text("x")
    with pytest.raises(ValueError, match="invalid_owner_id"):
        leases.lock_acquire(resource, "Bad Owner", 300)


def test_ttl_defaults_and_max(ws, leases):
    resource = str(ws / "g.txt")
    (ws / "g.txt").write_text("x")
    got = leases.lock_acquire(resource, "hermes")
    assert got["acquired"] is True
    resource2 = str(ws / "g2.txt")
    (ws / "g2.txt").write_text("x")
    with pytest.raises(ValueError, match="invalid_ttl"):
        leases.lock_acquire(resource2, "hermes", ttl_seconds=MAX_TTL_SECONDS + 1)


def test_normalize_resource_absolute(ws):
    path = normalize_resource(ws, str(ws / "sub" / "file.md"))
    assert path.startswith(str(ws.resolve()))


def test_lock_auth_required(monkeypatch, ws):
    write_workspace_token(ws, "secret")
    monkeypatch.setenv("AGENTBUS_TOKEN", "wrong-token")
    with pytest.raises(ValueError, match="unauthorized"):
        check_publish_token(ws)


def test_uses_events_db_not_separate_file(ws, leases):
    assert leases.db_path.name == "events.db"
    assert leases.db_path.parent.name == ".agentbus"


def test_same_owner_reacquire_returns_existing(ws, leases):
    resource = str(ws / "h.txt")
    (ws / "h.txt").write_text("x")
    first = leases.lock_acquire(resource, "hermes", 300)
    second = leases.lock_acquire(resource, "hermes", 300)
    assert second["acquired"] is True
    assert second["lease_id"] == first["lease_id"]