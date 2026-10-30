"""Batch 1 transaction safety and the conditional RBAC contract."""

from __future__ import annotations

import hashlib
import os
import re
from pathlib import Path

import pytest
import yaml

from agentbus import discovery as discovery_module
from agentbus.adapters.probe import CONFIG_MALFORMED, CONFIG_VALID, _load_document
from agentbus.adapters.writer import entry_matches
from agentbus.discovery import (
    ConcurrentEditError,
    FileTransaction,
    SetupWriteError,
)
from agentbus.rbac import ForbiddenError, check_publish_rbac


def test_opencode_reader_rejects_device_and_preserves_missing_file(tmp_path: Path) -> None:
    from agentbus.adapters.opencode import _read_json_document

    assert _read_json_document(tmp_path / "missing") == (None, None)
    device = Path("/dev/null")
    if not device.exists():
        pytest.skip("device unavailable")
    config = tmp_path / "opencode.json"
    config.symlink_to(device)
    document, problem = _read_json_document(config)
    assert document is None
    assert problem and "unreadable" in problem and "not a regular file" in problem


@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="FIFOs unavailable")
def test_opencode_variant_rejects_fifo_without_blocking(tmp_path: Path) -> None:
    import subprocess
    import sys

    config = tmp_path / "opencode.json"
    os.mkfifo(config)
    script = """
import sys
from pathlib import Path
from agentbus.adapters.opencode import detect_variant, OpenCodeVersionAmbiguityError
try:
    detect_variant(config_paths=[Path(sys.argv[1])], which=lambda _: None, probe_versions=False)
except OpenCodeVersionAmbiguityError as exc:
    assert any('unreadable' in note and 'not a regular file' in note for note in exc.evidence)
else:
    raise AssertionError('expected ambiguous variant')
"""
    subprocess.run([sys.executable, "-c", script, str(config)], check=True, timeout=5)

ROLES = {
    "roles": {
        "engineer": {"can_publish_topics": ["okf/*"]},
    },
    "producers": {"alice": "engineer"},
    "token_roles": {"token-abc": "engineer"},
}


def test_probe_yaml_preserves_yaml12_plain_scalars(tmp_path: Path) -> None:
    path = tmp_path / "config.yaml"
    path.write_text(
        "GOOSE_TOOLSHIM: off\nflag: on\nchoice: yes\nother: no\nactive: true\n"
        "entry:\n  command: agentbus\n  args: [off]\n  envs: {MODE: off}\n",
        encoding="utf-8",
    )
    state, document, detail = _load_document(path, "yaml")
    assert state == CONFIG_VALID and not detail
    assert [document[k] for k in ("GOOSE_TOOLSHIM", "flag", "choice", "other")] == [
        "off", "on", "yes", "no"
    ]
    assert document["active"] is True
    assert entry_matches(document["entry"], {
        "command": "agentbus", "args": ["off"], "envs": {"MODE": "off"}
    })


def test_probe_yaml_malformed_syntax_returns_config_malformed(tmp_path: Path) -> None:
    path = tmp_path / "config.yaml"
    path.write_text("entry: {", encoding="utf-8")
    state, document, detail = _load_document(path, "yaml")
    assert state == CONFIG_MALFORMED
    assert document is None
    assert detail.startswith("invalid yaml:")


def _write_roles(workspace: Path) -> Path:
    path = workspace / ".agentbus" / "roles.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(ROLES, sort_keys=False), encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# Pre-replace digest verification
# ---------------------------------------------------------------------------


def test_commit_refuses_when_file_was_created_externally(tmp_path: Path) -> None:
    path = tmp_path / "created.json"
    transaction = FileTransaction()
    transaction.stage(path, b"ours", original_bytes=None)
    path.write_text("external", encoding="utf-8")

    with pytest.raises(SetupWriteError) as excinfo:
        transaction.commit()

    cause = excinfo.value.cause
    assert isinstance(cause, ConcurrentEditError)
    assert cause.code == "file_created_externally"
    assert path.read_text(encoding="utf-8") == "external"


def test_commit_refuses_when_file_was_deleted_externally(tmp_path: Path) -> None:
    path = tmp_path / "deleted.json"
    path.write_text("original", encoding="utf-8")
    transaction = FileTransaction()
    transaction.stage(path, b"ours", original_bytes=b"original")
    path.unlink()

    with pytest.raises(SetupWriteError) as excinfo:
        transaction.commit()

    cause = excinfo.value.cause
    assert isinstance(cause, ConcurrentEditError)
    assert cause.code == "file_deleted_externally"
    assert not path.exists()


def test_commit_refuses_when_file_was_modified_externally(tmp_path: Path) -> None:
    path = tmp_path / "modified.json"
    path.write_text("original", encoding="utf-8")
    transaction = FileTransaction()
    transaction.stage(path, b"ours", original_bytes=b"original")
    path.write_text("edited elsewhere", encoding="utf-8")

    with pytest.raises(SetupWriteError) as excinfo:
        transaction.commit()

    cause = excinfo.value.cause
    assert isinstance(cause, ConcurrentEditError)
    assert cause.code == "file_modified_externally"
    assert path.read_text(encoding="utf-8") == "edited elsewhere"


def test_commit_refuses_when_symlink_is_repointed(tmp_path: Path) -> None:
    target_a = tmp_path / "a.json"
    target_b = tmp_path / "b.json"
    target_a.write_text("original", encoding="utf-8")
    target_b.write_text("other", encoding="utf-8")
    link = tmp_path / "link.json"
    link.symlink_to(target_a)
    transaction = FileTransaction()
    transaction.stage(link, b"ours", original_bytes=b"original")
    link.unlink()
    link.symlink_to(target_b)

    with pytest.raises(SetupWriteError) as excinfo:
        transaction.commit()

    cause = excinfo.value.cause
    assert isinstance(cause, ConcurrentEditError)
    assert cause.code == "file_modified_externally"
    assert link.resolve() == target_b
    assert target_b.read_text(encoding="utf-8") == "other"


def test_commit_writes_backup_with_unique_exclusive_name(tmp_path: Path) -> None:
    path = tmp_path / "config.json"
    path.write_text("original", encoding="utf-8")
    transaction = FileTransaction()
    transaction.stage(path, b"ours", original_bytes=b"original")
    outcome = transaction.commit()

    assert outcome.backups
    backup = Path(outcome.backups[0])
    assert re.fullmatch(
        r"config\.json\.agentbus\.bak\.\d{8}T\d{6}\.\d+\.0", backup.name
    ), backup.name
    assert backup.read_text(encoding="utf-8") == "original"
    assert path.read_bytes() == b"ours"


def test_backup_name_gets_a_suffix_only_on_collision(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        discovery_module.time, "strftime", lambda *_args: "20251030T000000"
    )
    path = tmp_path / "config.json"
    path.write_text("original", encoding="utf-8")

    first = FileTransaction()
    first.stage(path, b"ours", original_bytes=b"original")
    first_outcome = first.commit()

    second = FileTransaction()
    second.stage(path, b"again", original_bytes=b"ours")
    second_outcome = second.commit()

    names = sorted(
        item.name for item in tmp_path.glob("config.json.agentbus.bak.*")
    )
    assert len(names) == 2
    first_name = Path(first_outcome.backups[0]).name
    second_name = Path(second_outcome.backups[0]).name
    assert first_name.endswith(".0")
    assert second_name == first_name[:-2] + ".1"


def test_second_commit_after_external_edit_is_idempotently_refused(
    tmp_path: Path,
) -> None:
    path = tmp_path / "config.json"
    path.write_text("original", encoding="utf-8")
    transaction = FileTransaction()
    transaction.stage(path, b"ours", original_bytes=b"original")
    transaction.commit()

    stale = FileTransaction()
    stale.stage(path, b"second", original_bytes=b"original")
    with pytest.raises(SetupWriteError) as excinfo:
        stale.commit()
    cause = excinfo.value.cause
    assert isinstance(cause, ConcurrentEditError)
    assert cause.code == "file_modified_externally"
    assert path.read_bytes() == b"ours"


# ---------------------------------------------------------------------------
# Conditional rollback across create / replace / delete / symlink
# ---------------------------------------------------------------------------


def _fail_after_first_write(monkeypatch: pytest.MonkeyPatch, failing: Path) -> None:
    real = discovery_module._atomic_write

    def fake(path: Path, data: bytes, mode: int) -> None:
        if path == failing:
            raise OSError("simulated second-write failure")
        real(path, data, mode)

    monkeypatch.setattr(discovery_module, "_atomic_write", fake)


def test_rollback_removes_a_file_this_transaction_created(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    created = tmp_path / "created.json"
    second = tmp_path / "second.json"
    second.write_text("second", encoding="utf-8")
    _fail_after_first_write(monkeypatch, second)

    transaction = FileTransaction()
    transaction.stage(created, b"file body", original_bytes=None)
    transaction.stage(second, b"next", original_bytes=b"second")

    with pytest.raises(SetupWriteError):
        transaction.commit()

    assert not created.exists()


def test_rollback_create_keeps_an_externally_modified_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    created = tmp_path / "created.json"
    second = tmp_path / "second.json"
    second.write_text("second", encoding="utf-8")

    real = discovery_module._atomic_write

    def fake(path: Path, data: bytes, mode: int) -> None:
        if path == second:
            # The user edits the file AgentBus just created, then the second
            # write fails and rollback has to decide what to do.
            created.write_text("user content", encoding="utf-8")
            raise OSError("simulated second-write failure")
        real(path, data, mode)

    monkeypatch.setattr(discovery_module, "_atomic_write", fake)

    transaction = FileTransaction()
    transaction.stage(created, b"file body", original_bytes=None)
    transaction.stage(second, b"next", original_bytes=b"second")

    with pytest.raises(SetupWriteError) as excinfo:
        transaction.commit()

    assert created.read_text(encoding="utf-8") == "user content"
    conflicts = excinfo.value.rollback_conflicts
    assert any("no longer matches" in conflict.args[0] for conflict in conflicts)


def test_rollback_restores_a_replaced_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    first = tmp_path / "first.json"
    first.write_text("original", encoding="utf-8")
    second = tmp_path / "second.json"
    second.write_text("second", encoding="utf-8")
    _fail_after_first_write(monkeypatch, second)

    transaction = FileTransaction()
    transaction.stage(first, b"ours", original_bytes=b"original")
    transaction.stage(second, b"next", original_bytes=b"second")

    with pytest.raises(SetupWriteError) as excinfo:
        transaction.commit()

    assert first.read_text(encoding="utf-8") == "original"
    assert excinfo.value.rollback_errors == ()
    assert excinfo.value.rollback_conflicts == ()
    assert list(tmp_path.glob("first.json.agentbus.bak.*"))


def test_rollback_delete_reports_a_conflict(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    first = tmp_path / "first.json"
    first.write_text("original", encoding="utf-8")
    second = tmp_path / "second.json"
    second.write_text("second", encoding="utf-8")

    real = discovery_module._atomic_write

    def fake(path: Path, data: bytes, mode: int) -> None:
        if path == second:
            first.unlink()
            raise OSError("simulated second-write failure")
        real(path, data, mode)

    monkeypatch.setattr(discovery_module, "_atomic_write", fake)

    transaction = FileTransaction()
    transaction.stage(first, b"ours", original_bytes=b"original")
    transaction.stage(second, b"next", original_bytes=b"second")

    with pytest.raises(SetupWriteError) as excinfo:
        transaction.commit()

    conflicts = excinfo.value.rollback_conflicts
    assert any(
        "deleted after AgentBus wrote" in conflict.args[0] for conflict in conflicts
    )
    assert not first.exists()


def test_rollback_symlink_repointed_reports_a_conflict(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "target.json"
    target.write_text("original", encoding="utf-8")
    other = tmp_path / "other.json"
    other.write_text("outside", encoding="utf-8")
    link = tmp_path / "link.json"
    link.symlink_to(target)
    second = tmp_path / "second.json"
    second.write_text("second", encoding="utf-8")

    real = discovery_module._atomic_write

    def fake(path: Path, data: bytes, mode: int) -> None:
        if os.fspath(path) == os.fspath(second):
            link.unlink()
            link.symlink_to(other)
            raise OSError("simulated second-write failure")
        real(path, data, mode)

    monkeypatch.setattr(discovery_module, "_atomic_write", fake)

    transaction = FileTransaction()
    transaction.stage(link, b"ours", original_bytes=b"original")
    transaction.stage(second, b"next", original_bytes=b"second")

    with pytest.raises(SetupWriteError) as excinfo:
        transaction.commit()

    assert link.resolve() == other
    assert other.read_text(encoding="utf-8") == "outside"
    conflicts = excinfo.value.rollback_conflicts
    assert any("symlink was repointed" in conflict.args[0] for conflict in conflicts)


def test_staged_bytes_are_digested_not_trusted(tmp_path: Path) -> None:
    path = tmp_path / "file.json"
    path.write_bytes(b"original")
    transaction = FileTransaction()
    transaction.stage(path, b"ours", original_bytes=b"original")
    staged = transaction._staged[0]
    assert staged.original_digest == hashlib.sha256(b"original").hexdigest()


# ---------------------------------------------------------------------------
# RBAC contract: conditional on an active configuration
# ---------------------------------------------------------------------------


def test_unconfigured_workspace_is_unaffected(tmp_path: Path) -> None:
    check_publish_rbac(
        tmp_path, producer_id="local-user", topic="okf/handoff", payload={}
    )


def test_mapped_producer_can_publish(tmp_path: Path) -> None:
    _write_roles(tmp_path)
    check_publish_rbac(tmp_path, producer_id="alice", topic="okf/handoff", payload={})


def test_unmapped_producer_is_forbidden_in_strict_mode(tmp_path: Path) -> None:
    _write_roles(tmp_path)
    with pytest.raises(ForbiddenError) as excinfo:
        check_publish_rbac(
            tmp_path, producer_id="local-user", topic="okf/handoff", payload={}
        )
    assert str(excinfo.value).startswith("403 Forbidden")


def test_token_role_authorizes_an_unmapped_producer(tmp_path: Path) -> None:
    _write_roles(tmp_path)
    check_publish_rbac(
        tmp_path,
        producer_id="local-user",
        topic="okf/handoff",
        payload={},
        auth_token="token-abc",
    )


def test_rbac_disable_switch_only_applies_when_identity_is_unprotected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from agentbus.identity import bootstrap_workspace_identity, set_policy_mode

    _write_roles(tmp_path)
    monkeypatch.setenv("AGENTBUS_DISABLE_RBAC", "1")
    # Unprotected mode: switch bypasses RBAC for unmapped producer
    check_publish_rbac(
        tmp_path, producer_id="local-user", topic="okf/handoff", payload={}
    )

    # Protected mode: switch is ignored, unmapped producer is rejected
    bootstrap_workspace_identity(tmp_path)
    set_policy_mode(tmp_path, "protected")
    with pytest.raises(ForbiddenError):
        check_publish_rbac(
            tmp_path, producer_id="local-user", topic="okf/handoff", payload={}
        )


def test_connect_does_not_grant_roles_while_adding_entries(
    tmp_path: Path,
) -> None:
    """Connecting a client must never rewrite the workspace RBAC file."""
    import json
    from agentbus.adapters.command import McpCommand
    from agentbus.discovery import connect

    workspace = tmp_path / "project"
    workspace.mkdir()
    home = tmp_path / "home"
    home.mkdir()
    roles = _write_roles(workspace)
    before = roles.read_bytes()

    # Pre-create a detectable client config so connect actually writes an entry
    qwen_cfg = workspace / ".qwen" / "settings.json"
    qwen_cfg.parent.mkdir(parents=True)
    qwen_cfg.write_text("{}\n", encoding="utf-8")

    report = connect(
        workspace=workspace,
        home=home,
        platform="linux",
        environ={"HOME": str(home)},
        command=McpCommand(("/usr/bin/agentbus", "mcp-serve"), "test"),
        which=lambda name: "/usr/bin/qwen" if name == "qwen" else None,
        apply=True,
        allow_user_scope=True,
    )
    assert report.exit_code == 0
    written = json.loads(qwen_cfg.read_text(encoding="utf-8"))
    assert "agentbus" in written.get("mcpServers", {})

    assert roles.read_bytes() == before
