"""Regression tests for the 2026-09-06 lab-eval DX pack (F1, F2, F3, F5, F7).

Covers the first-ten-minutes frictions surfaced by the two-agent container
lab: lease-path error hints (F1), topic-registration hints + topic-list (F2),
HITL approver-coverage doctor check and actionable 403s (F3), await minutes
granularity (F5), and the root-owned-workspace doctor warning (F7).
"""

from __future__ import annotations

import json
from pathlib import Path

import yaml
from click.testing import CliRunner

from agentbus.cli import main as cli_main
from agentbus.doctor import check_hitl_reviewers, check_workspace
from agentbus.leases import normalize_resource
from agentbus.rbac import (
    ForbiddenError,
    RoleDef,
    RbacConfig,
    check_approve_rbac,
)
from agentbus.runner.wait_store import (
    DEFAULT_TIMEOUT_HOURS,
    MAX_TIMEOUT_HOURS,
    timeout_hours_from,
)
from agentbus.schemas import validate_topic


# ---------------------------------------------------------------------------
# F1 — lease resource path errors say where resources may live
# ---------------------------------------------------------------------------


def test_lease_resource_outside_workspace_error_names_workspace(tmp_path):
    outside = tmp_path.parent / "elsewhere" / "shared-file"
    assert not outside.resolve().is_relative_to(tmp_path.resolve())
    try:
        normalize_resource(tmp_path, str(outside))
        raise AssertionError("expected resource_outside_workspace")
    except ValueError as exc:
        message = str(exc)
        assert "resource_outside_workspace" in message
        assert str(tmp_path.resolve()) in message
        assert "inside the workspace" in message


def test_lease_resource_inside_workspace_still_accepted(tmp_path):
    inner = tmp_path / "shared" / "file.txt"
    resolved = normalize_resource(tmp_path, str(inner))
    assert resolved == str(inner.resolve())


# ---------------------------------------------------------------------------
# F2 — unknown topics point at the exact registration command
# ---------------------------------------------------------------------------


def test_unknown_topic_error_includes_register_hint(tmp_path):
    try:
        validate_topic("lab/custom", workspace=tmp_path)
        raise AssertionError("expected unknown_topic")
    except ValueError as exc:
        message = str(exc)
        assert "unknown_topic: lab/custom" in message
        assert "agentbus schema register --topic lab/custom" in message
        assert str(tmp_path) in message


def test_unknown_topic_error_without_workspace_still_hints():
    try:
        validate_topic("lab/custom", workspace=None)
        raise AssertionError("expected unknown_topic")
    except ValueError as exc:
        assert "agentbus schema register" in str(exc)


def test_topic_list_lists_builtins_and_hint(tmp_path):
    (tmp_path / ".agentbus").mkdir()
    runner = CliRunner()
    result = runner.invoke(
        cli_main, ["topic-list", "--workspace", str(tmp_path)]
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert "okf/handoff" in payload["builtin"]
    assert "schema register" in payload["hint"]


# ---------------------------------------------------------------------------
# F3 — approver coverage: actionable 403s and a doctor check
# ---------------------------------------------------------------------------


def _write_roles(tmp_path: Path, config: dict) -> None:
    roles_dir = tmp_path / ".agentbus"
    roles_dir.mkdir(exist_ok=True)
    (roles_dir / "roles.yaml").write_text(yaml.safe_dump(config))


def test_reviewer_denial_names_roles_yaml_fix(tmp_path):
    _write_roles(
        tmp_path,
        {"roles": {"engineer": {"can_publish_topics": ["okf/handoff"]}},
         "producers": {"human": "engineer"}},
    )
    try:
        check_approve_rbac(tmp_path, reviewer_id="human")
        raise AssertionError("expected ForbiddenError")
    except ForbiddenError as exc:
        assert "can_approve" in str(exc)
        assert "roles.yaml" in str(exc)


def test_reviewer_without_any_role_names_roles_yaml_fix(tmp_path):
    _write_roles(
        tmp_path,
        {"roles": {"engineer": {"can_publish_topics": ["okf/handoff"]}},
         "producers": {}},
    )
    try:
        check_approve_rbac(tmp_path, reviewer_id="human")
        raise AssertionError("expected ForbiddenError")
    except ForbiddenError as exc:
        assert "no RBAC role for reviewer 'human'" in str(exc)
        assert "roles.yaml" in str(exc)


def test_reviewer_with_approver_role_passes(tmp_path):
    _write_roles(
        tmp_path,
        {"roles": {"approver": {"can_approve": True, "can_publish_topics": ["okf/handoff"]}},
         "producers": {"human": "approver"}},
    )
    check_approve_rbac(tmp_path, reviewer_id="human")  # must not raise


def test_doctor_hitl_reviewers_warns_without_approver(tmp_path):
    _write_roles(
        tmp_path,
        {"roles": {"engineer": {"can_publish_topics": ["okf/handoff"]}},
         "producers": {"codex": "engineer"}},
    )
    check = check_hitl_reviewers(tmp_path)
    assert check.status == "WARN"
    assert "can_approve" in check.message
    assert check.details == {"approving_roles": []}


def test_doctor_hitl_reviewers_ok_with_approver(tmp_path):
    _write_roles(
        tmp_path,
        {"roles": {"approver": {"can_approve": True}},
         "producers": {"human": "approver"}},
    )
    check = check_hitl_reviewers(tmp_path)
    assert check.status == "OK"
    assert "approver" in check.message


def test_doctor_hitl_reviewers_ok_without_rbac(tmp_path):
    check = check_hitl_reviewers(tmp_path)
    assert check.status == "OK"


def test_doctor_hitl_reviewers_ok_when_role_exists_but_unused(tmp_path):
    config = RbacConfig(
        roles={"approver": RoleDef(can_approve=True)},
        producers={},
    )
    assert any(role.can_approve for role in config.roles.values())


# ---------------------------------------------------------------------------
# F5 — await timeout granularity: minutes supported, fractional hours honored
# ---------------------------------------------------------------------------


def test_timeout_minutes_take_precedence_over_hours():
    assert timeout_hours_from(minutes=30, hours=4) == 0.5
    assert timeout_hours_from(minutes=90, hours=1) == 1.5


def test_timeout_fractional_hours_honored():
    assert timeout_hours_from(minutes=None, hours=0.25) == 0.25


def test_timeout_invalid_falls_back_then_default():
    assert timeout_hours_from(minutes=None, hours=None) == float(DEFAULT_TIMEOUT_HOURS)
    assert timeout_hours_from(minutes=0, hours=-1) == float(DEFAULT_TIMEOUT_HOURS)
    assert timeout_hours_from(minutes=float("nan"), hours=None) == float(
        DEFAULT_TIMEOUT_HOURS
    )


def test_timeout_clamped_to_max():
    assert timeout_hours_from(minutes=24 * 60 + 1, hours=None) == float(MAX_TIMEOUT_HOURS)


def test_await_cli_timeout_minutes_writes_fractional_hours(tmp_path):
    (tmp_path / ".agentbus").mkdir()
    runner = CliRunner()
    result = runner.invoke(
        cli_main,
        [
            "await", "--workspace", str(tmp_path),
            "--event-id", "1",
            "--expect-from", "codex",
            "--timeout-minutes", "45",
        ],
        obj={},
    )
    assert result.exit_code == 75, result.output
    drop = json.loads(result.output)
    assert drop["timeout_hours"] == 0.75


# ---------------------------------------------------------------------------
# Cold start — `init --apply` creates a missing workspace directory
# (lab finding: bootstrap demanded a pre-existing directory)
# ---------------------------------------------------------------------------


def test_init_apply_creates_missing_workspace_directory(tmp_path):
    target = tmp_path / "fresh" / "ws"
    runner = CliRunner()
    result = runner.invoke(
        cli_main,
        ["init", "--workspace", str(target), "--producer-id", "codex", "--apply"],
        obj={},
    )
    assert result.exit_code == 0, result.output
    assert target.is_dir()
    assert (target / ".agentbus" / "workspace").is_file()


def test_non_init_commands_still_reject_missing_workspace(tmp_path):
    target = tmp_path / "does-not-exist"
    runner = CliRunner()
    result = runner.invoke(
        cli_main,
        [
            "publish", "--workspace", str(target), "--producer-id", "codex",
            "--topic", "okf/handoff",
            "--payload", '{"from":"codex","to":"x","summary":"s"}',
        ],
        obj={},
    )
    assert result.exit_code != 0
    combined = result.output + "\n" + (result.stderr or "")
    assert "Workspace not found" in combined


# ---------------------------------------------------------------------------
# F7 — doctor warns on root-owned / other-owned non-writable workspaces
# ---------------------------------------------------------------------------


def test_doctor_workspace_warns_when_owned_by_other_uid(tmp_path, monkeypatch):
    monkeypatch.setattr("agentbus.doctor.os.geteuid", lambda: 12345)
    check = check_workspace(tmp_path)
    assert check.status == "WARN"
    assert "chown" in check.message
    assert check.details and check.details["uid"] == tmp_path.stat().st_uid


def test_doctor_workspace_ok_for_owner(tmp_path):
    check = check_workspace(tmp_path)
    assert check.status == "OK"


def test_doctor_workspace_ok_when_group_writable(tmp_path, monkeypatch):
    import os
    import stat as stat_module

    monkeypatch.setattr("agentbus.doctor.os.geteuid", lambda: 12345)
    os.chmod(tmp_path, 0o775)
    check = check_workspace(tmp_path)
    mode = tmp_path.stat().st_mode & 0o777
    if mode & stat_module.S_IWGRP or mode & stat_module.S_IWOTH:
        assert check.status == "OK"
    else:  # umask stripped the group-write bit; warning is then correct
        assert check.status == "WARN"
