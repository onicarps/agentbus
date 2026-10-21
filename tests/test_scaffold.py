"""Conflict-safe OKF scaffolding tests."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
from importlib import resources
from pathlib import Path

import pytest

from agentbus import scaffold as scaffold_module
from agentbus.distill import STATE_NAME, distill_workspace
from agentbus.scaffold import (
    MANIFEST_PATH,
    TEMPLATE_PATHS,
    WorkspaceError,
    ensure_runtime_ignored,
    scaffold_workspace,
)
from agentbus.store import EventStore

requires_git = pytest.mark.skipif(
    shutil.which("git") is None, reason="git is required for this test"
)


def _git(*args: str, cwd: Path) -> None:
    subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True)


def _publish_handoff(root: Path) -> int:
    store = EventStore(root)
    try:
        event, _duplicate = store.publish(
            topic="okf/handoff",
            producer_id="codex",
            schema_version="1.0",
            payload={
                "from": "codex",
                "to": "factory",
                "summary": "scaffold ownership probe",
                "initiative": "agentbus",
            },
            idempotency_key="scaffold-ownership",
        )
        return event.event_id
    finally:
        store.close()


def test_scaffold_creates_manifested_packaged_templates(tmp_path: Path) -> None:
    root = tmp_path / "project"
    root.mkdir()

    result = scaffold_workspace(root)

    assert result["created"] == [
        path.as_posix() for path in (*TEMPLATE_PATHS, MANIFEST_PATH)
    ]
    assert result["project_name"] == "project"
    manifest = json.loads((root / MANIFEST_PATH).read_text(encoding="utf-8"))
    assert manifest["schema_version"] == 1
    assert manifest["template_version"] == "1"
    assert manifest["project_name"] == "project"
    for relative in TEMPLATE_PATHS:
        text = (root / relative).read_text(encoding="utf-8")
        assert "$PROJECT_NAME" not in text
        assert "$GENERATED_AT" not in text
        assert manifest["files"][relative.as_posix()]["sha256"] == hashlib.sha256(
            text.encode("utf-8")
        ).hexdigest()


def test_scaffold_records_explicit_project_name(tmp_path: Path) -> None:
    root = tmp_path / "dir-name"
    root.mkdir()

    result = scaffold_workspace(root, project_name="display-name")

    assert result["project_name"] == "display-name"
    assert "display-name" in (root / "okf" / "index.md").read_text(encoding="utf-8")
    assert (
        json.loads((root / MANIFEST_PATH).read_text(encoding="utf-8"))["project_name"]
        == "display-name"
    )


def test_scaffold_retains_recorded_project_name_on_rerun(tmp_path: Path) -> None:
    """A re-run must keep the manifest's project name, ignoring a new --project-name."""
    root = tmp_path / "dir-name"
    root.mkdir()
    scaffold_workspace(root, project_name="first-name")

    result = scaffold_workspace(root, project_name="second-name")

    assert result["project_name"] == "first-name"
    assert result["created"] == []
    assert result["conflicts"] == []
    assert (
        json.loads((root / MANIFEST_PATH).read_text(encoding="utf-8"))["project_name"]
        == "first-name"
    )
    index = (root / "okf" / "index.md").read_text(encoding="utf-8")
    assert "first-name" in index
    assert "second-name" not in index


def test_scaffold_recovers_a_manifest_less_partial_commit(tmp_path: Path) -> None:
    """A crash before the manifest lands must finish, not conflict on every file."""
    root = tmp_path / "project"
    root.mkdir()
    scaffold_workspace(root, generated_at="2026-10-21T00:00:00Z")
    before = {path: (root / path).read_bytes() for path in TEMPLATE_PATHS}
    # Simulate the crash window: templates committed, manifest never written.
    (root / MANIFEST_PATH).unlink()

    recovered = scaffold_workspace(root)

    assert recovered["generated_at"] == "2026-10-21T00:00:00Z"
    assert recovered["conflicts"] == []
    assert recovered["created"] == [MANIFEST_PATH.as_posix()]
    assert recovered["unchanged"] == [path.as_posix() for path in TEMPLATE_PATHS]
    assert {path: (root / path).read_bytes() for path in TEMPLATE_PATHS} == before
    assert (root / MANIFEST_PATH).is_file()


def test_scaffold_partial_commit_recovery_stays_a_conflict_when_stamp_is_ambiguous(
    tmp_path: Path,
) -> None:
    """Two disagreeing stamps cannot be recovered, so the strict refusal remains."""
    root = tmp_path / "project"
    root.mkdir()
    scaffold_workspace(root, generated_at="2026-10-21T00:00:00Z")
    (root / MANIFEST_PATH).unlink()
    (root / "okf" / "status.md").write_text(
        "---\ntype: Initiative Status\ngenerated:\n"
        "  by: agentbus/scaffold-v1\n  at: 2027-01-01T00:00:00Z\n---\n",
        encoding="utf-8",
    )

    with pytest.raises(WorkspaceError) as excinfo:
        scaffold_workspace(root)

    assert excinfo.value.code == "scaffold_conflict"


def test_scaffold_rejects_blank_project_name(tmp_path: Path) -> None:
    root = tmp_path / "project"
    root.mkdir()

    with pytest.raises(WorkspaceError) as excinfo:
        scaffold_workspace(root, project_name="  ")

    assert excinfo.value.code == "invalid_scaffold_project_name"
    assert not (root / "AGENTS.md").exists()


def test_scaffold_second_run_is_byte_identical(tmp_path: Path) -> None:
    root = tmp_path / "project"
    root.mkdir()
    scaffold_workspace(root)
    before = {path: (root / path).read_bytes() for path in (*TEMPLATE_PATHS, MANIFEST_PATH)}

    result = scaffold_workspace(root)

    assert result["created"] == []
    assert result["unchanged"] == [
        path.as_posix() for path in (*TEMPLATE_PATHS, MANIFEST_PATH)
    ]
    assert before == {path: (root / path).read_bytes() for path in before}


def test_scaffold_conflict_aborts_before_creating_any_template(tmp_path: Path) -> None:
    root = tmp_path / "project"
    root.mkdir()
    (root / "okf").mkdir()
    (root / "okf" / "status.md").write_text("team-owned\n", encoding="utf-8")

    with pytest.raises(WorkspaceError) as excinfo:
        scaffold_workspace(root)

    assert excinfo.value.code == "scaffold_conflict"
    assert (root / "okf" / "status.md").read_text(encoding="utf-8") == "team-owned\n"
    assert not (root / "okf" / "index.md").exists()
    assert not (root / MANIFEST_PATH).exists()


def test_scaffold_preserves_pre_existing_root_agents_md(tmp_path: Path) -> None:
    root = tmp_path / "project"
    root.mkdir()
    (root / "AGENTS.md").write_text("custom team runbook\n", encoding="utf-8")

    result = scaffold_workspace(root)
    assert result["preserved"] == ["AGENTS.md"]
    assert "AGENTS.md" not in result["created"]
    assert (root / "AGENTS.md").read_text(encoding="utf-8") == "custom team runbook\n"
    assert (root / "okf" / "index.md").is_file()
    assert (root / "okf" / "status.md").is_file()
    assert (root / MANIFEST_PATH).is_file()

    # Re-running scaffold must continue to report AGENTS.md as preserved, not modified or conflict
    again = scaffold_workspace(root)
    assert again["preserved"] == ["AGENTS.md"]
    assert again["modified"] == []
    assert (root / "AGENTS.md").read_text(encoding="utf-8") == "custom team runbook\n"


def test_scaffold_preserves_then_deletes_then_rescaffolds_agents_md(tmp_path: Path) -> None:
    root = tmp_path / "project"
    root.mkdir()
    (root / "AGENTS.md").write_text("pre-existing runbook\n", encoding="utf-8")

    # First run: preserves pre-existing AGENTS.md
    res1 = scaffold_workspace(root)
    assert res1["preserved"] == ["AGENTS.md"]
    assert (root / "AGENTS.md").read_text(encoding="utf-8") == "pre-existing runbook\n"

    # User subsequently deletes AGENTS.md
    (root / "AGENTS.md").unlink()

    # Re-running scaffold must create AGENTS.md and update manifest without conflict
    res2 = scaffold_workspace(root)
    assert "AGENTS.md" in res2["created"]
    assert (root / "AGENTS.md").is_file()
    assert (root / MANIFEST_PATH).is_file()

    # Third run is now cleanly unchanged
    res3 = scaffold_workspace(root)
    assert res3["created"] == []
    assert "AGENTS.md" in res3["unchanged"]


def test_scaffold_preserves_user_edits_when_manifest_exists(tmp_path: Path) -> None:
    root = tmp_path / "project"
    root.mkdir()
    scaffold_workspace(root)
    (root / "okf" / "status.md").write_text("edited by operator\n", encoding="utf-8")

    result = scaffold_workspace(root)
    assert result["modified"] == ["okf/status.md"]
    assert (root / "okf" / "status.md").read_text(encoding="utf-8") == "edited by operator\n"


def test_scaffold_conflict_on_first_run_without_manifest(tmp_path: Path) -> None:
    root = tmp_path / "project"
    root.mkdir()
    (root / "okf").mkdir()
    (root / "okf" / "status.md").write_text("pre-existing collision\n", encoding="utf-8")

    with pytest.raises(WorkspaceError) as excinfo:
        scaffold_workspace(root)

    assert excinfo.value.code == "scaffold_conflict"
    assert (root / "okf" / "status.md").read_text(encoding="utf-8") == "pre-existing collision\n"


def test_scaffold_preserves_a_distilled_log_index(tmp_path: Path) -> None:
    root = tmp_path / "project"
    root.mkdir()
    scaffold_workspace(root)
    _publish_handoff(root)
    distill_workspace(root)
    distilled = (root / "okf" / "log.md").read_text(encoding="utf-8")
    assert "type: Handoff Log" in distilled
    assert "agentbus/distill-v1" in distilled
    assert (root / ".agentbus" / STATE_NAME).is_file()

    result = scaffold_workspace(root)

    assert result["managed"] == ["okf/log.md"]
    assert result["conflicts"] == []
    assert (root / "okf" / "log.md").read_text(encoding="utf-8") == distilled


def test_scaffold_conflicts_on_an_unowned_distilled_log(tmp_path: Path) -> None:
    """A hand-written log that merely looks distilled must not be adopted."""
    root = tmp_path / "project"
    root.mkdir()
    scaffold_workspace(root)
    forged = (
        "---\ntype: Handoff Log\ntitle: handmade\n---\n\n"
        "# Recent handoffs\n\n- hand written entry\n"
    )
    (root / "okf" / "log.md").write_text(forged, encoding="utf-8")

    with pytest.raises(WorkspaceError) as excinfo:
        scaffold_workspace(root)

    assert excinfo.value.code == "scaffold_conflict"
    assert (root / "okf" / "log.md").read_text(encoding="utf-8") == forged


def test_scaffold_conflicts_when_distill_state_is_corrupt(tmp_path: Path) -> None:
    """Distiller-managed content without trustworthy state stays a conflict."""
    root = tmp_path / "project"
    root.mkdir()
    scaffold_workspace(root)
    _publish_handoff(root)
    distill_workspace(root)
    (root / ".agentbus" / STATE_NAME).write_text("{not json", encoding="utf-8")

    with pytest.raises(WorkspaceError) as excinfo:
        scaffold_workspace(root)

    assert excinfo.value.code == "scaffold_conflict"


def test_scaffold_cleans_staged_temporaries_when_a_later_stage_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "project"
    root.mkdir()
    real_stage_write = scaffold_module.stage_write
    calls = {"count": 0}

    def flaky_stage_write(target: Path, text: str) -> Path:
        calls["count"] += 1
        if calls["count"] == 3:
            raise RuntimeError("injected stage failure")
        return real_stage_write(target, text)

    monkeypatch.setattr(scaffold_module, "stage_write", flaky_stage_write)

    with pytest.raises(RuntimeError, match="injected stage failure"):
        scaffold_workspace(root)

    assert calls["count"] == 3
    assert list(root.rglob("*.tmp")) == []
    assert not (root / "AGENTS.md").exists()
    assert not (root / MANIFEST_PATH).exists()

    monkeypatch.undo()
    recovered = scaffold_workspace(root)

    assert recovered["created"] == [
        path.as_posix() for path in (*TEMPLATE_PATHS, MANIFEST_PATH)
    ]
    assert list(root.rglob("*.tmp")) == []


@requires_git
def test_scaffold_survives_a_platform_without_os_fchmod(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Windows Python 3.11/3.12 has no ``os.fchmod``; staging must still work."""
    root = tmp_path / "repo"
    root.mkdir()
    monkeypatch.delattr(os, "fchmod", raising=False)

    result = scaffold_workspace(root)

    assert result["created"] == [
        path.as_posix() for path in (*TEMPLATE_PATHS, MANIFEST_PATH)
    ]
    _git("init", cwd=root)
    ignore = ensure_runtime_ignored(root)

    assert ignore["applied"] is True
    assert ".agentbus/" in (root / ".gitignore").read_text(encoding="utf-8")


def test_scaffold_check_only_writes_nothing(tmp_path: Path) -> None:
    root = tmp_path / "project"
    root.mkdir()

    result = scaffold_workspace(root, check_only=True)

    assert result["check_only"] is True
    assert result["created"] == [
        path.as_posix() for path in (*TEMPLATE_PATHS, MANIFEST_PATH)
    ]
    assert result["unchanged"] == []
    assert result["managed"] == []
    assert result["conflicts"] == []
    assert list(root.iterdir()) == []


def test_scaffold_refuses_symlink_target(tmp_path: Path) -> None:
    root = tmp_path / "project"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.write_text("untouched\n", encoding="utf-8")
    (root / "AGENTS.md").symlink_to(outside)

    with pytest.raises(WorkspaceError) as excinfo:
        scaffold_workspace(root)

    assert excinfo.value.code == "unsafe_scaffold_path"
    assert outside.read_text(encoding="utf-8") == "untouched\n"


def test_scaffold_refuses_symlinked_directory(tmp_path: Path) -> None:
    root = tmp_path / "project"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (root / "okf").symlink_to(outside, target_is_directory=True)

    with pytest.raises(WorkspaceError) as excinfo:
        scaffold_workspace(root)

    assert excinfo.value.code == "unsafe_scaffold_path"
    assert list(outside.iterdir()) == []


def test_scaffold_reports_a_non_directory_parent_as_a_conflict(tmp_path: Path) -> None:
    root = tmp_path / "project"
    root.mkdir()
    (root / "okf").write_text("not a directory\n", encoding="utf-8")

    with pytest.raises(WorkspaceError) as excinfo:
        scaffold_workspace(root)

    assert excinfo.value.code == "scaffold_conflict"
    assert not (root / "AGENTS.md").exists()


def test_scaffold_rejects_unsupported_template_version(tmp_path: Path) -> None:
    root = tmp_path / "project"
    root.mkdir()
    (root / "okf").mkdir()
    (root / MANIFEST_PATH).write_text(
        json.dumps(
            {
                "schema_version": 1,
                "template_version": "0",
                "project_name": "project",
                "generated_at": "2026-10-21T00:00:00Z",
                "files": {},
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(WorkspaceError) as excinfo:
        scaffold_workspace(root)

    assert excinfo.value.code == "scaffold_upgrade_required"
    assert excinfo.value.message == "scaffold version '0' requires an explicit upgrade"
    assert not (root / "AGENTS.md").exists()
    assert not (root / "okf" / "index.md").exists()


def test_scaffold_requires_an_existing_workspace(tmp_path: Path) -> None:
    with pytest.raises(WorkspaceError) as excinfo:
        scaffold_workspace(tmp_path / "missing")

    assert excinfo.value.code == "scaffold_workspace_missing"


def test_template_assets_are_importable_package_resources() -> None:
    root = resources.files("agentbus").joinpath("templates").joinpath("okf")
    for relative in TEMPLATE_PATHS:
        node = root
        for part in relative.parts:
            node = node.joinpath(part)
        assert node.is_file(), relative


@requires_git
def test_ensure_runtime_ignored_appends_once(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    root.mkdir()
    _git("init", cwd=root)
    (root / ".gitignore").write_text("dist/\n", encoding="utf-8")

    first = ensure_runtime_ignored(root)

    assert first["applied"] is True
    assert first["scope"] == "gitignore"
    assert (root / ".gitignore").read_text(encoding="utf-8") == "dist/\n.agentbus/\n"

    second = ensure_runtime_ignored(root)

    assert second["applied"] is False
    assert (root / ".gitignore").read_text(encoding="utf-8") == "dist/\n.agentbus/\n"


@requires_git
def test_ensure_runtime_ignored_recognizes_existing_variants(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    root.mkdir()
    _git("init", cwd=root)
    (root / ".gitignore").write_text(".agentbus\n", encoding="utf-8")

    result = ensure_runtime_ignored(root)

    assert result["applied"] is False
    assert (root / ".gitignore").read_text(encoding="utf-8") == ".agentbus\n"


@requires_git
def test_ensure_runtime_ignored_bare_uses_git_exclude(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    root.mkdir()
    _git("init", cwd=root)

    result = ensure_runtime_ignored(root, bare=True)

    assert result["applied"] is True
    assert result["scope"] == "git-info-exclude"
    exclude = Path(result["path"])
    assert exclude.is_file()
    assert ".agentbus/" in exclude.read_text(encoding="utf-8")


def test_ensure_runtime_ignored_outside_git_is_a_noop(tmp_path: Path) -> None:
    root = tmp_path / "plain"
    root.mkdir()

    result = ensure_runtime_ignored(root)

    assert result == {"applied": False, "path": None, "scope": None}
    assert not (root / ".gitignore").exists()


@requires_git
def test_ensure_runtime_ignored_refuses_symlinked_gitignore(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    root.mkdir()
    _git("init", cwd=root)
    outside = tmp_path / "outside-ignore"
    outside.write_text("untouched\n", encoding="utf-8")
    (root / ".gitignore").symlink_to(outside)

    with pytest.raises(WorkspaceError) as excinfo:
        ensure_runtime_ignored(root)

    assert excinfo.value.code == "unsafe_ignore_path"
    assert outside.read_text(encoding="utf-8") == "untouched\n"


@requires_git
def test_ensure_runtime_ignored_refuses_symlinked_exclude(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    root.mkdir()
    _git("init", cwd=root)
    outside = tmp_path / "outside-exclude"
    outside.write_text("untouched\n", encoding="utf-8")
    exclude = root / ".git" / "info" / "exclude"
    exclude.parent.mkdir(parents=True, exist_ok=True)
    exclude.unlink(missing_ok=True)
    exclude.symlink_to(outside)

    with pytest.raises(WorkspaceError) as excinfo:
        ensure_runtime_ignored(root, bare=True)

    assert excinfo.value.code == "unsafe_ignore_path"
    assert outside.read_text(encoding="utf-8") == "untouched\n"


def test_scaffold_rejects_manifest_with_invalid_files_field(tmp_path: Path) -> None:
    root = tmp_path / "project"
    root.mkdir()
    manifest_file = root / MANIFEST_PATH
    manifest_file.parent.mkdir(parents=True, exist_ok=True)
    manifest_file.write_text(
        json.dumps({
            "schema_version": 1,
            "template_version": "1",
            "project_name": "test",
            "generated_at": "2026-10-21T00:00:00Z",
            "files": "not a dict",
        }),
        encoding="utf-8",
    )

    with pytest.raises(WorkspaceError) as excinfo:
        scaffold_workspace(root)

    assert excinfo.value.code == "invalid_scaffold_manifest"


def test_scaffold_preserves_distilled_log_in_fresh_clone_without_agentbus_state(tmp_path: Path) -> None:
    root = tmp_path / "project"
    root.mkdir()
    scaffold_workspace(root)
    _publish_handoff(root)
    distill_workspace(root)

    # Simulate fresh clone on another machine: remove .agentbus/ completely
    shutil.rmtree(root / ".agentbus")

    # scaffold_workspace must recognize the committed distilled log via self-digest
    result = scaffold_workspace(root)
    assert "okf/log.md" in result["managed"]
    assert result["conflicts"] == []


