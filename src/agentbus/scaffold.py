"""Conflict-safe, standard-library scaffolding for repository-local OKF planes.

AgentBus keeps runtime coordination state in ``.agentbus/`` (untracked) and durable
project knowledge in ``okf/`` (reviewed through Git). ``scaffold_workspace`` renders the
packaged OKF v0.2 templates into the workspace exactly once, records a SHA-256 manifest,
and refuses to overwrite any file a human has since edited.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import re
import subprocess
import tempfile
from datetime import datetime, timezone
from importlib import resources
from pathlib import Path
from string import Template
from typing import Any

SCAFFOLD_VERSION = "1"
MANIFEST_PATH = Path("okf/.scaffold-manifest.json")
STATE_DIRECTORY = ".agentbus"
TEMPLATE_PATHS = (
    Path("AGENTS.md"),
    Path("okf/index.md"),
    Path("okf/status.md"),
    Path("okf/intent.yaml"),
    Path("okf/log.md"),
    Path("okf/harness/runners.toml"),
    Path("okf/harness/droid.md"),
)
# The bounded handoff index is written by the distiller once handoffs exist. A
# distilled index legitimately differs from the scaffold placeholder, so a re-run
# must leave it alone instead of reporting a conflict and refusing to bootstrap.
# Ownership is never inferred from content alone; see ``_distilled_log_owned``.
DISTILL_MANAGED_PATHS = (Path("okf/log.md"),)
# Pre-existing user-owned root files that commonly exist in AI/agent repos prior
# to AgentBus init; never overwrite them, never block workspace bootstrapping.
USER_PRESERVED_PATHS = (Path("AGENTS.md"),)

__all__ = [
    "DISTILL_MANAGED_PATHS",
    "MANIFEST_PATH",
    "SCAFFOLD_VERSION",
    "STATE_DIRECTORY",
    "TEMPLATE_PATHS",
    "USER_PRESERVED_PATHS",
    "WorkspaceError",
    "ensure_runtime_ignored",
    "fsync_directory",
    "scaffold_workspace",
    "set_fd_mode",
    "stage_write",
    "utc_now",
]


class WorkspaceError(RuntimeError):
    """An actionable, user-facing workspace or lifecycle error."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message

    def to_dict(self) -> dict[str, Any]:
        return {"code": self.code, "message": self.message, "detail": {}}


def utc_now() -> str:
    """AgentBus canonical UTC timestamp (second precision, trailing ``Z``)."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _template_root():
    node = resources.files("agentbus")
    for part in ("templates", "okf"):
        node = node.joinpath(part)
    return node


def _template_text(relative: Path) -> str:
    node = _template_root()
    for part in relative.parts:
        node = node.joinpath(part)
    try:
        return node.read_text(encoding="utf-8")
    except (FileNotFoundError, OSError) as exc:
        raise WorkspaceError(
            "scaffold_template_missing",
            f"packaged scaffold template is unavailable: {relative}",
        ) from exc


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _load_manifest(path: Path) -> dict[str, Any] | None:
    if not path.exists() and not path.is_symlink():
        return None
    if path.is_symlink() or not path.is_file():
        raise WorkspaceError(
            "unsafe_scaffold_path", f"refusing scaffold manifest path: {path}"
        )
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise WorkspaceError(
            "invalid_scaffold_manifest", f"cannot read {path}: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise WorkspaceError(
            "invalid_scaffold_manifest", f"manifest must contain an object: {path}"
        )
    if value.get("template_version") != SCAFFOLD_VERSION:
        raise WorkspaceError(
            "scaffold_upgrade_required",
            f"scaffold version {value.get('template_version')!r} requires an explicit upgrade",
        )
    if not isinstance(value.get("generated_at"), str) or not value["generated_at"].strip():
        raise WorkspaceError(
            "invalid_scaffold_manifest", "manifest generated_at must be a string"
        )
    if not isinstance(value.get("project_name"), str) or not value["project_name"].strip():
        raise WorkspaceError(
            "invalid_scaffold_manifest", "manifest project_name must be a string"
        )
    files = value.get("files", {})
    if not isinstance(files, dict) or not all(
        isinstance(entry, dict) for entry in files.values()
    ):
        raise WorkspaceError(
            "invalid_scaffold_manifest", "manifest files must map paths to objects"
        )
    return value


def _render_templates(*, project_name: str, generated_at: str) -> dict[Path, str]:
    context = {
        "PROJECT_NAME": project_name,
        "PROJECT_NAME_YAML": json.dumps(project_name, ensure_ascii=False),
        "AGENT_TITLE_YAML": json.dumps(
            f"Agent instructions for {project_name}", ensure_ascii=False
        ),
        "INDEX_TITLE_YAML": json.dumps(
            f"{project_name} knowledge index", ensure_ascii=False
        ),
        "STATUS_TITLE_YAML": json.dumps(f"{project_name} status", ensure_ascii=False),
        "STATUS_DESCRIPTION_YAML": json.dumps(
            f"Canonical repository-local status for {project_name}.", ensure_ascii=False
        ),
        "LOG_TITLE_YAML": json.dumps(
            f"{project_name} recent handoffs", ensure_ascii=False
        ),
        "DROID_TITLE_YAML": json.dumps(
            f"Droid mission dispatch for {project_name}", ensure_ascii=False
        ),
        "GENERATED_AT": generated_at,
    }
    rendered: dict[Path, str] = {}
    for relative in TEMPLATE_PATHS:
        try:
            text = Template(_template_text(relative)).substitute(context)
        except (KeyError, ValueError) as exc:
            raise WorkspaceError(
                "invalid_scaffold_template",
                f"invalid placeholder in template {relative}: {exc}",
            ) from exc
        rendered[relative] = text if text.endswith("\n") else text + "\n"
    return rendered


def _manifest_text(
    *, project_name: str, generated_at: str, rendered: dict[Path, str]
) -> str:
    value = {
        "schema_version": 1,
        "template_version": SCAFFOLD_VERSION,
        "project_name": project_name,
        "generated_at": generated_at,
        "files": {
            path.as_posix(): {"sha256": _sha256(text)}
            for path, text in sorted(rendered.items(), key=lambda item: item[0].as_posix())
        },
    }
    return json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n"


def _manifest_only_adds(
    old: dict[str, Any], new: dict[str, Any], created: list[str]
) -> bool:
    old_files = old.get("files", {})
    new_files = new.get("files", {})
    if any(new_files.get(k) != v for k, v in old_files.items()):
        return False
    return all(k in created for k in new_files.keys() - old_files.keys())


def _assert_safe_target(root: Path, target: Path) -> None:
    try:
        relative = target.relative_to(root)
    except ValueError as exc:
        raise WorkspaceError(
            "unsafe_scaffold_path", f"scaffold target escapes workspace: {target}"
        ) from exc
    current = root
    for part in relative.parts[:-1]:
        current = current / part
        if current.is_symlink():
            raise WorkspaceError(
                "unsafe_scaffold_path",
                f"refusing symlinked scaffold directory: {current}",
            )
        if current.exists() and not current.is_dir():
            raise WorkspaceError(
                "scaffold_conflict", f"scaffold parent is not a directory: {current}"
            )
    if target.is_symlink():
        raise WorkspaceError(
            "unsafe_scaffold_path", f"refusing symlinked scaffold file: {target}"
        )


def _unlink_quiet(path: str | Path) -> None:
    try:
        os.unlink(path)
    except OSError:
        pass


def set_fd_mode(descriptor: int, mode: int) -> None:
    """Best-effort permission change on an open descriptor.

    ``os.fchmod`` is POSIX-only and absent on Windows for Python 3.11/3.12, where
    files inherit their ACL from the parent directory instead. Callers must not
    depend on the mode being applied.
    """
    fchmod = getattr(os, "fchmod", None)
    if fchmod is None:
        return
    fchmod(descriptor, mode)


def stage_write(target: Path, text: str) -> Path:
    """Write ``text`` to a temporary sibling of ``target`` and fsync it."""
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.parent.is_symlink():
        raise WorkspaceError(
            "unsafe_scaffold_path", f"refusing symlinked scaffold path: {target}"
        )
    fd, temporary = tempfile.mkstemp(
        prefix=f".{target.name}.", suffix=".tmp", dir=target.parent
    )
    owned = False
    try:
        set_fd_mode(fd, 0o644)
        stream = os.fdopen(fd, "w", encoding="utf-8", closefd=True)
        owned = True
        with stream:
            stream.write(text)
            stream.flush()
            os.fsync(stream.fileno())
    except Exception:
        if not owned:
            with contextlib.suppress(OSError):
                os.close(fd)
        _unlink_quiet(temporary)
        raise
    return Path(temporary)


def fsync_directory(path: Path) -> None:
    try:
        directory_fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(directory_fd)
    except OSError:
        pass
    finally:
        os.close(directory_fd)


def _distilled_log_owned(root: Path, text: str) -> bool:
    """Return whether a differing ``okf/log.md`` is provably distiller-managed.

    The bounded handoff index is legitimate output of the handoff distiller, so a
    re-run must preserve it instead of refusing to bootstrap. Ownership is confirmed
    by self-verifying embedded digest (surviving fresh clones without ``.agentbus/``)
    or by valid local durable state. If a local state file exists, it must be valid.
    """
    # Imported lazily: ``agentbus.distill`` imports this module at module scope.
    from agentbus.distill import (
        DISTILLER_VERSION,
        default_state_path,
        load_durable_state,
        validate_log_concepts,
        verify_distilled_log_digest,
    )

    if "type: Handoff Log" not in text:
        return False
    if f"agentbus/distill-v{DISTILLER_VERSION}" not in text:
        return False

    state_path = default_state_path(root)
    if state_path.exists():
        try:
            state = load_durable_state(state_path)
            if state is None:
                return False
        except WorkspaceError:
            return False

    if not verify_distilled_log_digest(text):
        return False
    return validate_log_concepts(root / "okf", text)


_SCAFFOLD_STAMP = re.compile(r"by:\s*agentbus/scaffold-v1\s*\n\s*at:\s*(\S+)")


def _recover_generated_at(root: Path) -> str | None:
    """Recover the scaffold timestamp from files left by a crashed partial commit.

    The manifest is written last, so a crash between the template writes and the
    manifest leaves rendered files that still embed their original ``generated.at``.
    Reusing that timestamp keeps the re-run byte-identical instead of conflicting on
    every template. An ambiguous, absent, or distiller-owned stamp returns ``None`` so
    the caller falls back to a fresh timestamp (and a strict conflict if files differ).
    """
    recovered: set[str] = set()
    for relative in TEMPLATE_PATHS:
        if relative in DISTILL_MANAGED_PATHS:
            # A distilled index carries a handoff timestamp, not the scaffold stamp.
            continue
        target = root / relative
        if target.is_symlink() or not target.is_file():
            continue
        try:
            text = target.read_text(encoding="utf-8")
        except OSError:
            continue
        match = _SCAFFOLD_STAMP.search(text)
        if match is not None:
            recovered.add(match.group(1))
    if len(recovered) == 1:
        return recovered.pop()
    return None


def _commit_staged(staged: list[tuple[Path, Path]]) -> None:
    """Atomically move every staged temporary onto its target.

    POSIX cannot rename several files transactionally; staging every payload first
    keeps the window for a partial commit to a bare ``os.replace`` loop. A crash
    before the manifest lands leaves re-runs to observe identical files and finish.
    """
    try:
        for target, temporary in staged:
            if target.is_symlink():
                raise WorkspaceError(
                    "unsafe_scaffold_path",
                    f"refusing symlinked scaffold file: {target}",
                )
            os.replace(temporary, target)
    except Exception:
        for _target, temporary in staged:
            _unlink_quiet(temporary)
        raise
    for target, _temporary in staged:
        fsync_directory(target.parent)


def scaffold_workspace(
    workspace: str | os.PathLike[str],
    *,
    project_name: str | None = None,
    check_only: bool = False,
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Materialize the packaged OKF scaffold without overwriting user content.

    ``project_name`` only applies on a first run; an existing manifest keeps its
    recorded name so re-runs stay byte-identical. A missing manifest whose rendered
    files are still present (a crash between the template writes and the manifest
    landing) recovers the original ``generated_at`` from those files, so the re-run
    finishes the commit instead of conflicting. With ``check_only`` the workspace is
    inspected and reported but nothing is written.
    """
    root = Path(workspace).expanduser()
    if not root.is_dir():
        raise WorkspaceError(
            "scaffold_workspace_missing", f"workspace is not a directory: {root}"
        )
    root = root.resolve()
    manifest_path = root / MANIFEST_PATH
    manifest = _load_manifest(manifest_path)
    if manifest is not None:
        resolved_name = manifest["project_name"]
        creation_time = manifest["generated_at"]
    else:
        candidate = (project_name or "").strip()
        if project_name is not None and not candidate:
            raise WorkspaceError(
                "invalid_scaffold_project_name", "project name must be non-empty"
            )
        resolved_name = candidate or root.name
        creation_time = generated_at or _recover_generated_at(root) or utc_now()
    if not creation_time.strip():
        raise WorkspaceError(
            "invalid_scaffold_time", "scaffold generated_at must be non-empty"
        )

    rendered = _render_templates(project_name=resolved_name, generated_at=creation_time)
    expected: dict[Path, str] = dict(rendered)

    created: list[str] = []
    unchanged: list[str] = []
    managed: list[str] = []
    modified: list[str] = []
    preserved: list[str] = []
    conflicts: list[str] = []
    for relative, text in rendered.items():
        target = root / relative
        _assert_safe_target(root, target)
        if target.exists():
            if not target.is_file():
                conflicts.append(relative.as_posix())
                continue
            try:
                existing = target.read_text(encoding="utf-8")
            except OSError as exc:
                raise WorkspaceError(
                    "scaffold_unreadable", f"cannot read scaffold target {target}: {exc}"
                ) from exc
            if existing == text:
                unchanged.append(relative.as_posix())
            elif relative in DISTILL_MANAGED_PATHS and _distilled_log_owned(root, existing):
                managed.append(relative.as_posix())
            elif (
                manifest is not None
                and relative not in DISTILL_MANAGED_PATHS
                and manifest.get("files", {}).get(relative.as_posix(), {}).get("sha256")
                == _sha256(text)
            ):
                # Scaffolded by us, since edited by a human: preserve, do not conflict.
                modified.append(relative.as_posix())
            elif manifest is None and relative in USER_PRESERVED_PATHS:
                # Pre-existing user-owned root file: preserve untouched, do not block scaffold.
                preserved.append(relative.as_posix())
            elif (
                manifest is not None
                and relative in USER_PRESERVED_PATHS
                and relative.as_posix() not in manifest.get("files", {})
            ):
                # Pre-existing user-owned file preserved during earlier scaffold.
                preserved.append(relative.as_posix())
            else:
                conflicts.append(relative.as_posix())
        else:
            created.append(relative.as_posix())

    scaffolded_rendered = {
        path: text
        for path, text in rendered.items()
        if path.as_posix() not in preserved
    }
    manifest_text = _manifest_text(
        project_name=resolved_name,
        generated_at=creation_time,
        rendered=scaffolded_rendered,
    )
    expected[MANIFEST_PATH] = manifest_text

    manifest_target = root / MANIFEST_PATH
    _assert_safe_target(root, manifest_target)
    if manifest_target.exists():
        if not manifest_target.is_file():
            conflicts.append(MANIFEST_PATH.as_posix())
        else:
            try:
                existing_manifest = manifest_target.read_text(encoding="utf-8")
            except OSError as exc:
                raise WorkspaceError(
                    "scaffold_unreadable",
                    f"cannot read scaffold target {manifest_target}: {exc}",
                ) from exc
            if existing_manifest == manifest_text:
                unchanged.append(MANIFEST_PATH.as_posix())
            elif manifest is not None and _manifest_only_adds(
                manifest, json.loads(manifest_text), created
            ):
                created.append(MANIFEST_PATH.as_posix())
            else:
                conflicts.append(MANIFEST_PATH.as_posix())
    else:
        created.append(MANIFEST_PATH.as_posix())

    if conflicts:
        raise WorkspaceError(
            "scaffold_conflict",
            "refusing to overwrite non-identical scaffold files: " + ", ".join(conflicts),
        )

    if not check_only:
        # The manifest is written last: it is the commit marker for the scaffold.
        ordered = [path for path in TEMPLATE_PATHS if path.as_posix() in created]
        if MANIFEST_PATH.as_posix() in created:
            ordered.append(MANIFEST_PATH)
        staged: list[tuple[Path, Path]] = []
        try:
            for relative in ordered:
                target = root / relative
                staged.append((target, stage_write(target, expected[relative])))
        except Exception:
            # A later stage_write must not strand the temporaries of earlier ones.
            for _target, temporary in staged:
                _unlink_quiet(temporary)
            raise
        _commit_staged(staged)

    return {
        "template_version": SCAFFOLD_VERSION,
        "manifest": str(manifest_path),
        "project_name": resolved_name,
        "generated_at": creation_time,
        "created": created,
        "unchanged": unchanged,
        "managed": managed,
        "modified": modified,
        "preserved": preserved,
        "conflicts": [],
        "check_only": check_only,
    }


def _append_ignore_line(target: Path) -> bool:
    """Append ``.agentbus/`` to ``target`` once; return whether it changed."""
    if target.is_symlink():
        raise WorkspaceError(
            "unsafe_ignore_path", f"refusing symlinked ignore file: {target}"
        )
    try:
        before = target.read_text(encoding="utf-8") if target.exists() else ""
    except OSError as exc:
        raise WorkspaceError(
            "ignore_unreadable", f"cannot read ignore file {target}: {exc}"
        ) from exc
    if any(
        line.strip() in {f"{STATE_DIRECTORY}/", STATE_DIRECTORY}
        for line in before.splitlines()
    ):
        return False
    suffix = "" if not before or before.endswith("\n") else "\n"
    updated = before + suffix + f"{STATE_DIRECTORY}/\n"
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.parent.is_symlink():
        raise WorkspaceError(
            "unsafe_ignore_path", f"refusing symlinked ignore directory: {target.parent}"
        )
    mode = target.stat().st_mode & 0o777 if target.exists() else 0o644
    fd, temporary = tempfile.mkstemp(
        prefix=f".{target.name}.", suffix=".tmp", dir=target.parent
    )
    owned = False
    try:
        set_fd_mode(fd, mode)
        stream = os.fdopen(fd, "w", encoding="utf-8", closefd=True)
        owned = True
        with stream:
            stream.write(updated)
            stream.flush()
            os.fsync(stream.fileno())
        if target.is_symlink():
            raise WorkspaceError(
                "unsafe_ignore_path", f"refusing symlinked ignore file: {target}"
            )
        os.replace(temporary, target)
        fsync_directory(target.parent)
    except Exception:
        if not owned:
            with contextlib.suppress(OSError):
                os.close(fd)
        _unlink_quiet(temporary)
        raise
    return True


def _git_path(root: Path, *arguments: str) -> Path | None:
    try:
        result = subprocess.run(
            ["git", "-C", str(root), "rev-parse", *arguments],
            capture_output=True,
            text=True,
            timeout=2,
            check=False,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        return None
    if result.returncode != 0 or not result.stdout.strip():
        return None
    candidate = Path(result.stdout.strip())
    if candidate.is_absolute():
        return candidate
    # Normalize ``..`` without resolving the final path so a symlinked exclude
    # file stays visible to the refusal in ``_append_ignore_line``.
    return Path(os.path.abspath(root / candidate))


def _is_git_worktree(root: Path) -> bool:
    try:
        result = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "--is-inside-work-tree"],
            capture_output=True,
            text=True,
            timeout=2,
            check=False,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        return False
    return result.returncode == 0 and result.stdout.strip() == "true"


def ensure_runtime_ignored(
    workspace: str | os.PathLike[str], *, bare: bool = False
) -> dict[str, Any]:
    """Ignore ``.agentbus/`` in tracked ``.gitignore`` or bare-mode local exclude.

    Outside a Git work tree there is no ignore surface to update, so the call is a
    no-op that reports ``applied: False`` rather than creating a stray ``.gitignore``.
    """
    root = Path(workspace).expanduser()
    if not root.is_dir():
        raise WorkspaceError(
            "scaffold_workspace_missing", f"workspace is not a directory: {root}"
        )
    root = root.resolve()
    if not _is_git_worktree(root):
        return {"applied": False, "path": None, "scope": None}
    if bare:
        target = _git_path(root, "--git-path", "info/exclude")
        scope = "git-info-exclude"
        if target is None:
            return {"applied": False, "path": None, "scope": scope}
    else:
        top = _git_path(root, "--show-toplevel")
        target = (top / ".gitignore") if top is not None else (root / ".gitignore")
        scope = "gitignore"
    applied = _append_ignore_line(target)
    return {"applied": applied, "path": str(target), "scope": scope}
