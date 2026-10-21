"""Deterministic handoff distillation tests."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

import agentbus
from agentbus.distill import (
    LOG_LIMIT,
    STATE_NAME,
    WorkspaceError,
    default_state_path,
    distill_workspace,
)
from agentbus.scaffold import MANIFEST_PATH, scaffold_workspace
from agentbus.store import EventStore


def _publish(
    store: EventStore,
    *,
    index: int,
    sender: str = "codex",
    recipient: str = "factory",
    summary: str | None = None,
    links: list[str] | None = None,
) -> int:
    event, _duplicate = store.publish(
        topic="okf/handoff",
        producer_id=sender,
        schema_version="1.0",
        payload={
            "from": sender,
            "to": recipient,
            "summary": summary or f"handoff {index}",
            "initiative": "agentbus",
            "links": links if links is not None else ["/initiatives/agentbus/status.md"],
        },
        idempotency_key=f"distill-{index}",
    )
    return event.event_id


def _publish_handoff(workspace: Path, summary: str = "handoff") -> int:
    store = EventStore(workspace)
    try:
        event, _ = store.publish(
            topic="okf/handoff",
            producer_id="codex",
            schema_version="1.0",
            payload={
                "from": "codex",
                "to": "factory",
                "summary": summary,
                "initiative": "agentbus",
                "links": ["/initiatives/agentbus/status.md"],
            },
        )
        return event.event_id
    finally:
        store.close()


def _scaffolded(tmp_path: Path) -> Path:
    root = tmp_path / "repo"
    root.mkdir()
    scaffold_workspace(root)
    return root


def _handoff_files(root: Path) -> list[Path]:
    return sorted((root / "okf" / "handoffs").rglob("*.md"))


def test_distill_writes_partitioned_concepts_and_bounded_log(tmp_path: Path) -> None:
    root = _scaffolded(tmp_path)
    store = EventStore(root)
    first = _publish(store, index=1, summary="first handoff")
    second = _publish(store, index=2, summary="second handoff")
    store.close()

    result = distill_workspace(root)

    assert result["count"] == 2
    files = _handoff_files(root)
    assert len(files) == 2
    assert files[0].name.startswith(f"{first:020d}-")
    assert files[1].name.startswith(f"{second:020d}-")
    for path in files:
        parts = path.relative_to(root / "okf").parts
        assert len(parts) == 4
        assert parts[0] == "handoffs"
        assert len(parts[1]) == 4 and parts[1].isdigit()
        assert len(parts[2]) == 2 and 1 <= int(parts[2]) <= 12

    text = files[0].read_text(encoding="utf-8")
    assert text.startswith("---\n")
    assert "type: Handoff" in text
    assert f'title: "AgentBus handoff {first}"' in text
    assert "status: stable" in text
    assert "generated:\n  by: \"codex\"" in text
    assert "distilled:\n  by: agentbus/distill-v1" in text
    assert f"  - id: agentbus-{first}" in text
    assert f"    resource: agentbus://events/{first}" in text
    assert "- **From:** codex" in text
    assert "- **To:** factory" in text
    assert "- **Topic:** okf/handoff" in text
    assert "- **Initiative:** agentbus" in text
    assert "> first handoff" in text
    assert "## Links" in text

    log = (root / "okf" / "log.md").read_text(encoding="utf-8")
    assert "type: Handoff Log" in log
    assert f"[#{first}]({files[0].relative_to(root / 'okf').as_posix()})" in log
    assert f"[#{second}]({files[1].relative_to(root / 'okf').as_posix()})" in log


def test_distill_is_idempotent_and_advances_high_water(tmp_path: Path) -> None:
    root = _scaffolded(tmp_path)
    store = EventStore(root)
    _publish(store, index=1)
    store.close()

    first = distill_workspace(root)
    assert first["count"] == 1
    state = json.loads((root / ".agentbus" / STATE_NAME).read_text(encoding="utf-8"))
    assert state["schema_version"] == 1
    assert state["distiller_version"] == "1"
    assert state["high_water_event_id"] == first["high_water_event_id"]

    log_before = (root / "okf" / "log.md").read_bytes()
    second = distill_workspace(root)

    assert second["count"] == 0
    assert second["created"] == []
    assert (root / "okf" / "log.md").read_bytes() == log_before


def test_distill_log_is_bounded_to_fifty_entries(tmp_path: Path) -> None:
    root = _scaffolded(tmp_path)
    store = EventStore(root)
    ids = [_publish(store, index=i, summary=f"handoff {i}") for i in range(1, 56)]
    store.close()

    result = distill_workspace(root, limit=1000)

    assert result["count"] == 55
    assert len(_handoff_files(root)) == 55
    log = (root / "okf" / "log.md").read_text(encoding="utf-8")
    body = [line for line in log.splitlines() if line.startswith("- `")]
    assert len(body) == LOG_LIMIT
    for event_id in ids[:5]:
        assert f"[#{event_id}]" not in log
    for event_id in ids[-LOG_LIMIT:]:
        assert f"[#{event_id}]" in log
    state = json.loads((root / ".agentbus" / STATE_NAME).read_text(encoding="utf-8"))
    assert len(state["entries"]) == LOG_LIMIT


def test_distill_log_links_point_at_existing_files(tmp_path: Path) -> None:
    root = _scaffolded(tmp_path)
    store = EventStore(root)
    _publish(store, index=1)
    store.close()
    distill_workspace(root)

    log = (root / "okf" / "log.md").read_text(encoding="utf-8")
    for line in log.splitlines():
        if not line.startswith("- `"):
            continue
        target = line.split("](", 1)[1].split(")", 1)[0]
        assert (root / "okf" / target).is_file(), target


def test_distill_stdout_preview_writes_nothing(tmp_path: Path) -> None:
    root = _scaffolded(tmp_path)
    store = EventStore(root)
    _publish(store, index=1)
    store.close()

    result = distill_workspace(root, stdout=True)

    assert result["count"] == 1
    assert "type: Handoff" in result["documents"][0]
    assert not (root / "okf" / "handoffs").exists()
    assert not (root / ".agentbus" / STATE_NAME).exists()
    assert (root / MANIFEST_PATH).is_file()


def test_distill_dry_run_writes_nothing(tmp_path: Path) -> None:
    root = _scaffolded(tmp_path)
    store = EventStore(root)
    _publish(store, index=1)
    store.close()

    result = distill_workspace(root, dry_run=True)

    assert result["dry_run"] is True
    assert result["count"] == 1
    assert result["created"]
    assert not (root / "okf" / "handoffs").exists()
    assert not (root / ".agentbus" / STATE_NAME).exists()


def test_distill_refuses_nonidentical_existing_handoff(tmp_path: Path) -> None:
    root = _scaffolded(tmp_path)
    store = EventStore(root)
    _publish(store, index=1)
    store.close()

    planned = distill_workspace(root, dry_run=True)["created"][0]
    path = root / "okf" / planned
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("tampered\n", encoding="utf-8")

    with pytest.raises(WorkspaceError) as excinfo:
        distill_workspace(root)

    assert excinfo.value.code == "distill_conflict"
    assert path.read_text(encoding="utf-8") == "tampered\n"
    assert not (root / ".agentbus" / STATE_NAME).exists()


def test_distill_reset_reconsiders_materialized_handoffs(tmp_path: Path) -> None:
    root = _scaffolded(tmp_path)
    store = EventStore(root)
    _publish(store, index=1)
    store.close()
    distill_workspace(root)
    path = _handoff_files(root)[0]
    before = path.read_bytes()

    result = distill_workspace(root, reset=True)

    assert result["count"] == 1
    assert result["created"] == []
    assert result["unchanged"] == [path.relative_to(root / "okf").as_posix()]
    assert path.read_bytes() == before


def test_distill_recovers_from_a_partial_previous_write(tmp_path: Path) -> None:
    root = _scaffolded(tmp_path)
    store = EventStore(root)
    _publish(store, index=1)
    _publish(store, index=2)
    store.close()

    # Simulate a crash after the concept files landed but before the state did.
    preview = distill_workspace(root, stdout=True)
    planned = distill_workspace(root, dry_run=True)["created"]
    for relative, text in zip(planned, preview["documents"], strict=True):
        path = root / "okf" / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")

    result = distill_workspace(root)

    assert result["created"] == []
    assert sorted(result["unchanged"]) == sorted(planned)
    assert (root / "okf" / "log.md").is_file()
    state = json.loads((root / ".agentbus" / STATE_NAME).read_text(encoding="utf-8"))
    assert state["high_water_event_id"] == result["high_water_event_id"]
    assert len(_handoff_files(root)) == 2


def test_distill_serializes_concurrent_writers(tmp_path: Path) -> None:
    from concurrent.futures import ThreadPoolExecutor

    root = _scaffolded(tmp_path)
    store = EventStore(root)
    ids = [_publish(store, index=index) for index in range(1, 9)]
    store.close()

    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(lambda _n: distill_workspace(root, limit=4), range(4)))

    assert len(_handoff_files(root)) == len(ids)
    state = json.loads((root / ".agentbus" / STATE_NAME).read_text(encoding="utf-8"))
    assert state["high_water_event_id"] == max(ids)
    assert max(result["high_water_event_id"] for result in results) == max(ids)
    assert sum(result["count"] for result in results) == len(ids)
    assert all(result["unchanged"] == [] for result in results)
    log = (root / "okf" / "log.md").read_text(encoding="utf-8")
    for event_id in ids:
        assert f"[#{event_id}]" in log


def test_distill_rejects_invalid_limit(tmp_path: Path) -> None:
    root = _scaffolded(tmp_path)

    with pytest.raises(WorkspaceError) as excinfo:
        distill_workspace(root, limit=0)

    assert excinfo.value.code == "invalid_distill_limit"


def test_distill_rejects_corrupt_state(tmp_path: Path) -> None:
    root = _scaffolded(tmp_path)
    state_path = root / ".agentbus" / STATE_NAME
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text("{not json", encoding="utf-8")

    with pytest.raises(WorkspaceError) as excinfo:
        distill_workspace(root)

    assert excinfo.value.code == "invalid_distill_state"


def test_distill_requires_an_existing_workspace(tmp_path: Path) -> None:
    with pytest.raises(WorkspaceError) as excinfo:
        distill_workspace(tmp_path / "missing")

    assert excinfo.value.code == "distill_workspace_missing"


def test_distill_respects_limit_per_run(tmp_path: Path) -> None:
    root = _scaffolded(tmp_path)
    store = EventStore(root)
    for index in range(1, 6):
        _publish(store, index=index)
    store.close()

    first = distill_workspace(root, limit=2)
    assert first["count"] == 2
    second = distill_workspace(root, limit=2)
    assert second["count"] == 2
    third = distill_workspace(root, limit=2)
    assert third["count"] == 1
    assert len(_handoff_files(root)) == 5


def test_distill_default_state_path_is_the_canonical_plane(tmp_path: Path) -> None:
    root = _scaffolded(tmp_path)

    assert default_state_path(root) == root / ".agentbus" / STATE_NAME
    result = distill_workspace(root)

    assert Path(result["state"]) == root / ".agentbus" / STATE_NAME


def test_distill_custom_out_cannot_hide_or_contaminate_the_default_plane(
    tmp_path: Path,
) -> None:
    """A custom --out keeps its own high-water mark; the default plane stays fresh."""
    root = _scaffolded(tmp_path)
    store = EventStore(root)
    event_id = _publish(store, index=1)
    store.close()
    custom = tmp_path / "custom-knowledge"

    custom_result = distill_workspace(root, output_root=custom)

    assert custom_result["count"] == 1
    assert (custom / "log.md").is_file()
    assert list((custom / "handoffs").rglob("*.md"))
    custom_state = Path(custom_result["state"])
    assert custom_state.is_file()
    assert custom_state != root / ".agentbus" / STATE_NAME
    assert custom_state.name.startswith(".distill-state.")
    # The default plane must not inherit the custom run's progress.
    assert not (root / ".agentbus" / STATE_NAME).exists()
    assert not (root / "okf" / "handoffs").exists()
    custom_log_before = (custom / "log.md").read_bytes()
    custom_state_before = custom_state.read_bytes()

    default_result = distill_workspace(root)

    assert default_result["count"] == 1
    assert default_result["high_water_event_id"] == event_id
    assert list((root / "okf" / "handoffs").rglob("*.md"))
    assert (root / "okf" / "log.md").is_file()
    assert json.loads(
        (root / ".agentbus" / STATE_NAME).read_text(encoding="utf-8")
    )["high_water_event_id"] == event_id
    # ...and the default run must not rewrite the custom plane.
    assert (custom / "log.md").read_bytes() == custom_log_before
    assert custom_state.read_bytes() == custom_state_before

    # Re-running the default plane stays idempotent now that its state exists.
    assert distill_workspace(root)["count"] == 0
    assert distill_workspace(root, output_root=custom)["count"] == 0


def test_distill_requires_a_resolved_workspace_for_custom_out(tmp_path: Path) -> None:
    root = _scaffolded(tmp_path)
    store = EventStore(root)
    _publish(store, index=1)
    store.close()

    # ``--out`` pointing back at the default plane shares the canonical state file.
    result = distill_workspace(root, output_root=root / "okf")

    assert Path(result["state"]) == root / ".agentbus" / STATE_NAME


def test_distill_default_then_custom_planes_stay_isolated(tmp_path: Path) -> None:
    """Distilling the default plane first must not suppress a later custom run."""
    root = _scaffolded(tmp_path)
    store = EventStore(root)
    event_id = _publish(store, index=1)
    store.close()
    custom = tmp_path / "custom-knowledge"

    default_result = distill_workspace(root)

    assert default_result["count"] == 1
    assert (root / "okf" / "log.md").is_file()
    assert json.loads(
        (root / ".agentbus" / STATE_NAME).read_text(encoding="utf-8")
    )["high_water_event_id"] == event_id
    assert not (custom / "log.md").exists()

    custom_result = distill_workspace(root, output_root=custom)

    assert custom_result["count"] == 1
    assert custom_result["high_water_event_id"] == event_id
    assert (custom / "log.md").is_file()
    assert list((custom / "handoffs").rglob("*.md"))
    custom_state = Path(custom_result["state"])
    assert custom_state.is_file()
    assert custom_state != root / ".agentbus" / STATE_NAME
    # Neither plane's progress may hide the other's once both are materialized.
    assert distill_workspace(root)["count"] == 0
    assert distill_workspace(root, output_root=custom)["count"] == 0


def test_distill_uses_isolated_locks_per_output(tmp_path: Path) -> None:
    root = _scaffolded(tmp_path)
    store = EventStore(root)
    _publish(store, index=1)
    store.close()
    custom = tmp_path / "custom-knowledge"

    distill_workspace(root)
    distill_workspace(root, output_root=custom)

    locks = sorted(path.name for path in (root / ".agentbus").glob(".distill*.lock"))
    assert ".distill.lock" in locks
    assert any(
        name.startswith(".distill.") and name != ".distill.lock" for name in locks
    ), locks


def test_distill_rejects_state_with_unknown_distiller_version(tmp_path: Path) -> None:
    root = _scaffolded(tmp_path)
    store = EventStore(root)
    _publish(store, index=1)
    store.close()
    distill_workspace(root)
    state_path = root / ".agentbus" / STATE_NAME
    state = json.loads(state_path.read_text(encoding="utf-8"))
    state["distiller_version"] = "999"
    state_path.write_text(json.dumps(state), encoding="utf-8")

    with pytest.raises(WorkspaceError) as excinfo:
        distill_workspace(root)

    assert excinfo.value.code == "invalid_distill_state"


def test_distill_rejects_state_bound_to_another_output(tmp_path: Path) -> None:
    root = _scaffolded(tmp_path)
    store = EventStore(root)
    _publish(store, index=1)
    store.close()
    distill_workspace(root)
    state_path = root / ".agentbus" / STATE_NAME
    state = json.loads(state_path.read_text(encoding="utf-8"))
    state["output_root"] = (tmp_path / "elsewhere").as_posix()
    state_path.write_text(json.dumps(state), encoding="utf-8")

    with pytest.raises(WorkspaceError) as excinfo:
        distill_workspace(root)

    assert excinfo.value.code == "distill_state_mismatch"


def test_distill_rejects_state_with_escaping_entry_path(tmp_path: Path) -> None:
    root = _scaffolded(tmp_path)
    state_path = root / ".agentbus" / STATE_NAME
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "distiller_version": "1",
                "high_water_event_id": 1,
                "entries": [
                    {
                        "event_id": 1,
                        "timestamp": "2026-10-21T00:00:00Z",
                        "from": "codex",
                        "to": "factory",
                        "summary": "escape probe",
                        "path": "../escape.md",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(WorkspaceError) as excinfo:
        distill_workspace(root)

    assert excinfo.value.code == "invalid_distill_state"
    assert not (root / "okf" / "escape.md").exists()


def test_distill_refuses_symlinked_output_root(tmp_path: Path) -> None:
    root = _scaffolded(tmp_path)
    store = EventStore(root)
    _publish(store, index=1)
    store.close()
    outside = tmp_path / "outside"
    outside.mkdir()
    link = tmp_path / "out-link"
    link.symlink_to(outside, target_is_directory=True)

    with pytest.raises(WorkspaceError) as excinfo:
        distill_workspace(root, output_root=link)

    assert excinfo.value.code == "unsafe_distill_path"
    assert list(outside.iterdir()) == []


def test_distill_refuses_symlinked_output_parent(tmp_path: Path) -> None:
    root = _scaffolded(tmp_path)
    store = EventStore(root)
    _publish(store, index=1)
    store.close()
    outside = tmp_path / "outside"
    outside.mkdir()
    (root / "okf" / "handoffs").symlink_to(outside, target_is_directory=True)

    with pytest.raises(WorkspaceError) as excinfo:
        distill_workspace(root)

    assert excinfo.value.code == "unsafe_distill_path"
    assert list(outside.iterdir()) == []
    assert not (root / ".agentbus" / STATE_NAME).exists()


def test_distill_refuses_symlinked_runtime_directory(tmp_path: Path) -> None:
    root = _scaffolded(tmp_path)
    outside = tmp_path / "runtime"
    outside.mkdir()
    (root / ".agentbus").symlink_to(outside, target_is_directory=True)

    with pytest.raises(WorkspaceError) as excinfo:
        distill_workspace(root)

    assert excinfo.value.code == "unsafe_distill_path"
    assert list(outside.iterdir()) == []


def test_distill_refuses_symlinked_state_file(tmp_path: Path) -> None:
    root = _scaffolded(tmp_path)
    store = EventStore(root)
    _publish(store, index=1)
    store.close()
    runtime = root / ".agentbus"
    runtime.mkdir(parents=True, exist_ok=True)
    outside = tmp_path / "state.json"
    outside.write_text("{}", encoding="utf-8")
    (runtime / STATE_NAME).symlink_to(outside)

    with pytest.raises(WorkspaceError) as excinfo:
        distill_workspace(root)

    assert excinfo.value.code == "unsafe_distill_path"
    assert outside.read_text(encoding="utf-8") == "{}"


_SUBPROCESS_WINDOWS_PROBE = textwrap.dedent(
    """
    import importlib.abc
    import json
    import sys
    import types
    from pathlib import Path


    class _BlockFcntl(importlib.abc.MetaPathFinder):
        def find_spec(self, fullname, path=None, target=None):
            if fullname == "fcntl" or fullname.startswith("fcntl."):
                raise ImportError("fcntl is unavailable on this platform")
            return None


    sys.meta_path.insert(0, _BlockFcntl())

    # Import the scaffolding layer first: ``subprocess`` probes ``msvcrt`` at import
    # time to decide its platform branch, so the stand-in must land afterwards.
    import agentbus.scaffold

    fake_msvcrt = types.ModuleType("msvcrt")
    fake_msvcrt.LK_LOCK = 1
    fake_msvcrt.LK_UNLCK = 2
    fake_msvcrt.calls = []

    def _locking(fd, mode, nbytes):
        fake_msvcrt.calls.append((mode, nbytes))

    fake_msvcrt.locking = _locking
    sys.modules["msvcrt"] = fake_msvcrt

    import agentbus
    from agentbus import distill, scaffold

    assert distill.fcntl is None, "fcntl must be blocked for this probe"
    assert distill.msvcrt is not None, "msvcrt stand-in must load"

    workspace = Path(sys.argv[1])
    workspace.mkdir(parents=True, exist_ok=True)
    scaffold.scaffold_workspace(workspace)

    from agentbus.store import EventStore

    store = EventStore(workspace)
    store.publish(
        topic="okf/handoff",
        producer_id="codex",
        schema_version="1.0",
        payload={"from": "codex", "to": "factory", "summary": "windows safe"},
        idempotency_key="win-safe-1",
    )
    store.close()

    result = distill.distill_workspace(workspace)
    assert result["count"] == 1, result
    assert (workspace / "okf" / "log.md").is_file()
    assert (workspace / ".agentbus" / distill.STATE_NAME).is_file()
    lock_calls = list(fake_msvcrt.calls)
    assert (fake_msvcrt.LK_LOCK, 1) in lock_calls, lock_calls
    assert (fake_msvcrt.LK_UNLCK, 1) in lock_calls, lock_calls

    real_stage_write = scaffold.stage_write
    calls = {"count": 0}

    def flaky_stage_write(target, text):
        calls["count"] += 1
        if calls["count"] == 3:
            raise RuntimeError("injected stage failure")
        return real_stage_write(target, text)

    scaffold.stage_write = flaky_stage_write
    probe = workspace.parent / "probe"
    probe.mkdir()
    try:
        scaffold.scaffold_workspace(probe)
    except RuntimeError:
        pass
    else:
        raise AssertionError("injected failure did not propagate")

    leftover = sorted(path.name for path in probe.rglob("*.tmp"))
    print(
        json.dumps(
            {
                "module": str(Path(agentbus.__file__).resolve()),
                "fcntl": distill.fcntl,
                "msvcrt": distill.msvcrt is not None,
                "lock_calls": lock_calls,
                "leftover": leftover,
            }
        )
    )
    """
)


def test_distill_imports_and_locks_without_fcntl(tmp_path: Path) -> None:
    """Model Windows: no fcntl at import time, msvcrt locking, no stranded temps."""
    script = tmp_path / "windows_probe.py"
    script.write_text(_SUBPROCESS_WINDOWS_PROBE, encoding="utf-8")
    source_root = Path(agentbus.__file__).resolve().parents[1]
    existing = os.environ.get("PYTHONPATH", "")
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(
        part for part in (str(source_root), existing) if part
    )

    result = subprocess.run(
        [sys.executable, str(script), str(tmp_path / "ws")],
        capture_output=True,
        text=True,
        env=env,
        cwd=str(tmp_path),
        timeout=120,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout.strip().splitlines()[-1])
    assert payload["module"].startswith(str(source_root))
    assert payload["fcntl"] is None
    assert payload["msvcrt"] is True
    assert [1, 2] == [entry[0] for entry in payload["lock_calls"]]
    assert payload["leftover"] == []
    assert list((tmp_path / "ws").rglob("*.tmp")) == []


def test_distill_refuses_to_overwrite_unowned_log(tmp_path: Path) -> None:
    ws = tmp_path / "ws"
    ws.mkdir()
    out = ws / "okf"
    out.mkdir()
    unowned_log = out / "log.md"
    unowned_log.write_text("# Human-Written Unowned Log\n\nDo not overwrite me!\n", encoding="utf-8")
    _publish_handoff(ws)

    with pytest.raises(WorkspaceError) as exc_info:
        distill_workspace(ws)
    assert exc_info.value.code == "distill_conflict"
    assert "refusing to overwrite non-distilled index" in exc_info.value.message
    assert unowned_log.read_text(encoding="utf-8") == "# Human-Written Unowned Log\n\nDo not overwrite me!\n"


def test_distill_cursor_does_not_pass_pending_handoff(tmp_path: Path) -> None:
    ws = tmp_path / "ws"
    ws.mkdir()
    ev1 = _publish_handoff(ws, summary="Handoff 1 (published)")
    ev2 = _publish_handoff(ws, summary="Handoff 2 (pending)")
    ev3 = _publish_handoff(ws, summary="Handoff 3 (published)")

    store = EventStore(ws)
    try:
        from agentbus.store import STATUS_PENDING
        store._conn.execute("UPDATE events SET status = ? WHERE event_id = ?", (STATUS_PENDING, ev2))
        store._conn.commit()
    finally:
        store.close()

    first_res = distill_workspace(ws)
    assert first_res["count"] == 1
    assert first_res["high_water_event_id"] == ev1

    store = EventStore(ws)
    try:
        store._conn.execute("UPDATE events SET status = 'PUBLISHED' WHERE event_id = ?", (ev2,))
        store._conn.commit()
    finally:
        store.close()

    second_res = distill_workspace(ws)
    assert second_res["count"] == 2
    assert second_res["high_water_event_id"] == ev3


def test_distill_refuses_to_overwrite_edited_scaffold_placeholder(tmp_path: Path) -> None:
    ws = tmp_path / "ws"
    ws.mkdir()
    scaffold_workspace(ws)
    log_file = ws / "okf" / "log.md"
    original = log_file.read_text(encoding="utf-8")
    # Edit placeholder body while retaining frontmatter markers
    edited = original + "\n## Human Notes\nKeep these human edits safe.\n"
    log_file.write_text(edited, encoding="utf-8")

    _publish_handoff(ws, summary="New handoff after edit")

    with pytest.raises(WorkspaceError) as exc_info:
        distill_workspace(ws)
    assert exc_info.value.code == "distill_conflict"
    assert "refusing to overwrite non-distilled index" in exc_info.value.message
    assert log_file.read_text(encoding="utf-8") == edited


def test_distill_survives_workspace_relocation(tmp_path: Path) -> None:
    ws1 = tmp_path / "ws1"
    ws1.mkdir()
    scaffold_workspace(ws1)
    ev1 = _publish_handoff(ws1, summary="Handoff prior to move")
    first_res = distill_workspace(ws1)
    assert first_res["count"] == 1
    assert first_res["high_water_event_id"] == ev1

    # Relocate workspace directory
    ws2 = tmp_path / "ws2"
    ws1.rename(ws2)

    # Distillation in relocated workspace must not fail with distill_state_mismatch
    ev2 = _publish_handoff(ws2, summary="Handoff after move")
    second_res = distill_workspace(ws2)
    assert second_res["count"] == 1
    assert second_res["high_water_event_id"] == ev2


def test_distill_recovers_when_state_write_fails_after_log(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _scaffolded(tmp_path)
    store = EventStore(root)
    _publish(store, index=1)
    store.close()

    from agentbus import distill as distill_module

    real_replace = distill_module._atomic_replace
    state_file = root / ".agentbus" / distill_module.STATE_NAME
    call_count = 0

    def mock_replace(path: Path, text: str) -> None:
        nonlocal call_count
        if path == state_file:
            call_count += 1
            if call_count > 1:
                raise OSError("Simulated crash writing state file")
        real_replace(path, text)

    monkeypatch.setattr(distill_module, "_atomic_replace", mock_replace)

    with pytest.raises(OSError, match="Simulated crash"):
        distill_workspace(root)

    # Log was already written, and state holds the journaled pending_log_sha256
    assert (root / "okf" / "log.md").is_file()
    assert state_file.is_file()
    journal = json.loads(state_file.read_text(encoding="utf-8"))
    assert journal.get("pending_log_sha256") is not None

    monkeypatch.undo()

    # Subsequent distill must recover smoothly and not raise distill_conflict
    res = distill_workspace(root)
    assert res["count"] == 1
    final_state = json.loads(state_file.read_text(encoding="utf-8"))
    assert final_state.get("log_sha256") is not None


def test_distill_recovers_state_failure_when_new_handoff_arrives(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _scaffolded(tmp_path)
    store = EventStore(root)
    first = _publish(store, index=1)
    store.close()

    from agentbus import distill as distill_module

    real_replace = distill_module._atomic_replace
    state_file = root / ".agentbus" / distill_module.STATE_NAME
    call_count = 0

    def failing(path: Path, text: str) -> None:
        nonlocal call_count
        if path == state_file:
            call_count += 1
            if call_count > 1:
                raise OSError("Simulated state write failure")
        real_replace(path, text)

    monkeypatch.setattr(distill_module, "_atomic_replace", failing)
    with pytest.raises(OSError, match="Simulated state write failure"):
        distill_workspace(root)
    monkeypatch.undo()

    store = EventStore(root)
    second = _publish(store, index=2)
    store.close()

    res = distill_workspace(root)
    assert res["count"] == 2
    log = (root / "okf" / "log.md").read_text(encoding="utf-8")
    assert f"[#{first}]" in log and f"[#{second}]" in log


def test_distill_relative_out_resolves_against_workspace(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ws = tmp_path / "ws"
    ws.mkdir()
    other_dir = tmp_path / "other"
    other_dir.mkdir()
    monkeypatch.chdir(other_dir)

    _publish_handoff(ws)
    res = distill_workspace(ws, output_root="custom_okf")
    assert res["count"] == 1
    assert (ws / "custom_okf").is_dir()
    assert not (other_dir / "custom_okf").exists()


def test_distill_handles_malformed_payload_and_string_links(tmp_path: Path) -> None:
    ws = tmp_path / "ws"
    ws.mkdir()

    # Event 1: String links instead of list
    store = EventStore(ws)
    ev1, _ = store.publish(
        topic="okf/handoff",
        producer_id="codex",
        schema_version="1.0",
        payload={
            "from": "codex",
            "to": "factory",
            "summary": "String links",
            "links": "https://example.com/one-link",
        },
    )
    # Event 2: Raw non-dict payload in database
    store._conn.execute(
        """
        INSERT INTO events (topic, producer_id, timestamp, schema_version, payload, status)
        VALUES ('okf/handoff', 'codex', '2026-10-21T12:00:00Z', '1.0', '"malformed string payload"', 'PUBLISHED')
        """
    )
    store._conn.commit()
    ev2 = store._conn.execute("SELECT MAX(event_id) FROM events").fetchone()[0]
    assert ev2 > ev1.event_id

    # Event 3: Valid normal handoff
    ev3, _ = store.publish(
        topic="okf/handoff",
        producer_id="codex",
        schema_version="1.0",
        payload={"from": "codex", "to": "factory", "summary": "Valid"},
    )
    store.close()

    res = distill_workspace(ws)
    # Event 2 was skipped because payload was not a dict, but cursor advanced past ev2 and ev3
    assert res["count"] == 2
    assert res["high_water_event_id"] == ev3.event_id

    # Check string links rendered as one link bullet, not one bullet per character
    handoff_files = list((ws / "okf" / "handoffs").rglob("*.md"))
    assert len(handoff_files) == 2
    content = next(f.read_text(encoding="utf-8") for f in handoff_files if f"{ev1.event_id:020d}" in f.name)
    assert "- https://example.com/one-link\n" in content


def test_distill_bounded_reset_preserves_newest_entries(tmp_path: Path) -> None:
    root = _scaffolded(tmp_path)
    store = EventStore(root)
    ids = [_publish(store, index=i) for i in range(1, 6)]
    store.close()

    # Initial full distillation
    first_res = distill_workspace(root)
    assert first_res["count"] == 5
    assert first_res["high_water_event_id"] == ids[-1]

    for path in _handoff_files(root):
        path.unlink()

    # Bounded reset with limit 2: processes events 1 and 2, but log retains newest entries
    reset_res = distill_workspace(root, limit=2, reset=True)
    assert reset_res["count"] == 2
    assert reset_res["high_water_event_id"] == ids[1]

    second_res = distill_workspace(root, limit=2)
    third_res = distill_workspace(root, limit=2)
    assert second_res["count"] == 2
    assert third_res["count"] == 1
    assert len(_handoff_files(root)) == 5

    log = (root / "okf" / "log.md").read_text(encoding="utf-8")
    assert f"[#{ids[-1]}]" in log
    for line in log.splitlines():
        if line.startswith("- `"):
            target = line.split("](", 1)[1].split(")", 1)[0]
            assert (root / "okf" / target).is_file(), target


def test_distill_custom_out_survives_workspace_relocation(tmp_path: Path) -> None:
    ws1 = tmp_path / "ws1"
    ws1.mkdir()
    store = EventStore(ws1)
    ev1 = _publish(store, index=1)
    store.close()

    res1 = distill_workspace(ws1, output_root="custom_plane")
    assert res1["count"] == 1
    assert res1["high_water_event_id"] == ev1

    ws2 = tmp_path / "ws2"
    ws1.rename(ws2)

    store = EventStore(ws2)
    ev2 = _publish(store, index=2)
    store.close()

    res2 = distill_workspace(ws2, output_root="custom_plane")
    assert res2["count"] == 1
    assert res2["high_water_event_id"] == ev2


def test_distill_survives_fresh_clone_without_agentbus_state(tmp_path: Path) -> None:
    root = tmp_path / "project"
    root.mkdir()
    scaffold_workspace(root)
    store = EventStore(root)
    ev1 = _publish(store, index=1, summary="Handoff prior to clone")
    store.close()
    distill_workspace(root)

    log_before = (root / "okf" / "log.md").read_text(encoding="utf-8")
    assert "Handoff prior to clone" in log_before
    assert f"[#{ev1}]" in log_before

    # Simulate fresh clone: delete .agentbus/ (where SQLite DB & distill-state live)
    shutil.rmtree(root / ".agentbus")

    # Publishing a new handoff on the new clone and distilling must succeed
    store2 = EventStore(root)
    ev2 = _publish(store2, index=2, summary="Handoff after clone")
    store2.close()
    res = distill_workspace(root)
    assert res["count"] == 1
    assert res["high_water_event_id"] == ev2

    log = (root / "okf" / "log.md").read_text(encoding="utf-8")
    # Both the pre-clone handoff AND the post-clone handoff MUST be linked in log.md
    assert "Handoff prior to clone" in log
    assert f"[#{ev1}]" in log
    assert "Handoff after clone" in log
    assert f"[#{ev2}]" in log


def test_distill_fresh_clone_with_colliding_event_id(tmp_path: Path) -> None:
    root = tmp_path / "project"
    root.mkdir()
    scaffold_workspace(root)
    store = EventStore(root)
    ev1 = _publish(store, index=1, summary="Initial event 1 on origin")
    assert ev1 == 1
    store.close()
    distill_workspace(root)

    # Delete .agentbus/ so the new SQLite database starts event_id counter back at 1
    shutil.rmtree(root / ".agentbus")

    store2 = EventStore(root)
    ev2 = _publish(store2, index=1, summary="Second event 1 on clone")
    assert ev2 == 1  # Verify the new DB indeed generated event_id 1
    store2.close()

    res = distill_workspace(root)
    assert res["count"] == 1
    assert res["high_water_event_id"] == 1

    log = (root / "okf" / "log.md").read_text(encoding="utf-8")
    assert "Initial event 1 on origin" in log
    assert "Second event 1 on clone" in log
    # Both events are referenced in markdown
    assert log.count("[#1]") == 2
    from agentbus.distill import verify_distilled_log_digest
    assert verify_distilled_log_digest(log)


def test_distill_detects_frontmatter_tampering(tmp_path: Path) -> None:
    root = tmp_path / "project"
    root.mkdir()
    scaffold_workspace(root)
    store = EventStore(root)
    _publish(store, index=1, summary="Legitimate handoff")
    store.close()
    distill_workspace(root)

    log_file = root / "okf" / "log.md"
    content = log_file.read_text(encoding="utf-8")
    # Tamper with the entries: block in frontmatter
    tampered = content.replace("summary: Legitimate handoff", "summary: Forged handoff")
    assert tampered != content
    log_file.write_text(tampered, encoding="utf-8")

    from agentbus.distill import verify_distilled_log_digest
    assert not verify_distilled_log_digest(tampered)

    store2 = EventStore(root)
    _publish(store2, index=2, summary="Another handoff")
    store2.close()

    with pytest.raises(WorkspaceError) as exc_info:
        distill_workspace(root)
    assert exc_info.value.code == "distill_conflict"

    with pytest.raises(WorkspaceError) as exc_info:
        scaffold_workspace(root)
    assert exc_info.value.code == "scaffold_conflict"


def test_distill_skips_non_published_terminal_statuses(tmp_path: Path) -> None:
    root = tmp_path / "project"
    root.mkdir()
    scaffold_workspace(root)
    store = EventStore(root)
    ev1 = _publish(store, index=1, summary="First published")
    assert ev1 == 1
    ev2_obj, _ = store.publish(
        topic="okf/handoff",
        producer_id="codex",
        schema_version="1.0",
        payload={"from": "codex", "to": "factory", "summary": "Rejected handoff"},
        status="REJECTED",
    )
    ev2 = ev2_obj.event_id
    assert ev2 == 2
    ev3 = _publish(store, index=3, summary="Third published")
    store.close()

    res = distill_workspace(root)
    assert res["count"] == 2
    assert res["high_water_event_id"] == ev3
    log = (root / "okf" / "log.md").read_text(encoding="utf-8")
    assert "First published" in log
    assert "Third published" in log
    assert "Rejected handoff" not in log


def test_distill_tampered_log_without_new_events_preserves_state(tmp_path: Path) -> None:
    root = tmp_path / "project"
    root.mkdir()
    scaffold_workspace(root)
    store = EventStore(root)
    _publish(store, index=1, summary="Legitimate initial handoff")
    store.close()
    res1 = distill_workspace(root)
    assert res1["count"] == 1
    assert len(res1["entries"]) == 1

    # Tamper with log.md by adding a forged entry
    log_file = root / "okf" / "log.md"
    content = log_file.read_text(encoding="utf-8")
    tampered = content.replace("Legitimate initial handoff", "Forged handoff")
    log_file.write_text(tampered, encoding="utf-8")

    # Run distill with NO new events
    res2 = distill_workspace(root)
    assert res2["count"] == 0
    # State entries must remain the authentic entries from state, not the forged entries
    assert len(res2["entries"]) == 1
    assert res2["entries"][0]["summary"] == "Legitimate initial handoff"


def test_distill_rejects_tampered_log_even_with_recomputed_digest(tmp_path: Path) -> None:
    root = tmp_path / "project"
    root.mkdir()
    scaffold_workspace(root)
    store = EventStore(root)
    ev1 = _publish(store, index=1, summary="Legitimate initial handoff")
    assert ev1 == 1
    store.close()
    res1 = distill_workspace(root)
    assert res1["count"] == 1

    log_file = root / "okf" / "log.md"
    content = log_file.read_text(encoding="utf-8")
    tampered = content.replace("Legitimate initial handoff", "Forged handoff")

    parts = tampered.split("\n---\n", 1)
    match = re.search(r"^distilled_sha256:\s*([a-f0-9]{64})\s*\n", parts[0], re.MULTILINE)
    assert match is not None
    unsigned = parts[0][: match.start()] + parts[0][match.end() :] + "\n---\n" + parts[1]
    new_digest = hashlib.sha256(unsigned.encode("utf-8")).hexdigest()
    rehashed = (
        parts[0][: match.start()]
        + f"distilled_sha256: {new_digest}\n"
        + parts[0][match.end() :]
        + "\n---\n"
        + parts[1]
    )

    from agentbus.distill import verify_distilled_log_digest

    assert verify_distilled_log_digest(rehashed)
    log_file.write_text(rehashed, encoding="utf-8")

    store2 = EventStore(root)
    _publish(store2, index=2, summary="Second handoff")
    store2.close()

    with pytest.raises(WorkspaceError) as exc_info:
        distill_workspace(root)
    assert exc_info.value.code == "distill_conflict"

    with pytest.raises(WorkspaceError) as exc_info:
        scaffold_workspace(root)
    assert exc_info.value.code == "scaffold_conflict"


def test_distill_merges_pulled_entries_with_existing_local_state(tmp_path: Path) -> None:
    # Machine A distills event 1
    ws_a = tmp_path / "ws_a"
    ws_a.mkdir()
    scaffold_workspace(ws_a)
    store_a = EventStore(ws_a)
    _publish(store_a, index=1, summary="Machine A handoff")
    store_a.close()
    distill_workspace(ws_a)

    # Machine B distills event 2 locally
    ws_b = tmp_path / "ws_b"
    ws_b.mkdir()
    scaffold_workspace(ws_b)
    store_b = EventStore(ws_b)
    _publish(store_b, index=2, summary="Machine B handoff")
    store_b.close()
    distill_workspace(ws_b)

    # Simulate Machine B pulling Machine A's commit: copy okf/ from ws_a to ws_b
    shutil.copytree(ws_a / "okf" / "handoffs", ws_b / "okf" / "handoffs", dirs_exist_ok=True)
    shutil.copy(ws_a / "okf" / "log.md", ws_b / "okf" / "log.md")

    # Machine B now produces a new handoff (event 3) and runs distill
    store_b2 = EventStore(ws_b)
    _publish(store_b2, index=3, summary="Machine B subsequent handoff")
    store_b2.close()

    res = distill_workspace(ws_b)
    assert res["count"] == 1
    log = (ws_b / "okf" / "log.md").read_text(encoding="utf-8")
    assert "Machine A handoff" in log
    assert "Machine B handoff" in log
    assert "Machine B subsequent handoff" in log










