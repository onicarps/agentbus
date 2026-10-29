"""Developer experience helpers: init, monitor, ping."""

from __future__ import annotations

import json
import os
import shutil
import stat
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone, tzinfo
from pathlib import Path
from typing import Any

try:
    import zoneinfo
except ImportError:
    zoneinfo = None  # type: ignore

from agentbus.auth import ensure_ephemeral_token, token_path
from agentbus.rbac import bootstrap_default_producer, ensure_default_roles
from agentbus.store import EventStore

AGENTBUS_SERVER_KEY = "agentbus"


class InvalidMcpConfigError(ValueError):
    """A discovered MCP client config is not a valid JSON object."""


@dataclass
class ConfigTarget:
    client: str
    path: Path
    format: str  # "json" | "yaml"


@dataclass
class InitResult:
    workspace: Path
    token_path: Path
    scanned: list[ConfigTarget] = field(default_factory=list)
    updated: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    backups: list[str] = field(default_factory=list)
    bootstrap_role: str | None = None
    dry_run: bool = True
    lock_wait_seconds: float = 0.0
    runtime_initialized: bool = False
    mcp_command: Any | None = None


def resolve_workspace(path: str | Path | None = None) -> Path:
    """Resolve an explicit workspace exactly, or discover one from the cwd.

    Enforces native-FS constraint (rejects WSL DrvFS ``/mnt/c`` etc.).
    """
    from agentbus.workspace_guard import assert_workspace_supported

    if path is not None:
        explicit = Path(path).expanduser().resolve()
        if not explicit.is_dir():
            raise ValueError(f"Workspace not found: {explicit}")
        return assert_workspace_supported(explicit)

    start = Path.cwd().resolve()
    if not start.is_dir():
        raise ValueError(f"Workspace not found: {start}")
    resolved = start
    for candidate in [start, *start.parents]:
        if (candidate / ".git").exists():
            resolved = candidate
            break
        if (candidate / ".agentbus").exists():
            marker = candidate / ".agentbus" / "workspace"
            if marker.exists():
                text = marker.read_text(encoding="utf-8").strip()
                if text:
                    resolved = Path(text).resolve()
                    break
            resolved = candidate
            break
    return assert_workspace_supported(resolved)


def resolve_mcp_command() -> list[str]:
    """Command argv for MCP clients (durable and validated).

    Delegates to :mod:`agentbus.adapters.command`, which prefers an absolute
    console script next to the running interpreter and falls back to
    ``<python> -m agentbus.cli``.  Raises ``McpCommandError`` when no dependable
    command exists instead of returning one that cannot start.
    """
    from agentbus.adapters.command import resolve_mcp_command as _resolve

    return list(_resolve().argv)


def agentbus_mcp_entry(workspace: Path, producer_id: str) -> dict[str, Any]:
    from agentbus.adapters.command import resolve_mcp_command as _resolve
    from agentbus.adapters.manifest import build_command_entry

    cmd = _resolve()
    return build_command_entry(
        cmd.argv, workspace=workspace.resolve(), producer_id=producer_id
    )


def discover_config_targets(workspace: Path) -> list[ConfigTarget]:
    """Deprecated: discovered config files, derived from packaged manifests.

    Read-only and kept for the TUI summary.  New code should use
    :func:`agentbus.discovery.discover`, which distinguishes present, absent,
    and unknown installations.
    """
    from agentbus.adapters.probe import CONFIG_VALID, probe_installations

    targets: list[ConfigTarget] = []
    for probe in probe_installations(workspace=workspace.resolve()):
        if probe.config_state != CONFIG_VALID or probe.config_path is None:
            continue
        targets.append(
            ConfigTarget(
                client=probe.installation_id,
                path=Path(probe.config_path),
                format=probe.format,
            )
        )
    return targets


def _load_json_config(path: Path) -> dict[str, Any]:
    try:
        from agentbus.safe_file import read_regular_file

        content = read_regular_file(path).decode("utf-8").strip()
    except UnicodeDecodeError as exc:
        raise InvalidMcpConfigError(f"{path}: expected UTF-8 JSON") from exc
    except OSError as exc:
        raise InvalidMcpConfigError(f"{path}: unreadable: {exc}") from exc
    if not content:
        return {}
    try:
        from agentbus.jsonc import loads

        config = loads(content)
    except json.JSONDecodeError as exc:
        raise InvalidMcpConfigError(f"{path}: {exc}") from exc
    if not isinstance(config, dict):
        raise InvalidMcpConfigError(f"{path}: expected a JSON object")
    if not isinstance(config.get("mcpServers", {}), dict):
        raise InvalidMcpConfigError(
            f"{path}: mcpServers must be a JSON object"
        )
    return config


def _save_json_config(path: Path, data: dict[str, Any]) -> None:
    path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")


def _backup_path(path: Path) -> Path:
    return path.with_suffix(path.suffix + ".agentbus.bak")


def merge_json_config(
    config: dict[str, Any],
    entry: dict[str, Any],
) -> tuple[dict[str, Any], bool]:
    """Merge agentbus into mcpServers; return (config, changed)."""
    servers = config.setdefault("mcpServers", {})
    existing = servers.get(AGENTBUS_SERVER_KEY)
    if existing == entry:
        return config, False
    servers[AGENTBUS_SERVER_KEY] = entry
    return config, True


def _invalid_config_error(path: str | None, fmt: str, reason: str) -> InvalidMcpConfigError:
    """Build the legacy ``invalid_mcp_config`` error with a precise detail."""
    if path is None:  # pragma: no cover - probe always resolves a path
        return InvalidMcpConfigError(f"invalid client config: {reason}")
    candidate = Path(path)
    if fmt == "json":
        try:
            _load_json_config(candidate)
        except InvalidMcpConfigError as exc:
            return exc
        return InvalidMcpConfigError(f"{candidate}: {reason}")
    return InvalidMcpConfigError(f"{candidate}: {reason}")


def apply_init(
    workspace: Path,
    *,
    producer_id: str,
    dry_run: bool = True,
    clients: list[str] | None = None,
    discover_clients: bool = True,
    command: Any | None = None,
    home: Path | None = None,
    platform: str | None = None,
    environ: Any | None = None,
    which: Any = shutil.which,
    manifests: Any | None = None,
    lock_timeout: float = 10.0,
    allow_user_scope: bool = False,
    interactive: bool = False,
    confirm: Any | None = None,
) -> InitResult:
    """Discover clients, wire agentbus, and bootstrap workspace runtime state.

    ``dry_run=True`` is strictly read-only: no token, marker, roles, database,
    lock, backup, or client file is created or modified.  Mutating runs take
    the workspace setup lock and install every client config inside one
    rollback-capable transaction.  User-scope (global) configurations are
    skipped unless ``allow_user_scope`` is true, and their writes additionally
    take a global config lock shared by every workspace.

    Failure boundary: the workspace runtime bootstrap (token, roles, marker)
    and the client configs are not one atomic unit.  If a client write fails,
    config files are rolled back, while an already-bootstrapped runtime stays.
    """
    from agentbus.adapters.command import McpCommandError, resolve_mcp_command as _resolve
    from agentbus.adapters.manifest import build_entry, load_manifests
    from agentbus.adapters.probe import (
        CONFIG_MALFORMED,
        CONFIG_UNREADABLE,
        ENTRY_CONFLICT,
        STATUS_ABSENT,
        STATUS_UNKNOWN,
        probe_installations,
    )
    from agentbus.adapters.writer import ConfigConflictError, plan_patch
    from agentbus.discovery import (
        SetupWriteError,
        FileTransaction,
        _escapes_workspace,
        global_config_lock,
        setup_lock,
        client_selected,
        auto_connect_allowed,
    )

    resolved = workspace.resolve()
    result = InitResult(
        workspace=workspace,
        token_path=token_path(workspace),
        dry_run=dry_run,
    )
    marker = resolved / ".agentbus" / "workspace"

    def _bootstrap_runtime() -> None:
        from agentbus.rbac import roles_path

        for runtime_file in (token_path(workspace), roles_path(workspace)):
            if runtime_file.is_symlink():
                raise OSError(f"refusing symlinked runtime file: {runtime_file}")
        if marker.is_symlink():
            raise OSError("refusing symlinked workspace marker")
        ensure_ephemeral_token(workspace, rotate=False)
        result.bootstrap_role = bootstrap_default_producer(workspace, producer_id)
        ensure_default_roles(workspace)
        marker.parent.mkdir(parents=True, exist_ok=True)
        flags = os.O_WRONLY | os.O_CREAT | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
        fd = os.open(marker, flags, 0o600)
        try:
            if not stat.S_ISREG(os.fstat(fd).st_mode):
                raise OSError("workspace marker is not a regular file")
            os.ftruncate(fd, 0)
            os.write(fd, (str(resolved) + "\n").encode("utf-8"))
        finally:
            os.close(fd)

    if not discover_clients:
        if not dry_run:
            lock = setup_lock(resolved, timeout=lock_timeout)
            lock.acquire()
            try:
                result.lock_wait_seconds = lock.waited
                _bootstrap_runtime()
                result.runtime_initialized = True
            finally:
                lock.release()
        return result

    catalogue = tuple(load_manifests() if manifests is None else manifests)
    try:
        cmd = command or _resolve(environ=environ, which=which, validate=True)
    except McpCommandError as exc:
        result.skipped.append(f"mcp: no durable command ({exc.code}); clients not wired")
        if not dry_run:
            lock = setup_lock(resolved, timeout=lock_timeout)
            lock.acquire()
            try:
                result.lock_wait_seconds = lock.waited
                _bootstrap_runtime()
                result.runtime_initialized = True
            finally:
                lock.release()
        return result
    result.mcp_command = cmd
    expected = {
        installation.installation_id: build_entry(
            installation,
            argv=cmd.argv,
            workspace=resolved,
            producer_id=producer_id,
        )
        for manifest in catalogue
        for installation in manifest.installations
    }
    installations = {
        installation.installation_id: installation
        for manifest in catalogue
        for installation in manifest.installations
    }
    probes = probe_installations(
        workspace=resolved,
        home=home,
        platform=platform,
        environ=environ,
        expected_entries=expected,
        manifests=catalogue,
        which=which,
    )

    planned: list[tuple[Any, Path, Any]] = []
    wanted = set(clients) if clients else set()
    for probe in probes:
        installation = installations[probe.installation_id]
        named = client_selected(probe, wanted)
        if clients and not named:
            result.skipped.append(f"{probe.installation_id}: filtered")
            continue
        result.scanned.append(
            ConfigTarget(
                client=probe.installation_id,
                path=Path(probe.config_path) if probe.config_path else Path(),
                format=probe.format,
            )
        )
        if not auto_connect_allowed(probe, installation, wanted):
            result.skipped.append(
                f"{probe.installation_id}: auto_connect disabled in manifest"
            )
            continue
        if probe.status == STATUS_ABSENT:
            result.skipped.append(
                f"{probe.installation_id}: not detected ({probe.reason})"
            )
            continue
        if probe.status == STATUS_UNKNOWN:
            # A user-scope config this run can never write (no authorization,
            # or a dry run) must not abort project-scope setup: a broken
            # global IDE file is not a reason to leave the project unwired.
            if installation.scope == "user" and (dry_run or not allow_user_scope):
                result.skipped.append(
                    f"{probe.installation_id}: {probe.reason} "
                    "(user scope not authorized in this run)"
                )
                continue
            if (
                probe.config_state in (CONFIG_MALFORMED, CONFIG_UNREADABLE)
                or probe.entry_state == ENTRY_CONFLICT
            ):
                raise _invalid_config_error(
                    probe.config_path, probe.format, probe.reason
                )
            result.skipped.append(f"{probe.installation_id}: {probe.reason}")
            continue
        if installation.scope == "user" and not allow_user_scope and dry_run:
            result.skipped.append(
                f"{probe.installation_id}: user_scope_requires_authorization "
                "(pass --allow-user-scope to preview global configs)"
            )
            continue
        if installation.scope == "user" and not allow_user_scope and not dry_run:
            authorized = False
            if interactive and confirm is not None:
                authorized = confirm(
                    f"Write AgentBus server config to user-scope file "
                    f"{probe.config_path}?"
                )
            if not authorized:
                result.skipped.append(
                    f"{probe.installation_id}: "
                    "user_scope_requires_authorization "
                    "(pass --allow-user-scope to write global configs)"
                )
                continue
        path = Path(probe.config_path or "")
        if installation.scope == "project" and _escapes_workspace(path, resolved):
            raise InvalidMcpConfigError(f"project_path_escapes_workspace: {path}")
        try:
            patch = plan_patch(
                installation,
                path=path,
                entry=expected[probe.installation_id],
            )
        except ConfigConflictError as exc:
            raise InvalidMcpConfigError(str(exc)) from exc
        if not patch.changed:
            result.skipped.append(f"{probe.installation_id}: already configured")
            continue
        if dry_run:
            result.updated.append(
                f"{probe.installation_id}: would update {path}"
            )
            continue
        planned.append((installation, path, patch))

    if dry_run:
        return result

    lock = setup_lock(resolved, timeout=lock_timeout)
    lock.acquire()
    result.lock_wait_seconds = lock.waited
    user_lock = None
    try:
        if any(installation.scope == "user" for installation, _p, _s in planned):
            user_root = Path(home) if home is not None else Path.home()
            user_lock = global_config_lock(user_root, timeout=lock_timeout)
            user_lock.acquire()
        _bootstrap_runtime()
        transaction = FileTransaction()
        staged: list[str] = []
        for installation, path, _first in planned:
            try:
                if installation.scope == "project" and _escapes_workspace(path, resolved):
                    raise ConfigConflictError("project_path_escapes_workspace", str(path))
                fresh = plan_patch(
                    installation,
                    path=path,
                    entry=expected[installation.installation_id],
                )
            except ConfigConflictError as exc:
                # A concurrent writer may have made the file malformed,
                # unowned, or unreadable since the pre-lock plan; surface the
                # same typed error the first pass produces.
                raise InvalidMcpConfigError(str(exc)) from exc
            if not fresh.changed:
                result.skipped.append(
                    f"{installation.installation_id}: already configured"
                )
                continue
            assert fresh.new_bytes is not None
            transaction.stage(
                path,
                fresh.new_bytes,
                original_bytes=fresh.original_bytes,
            )
            staged.append(f"{installation.installation_id}: updated {path}")
        outcome = transaction.commit()
        result.updated.extend(staged)
        result.backups.extend(outcome.backups)
        result.runtime_initialized = True
        return result
    except SetupWriteError as exc:
        # The runtime bootstrap (token/roles/marker) already happened; say so.
        exc.runtime_initialized = True  # type: ignore[attr-defined]
        raise
    finally:
        if user_lock is not None:
            user_lock.release()
        lock.release()


def detect_factory() -> bool:
    return (Path.home() / ".factory").exists()


def format_init_summary(result: InitResult) -> str:
    lines = [
        f"Workspace: {result.workspace}",
        f"Token: {result.token_path}",
        f"Mode: {'dry-run' if result.dry_run else 'apply'}",
    ]
    if result.scanned:
        lines.append("Scanned:")
        for t in result.scanned:
            lines.append(f"  - {t.client}: {t.path}")
    else:
        lines.append("Scanned: (no MCP configs found)")
    if detect_factory():
        lines.append("Factory: detected (~/.factory) — wire manually via missions")
    if result.updated:
        lines.append("Updates:")
        lines.extend(f"  - {u}" for u in result.updated)
    if result.bootstrap_role:
        lines.append(
            f"RBAC: mapped new producer to least-privileged role "
            f"'{result.bootstrap_role}'"
        )
    if result.backups:
        lines.append("Backups:")
        lines.extend(f"  - {b}" for b in result.backups)
    if result.skipped:
        lines.append("Skipped:")
        lines.extend(f"  - {s}" for s in result.skipped)
    return "\n".join(lines)


def poll_events_snapshot(
    workspace: Path,
    *,
    topic: str | None = None,
    since_id: int = 0,
    limit: int = 50,
    retention_days: int = 7,
) -> list[dict[str, Any]]:
    store = EventStore(workspace, retention_days=retention_days)
    try:
        if topic:
            data = store.poll(topic=topic, since_id=since_id, limit=limit)
            return data.get("events", [])
        conn = store._conn  # read-only tail for monitor
        rows = conn.execute(
            """
            SELECT *
            FROM events
            WHERE event_id > ?
            ORDER BY event_id DESC
            LIMIT ?
            """,
            (since_id, limit),
        ).fetchall()
        return [
            event.to_dict()
            for event in reversed(store._authoritative_events_from_rows(rows))
        ]
    finally:
        store.close()


def resolve_timezone(tz: str | tzinfo | None) -> tzinfo:
    """Resolve a timezone string or object to a datetime tzinfo.

    Accepts:
    - None / "utc" / "UTC": timezone.utc
    - "local" / "LOCAL": local system timezone
    - IANA timezone name (e.g. "America/New_York", "Asia/Tokyo")
    """
    if tz is None:
        return timezone.utc
    if isinstance(tz, tzinfo):
        return tz
    tz_str = str(tz).strip()
    if not tz_str or tz_str.upper() == "UTC":
        return timezone.utc
    if tz_str.lower() == "local":
        local_tz = datetime.now().astimezone().tzinfo
        return local_tz if local_tz is not None else timezone.utc
    if zoneinfo is not None:
        try:
            return zoneinfo.ZoneInfo(tz_str)
        except Exception:
            pass
    return timezone.utc


def _format_offset(offset: str) -> str:
    """Render a %z offset like '+0800' as '+08:00' (no 'UTC' prefix)."""
    if len(offset) == 5 and offset[0] in "+-":
        return f"{offset[:3]}:{offset[3:]}"
    return offset


def format_timezone_label(tz: str | tzinfo | None) -> str:
    """Return a clean human-readable label for the current timezone."""
    if tz is None:
        return "UTC"
    if isinstance(tz, str):
        s = tz.strip()
        if not s or s.upper() == "UTC":
            return "UTC"
        if s.lower() == "local":
            now = datetime.now().astimezone()
            return f"Local ({_format_offset(now.strftime('%z'))})"
        if zoneinfo is not None:
            try:
                zi = zoneinfo.ZoneInfo(s)
                return zi.key
            except Exception:
                return "UTC"
        return "UTC"
    if tz == timezone.utc:
        return "UTC"
    now = datetime.now(tz)
    name = getattr(tz, "key", None)
    if not name:
        # A plain fixed-offset timezone renders str() as 'UTC+08:00'; the
        # offset alone is the honest label (mission spec: 'Local (+08:00)').
        if isinstance(tz, timezone) and tz is not timezone.utc:
            return _format_offset(now.strftime("%z"))
        name = str(tz)
    return f"{name} ({_format_offset(now.strftime('%z'))})"


def format_timestamp(ts: str | None, tz: str | tzinfo | None = None) -> str:
    """Format an ISO timestamp to HH:MM:SS in the target timezone."""
    if not ts:
        return ""
    try:
        raw = str(ts).strip()
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        if isinstance(tz, str) and tz.strip().lower() == "local":
            dt_target = dt.astimezone()
        else:
            target_tz = resolve_timezone(tz)
            dt_target = dt.astimezone(target_tz)
        return dt_target.strftime("%H:%M:%S")
    except Exception:
        return str(ts)[-8:]


def _clip(value: Any, max_len: int) -> str:
    s = str(value if value is not None else "")
    if len(s) <= max_len:
        return s
    if max_len <= 3:
        return s[:max_len]
    return s[: max_len - 3] + "..."


def format_event_row(event: dict[str, Any], tz: str | tzinfo | None = None) -> dict[str, str]:
    """Normalize one event for monitor tables / plain CLI.

    Always extracts payload ``from`` / ``to`` when present (Agy #210 / v0.14).
    Falls back to producer_id for from; em-dash for missing to (system events).
    """
    payload = event.get("payload") or {}
    if not isinstance(payload, dict):
        payload = {}
    frm = payload.get("from")
    if frm is None or frm == "":
        frm = event.get("producer_id") or "?"
    to = payload.get("to")
    if to is None or to == "":
        to = "—"
    summary = payload.get("summary", "")
    if not summary and event.get("topic", "").startswith("system/"):
        # Compact system detail for stream when no handoff summary
        summary = str(payload)[:60]
    return {
        "id": str(event.get("event_id", "")),
        "time": format_timestamp(event.get("timestamp"), tz=tz),
        "topic": _clip(event.get("topic", ""), 16),
        "from": _clip(frm, 14),
        "to": _clip(to, 14),
        "summary": _clip(summary, 60),
    }


def _build_monitor_table(workspace: Path, events: list[dict[str, Any]], tz: str | tzinfo | None = None):
    from rich.table import Table
    from agentbus import __version__

    tz_label = format_timezone_label(tz)
    table = Table(
        title=f"AgentBus v{__version__} — {workspace} (TZ: {tz_label})",
        caption=f"AgentBus v{__version__}  |  TZ: {tz_label}",
    )
    for col in ("id", "time", "topic", "from", "to", "summary"):
        table.add_column(col)
    for ev in events[-20:]:
        row = format_event_row(ev, tz=tz)
        table.add_row(
            row["id"],
            row["time"],
            row["topic"],
            row["from"],
            row["to"],
            row["summary"],
        )
    return table


def run_monitor(
    workspace: Path,
    *,
    topic: str | None = None,
    interval: float = 1.0,
    once: bool = False,
    retention_days: int = 7,
    plain: bool = False,
    tz: str | None = None,
) -> None:
    """Tail events.db; Textual TUI (default), rich snapshot, or plain poll."""
    if not once and not plain and topic is None:
        try:
            from agentbus.tui import run_monitor_tui

            run_monitor_tui(workspace, interval=interval, retention_days=retention_days, tz=tz)
            return
        except RuntimeError:
            pass

    try:
        from rich.console import Console
        from rich.live import Live

        use_rich = True
    except ImportError:
        use_rich = False

    since_id = 0

    if not use_rich:
        while True:
            events = poll_events_snapshot(
                workspace,
                topic=topic,
                since_id=since_id,
                limit=50,
                retention_days=retention_days,
            )
            for ev in events:
                row = format_event_row(ev, tz=tz)
                # Dedicated from/to columns (not only arrow-joined)
                print(
                    f"{row['id']:>4} {row['time']} {row['topic']:<16} "
                    f"{row['from']:<14} {row['to']:<14} {row['summary']}"
                )
            if events:
                since_id = max(since_id, events[-1]["event_id"])
            if once:
                return
            time.sleep(interval)

    console = Console()
    if once:
        snapshot = poll_events_snapshot(
            workspace,
            topic=topic,
            since_id=0,
            limit=50,
            retention_days=retention_days,
        )
        console.print(_build_monitor_table(workspace, snapshot, tz=tz))
        return

    with Live(
        _build_monitor_table(workspace, [], tz=tz),
        console=console,
        refresh_per_second=4,
    ) as live:
        while True:
            new_events = poll_events_snapshot(
                workspace,
                topic=topic,
                since_id=since_id,
                limit=50,
                retention_days=retention_days,
            )
            if new_events:
                since_id = max(since_id, new_events[-1]["event_id"])
            snapshot = poll_events_snapshot(
                workspace,
                topic=topic,
                since_id=max(0, since_id - 50),
                limit=50,
                retention_days=retention_days,
            )
            live.update(_build_monitor_table(workspace, snapshot, tz=tz))
            time.sleep(interval)


def publish_ping(
    workspace: Path,
    *,
    producer_id: str,
    retention_days: int = 7,
) -> dict[str, Any]:
    from agentbus.auth import check_publish_token
    from agentbus.schemas import validate_payload

    check_publish_token(workspace, auth_token=None)
    payload = validate_payload(
        "okf/handoff",
        {
            "from": producer_id,
            "to": "all",
            "summary": f"PING from {producer_id}",
        },
        producer_id=producer_id,
    )
    store = EventStore(workspace, retention_days=retention_days)
    try:
        event, duplicate = store.publish(
            topic="okf/handoff",
            producer_id=producer_id,
            schema_version="1.0",
            payload=payload,
            skip_rbac=True,
        )
        return {
            "event_id": event.event_id,
            "duplicate": duplicate,
            "timestamp": event.timestamp,
        }
    finally:
        store.close()
