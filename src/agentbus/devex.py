"""Developer experience helpers: init, monitor, ping."""

from __future__ import annotations

import json
import shutil
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from agentbus.auth import ensure_ephemeral_token, token_path
from agentbus.store import EventStore


AGENTBUS_SERVER_KEY = "agentbus"


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
    dry_run: bool = True


def resolve_workspace(path: str | Path | None = None) -> Path:
    """Resolve workspace root: walk up to git root or .agentbus from path or cwd."""
    start = Path(path).expanduser().resolve() if path else Path.cwd().resolve()
    if not start.is_dir():
        raise ValueError(f"Workspace not found: {start}")
    for candidate in [start, *start.parents]:
        if (candidate / ".git").exists():
            return candidate
        if (candidate / ".agentbus").exists():
            marker = candidate / ".agentbus" / "workspace"
            if marker.exists():
                text = marker.read_text(encoding="utf-8").strip()
                if text:
                    return Path(text).resolve()
            return candidate
    return start


def resolve_mcp_command() -> list[str]:
    """Command argv for MCP clients (pip-friendly)."""
    if shutil.which("agentbus"):
        return ["agentbus", "mcp-serve"]
    return ["python3", "-m", "agentbus.cli", "mcp-serve"]


def agentbus_mcp_entry(workspace: Path, producer_id: str) -> dict[str, Any]:
    cmd = resolve_mcp_command()
    return {
        "command": cmd[0],
        "args": [*cmd[1:], "--workspace", str(workspace)],
        "env": {
            "AGENTBUS_WORKSPACE": str(workspace),
            "AGENTBUS_PRODUCER_ID": producer_id,
        },
    }


def discover_config_targets(workspace: Path) -> list[ConfigTarget]:
    home = Path.home()
    targets: list[ConfigTarget] = []

    candidates: list[tuple[str, Path, str]] = [
        ("cursor-user", home / ".cursor" / "mcp.json", "json"),
        ("cursor-project", workspace / ".cursor" / "mcp.json", "json"),
        ("claude-desktop", home / ".config" / "claude" / "claude_desktop_config.json", "json"),
        ("gemini", home / ".gemini" / "config" / "mcp_config.json", "json"),
        ("vscode", workspace / ".vscode" / "mcp.json", "json"),
    ]

    for client, path, fmt in candidates:
        if path.exists():
            targets.append(ConfigTarget(client=client, path=path, format=fmt))

    return targets


def _load_json_config(path: Path) -> dict[str, Any]:
    content = path.read_text(encoding="utf-8").strip()
    if not content:
        return {}
    return json.loads(content)


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


def apply_init(
    workspace: Path,
    *,
    producer_id: str,
    dry_run: bool = True,
    clients: list[str] | None = None,
) -> InitResult:
    ensure_ephemeral_token(workspace, rotate=False)
    marker = workspace / ".agentbus" / "workspace"
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text(str(workspace.resolve()) + "\n", encoding="utf-8")
    entry = agentbus_mcp_entry(workspace, producer_id)
    result = InitResult(
        workspace=workspace,
        token_path=token_path(workspace),
        dry_run=dry_run,
    )

    for target in discover_config_targets(workspace):
        if clients and target.client not in clients:
            result.skipped.append(f"{target.client}: filtered")
            continue
        result.scanned.append(target)

        if target.format != "json":
            result.skipped.append(f"{target.client}: unsupported format")
            continue

        config = _load_json_config(target.path)
        merged, changed = merge_json_config(config, entry)
        if not changed:
            result.skipped.append(f"{target.client}: already configured")
            continue

        if dry_run:
            result.updated.append(f"{target.client}: would update {target.path}")
            continue

        backup = _backup_path(target.path)
        if not backup.exists():
            shutil.copy2(target.path, backup)
            result.backups.append(str(backup))
        _save_json_config(target.path, merged)
        result.updated.append(f"{target.client}: updated {target.path}")

    return result


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
            SELECT event_id, topic, producer_id, timestamp, payload
            FROM events
            WHERE event_id > ?
            ORDER BY event_id DESC
            LIMIT ?
            """,
            (since_id, limit),
        ).fetchall()
        events = []
        for row in reversed(rows):
            payload = json.loads(row["payload"])
            events.append(
                {
                    "event_id": row["event_id"],
                    "topic": row["topic"],
                    "producer_id": row["producer_id"],
                    "timestamp": row["timestamp"],
                    "payload": payload,
                }
            )
        return events
    finally:
        store.close()


def format_event_row(event: dict[str, Any]) -> dict[str, str]:
    payload = event.get("payload") or {}
    frm = payload.get("from", event.get("producer_id", "?"))
    to = payload.get("to", "—")
    summary = payload.get("summary", "")
    if len(summary) > 60:
        summary = summary[:57] + "..."
    return {
        "id": str(event["event_id"]),
        "time": event.get("timestamp", "")[-8:],  # HH:MM:SSZ tail
        "topic": event.get("topic", ""),
        "from": str(frm),
        "to": str(to),
        "summary": summary,
    }


def _build_monitor_table(workspace: Path, events: list[dict[str, Any]]):
    from rich.table import Table

    table = Table(title=f"AgentBus — {workspace}")
    for col in ("id", "time", "topic", "from", "to", "summary"):
        table.add_column(col)
    for ev in events[-20:]:
        row = format_event_row(ev)
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
) -> None:
    """Tail events.db; Textual TUI (default), rich snapshot, or plain poll."""
    if not once and not plain and topic is None:
        try:
            from agentbus.tui import run_monitor_tui

            run_monitor_tui(workspace, interval=interval, retention_days=retention_days)
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
                row = format_event_row(ev)
                print(
                    f"{row['id']:>4} {row['time']} {row['topic']:<14} "
                    f"{row['from']}->{row['to']} {row['summary']}"
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
        console.print(_build_monitor_table(workspace, snapshot))
        return

    with Live(
        _build_monitor_table(workspace, []),
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
            live.update(_build_monitor_table(workspace, snapshot))
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