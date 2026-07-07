"""AgentBus CLI."""

from __future__ import annotations

import json
import os
from pathlib import Path

import click

from agentbus.auth import (
    check_publish_token,
    ensure_ephemeral_token,
    read_workspace_token,
    token_path,
)
from agentbus.devex import (
    apply_init,
    format_init_summary,
    publish_ping,
    resolve_workspace,
    run_monitor,
)
from agentbus.leases import LeaseStore
from agentbus.project_log import project_handoffs
from agentbus.schemas import validate_payload
from agentbus.server import run_stdio
from agentbus.store import EventStore

DEFAULT_WORKSPACE = os.environ.get("AGENTBUS_WORKSPACE", str(Path.cwd()))


def _producer_id(override: str | None) -> str:
    pid = override or os.environ.get("AGENTBUS_PRODUCER_ID", "")
    if not pid:
        raise click.ClickException("Set --producer-id or AGENTBUS_PRODUCER_ID")
    return pid


def _open_store(workspace: str, retention_days: int) -> EventStore:
    return EventStore(Path(workspace), retention_days=retention_days)


def _open_lease_store(workspace: str) -> LeaseStore:
    return LeaseStore(Path(workspace))


def _auth(ws: Path, token: str | None) -> None:
    try:
        check_publish_token(ws, auth_token=token)
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc


@click.group()
def main() -> None:
    """Local MCP event log for multi-agent workspaces."""


@main.command("mcp-serve")
@click.option(
    "--workspace",
    type=click.Path(exists=True, file_okay=False, path_type=str),
    default=DEFAULT_WORKSPACE,
    show_default=True,
)
@click.option("--retention-days", default=7, show_default=True)
@click.option(
    "--rotate-token",
    is_flag=True,
    help="Regenerate workspace token on startup",
)
def mcp_serve(workspace: str, retention_days: int, rotate_token: bool) -> None:
    """MCP entrypoint for IDE configs (ensures token, then serve)."""
    ws = Path(workspace)
    ensure_ephemeral_token(ws, rotate=rotate_token)
    os.environ["AGENTBUS_TOKEN"] = read_workspace_token(ws) or ""
    run_stdio(ws, retention_days=retention_days, rotate_token=False)


@main.command()
@click.option(
    "--workspace",
    type=click.Path(exists=True, file_okay=False, path_type=str),
    default=DEFAULT_WORKSPACE,
    show_default=True,
)
@click.option("--retention-days", default=7, show_default=True)
@click.option(
    "--rotate-token",
    is_flag=True,
    help="Regenerate workspace token on startup",
)
def serve(workspace: str, retention_days: int, rotate_token: bool) -> None:
    """Run MCP server on stdio."""
    run_stdio(Path(workspace), retention_days=retention_days, rotate_token=rotate_token)


@main.command()
@click.option("--workspace", default=DEFAULT_WORKSPACE, show_default=True)
@click.option("--topic", required=True)
@click.option("--payload", "payload_json", default=None, help="JSON object string")
@click.option("--payload-file", type=click.Path(exists=True, dir_okay=False), default=None)
@click.option("--schema-version", default="1.0", show_default=True)
@click.option("--producer-id", default=None)
@click.option("--causation-id", type=int, default=None)
@click.option("--idempotency-key", default=None)
@click.option("--token", default=None, help="Publish auth token (default: workspace file)")
@click.option("--retention-days", default=7, show_default=True)
def publish(
    workspace: str,
    topic: str,
    payload_json: str | None,
    payload_file: str | None,
    schema_version: str,
    producer_id: str | None,
    causation_id: int | None,
    idempotency_key: str | None,
    token: str | None,
    retention_days: int,
) -> None:
    """Append one event (CLI fallback for non-MCP clients like Agy)."""
    if payload_file:
        payload = json.loads(Path(payload_file).read_text(encoding="utf-8"))
    elif payload_json:
        payload = json.loads(payload_json)
    else:
        raise click.ClickException("Provide --payload or --payload-file")

    ws = Path(workspace)
    _auth(ws, token)
    payload = validate_payload(topic, payload)
    store = _open_store(workspace, retention_days)
    try:
        event, duplicate = store.publish(
            topic=topic,
            producer_id=_producer_id(producer_id),
            schema_version=schema_version,
            payload=payload,
            causation_id=causation_id,
            idempotency_key=idempotency_key,
        )
        click.echo(
            json.dumps(
                {
                    "event_id": event.event_id,
                    "topic": event.topic,
                    "timestamp": event.timestamp,
                    "duplicate": duplicate,
                }
            )
        )
    finally:
        store.close()


@main.command()
@click.option("--workspace", default=DEFAULT_WORKSPACE, show_default=True)
@click.option("--topic", required=True)
@click.option("--since-id", type=int, default=0, show_default=True)
@click.option("--limit", type=int, default=50, show_default=True)
@click.option("--retention-days", default=7, show_default=True)
def poll(
    workspace: str,
    topic: str,
    since_id: int,
    limit: int,
    retention_days: int,
) -> None:
    """Fetch events after cursor."""
    store = _open_store(workspace, retention_days)
    try:
        click.echo(json.dumps(store.poll(topic=topic, since_id=since_id, limit=limit)))
    finally:
        store.close()


@main.command()
@click.option("--workspace", default=DEFAULT_WORKSPACE, show_default=True)
@click.option("--producer-id", default=None)
@click.option("--retention-days", default=7, show_default=True)
def status(workspace: str, producer_id: str | None, retention_days: int) -> None:
    """Workspace bus health."""
    store = _open_store(workspace, retention_days)
    try:
        pid = producer_id or os.environ.get("AGENTBUS_PRODUCER_ID", "")
        click.echo(json.dumps(store.status(producer_id=pid or None)))
    finally:
        store.close()


@main.group()
def lock() -> None:
    """Advisory lease locks (Phase 5)."""


@lock.command("acquire")
@click.option("--workspace", default=DEFAULT_WORKSPACE, show_default=True)
@click.option("--resource", required=True, help="Absolute path within workspace")
@click.option("--owner-id", required=True)
@click.option("--ttl-seconds", type=int, default=None)
@click.option("--token", default=None)
def lock_acquire(
    workspace: str,
    resource: str,
    owner_id: str,
    ttl_seconds: int | None,
    token: str | None,
) -> None:
    """Acquire a lease on a resource."""
    ws = Path(workspace)
    _auth(ws, token)
    store = _open_lease_store(workspace)
    try:
        click.echo(json.dumps(store.lock_acquire(resource, owner_id, ttl_seconds)))
    finally:
        store.close()


@lock.command("release")
@click.option("--workspace", default=DEFAULT_WORKSPACE, show_default=True)
@click.option("--resource", required=True)
@click.option("--lease-id", required=True)
@click.option("--owner-id", required=True)
@click.option("--token", default=None)
def lock_release(
    workspace: str,
    resource: str,
    lease_id: str,
    owner_id: str,
    token: str | None,
) -> None:
    """Release a held lease."""
    ws = Path(workspace)
    _auth(ws, token)
    store = _open_lease_store(workspace)
    try:
        click.echo(json.dumps(store.lock_release(resource, lease_id, owner_id)))
    finally:
        store.close()


@lock.command("renew")
@click.option("--workspace", default=DEFAULT_WORKSPACE, show_default=True)
@click.option("--resource", required=True)
@click.option("--lease-id", required=True)
@click.option("--owner-id", required=True)
@click.option("--ttl-seconds", type=int, default=None)
@click.option("--token", default=None)
def lock_renew(
    workspace: str,
    resource: str,
    lease_id: str,
    owner_id: str,
    ttl_seconds: int | None,
    token: str | None,
) -> None:
    """Renew (extend) a held lease."""
    ws = Path(workspace)
    _auth(ws, token)
    store = _open_lease_store(workspace)
    try:
        click.echo(json.dumps(store.lock_renew(resource, lease_id, owner_id, ttl_seconds)))
    finally:
        store.close()


@lock.command("status")
@click.option("--workspace", default=DEFAULT_WORKSPACE, show_default=True)
@click.option("--resource", required=True)
def lock_status(workspace: str, resource: str) -> None:
    """Check lock state (no auth)."""
    store = _open_lease_store(workspace)
    try:
        click.echo(json.dumps(store.lock_status(resource)))
    finally:
        store.close()


@main.command("project-log")
@click.option("--workspace", default=DEFAULT_WORKSPACE, show_default=True)
@click.option(
    "--log-file",
    default="log.md",
    show_default=True,
    help="Log file relative to workspace",
)
@click.option("--dry-run", is_flag=True, help="Print lines without writing log.md")
@click.option("--reset", is_flag=True, help="Re-project all events from event_id 0")
@click.option("--retention-days", default=7, show_default=True)
def project_log(
    workspace: str,
    log_file: str,
    dry_run: bool,
    reset: bool,
    retention_days: int,
) -> None:
    """Project okf/handoff events into OKF log.md format."""
    ws = Path(workspace)
    log_path = ws / log_file
    store = _open_store(workspace, retention_days)
    try:
        result = project_handoffs(
            store, ws, log_path, dry_run=dry_run, reset=reset
        )
        click.echo(json.dumps({k: v for k, v in result.items() if k != "lines"}))
        if dry_run and result["lines"]:
            click.echo("---")
            click.echo("\n\n".join(result["lines"]))
    finally:
        store.close()


@main.group()
def token() -> None:
    """Workspace ephemeral token management."""


@token.command("show")
@click.option("--workspace", default=DEFAULT_WORKSPACE, show_default=True)
@click.option("--quiet", is_flag=True, help="Print token only (no path)")
def token_show(workspace: str, quiet: bool) -> None:
    """Print the workspace publish token."""
    ws = Path(workspace)
    value = read_workspace_token(ws)
    if not value:
        raise click.ClickException(
            f"No token at {token_path(ws)} — run: agentbus token ensure"
        )
    if quiet:
        click.echo(value)
    else:
        click.echo(json.dumps({"path": str(token_path(ws)), "token": value}))


@token.command("ensure")
@click.option("--workspace", default=DEFAULT_WORKSPACE, show_default=True)
@click.option("--quiet", is_flag=True, help="Print token only")
def token_ensure(workspace: str, quiet: bool) -> None:
    """Create workspace token if missing."""
    ws = Path(workspace)
    value = ensure_ephemeral_token(ws, rotate=False)
    if quiet:
        click.echo(value)
    else:
        click.echo(
            json.dumps(
                {"path": str(token_path(ws)), "token": value, "created": True}
            )
        )


@token.command("rotate")
@click.option("--workspace", default=DEFAULT_WORKSPACE, show_default=True)
@click.option("--quiet", is_flag=True, help="Print token only")
def token_rotate(workspace: str, quiet: bool) -> None:
    """Regenerate workspace publish token."""
    ws = Path(workspace)
    value = ensure_ephemeral_token(ws, rotate=True)
    if quiet:
        click.echo(value)
    else:
        click.echo(
            json.dumps(
                {"path": str(token_path(ws)), "token": value, "rotated": True}
            )
        )


@main.command()
@click.option("--workspace", default=None, help="Workspace root (default: git root or cwd)")
@click.option("--producer-id", default=None, help="MCP producer id for this client")
@click.option("--apply", is_flag=True, help="Write MCP config updates (default: dry-run)")
@click.option("--client", "clients", multiple=True, help="Limit to client id (repeatable)")
def init(
    workspace: str | None,
    producer_id: str | None,
    apply: bool,
    clients: tuple[str, ...],
) -> None:
    """Auto-discover MCP clients and wire agentbus (idempotent)."""
    try:
        ws = resolve_workspace(workspace)
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc
    pid = _producer_id(producer_id)
    result = apply_init(
        ws,
        producer_id=pid,
        dry_run=not apply,
        clients=list(clients) if clients else None,
    )
    click.echo(format_init_summary(result))


@main.command()
@click.option("--workspace", default=None, help="Workspace root (default: git root or cwd)")
@click.option("--topic", default=None, help="Filter by topic")
@click.option("--interval", default=1.0, show_default=True, help="Refresh seconds")
@click.option("--once", is_flag=True, help="Print snapshot and exit")
@click.option("--retention-days", default=7, show_default=True)
def monitor(
    workspace: str | None,
    topic: str | None,
    interval: float,
    once: bool,
    retention_days: int,
) -> None:
    """Tail events.db (rich TUI when available)."""
    try:
        ws = resolve_workspace(workspace)
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc
    run_monitor(ws, topic=topic, interval=interval, once=once, retention_days=retention_days)


@main.command()
@click.option("--workspace", default=DEFAULT_WORKSPACE, show_default=True)
@click.option("--producer-id", default=None)
@click.option("--retention-days", default=7, show_default=True)
def ping(workspace: str, producer_id: str | None, retention_days: int) -> None:
    """Publish a synthetic okf/handoff PING event."""
    ws = Path(workspace)
    pid = _producer_id(producer_id)
    try:
        result = publish_ping(ws, producer_id=pid, retention_days=retention_days)
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(json.dumps(result))


if __name__ == "__main__":
    main()