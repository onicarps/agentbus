"""AgentBus CLI."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import click

from agentbus.auth import (
    check_publish_token,
    ensure_ephemeral_token,
    read_workspace_token,
    token_path,
)
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


@click.group()
def main() -> None:
    """Local MCP event log for multi-agent workspaces."""


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
    run_stdio(
        Path(workspace),
        retention_days=retention_days,
        rotate_token=rotate_token,
    )


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
    try:
        check_publish_token(ws, auth_token=token)
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc

    validate_payload(topic, payload)
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
                {
                    "path": str(token_path(ws)),
                    "token": value,
                    "created": True,
                }
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
                {
                    "path": str(token_path(ws)),
                    "token": value,
                    "rotated": True,
                }
            )
        )


if __name__ == "__main__":
    main()