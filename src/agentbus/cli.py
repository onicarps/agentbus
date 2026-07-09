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
from agentbus.intercepts import InterceptRule, add_rule, load_config
from agentbus.artifacts import PayloadTooLargeError, artifact_from_file
from agentbus.rbac import ForbiddenError, assign_producer_role, ensure_default_roles, mint_droid_proof
from agentbus.leases import LeaseStore
from agentbus.project_log import project_handoffs
from agentbus.schema_registry import import_schema_file, list_schemas, register_schema
from agentbus.schemas import set_validation_workspace, validate_payload
from agentbus.server import run_stdio
from agentbus.store import EventStore
from agentbus.workspace_config import resolve_retention_days

def _cli_workspace(workspace: str | None) -> Path:
    if workspace:
        return resolve_workspace(workspace)
    env = os.environ.get("AGENTBUS_WORKSPACE")
    if env:
        return Path(env).resolve()
    return resolve_workspace()


def _producer_id(override: str | None) -> str:
    pid = override or os.environ.get("AGENTBUS_PRODUCER_ID", "")
    if not pid:
        raise click.ClickException("Set --producer-id or AGENTBUS_PRODUCER_ID")
    return pid


def _open_store(workspace: str | None, retention_days: int) -> EventStore:
    ws = _cli_workspace(workspace)
    set_validation_workspace(ws)
    days = resolve_retention_days(ws, retention_days)
    return EventStore(ws, retention_days=days)


def _open_lease_store(workspace: str | None) -> LeaseStore:
    return LeaseStore(_cli_workspace(workspace))


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
    default=None,
    envvar="AGENTBUS_WORKSPACE",
)
@click.option("--retention-days", default=7, show_default=True)
@click.option(
    "--rotate-token",
    is_flag=True,
    help="Regenerate workspace token on startup",
)
def mcp_serve(workspace: str | None, retention_days: int, rotate_token: bool) -> None:
    """MCP entrypoint for IDE configs (ensures token, then serve)."""
    ws = _cli_workspace(workspace)
    ensure_ephemeral_token(ws, rotate=rotate_token)
    os.environ["AGENTBUS_TOKEN"] = read_workspace_token(ws) or ""
    run_stdio(ws, retention_days=retention_days, rotate_token=False)


@main.command()
@click.option(
    "--workspace",
    type=click.Path(exists=True, file_okay=False, path_type=str),
    default=None,
    envvar="AGENTBUS_WORKSPACE",
)
@click.option("--retention-days", default=7, show_default=True)
@click.option(
    "--rotate-token",
    is_flag=True,
    help="Regenerate workspace token on startup",
)
def serve(workspace: str | None, retention_days: int, rotate_token: bool) -> None:
    """Run MCP server on stdio."""
    run_stdio(_cli_workspace(workspace), retention_days=retention_days, rotate_token=rotate_token)


@main.command()
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
@click.option("--topic", required=True)
@click.option("--payload", "payload_json", default=None, help="JSON object string")
@click.option("--payload-file", type=click.Path(exists=True, dir_okay=False), default=None)
@click.option(
    "--attach",
    multiple=True,
    type=click.Path(exists=True, dir_okay=False),
    help="Attach file as artifact (repeatable)",
)
@click.option("--schema-version", default="1.0", show_default=True)
@click.option("--producer-id", default=None)
@click.option("--causation-id", type=int, default=None)
@click.option("--idempotency-key", default=None)
@click.option(
    "--sla-timeout-minutes",
    type=int,
    default=None,
    help="SLA window; auto-escalate to okf/dead-letter if no causation_id reply",
)
@click.option("--trace-id", default=None, help="Distributed trace ID (W3C-style lineage)")
@click.option("--parent-span-id", default=None, help="Parent span for trace waterfall")
@click.option("--token", default=None, help="Publish auth token (default: workspace file)")
@click.option("--retention-days", default=7, show_default=True)
def publish(
    workspace: str,
    topic: str,
    payload_json: str | None,
    payload_file: str | None,
    attach: tuple[str, ...],
    schema_version: str,
    producer_id: str | None,
    causation_id: int | None,
    idempotency_key: str | None,
    sla_timeout_minutes: int | None,
    trace_id: str | None,
    parent_span_id: str | None,
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

    ws = _cli_workspace(workspace)
    _auth(ws, token)
    try:
        if attach:
            arts = list(payload.get("artifacts") or [])
            for path in attach:
                arts.append(artifact_from_file(Path(path)))
            payload["artifacts"] = arts
        payload = validate_payload(topic, payload, workspace=ws)
    except PayloadTooLargeError as exc:
        raise click.ClickException(str(exc)) from exc
    store = _open_store(workspace, retention_days)
    try:
        try:
            event, duplicate = store.publish(
                topic=topic,
                producer_id=_producer_id(producer_id),
                schema_version=schema_version,
                payload=payload,
                causation_id=causation_id,
                idempotency_key=idempotency_key,
                auth_token=token,
                sla_timeout_minutes=sla_timeout_minutes,
                trace_id=trace_id,
                parent_span_id=parent_span_id,
            )
        except (ForbiddenError, PayloadTooLargeError) as exc:
            raise click.ClickException(str(exc)) from exc
        out = {
            "event_id": event.event_id,
            "topic": event.topic,
            "timestamp": event.timestamp,
            "duplicate": duplicate,
        }
        if event.span_id:
            out["span_id"] = event.span_id
        if event.trace_id:
            out["trace_id"] = event.trace_id
        click.echo(json.dumps(out))
    finally:
        store.close()


@main.command("publish-batch")
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
@click.option(
    "--file",
    "batch_file",
    required=True,
    type=click.Path(exists=True, dir_okay=False),
    help="JSONL file: one publish spec per line",
)
@click.option("--producer-id", default=None)
@click.option("--token", default=None)
@click.option("--retention-days", default=7, show_default=True)
def publish_batch(
    workspace: str | None,
    batch_file: str,
    producer_id: str | None,
    token: str | None,
    retention_days: int,
) -> None:
    """Publish many events in one process (faster than repeated CLI subprocesses)."""
    ws = _cli_workspace(workspace)
    _auth(ws, token)
    pid = _producer_id(producer_id)
    store = _open_store(workspace, retention_days)
    results: list[dict] = []
    try:
        for line_no, line in enumerate(Path(batch_file).read_text(encoding="utf-8").splitlines(), 1):
            if not line.strip():
                continue
            try:
                spec = json.loads(line)
            except json.JSONDecodeError as exc:
                raise click.ClickException(f"line {line_no}: invalid JSON") from exc
            topic = spec.get("topic")
            payload = spec.get("payload")
            if not topic or not isinstance(payload, dict):
                raise click.ClickException(f"line {line_no}: require topic and payload object")
            payload = validate_payload(topic, payload, workspace=ws)
            try:
                event, duplicate = store.publish(
                    topic=topic,
                    producer_id=spec.get("producer_id") or pid,
                    schema_version=spec.get("schema_version", "1.0"),
                    payload=payload,
                    causation_id=spec.get("causation_id"),
                    idempotency_key=spec.get("idempotency_key"),
                    auth_token=token,
                    sla_timeout_minutes=spec.get("sla_timeout_minutes"),
                    trace_id=spec.get("trace_id"),
                    parent_span_id=spec.get("parent_span_id"),
                )
            except (ForbiddenError, PayloadTooLargeError) as exc:
                raise click.ClickException(str(exc)) from exc
            results.append(
                {
                    "line": line_no,
                    "event_id": event.event_id,
                    "duplicate": duplicate,
                    "topic": event.topic,
                }
            )
        click.echo(json.dumps({"count": len(results), "events": results}))
    finally:
        store.close()


@main.command()
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
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
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
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


@main.group(invoke_without_command=True)
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
@click.option("--retention-days", default=7, show_default=True)
@click.pass_context
def sla(ctx: click.Context, workspace: str, retention_days: int) -> None:
    """SLA timeout management."""
    ctx.obj = {"workspace": workspace, "retention_days": retention_days}
    if ctx.invoked_subcommand is None:
        ctx.invoke(sla_list)


@sla.command("list")
@click.pass_context
def sla_list(ctx: click.Context) -> None:
    """List active SLA deadlines."""
    opts = ctx.obj or {}
    store = _open_store(opts.get("workspace"), opts.get("retention_days", 7))
    try:
        click.echo(json.dumps(store.list_active_slas()))
    finally:
        store.close()


@sla.command("clear")
@click.pass_context
@click.argument("event_id", type=int)
def sla_clear(ctx: click.Context, event_id: int) -> None:
    """Clear an active SLA deadline."""
    opts = ctx.obj or {}
    store = _open_store(opts.get("workspace"), opts.get("retention_days", 7))
    try:
        store._clear_sla(event_id)
        click.echo(json.dumps({"event_id": event_id, "sla_cleared": True}))
    finally:
        store.close()


@main.group()
def lock() -> None:
    """Advisory lease locks (Phase 5)."""


@lock.command("acquire")
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
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
    ws = _cli_workspace(workspace)
    _auth(ws, token)
    store = _open_lease_store(workspace)
    try:
        click.echo(json.dumps(store.lock_acquire(resource, owner_id, ttl_seconds)))
    finally:
        store.close()


@lock.command("release")
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
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
    ws = _cli_workspace(workspace)
    _auth(ws, token)
    store = _open_lease_store(workspace)
    try:
        click.echo(json.dumps(store.lock_release(resource, lease_id, owner_id)))
    finally:
        store.close()


@lock.command("renew")
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
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
    ws = _cli_workspace(workspace)
    _auth(ws, token)
    store = _open_lease_store(workspace)
    try:
        click.echo(json.dumps(store.lock_renew(resource, lease_id, owner_id, ttl_seconds)))
    finally:
        store.close()


@lock.command("status")
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
@click.option("--resource", required=True)
def lock_status(workspace: str, resource: str) -> None:
    """Check lock state (no auth)."""
    store = _open_lease_store(workspace)
    try:
        click.echo(json.dumps(store.lock_status(resource)))
    finally:
        store.close()


@main.command("project-log")
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
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
    ws = _cli_workspace(workspace)
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
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
@click.option("--quiet", is_flag=True, help="Print token only (no path)")
def token_show(workspace: str, quiet: bool) -> None:
    """Print the workspace publish token."""
    ws = _cli_workspace(workspace)
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
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
@click.option("--quiet", is_flag=True, help="Print token only")
def token_ensure(workspace: str, quiet: bool) -> None:
    """Create workspace token if missing."""
    ws = _cli_workspace(workspace)
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
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
@click.option("--quiet", is_flag=True, help="Print token only")
def token_rotate(workspace: str, quiet: bool) -> None:
    """Regenerate workspace publish token."""
    ws = _cli_workspace(workspace)
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
@click.option("--plain", is_flag=True, help="Plain/rich poll loop (no Textual TUI)")
@click.option("--retention-days", default=7, show_default=True)
def monitor(
    workspace: str | None,
    topic: str | None,
    interval: float,
    once: bool,
    plain: bool,
    retention_days: int,
) -> None:
    """Mission-control TUI (Textual) or tail events.db."""
    try:
        ws = resolve_workspace(workspace)
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc
    run_monitor(
        ws,
        topic=topic,
        interval=interval,
        once=once,
        plain=plain,
        retention_days=retention_days,
    )


@main.group()
def config() -> None:
    """Workspace intercept rules (HITL)."""


@config.command("set-intercept")
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
@click.option("--topic", required=True)
@click.option("--contains", required=True, help="Substring match in JSON payload")
@click.option("--ttl-minutes", default=60, show_default=True)
def config_set_intercept(
    workspace: str, topic: str, contains: str, ttl_minutes: int
) -> None:
    """Add or update an intercept rule (matched events require human approval)."""
    ws = _cli_workspace(workspace)
    rule = InterceptRule(topic=topic, contains=contains, ttl_minutes=ttl_minutes)
    config_data = add_rule(ws, rule)
    click.echo(json.dumps({"path": str(ws / ".agentbus" / "intercepts.json"), "rules": config_data.to_dict()["rules"]}))


@config.command("list-intercepts")
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
def config_list_intercepts(workspace: str) -> None:
    """List configured intercept rules."""
    ws = _cli_workspace(workspace)
    click.echo(json.dumps(load_config(ws).to_dict()))


@config.command("init-rbac")
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
def config_init_rbac(workspace: str) -> None:
    """Install default .agentbus/roles.yaml (Swarm RBAC)."""
    ws = _cli_workspace(workspace)
    config = ensure_default_roles(ws)
    click.echo(
        json.dumps(
            {
                "path": str(ws / ".agentbus" / "roles.yaml"),
                "producers": config.producers,
                "roles": list(config.roles.keys()),
            }
        )
    )


@main.group()
def droid() -> None:
    """Factory droid cryptographic proof tokens."""


@droid.command("mint")
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
@click.option("--mission-id", default=None)
@click.option("--ttl-minutes", default=30, show_default=True)
def droid_mint(workspace: str, mission_id: str | None, ttl_minutes: int) -> None:
    """Mint a short-lived droid_proof for qa_droid role publishes."""
    ws = _cli_workspace(workspace)
    click.echo(json.dumps(mint_droid_proof(ws, mission_id=mission_id, ttl_minutes=ttl_minutes)))


@main.command()
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
@click.option("--topic", default=None, help="Filter by topic")
@click.option("--limit", default=50, show_default=True)
@click.option("--retention-days", default=7, show_default=True)
def review(workspace: str, topic: str | None, limit: int, retention_days: int) -> None:
    """List events pending human approval (hidden from agent poll)."""
    store = _open_store(workspace, retention_days)
    try:
        click.echo(json.dumps(store.review_pending(topic=topic, limit=limit)))
    finally:
        store.close()


@main.command()
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
@click.argument("event_id", type=int)
@click.option("--reviewer-id", default=None)
@click.option("--retention-days", default=7, show_default=True)
def approve(
    workspace: str, event_id: int, reviewer_id: str | None, retention_days: int
) -> None:
    """Approve a pending event — makes it visible to agent poll."""
    store = _open_store(workspace, retention_days)
    try:
        rid = reviewer_id or os.environ.get("AGENTBUS_PRODUCER_ID", "human")
        click.echo(
            json.dumps(store.approve_event(event_id, reviewer_id=rid, auth_token=None))
        )
    except (ValueError, ForbiddenError) as exc:
        raise click.ClickException(str(exc)) from exc
    finally:
        store.close()


@main.command()
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
@click.argument("event_id", type=int)
@click.option("--reason", default="rejected by human reviewer", show_default=True)
@click.option("--reviewer-id", default=None)
@click.option("--retention-days", default=7, show_default=True)
def reject(
    workspace: str,
    event_id: int,
    reason: str,
    reviewer_id: str | None,
    retention_days: int,
) -> None:
    """Reject a pending event and notify the originating agent."""
    store = _open_store(workspace, retention_days)
    try:
        rid = reviewer_id or os.environ.get("AGENTBUS_PRODUCER_ID", "human")
        click.echo(
            json.dumps(
                store.reject_event(event_id, reviewer_id=rid, reason=reason, auth_token=None)
            )
        )
    except (ValueError, ForbiddenError) as exc:
        raise click.ClickException(str(exc)) from exc
    finally:
        store.close()


@main.group()
def schema() -> None:
    """Pluggable topic schema registry."""


@schema.command("import")
@click.argument("file", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
def schema_import(file: Path, workspace: str) -> None:
    """Import topic schema from JSON (topic + json_schema)."""
    try:
        click.echo(json.dumps(import_schema_file(_cli_workspace(workspace), file)))
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc


@schema.command("register")
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
@click.option("--topic", required=True)
@click.option("--schema-file", type=click.Path(exists=True, dir_okay=False), required=True)
@click.option("--version", default="1.0", show_default=True)
def schema_register(workspace: str, topic: str, schema_file: str, version: str) -> None:
    """Register a topic JSON Schema from a file."""
    schema_obj = json.loads(Path(schema_file).read_text(encoding="utf-8"))
    try:
        click.echo(
            json.dumps(
                register_schema(_cli_workspace(workspace), topic, schema_obj, version=version)
            )
        )
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc


@schema.command("list")
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
def schema_list(workspace: str) -> None:
    """List registered pluggable topic schemas."""
    click.echo(json.dumps(list_schemas(_cli_workspace(workspace))))


@main.command()
@click.argument("trace_id")
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
@click.option("--retention-days", default=7, show_default=True)
def trace(workspace: str, trace_id: str, retention_days: int) -> None:
    """Render a hierarchical trace waterfall for a trace_id."""
    from agentbus.tracing import build_trace_tree, render_trace_tree

    store = _open_store(workspace, retention_days)
    try:
        events = store.fetch_trace_events(trace_id)
        roots = build_trace_tree(events)
        try:
            click.echo(render_trace_tree(trace_id, roots))
        except ImportError as exc:
            raise click.ClickException(
                "rich required for trace visualization — pip install 'okf-agentbus[devex]'"
            ) from exc
    finally:
        store.close()


@main.command()
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
@click.option("--producer-id", default=None)
@click.option("--retention-days", default=7, show_default=True)
def ping(workspace: str, producer_id: str | None, retention_days: int) -> None:
    """Publish a synthetic okf/handoff PING event."""
    ws = _cli_workspace(workspace)
    pid = _producer_id(producer_id)
    try:
        result = publish_ping(ws, producer_id=pid, retention_days=retention_days)
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(json.dumps(result))


if __name__ == "__main__":
    main()