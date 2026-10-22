"""AgentBus CLI."""

from __future__ import annotations

import contextlib
import importlib
import io
import json
import logging
import os
import sqlite3
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

import click

if TYPE_CHECKING:
    from agentbus.client import EventTransport
    from agentbus.devex import InitResult


def _lazy(module: str, name: str):
    """Return a call-through that imports a command dependency on first use."""

    def invoke(*args, **kwargs):
        return getattr(importlib.import_module(module), name)(*args, **kwargs)

    invoke.__name__ = name
    return invoke


def _exception(module: str, name: str):
    return getattr(importlib.import_module(module), name)


def _identity_error():
    return _exception("agentbus.errors", "IdentityError")


def _payload_too_large_error():
    return _exception("agentbus.artifacts", "PayloadTooLargeError")


def _access_denied_error():
    return _exception("agentbus.mcpsafe", "AccessDeniedError")


def _forbidden_error():
    return _exception("agentbus.rbac", "ForbiddenError")


def _workspace_error():
    return _exception("agentbus.scaffold", "WorkspaceError")


def _invalid_mcp_config_error():
    return _exception("agentbus.devex", "InvalidMcpConfigError")


check_publish_token = _lazy("agentbus.auth", "check_publish_token")
ensure_ephemeral_token = _lazy("agentbus.auth", "ensure_ephemeral_token")
read_workspace_token = _lazy("agentbus.auth", "read_workspace_token")
token_path = _lazy("agentbus.auth", "token_path")
apply_init = _lazy("agentbus.devex", "apply_init")
format_init_summary = _lazy("agentbus.devex", "format_init_summary")
publish_ping = _lazy("agentbus.devex", "publish_ping")
resolve_workspace = _lazy("agentbus.devex", "resolve_workspace")
run_monitor = _lazy("agentbus.devex", "run_monitor")
InterceptRule = _lazy("agentbus.intercepts", "InterceptRule")
add_rule = _lazy("agentbus.intercepts", "add_rule")
intercept_config_path = _lazy("agentbus.intercepts", "config_path")
load_config = _lazy("agentbus.intercepts", "load_config")
bootstrap_workspace_identity = _lazy("agentbus.identity", "bootstrap_workspace_identity")
configure_reference_monitor = _lazy("agentbus.identity", "configure_reference_monitor")
delegate_identity = _lazy("agentbus.identity", "delegate_identity")
enroll_identity = _lazy("agentbus.identity", "enroll_identity")
issue_wake_capability = _lazy("agentbus.identity", "issue_wake_capability")
load_trust_state = _lazy("agentbus.identity", "load_trust_state")
record_break_glass_recovery = _lazy("agentbus.identity", "record_break_glass_recovery")
revoke_identity_key = _lazy("agentbus.identity", "revoke_identity_key")
rotate_identity = _lazy("agentbus.identity", "rotate_identity")
set_policy_mode = _lazy("agentbus.identity", "set_policy_mode")
strict_json_loads = _lazy("agentbus.jsonutil", "strict_json_loads")
export_policy_request = _lazy("agentbus.ceremony", "export_policy_request")
generate_enrollment_request = _lazy("agentbus.ceremony", "generate_enrollment_request")
generate_offline_root = _lazy("agentbus.ceremony", "generate_offline_root")
import_signed_policy = _lazy("agentbus.ceremony", "import_signed_policy")
initialize_offline_identity = _lazy("agentbus.ceremony", "initialize_offline_identity")
sign_policy_request = _lazy("agentbus.ceremony", "sign_policy_request")
artifact_from_file = _lazy("agentbus.artifacts", "artifact_from_file")
ensure_default_roles = _lazy("agentbus.rbac", "ensure_default_roles")
mint_droid_proof = _lazy("agentbus.rbac", "mint_droid_proof")
LeaseStore = _lazy("agentbus.leases", "LeaseStore")
distill_workspace = _lazy("agentbus.distill", "distill_workspace")
project_handoffs = _lazy("agentbus.project_log", "project_handoffs")
ensure_runtime_ignored = _lazy("agentbus.scaffold", "ensure_runtime_ignored")
scaffold_workspace = _lazy("agentbus.scaffold", "scaffold_workspace")
import_schema_file = _lazy("agentbus.schema_registry", "import_schema_file")
list_schemas = _lazy("agentbus.schema_registry", "list_schemas")
register_schema = _lazy("agentbus.schema_registry", "register_schema")
set_validation_workspace = _lazy("agentbus.schemas", "set_validation_workspace")
validate_payload = _lazy("agentbus.schemas", "validate_payload")
open_event_transport = _lazy("agentbus.client", "open_event_transport")
EventStore = _lazy("agentbus.store", "EventStore")
resolve_retention_days = _lazy("agentbus.workspace_config", "resolve_retention_days")
run_tail = _lazy("agentbus.tail", "run_tail")
run_watch = _lazy("agentbus.watch", "run_watch")
list_processes = _lazy("agentbus.swarm", "list_processes")
load_swarm_config = _lazy("agentbus.swarm", "load_swarm_config")
stop_all = _lazy("agentbus.swarm", "stop_all")
swarm_up = _lazy("agentbus.swarm", "swarm_up")
swarm_yaml_path = _lazy("agentbus.swarm", "swarm_yaml_path")
tail_service_logs = _lazy("agentbus.swarm", "tail_service_logs")
write_example_swarm = _lazy("agentbus.swarm", "write_example_swarm")

MANIFEST_PATH = Path("okf/.scaffold-manifest.json")


COMPANION_PRODUCER_ID = "local-user"
COMPANION_HITL_MARKER = '"requires_approval": true'


def _cli_workspace(workspace: str | None, create: bool = False) -> Path:
    from agentbus.workspace_guard import assert_workspace_supported

    if workspace:
        explicit = Path(workspace).expanduser().resolve()
        if not explicit.is_dir():
            # init is the bootstrap command: it may create the target dir
            # (lab finding: cold-start demanded a pre-existing directory).
            if create:
                try:
                    explicit.mkdir(parents=True, exist_ok=True)
                    click.echo(f"created workspace directory: {explicit}")
                except OSError as exc:
                    raise ValueError(
                        f"Workspace not found and could not be created: {explicit} ({exc})"
                    ) from exc
            else:
                raise ValueError(
                    f"Workspace not found: {explicit} (create it first, or run "
                    "init with --apply to create it)"
                )
        return assert_workspace_supported(explicit)
    env = os.environ.get("AGENTBUS_WORKSPACE")
    if env:
        # Env may point at an absolute tree that is not a git root; still guard FS.
        return assert_workspace_supported(Path(env).expanduser())
    return resolve_workspace()


def _producer_id(override: str | None) -> str:
    pid = override or os.environ.get("AGENTBUS_PRODUCER_ID", "")
    if not pid:
        raise click.ClickException("Set --producer-id or AGENTBUS_PRODUCER_ID")
    return pid


def _open_store(workspace: str | None, retention_days: int) -> EventTransport:
    ws = _cli_workspace(workspace)
    set_validation_workspace(ws)
    days = resolve_retention_days(ws, retention_days)
    return open_event_transport(ws, retention_days=days)


def _open_lease_store(workspace: str | None) -> LeaseStore:
    return LeaseStore(_cli_workspace(workspace))


def _auth(ws: Path, token: str | None) -> None:
    try:
        check_publish_token(ws, auth_token=token)
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc


def _resolve_go_serve_binary() -> Path:
    """Locate agentbus-go-serve: env → wheel bundle → go-core/dev → PATH."""
    from agentbus.bin_resolve import resolve_go_binary

    here = Path(__file__).resolve()
    try:
        return resolve_go_binary(
            "agentbus-go-serve",
            env_var="AGENTBUS_GO_SERVE",
            dev_candidates=[
                here.parents[2] / "go-core" / "bin" / "agentbus-go-serve",
                Path.cwd() / "go-core" / "bin" / "agentbus-go-serve",
                Path.cwd() / "bin" / "agentbus-go-serve",
            ],
        )
    except FileNotFoundError as exc:
        raise click.ClickException(str(exc)) from exc


def _exec_go_serve(workspace: Path) -> None:
    """Replace process with Go MCP stdio server for workspace."""
    binary = _resolve_go_serve_binary()
    env = os.environ.copy()
    env["AGENTBUS_WORKSPACE"] = str(workspace.resolve())
    os.execve(str(binary), [str(binary)], env)


def _resolve_go_worker_binary() -> Path:
    """Locate agentbus-go-worker: env → wheel bundle → go-core/dev → PATH."""
    from agentbus.bin_resolve import resolve_go_binary

    here = Path(__file__).resolve()
    try:
        return resolve_go_binary(
            "agentbus-go-worker",
            env_var="AGENTBUS_GO_WORKER",
            dev_candidates=[
                here.parents[2] / "go-core" / "bin" / "agentbus-go-worker",
                Path.cwd() / "go-core" / "bin" / "agentbus-go-worker",
                Path.cwd() / "bin" / "agentbus-go-worker",
            ],
        )
    except FileNotFoundError as exc:
        raise click.ClickException(str(exc)) from exc


def _run_go_worker(workspace: Path, *args: str) -> None:
    """Run agentbus-go-worker as subprocess (inherit stdio) or exec for long-run."""
    import subprocess

    binary = _resolve_go_worker_binary()
    env = os.environ.copy()
    env["AGENTBUS_WORKSPACE"] = str(workspace.resolve())
    cmd = [str(binary), "--workspace", str(workspace.resolve()), *args]
    # long-running "up" replaces process
    is_up = any(a == "up" for a in args)
    if is_up:
        os.execve(str(binary), cmd, env)
    proc = subprocess.run(cmd, env=env)
    raise SystemExit(proc.returncode)


def _configure_cli_logging(*, quiet: bool) -> None:
    """Keep MCP stdout clean: logs always go to stderr; quiet raises threshold."""
    level = logging.CRITICAL if quiet else logging.WARNING
    root = logging.getLogger()
    root.handlers.clear()
    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(level)
    handler.setFormatter(logging.Formatter("%(levelname)s %(name)s: %(message)s"))
    root.addHandler(handler)
    root.setLevel(level)


def _is_interactive_terminal() -> bool:
    """Return whether companion mode can safely prompt or mount a TUI."""
    return sys.stdin.isatty() and sys.stdout.isatty()


def _companion_producer_id() -> str:
    """Choose a stable first-run identity without requiring another prompt."""
    return os.environ.get("AGENTBUS_PRODUCER_ID", "").strip() or COMPANION_PRODUCER_ID


def _run_companion(ctx: click.Context, *, quiet: bool) -> None:
    """Route a bare invocation to onboarding, monitor, or headless status."""
    try:
        ws = _cli_workspace(None)
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc

    # Quiet mode is machine-oriented even on a TTY: no prompts or TUI, JSON only.
    interactive = _is_interactive_terminal() and not quiet
    # v0.26's locked launcher contract intentionally defines this database as
    # the initialization boundary, including stores created by another command.
    initialized = (ws / ".agentbus" / "events.db").is_file()

    if initialized:
        if not interactive:
            ctx.invoke(
                status,
                workspace=str(ws),
                producer_id=None,
                retention_days=30,
            )
            return
        try:
            ctx.invoke(
                monitor,
                workspace=str(ws),
                topic=None,
                interval=0.5,
                once=False,
                plain=False,
                retention_days=30,
                tz=None,
            )
        except (click.exceptions.Exit, click.Abort, click.ClickException):
            raise
        except Exception as exc:
            click.echo(
                "Mission-control monitor unavailable; showing workspace status: "
                f"{exc}",
                err=True,
            )
            ctx.invoke(
                status,
                workspace=str(ws),
                producer_id=None,
                retention_days=30,
            )
        return

    if interactive:
        click.secho("AgentBus companion setup", bold=True)
        click.echo(f"Workspace: {ws}")
        scaffold_okf = click.confirm(
            "Scaffold OKF v0.2 knowledge plane (AGENTS.md, okf/status.md)?",
            default=True,
        )
        enable_hitl = click.confirm(
            "Enable Human-In-The-Loop (HITL) approval intercepts?",
            default=False,
        )
        discover_mcp = click.confirm(
            "Auto-wire detected MCP configs (Cursor, Claude Desktop, Gemini, VS Code)?",
            default=True,
        )
    else:
        scaffold_okf = True
        enable_hitl = False
        discover_mcp = True

    emit_json = quiet or not interactive
    invocation = {
        "workspace": str(ws),
        "producer_id": _companion_producer_id(),
        "apply": True,
        # Headless discovery is intentionally project-scoped: a pipeline in
        # project B must never retarget a user-global IDE config from project A.
        "clients": () if interactive else ("cursor-project", "vscode"),
        "okf": scaffold_okf,
        "bare": False,
        "project_name": None,
        "mcp": discover_mcp,
        "hitl": enable_hitl,
    }
    if emit_json:
        with contextlib.redirect_stdout(io.StringIO()):
            init_result = ctx.invoke(init, **invocation)
    else:
        init_result = ctx.invoke(init, **invocation)

    summary = {
        "action": "initialized",
        "workspace": str(ws),
        "producer_id": invocation["producer_id"],
        "events_db": str(ws / ".agentbus" / "events.db"),
        "okf_scaffolded": (ws / MANIFEST_PATH).is_file(),
        "hitl_intercepts_enabled": enable_hitl,
        "mcp_discovery_enabled": discover_mcp,
        "mcp_scope": (
            "all" if interactive else "workspace"
        ) if discover_mcp else "disabled",
        "mcp_updates": init_result.updated,
        "mcp_backups": init_result.backups,
        "next": ["agentbus up", "agentbus monitor"],
    }
    if emit_json:
        click.echo(json.dumps(summary, sort_keys=True))
    else:
        click.secho("AgentBus is ready.", fg="green", bold=True)
        click.echo("Next: agentbus up  (services) or agentbus monitor  (mission control)")


@click.group(invoke_without_command=True)
@click.option(
    "-q",
    "--quiet",
    is_flag=True,
    help="Suppress non-critical logs (MCP/CI-safe; logs stay on stderr).",
)
@click.version_option(
    version=None,
    package_name="okf-agentbus",
    prog_name="agentbus",
    message="%(prog)s %(version)s",
)
@click.pass_context
def main(ctx: click.Context, quiet: bool) -> None:
    """Local MCP event log for multi-agent workspaces."""
    ctx.ensure_object(dict)
    ctx.obj["quiet"] = quiet

    prev_env = os.environ.get("AGENTBUS_QUIET")
    prev_level = logging.getLogger().level
    prev_handlers = list(logging.getLogger().handlers)

    if quiet:
        os.environ["AGENTBUS_QUIET"] = "1"
    _configure_cli_logging(quiet=quiet)

    def _restore_logging() -> None:
        if prev_env is None:
            os.environ.pop("AGENTBUS_QUIET", None)
        else:
            os.environ["AGENTBUS_QUIET"] = prev_env
        root = logging.getLogger()
        root.handlers.clear()
        for h in prev_handlers:
            root.addHandler(h)
        root.setLevel(prev_level)

    ctx.call_on_close(_restore_logging)

    if ctx.invoked_subcommand is None:
        _run_companion(ctx, quiet=quiet)


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
@click.option(
    "--wiretap",
    is_flag=True,
    help="God View: publish system/mcp for every tools/call (opt-in)",
)
@click.option(
    "--wiretap-log",
    type=click.Path(dir_okay=False, path_type=str),
    default=None,
    help="Optional JSONL path for wiretap frames (default: .agentbus/wiretap.jsonl)",
)
@click.option(
    "--enable-mcpsafe",
    is_flag=True,
    default=False,
    help="Enforce .mcpsafe.lock tool policy on MCP tools/publish payloads",
)
@click.option(
    "--mcpsafe-lock",
    type=click.Path(dir_okay=False, path_type=str),
    default=None,
    envvar="AGENTBUS_MCPSAFE_LOCK",
    help="Path to .mcpsafe.lock (default: <workspace>/.mcpsafe.lock)",
)
@click.option(
    "--engine",
    type=click.Choice(["python", "go"], case_sensitive=False),
    default=None,
    envvar="AGENTBUS_ENGINE",
    help="Serve engine: python (default) or go (Strangler sidecar spike)",
)
def mcp_serve(
    workspace: str | None,
    retention_days: int,
    rotate_token: bool,
    wiretap: bool,
    wiretap_log: str | None,
    enable_mcpsafe: bool,
    mcpsafe_lock: str | None,
    engine: str | None,
) -> None:
    """MCP entrypoint for IDE configs (ensures token, then serve)."""
    from agentbus.server import run_stdio

    ws = _cli_workspace(workspace)
    ensure_ephemeral_token(ws, rotate=rotate_token)
    os.environ["AGENTBUS_TOKEN"] = read_workspace_token(ws) or ""
    if (engine or "python").lower() == "go":
        _exec_go_serve(ws)
        return
    run_stdio(
        ws,
        retention_days=retention_days,
        rotate_token=False,
        wiretap=wiretap,
        wiretap_log=wiretap_log,
        enable_mcpsafe=enable_mcpsafe or None,
        mcpsafe_lock=mcpsafe_lock,
    )


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
@click.option("--wiretap", is_flag=True, help="God View wiretap (system/mcp events)")
@click.option(
    "--wiretap-log", type=click.Path(dir_okay=False, path_type=str), default=None
)
@click.option(
    "--enable-mcpsafe",
    is_flag=True,
    default=False,
    help="Enforce .mcpsafe.lock tool policy on MCP tools/publish payloads",
)
@click.option(
    "--mcpsafe-lock",
    type=click.Path(dir_okay=False, path_type=str),
    default=None,
    envvar="AGENTBUS_MCPSAFE_LOCK",
    help="Path to .mcpsafe.lock (default: <workspace>/.mcpsafe.lock)",
)
@click.option(
    "--engine",
    type=click.Choice(["python", "go"], case_sensitive=False),
    default=None,
    envvar="AGENTBUS_ENGINE",
    help="Serve engine: python (default) or go (Strangler sidecar spike)",
)
def serve(
    workspace: str | None,
    retention_days: int,
    rotate_token: bool,
    wiretap: bool,
    wiretap_log: str | None,
    enable_mcpsafe: bool,
    mcpsafe_lock: str | None,
    engine: str | None,
) -> None:
    """Run MCP server on stdio."""
    from agentbus.server import run_stdio

    ws = _cli_workspace(workspace)
    if (engine or "python").lower() == "go":
        ensure_ephemeral_token(ws, rotate=rotate_token)
        _exec_go_serve(ws)
        return
    run_stdio(
        ws,
        retention_days=retention_days,
        rotate_token=rotate_token,
        wiretap=wiretap,
        wiretap_log=wiretap_log,
        enable_mcpsafe=enable_mcpsafe or None,
        mcpsafe_lock=mcpsafe_lock,
    )


@main.command()
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
@click.option("--topic", required=True)
@click.option("--payload", "payload_json", default=None, help="JSON object string")
@click.option(
    "--payload-file", type=click.Path(exists=True, dir_okay=False), default=None
)
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
@click.option("--action", "action_json", default=None, help="Signed typed action JSON object")
@click.option(
    "--sla-timeout-minutes",
    type=int,
    default=None,
    help="SLA window; auto-escalate to okf/dead-letter if no causation_id reply",
)
@click.option(
    "--trace-id", default=None, help="Distributed trace ID (W3C-style lineage)"
)
@click.option("--parent-span-id", default=None, help="Parent span for trace waterfall")
@click.option(
    "--token", default=None, help="Publish auth token (default: workspace file)"
)
@click.option("--retention-days", default=7, show_default=True)
@click.option(
    "--enable-mcpsafe",
    is_flag=True,
    default=False,
    help="Enforce .mcpsafe.lock payload policy on this publish",
)
@click.option(
    "--mcpsafe-lock",
    type=click.Path(dir_okay=False, path_type=str),
    default=None,
    envvar="AGENTBUS_MCPSAFE_LOCK",
    help="Path to .mcpsafe.lock (default: <workspace>/.mcpsafe.lock)",
)
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
    action_json: str | None,
    sla_timeout_minutes: int | None,
    trace_id: str | None,
    parent_span_id: str | None,
    token: str | None,
    retention_days: int,
    enable_mcpsafe: bool,
    mcpsafe_lock: str | None,
) -> None:
    """Append one event (CLI fallback for non-MCP clients like Agy)."""
    from agentbus.mcpsafe import load_enforcer, mcpsafe_enabled_from_env

    try:
        if payload_file:
            payload = strict_json_loads(Path(payload_file).read_bytes())
        elif payload_json:
            payload = strict_json_loads(payload_json)
        else:
            raise click.ClickException("Provide --payload or --payload-file")
        action = strict_json_loads(action_json) if action_json else None
        if action is not None and not isinstance(action, dict):
            raise click.ClickException("--action must be a JSON object")
        if not isinstance(payload, dict):
            raise click.ClickException(
                "invalid_payload: payload must be a JSON object "
                f"(got {type(payload).__name__})"
            )
    except _identity_error() as exc:
        raise click.ClickException(
            f"invalid input: {exc} — check JSON syntax and encoding"
        ) from exc

    try:
        ws = _cli_workspace(workspace)
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc
    _auth(ws, token)
    try:
        if attach:
            arts = list(payload.get("artifacts") or [])
            for path in attach:
                arts.append(artifact_from_file(Path(path)))
            payload["artifacts"] = arts
        payload = validate_payload(
            topic, payload, workspace=ws, producer_id=_producer_id(producer_id)
        )
    except _payload_too_large_error() as exc:
        raise click.ClickException(str(exc)) from exc
    except ValueError as exc:
        raise click.ClickException(
            f"invalid_payload for topic '{topic}': {exc} — "
            f"run 'agentbus schema --topic {topic}' if registered, or check the "
            "payload against the handoff schema (from/to/summary required)"
        ) from exc
    store = _open_store(workspace, retention_days)
    try:
        try:
            enforcer = load_enforcer(
                ws,
                enabled=enable_mcpsafe or mcpsafe_enabled_from_env(),
                lockfile=mcpsafe_lock,
            )
            if enforcer is not None:
                store.set_mcpsafe(enforcer)
            event, duplicate = store.publish(
                topic=topic,
                producer_id=_producer_id(producer_id),
                schema_version=schema_version,
                payload=payload,
                causation_id=causation_id,
                idempotency_key=idempotency_key,
                action=action,
                auth_token=token,
                sla_timeout_minutes=sla_timeout_minutes,
                trace_id=trace_id,
                parent_span_id=parent_span_id,
                auto_sign=bool(os.environ.get("AGENTBUS_IDENTITY_PRIVATE_KEY")),
                signing_key_path=(
                    Path(os.environ["AGENTBUS_IDENTITY_PRIVATE_KEY"])
                    if os.environ.get("AGENTBUS_IDENTITY_PRIVATE_KEY")
                    else None
                ),
            )
        except (
            _forbidden_error(),
            _payload_too_large_error(),
            _access_denied_error(),
            _identity_error(),
        ) as exc:
            raise click.ClickException(str(exc)) from exc
        except sqlite3.Error as exc:
            raise click.ClickException(
                f"storage error during publish: {exc} — "
                "the workspace database may be locked or corrupted; "
                "run 'agentbus doctor' for diagnosis"
            ) from exc
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
@click.option(
    "--droid-proof",
    default=None,
    help="Batch-scoped droid proof (F4): applied to every line whose payload "
    "lacks its own droid_proof; each publish consumes one use. Mint with "
    "'agentbus droid mint --max-uses <line-count>'.",
)
@click.option("--retention-days", default=7, show_default=True)
def publish_batch(
    workspace: str | None,
    batch_file: str,
    producer_id: str | None,
    token: str | None,
    droid_proof: str | None,
    retention_days: int,
) -> None:
    """Publish many events in one process (faster than repeated CLI subprocesses)."""
    ws = _cli_workspace(workspace)
    _auth(ws, token)
    pid = _producer_id(producer_id)
    store = _open_store(workspace, retention_days)
    results: list[dict] = []
    try:
        for line_no, line in enumerate(
            Path(batch_file).read_text(encoding="utf-8").splitlines(), 1
        ):
            if not line.strip():
                continue
            try:
                spec = json.loads(line)
            except json.JSONDecodeError as exc:
                raise click.ClickException(f"line {line_no}: invalid JSON") from exc
            topic = spec.get("topic")
            payload = spec.get("payload")
            if not topic or not isinstance(payload, dict):
                raise click.ClickException(
                    f"line {line_no}: require topic and payload object"
                )
            if droid_proof and not payload.get("droid_proof"):
                payload = dict(payload)
                payload["droid_proof"] = droid_proof
            payload = validate_payload(
                topic,
                payload,
                workspace=ws,
                producer_id=spec.get("producer_id") or pid,
            )
            try:
                event, duplicate = store.publish(
                    topic=topic,
                    producer_id=spec.get("producer_id") or pid,
                    schema_version=spec.get("schema_version", "1.0"),
                    payload=payload,
                    causation_id=spec.get("causation_id"),
                    idempotency_key=spec.get("idempotency_key"),
                    action=spec.get("action"),
                    auth_token=token,
                    sla_timeout_minutes=spec.get("sla_timeout_minutes"),
                    trace_id=spec.get("trace_id"),
                    parent_span_id=spec.get("parent_span_id"),
                    auto_sign=bool(
                        os.environ.get("AGENTBUS_IDENTITY_PRIVATE_KEY")
                    ),
                    signing_key_path=(
                        Path(os.environ["AGENTBUS_IDENTITY_PRIVATE_KEY"])
                        if os.environ.get("AGENTBUS_IDENTITY_PRIVATE_KEY")
                        else None
                    ),
                )
            except (
                _forbidden_error(),
                _payload_too_large_error(),
                _identity_error(),
            ) as exc:
                raise click.ClickException(str(exc)) from exc
            except sqlite3.Error as exc:
                raise click.ClickException(
                    f"storage error during batch publish: {exc} — "
                    "run 'agentbus doctor' for diagnosis"
                ) from exc
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
@click.option(
    "--compact",
    is_flag=True,
    default=False,
    help="One line per event: id, time, from→to, trimmed summary (human scan).",
)
@click.option(
    "--since",
    default=None,
    help="Filter fetched events to timestamp >= value (ISO 8601 or e.g. 10m, 2h, 1d). "
    "Applied after the cursor fetch; combine with --limit to widen the window.",
)
@click.option("--retention-days", default=7, show_default=True)
def poll(
    workspace: str,
    topic: str,
    since_id: int,
    limit: int,
    compact: bool,
    since: str | None,
    retention_days: int,
) -> None:
    """Fetch events after cursor."""
    since_cutoff = _parse_since(since) if since else None
    store = _open_store(workspace, retention_days)
    try:
        result = store.poll(topic=topic, since_id=since_id, limit=limit)
        if since_cutoff is not None:
            kept = [
                e
                for e in result.get("events", [])
                if _event_timestamp(e) is not None
                and _event_timestamp(e) >= since_cutoff
            ]
            result = {**result, "events": kept, "count": len(kept)}
        if compact:
            for e in result.get("events", []):
                p = e.get("payload") or {}
                summary = str(p.get("summary") or "").replace("\n", " ")
                if len(summary) > 120:
                    summary = summary[:117] + "..."
                click.echo(
                    f"#{e.get('event_id')} {e.get('timestamp', '')} "
                    f"{p.get('from', e.get('producer_id', '?'))}"
                    f"->{p.get('to', '?')}: {summary}"
                )
        else:
            click.echo(json.dumps(result))
    finally:
        store.close()


def _parse_since(value: str) -> datetime:
    """Parse --since as ISO 8601 or a relative duration (10m, 2h, 1d)."""
    raw = value.strip()
    if raw and raw[-1].isdigit() is False and raw[0].isdigit():
        unit = raw[-1].lower()
        try:
            amount = int(raw[:-1])
            deltas = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}
            if unit in deltas and amount >= 0:
                return datetime.now(timezone.utc) - timedelta(
                    seconds=deltas[unit] * amount
                )
        except ValueError:
            pass
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except ValueError as exc:
        raise click.BadParameter(
            f"cannot parse --since {value!r}: use ISO 8601 or a duration "
            "like 30s, 10m, 2h, 1d"
        ) from exc


def _event_timestamp(event: dict) -> datetime | None:
    ts = event.get("timestamp")
    if not ts:
        return None
    try:
        parsed = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except ValueError:
        return None


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


@main.command()
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
@click.option("--retention-days", default=7, show_default=True)
@click.option(
    "--since-checkpoint",
    is_flag=True,
    help="Verify only the post-cutover chain; trust the stored legacy anchor.",
)
def verify(workspace: str, retention_days: int, since_checkpoint: bool) -> None:
    """Verify the forward SHA-256 event chain."""
    ws = _cli_workspace(workspace)
    db_path = ws / ".agentbus" / "events.db"
    if not db_path.is_file():
        raise click.ClickException(f"event store not found: {db_path}")
    store = EventStore(ws, retention_days=retention_days, auto_prune=False)
    try:
        report = store.verify_chain(since_checkpoint=since_checkpoint)
        click.echo(json.dumps(report, sort_keys=True))
        if not report["ok"]:
            raise click.exceptions.Exit(1)
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
    except _identity_error() as exc:
        raise click.ClickException(str(exc)) from exc
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
    except _identity_error() as exc:
        raise click.ClickException(str(exc)) from exc
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
        try:
            result = store.lock_acquire(resource, owner_id, ttl_seconds)
        except ValueError as exc:
            raise click.ClickException(str(exc)) from exc
        click.echo(json.dumps(result))
    finally:
        store.close()


@lock.command("release")
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
@click.option("--resource", required=True)
@click.option(
    "--lease-id",
    default=None,
    help="Lease UUID from acquire. Omit to release the caller's active lease "
    "on the resource (looked up by --owner-id).",
)
@click.option("--owner-id", required=True)
@click.option("--token", default=None)
def lock_release(
    workspace: str,
    resource: str,
    lease_id: str | None,
    owner_id: str,
    token: str | None,
) -> None:
    """Release a held lease (by lease-id, or by resource + owner)."""
    ws = _cli_workspace(workspace)
    _auth(ws, token)
    store = _open_lease_store(workspace)
    try:
        if lease_id is None:
            state = store.lock_status(resource)
            if not state.get("locked"):
                raise click.ClickException(
                    f"no active lease on resource '{resource}' — nothing to release"
                )
            if state.get("current_owner") != owner_id:
                raise click.ClickException(
                    f"resource '{resource}' is held by "
                    f"'{state.get('current_owner')}', not '{owner_id}' — "
                    "release requires the holding owner"
                )
            lease_id = str(state["lease_id"])
        try:
            result = store.lock_release(resource, lease_id, owner_id)
        except ValueError as exc:
            raise click.ClickException(str(exc)) from exc
        click.echo(json.dumps(result))
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
        try:
            result = store.lock_renew(resource, lease_id, owner_id, ttl_seconds)
        except ValueError as exc:
            raise click.ClickException(str(exc)) from exc
        click.echo(json.dumps(result))
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


@main.group()
def posthog() -> None:
    """Outbound PostHog telemetry (no inbound webhook handling)."""


@posthog.command("stream")
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
@click.option("--once", is_flag=True, help="Process at most one source batch")
def posthog_stream(workspace: str | None, once: bool) -> None:
    """Stream allowlisted AgentBus event metadata to PostHog."""
    from agentbus.posthog import load_config, run_stream

    ws = _cli_workspace(workspace)
    try:
        config = load_config()
        run_stream(ws, config, once=once)
    except (OSError, RuntimeError, ValueError) as exc:
        raise click.ClickException(str(exc)) from exc


@posthog.command("test")
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
def posthog_test(workspace: str | None) -> None:
    """Verify TLS, endpoint connectivity, and the project API key."""
    from agentbus.posthog import PostHogExporter, load_config

    ws = _cli_workspace(workspace)
    try:
        click.echo(json.dumps(PostHogExporter(ws, load_config()).send_test()))
    except (OSError, RuntimeError, ValueError) as exc:
        raise click.ClickException(str(exc)) from exc


@posthog.command("status")
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
def posthog_status(workspace: str | None) -> None:
    """Show durable cursor, lag, delivery, and quarantine health."""
    from agentbus.posthog import PostHogExporter, load_config

    ws = _cli_workspace(workspace)
    try:
        click.echo(json.dumps(PostHogExporter(ws, load_config(require_key=False)).status()))
    except (OSError, RuntimeError, ValueError) as exc:
        raise click.ClickException(str(exc)) from exc


def _posthog_output(result: dict, output_format: str) -> str:
    """Render bounded query data without introducing an optional CLI dependency."""
    if output_format == "json":
        return json.dumps(result, sort_keys=True)
    payload = result.get("result", {})
    if output_format == "summary":
        values = payload.get("results", payload) if isinstance(payload, dict) else payload
        return json.dumps(values, sort_keys=True)
    if not isinstance(payload, dict):
        return json.dumps(payload, sort_keys=True)
    columns = payload.get("columns")
    rows = payload.get("results")
    if not isinstance(columns, list) or not isinstance(rows, list):
        return json.dumps(payload, sort_keys=True)
    labels = [str(column) for column in columns]
    rendered_rows = [[str(value) for value in row] if isinstance(row, list) else [str(row)] for row in rows]
    widths = [len(label) for label in labels]
    for row in rendered_rows:
        for index, value in enumerate(row[: len(widths)]):
            widths[index] = max(widths[index], len(value))
    line = " | ".join(label.ljust(widths[index]) for index, label in enumerate(labels))
    separator = "-+-".join("-" * width for width in widths)
    body = [" | ".join(value.ljust(widths[index]) for index, value in enumerate(row[: len(widths)])) for row in rendered_rows]
    return "\n".join([line, separator, *body])


@posthog.command("query")
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
@click.option("--preset", type=click.Choice(["swarm_health", "agent_throughput", "qa_summary", "lock_concurrency", "llm_cost_latency"]))
@click.option("--sql", default=None, help="Advanced: one restricted, read-only, time-bounded HogQL statement (prefer --preset)")
@click.option("--time-range", default="7d", show_default=True)
@click.option("--limit", default=50, show_default=True, type=click.IntRange(1, 100))
@click.option("--format", "output_format", default="summary", show_default=True, type=click.Choice(["summary", "json", "table"]))
def posthog_query(
    workspace: str | None, preset: str | None, sql: str | None, time_range: str, limit: int, output_format: str
) -> None:
    """Run a curated swarm KPI preset (preferred) or restricted HogQL query."""
    from agentbus.posthog import PostHogQueryClient, load_query_config, parse_time_range, preset_hogql

    _cli_workspace(workspace)
    if bool(preset) == bool(sql):
        raise click.ClickException("provide exactly one of --preset or --sql")
    try:
        query = preset_hogql(preset, parse_time_range(time_range)) if preset else str(sql)
        result = PostHogQueryClient(load_query_config()).query(query, limit=limit)
        click.echo(_posthog_output(result, output_format))
    except (OSError, RuntimeError, ValueError) as exc:
        raise click.ClickException(str(exc)) from exc


@posthog.command("report")
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
@click.option("--preset", default="weekly_digest", type=click.Choice(["weekly_digest"]))
@click.option("--format", "output_format", default="summary", show_default=True, type=click.Choice(["summary", "json", "table"]))
def posthog_report(workspace: str | None, preset: str, output_format: str) -> None:
    """Produce a bounded executive report from curated seven-day KPI queries."""
    from agentbus.posthog import PostHogQueryClient, load_query_config, preset_hogql

    _cli_workspace(workspace)
    try:
        client = PostHogQueryClient(load_query_config())
        report = {name: client.query(preset_hogql(name, 7))["result"] for name in ("swarm_health", "agent_throughput", "qa_summary", "lock_concurrency", "llm_cost_latency")}
        click.echo(_posthog_output({"result": {"preset": preset, "period": "7d", "metrics": report}}, output_format))
    except (OSError, RuntimeError, ValueError) as exc:
        raise click.ClickException(str(exc)) from exc


@main.group("pricing")
def pricing() -> None:
    """Manage dynamic LLM pricing catalogs and OpenRouter sync."""


@pricing.command("sync")
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
@click.option(
    "--endpoint",
    default="https://openrouter.ai/api/v1/models",
    show_default=True,
    help="OpenRouter models endpoint",
)
@click.option("--timeout", default=5.0, show_default=True, type=float, help="Request timeout in seconds")
@click.option(
    "--output-format",
    type=click.Choice(["json", "text"]),
    default="text",
    show_default=True,
)
def pricing_sync(
    workspace: str | None,
    endpoint: str,
    timeout: float,
    output_format: str,
) -> None:
    """Fetch live model pricing from OpenRouter and update local disk cache."""
    from agentbus.pricing import sync_openrouter_pricing

    ws = _cli_workspace(workspace)
    try:
        res = sync_openrouter_pricing(workspace=ws, endpoint=endpoint, timeout=timeout)
        if output_format == "json":
            click.echo(json.dumps(res, indent=2))
        else:
            click.echo(
                f"Synced {res['models_count']} model aliases ({res['raw_models_count']} raw models) "
                f"from OpenRouter to {res['cache_path']}"
            )
    except Exception as exc:
        raise click.ClickException(f"Failed to sync pricing: {exc}") from exc


@pricing.command("status")
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
@click.option(
    "--output-format",
    type=click.Choice(["json", "text"]),
    default="text",
    show_default=True,
)
def pricing_status(workspace: str | None, output_format: str) -> None:
    """Show pricing catalog status and cache details."""
    from agentbus.pricing import get_pricing_status

    ws = _cli_workspace(workspace)
    st = get_pricing_status(ws)
    if output_format == "json":
        click.echo(json.dumps(st, indent=2))
    else:
        click.echo(f"Cache path:        {st['cache_path']}")
        click.echo(f"Cache exists:      {st['cache_exists']}")
        click.echo(f"Last updated:      {st['updated_at'] or 'never'}")
        click.echo(f"Dynamic models:    {st['dynamic_models_count']}")
        click.echo(f"Fallback models:   {st['fallback_models_count']}")
        click.echo(f"Primary source:    {st['primary_source']}")


@pricing.command("estimate")
@click.argument("model")
@click.option("--input-tokens", default=0, type=int, help="Input / prompt tokens")
@click.option("--output-tokens", default=0, type=int, help="Output / completion tokens")
@click.option("--cache-tokens", default=0, type=int, help="Cache read tokens")
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
@click.option(
    "--output-format",
    type=click.Choice(["json", "text"]),
    default="text",
    show_default=True,
)
def pricing_estimate(
    model: str,
    input_tokens: int,
    output_tokens: int,
    cache_tokens: int,
    workspace: str | None,
    output_format: str,
) -> None:
    """Calculate estimated USD cost for a model and token usage."""
    from agentbus.pricing import estimate_llm_cost

    ws = _cli_workspace(workspace)
    cost, source = estimate_llm_cost(
        model=model,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cache_read_tokens=cache_tokens,
        workspace=ws,
    )
    result = {
        "model": model,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "cache_tokens": cache_tokens,
        "estimated_cost_usd": cost,
        "pricing_source": source,
    }
    if output_format == "json":
        click.echo(json.dumps(result, indent=2))
    else:
        click.echo(f"Model:           {model}")
        click.echo(f"Estimated Cost:  ${cost:.6f} USD")
        click.echo(f"Pricing Source:  {source}")


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
    """Project okf/handoff events into OKF log.md format.

    When the workspace has a scaffolded OKF knowledge plane the handoffs are routed
    through the bounded distillation subsystem (`okf/log.md` plus partitioned
    `okf/handoffs/YYYY/MM/` concepts). Otherwise the legacy flat projection into
    `--log-file` is retained.
    """
    ws = _cli_workspace(workspace)
    if log_file == "log.md" and (ws / MANIFEST_PATH).is_file():
        try:
            result = distill_workspace(
                ws, limit=100, dry_run=dry_run, reset=reset
            )
        except ValueError as exc:
            raise click.ClickException(str(exc)) from exc
        except _workspace_error() as exc:
            raise click.ClickException(f"{exc.code}: {exc.message}") from exc
        except OSError as exc:
            raise click.ClickException(
                f"project_log_failed: could not update project log: {exc}"
            ) from exc
        summary = {
            "projected": result["count"],
            "last_event_id": result["high_water_event_id"],
            "dry_run": dry_run,
            "knowledge_plane": "okf",
            "created": result.get("created", []),
            "unchanged": result.get("unchanged", []),
        }
        click.echo(json.dumps(summary))
        if dry_run and result.get("documents"):
            click.echo("---")
            click.echo("\n\n".join(result["documents"]))
        return
    log_path = ws / log_file
    store = _open_store(workspace, retention_days)
    try:
        result = project_handoffs(store, ws, log_path, dry_run=dry_run, reset=reset)
        click.echo(json.dumps({k: v for k, v in result.items() if k != "lines"}))
        if dry_run and result["lines"]:
            click.echo("---")
            click.echo("\n\n".join(result["lines"]))
    except _identity_error() as exc:
        raise click.ClickException(str(exc)) from exc
    finally:
        store.close()


@main.command("distill")
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
@click.option("--out", default=None, help="Output directory (default: okf/)")
@click.option("--stdout", is_flag=True, help="Print distilled markdown to stdout")
@click.option("--limit", default=100, show_default=True, type=int)
@click.option(
    "--dry-run", is_flag=True, help="Report without writing files or advancing state"
)
@click.option(
    "--reset", is_flag=True, help="Rewind the high-water mark and reconsider all handoffs"
)
def distill(
    workspace: str | None,
    out: str | None,
    stdout: bool,
    limit: int,
    dry_run: bool,
    reset: bool,
) -> None:
    """Materialize okf/handoff events into the bounded OKF knowledge plane."""
    try:
        ws = _cli_workspace(workspace)
        result = distill_workspace(
            ws,
            output_root=out,
            stdout=stdout,
            limit=limit,
            dry_run=dry_run,
            reset=reset,
        )
    except ValueError as exc:
        # ``_cli_workspace`` reports an unusable --workspace as a ValueError.
        raise click.ClickException(str(exc)) from exc
    except _workspace_error() as exc:
        raise click.ClickException(f"{exc.code}: {exc.message}") from exc
    except OSError as exc:
        raise click.ClickException(
            f"distill_write_failed: could not write the knowledge plane: {exc}"
        ) from exc
    if stdout:
        if result["documents"]:
            click.echo("\n\n".join(result["documents"]))
        return
    summary = {k: v for k, v in result.items() if k != "documents"}
    click.echo(json.dumps(summary))
    if dry_run and result.get("documents"):
        click.echo("---")
        click.echo("\n\n".join(result["documents"]))


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
            json.dumps({"path": str(token_path(ws)), "token": value, "created": True})
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
            json.dumps({"path": str(token_path(ws)), "token": value, "rotated": True})
        )


def _apply_runtime_ignore(ws: Path, *, bare: bool, phase: str) -> dict[str, Any]:
    try:
        return ensure_runtime_ignored(ws, bare=bare)
    except _workspace_error() as exc:
        raise click.ClickException(f"{exc.code}: {exc.message}") from exc
    except OSError as exc:
        raise click.ClickException(
            f"runtime_ignore_failed: {phase}, but the runtime ignore "
            f"could not be updated: {exc}"
        ) from exc


def _finalize_init_runtime(ws: Path, *, apply: bool, hitl: bool) -> None:
    """Commit optional HITL configuration and the runtime database safely."""
    if not apply:
        return

    def open_and_close_store(workspace: Path) -> None:
        store: EventStore | None = None
        try:
            store = EventStore(workspace, auto_prune=False)
        except (sqlite3.Error, OSError, RuntimeError, ValueError) as exc:
            raise click.ClickException(
                f"runtime_store_init_failed: {exc}"
            ) from exc
        finally:
            if store is not None:
                store.close()

    def restore_hitl_rule(path: Path, previous: bytes | None) -> None:
        if previous is None:
            path.unlink(missing_ok=True)
        else:
            path.write_bytes(previous)

    def install_hitl_rule() -> tuple[Path, bytes | None]:
        path = intercept_config_path(ws)
        previous = path.read_bytes() if path.is_file() else None
        try:
            add_rule(
                ws,
                InterceptRule(
                    topic="okf/handoff",
                    contains=COMPANION_HITL_MARKER,
                ),
            )
        except (
            json.JSONDecodeError,
            KeyError,
            TypeError,
            ValueError,
            OSError,
        ) as exc:
            try:
                restore_hitl_rule(path, previous)
            except OSError as rollback_exc:
                raise click.ClickException(
                    "invalid_intercept_config: "
                    f"{exc}; rollback_failed: {rollback_exc}"
                ) from exc
            raise click.ClickException(
                f"invalid_intercept_config: {exc}"
            ) from exc
        return path, previous

    db_path = ws / ".agentbus" / "events.db"
    if db_path.is_file():
        open_and_close_store(ws)
        if hitl:
            install_hitl_rule()
            click.echo(
                "HITL: enabled for handoff actions marked requires_approval=true"
            )
        return

    runtime_dir = db_path.parent
    try:
        runtime_dir.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix=".init-", dir=runtime_dir) as tmp:
            staged_workspace = Path(tmp)
            open_and_close_store(staged_workspace)
            staged_db = staged_workspace / ".agentbus" / "events.db"
            hitl_snapshot: tuple[Path, bytes | None] | None = None
            if hitl:
                hitl_snapshot = install_hitl_rule()
            try:
                os.replace(staged_db, db_path)
            except OSError:
                if hitl_snapshot is not None:
                    restore_hitl_rule(*hitl_snapshot)
                raise
    except click.ClickException:
        raise
    except OSError as exc:
        raise click.ClickException(f"runtime_store_init_failed: {exc}") from exc

    if hitl:
        click.echo(
            "HITL: enabled for handoff actions marked requires_approval=true"
        )


@main.command()
@click.option(
    "--workspace", default=None, help="Workspace root (default: git root or cwd)"
)
@click.option("--producer-id", default=None, help="MCP producer id for this client")
@click.option(
    "--apply", is_flag=True, help="Write MCP config updates (default: dry-run)"
)
@click.option(
    "--client", "clients", multiple=True, help="Limit to client id (repeatable)"
)
@click.option(
    "--okf/--no-okf",
    "okf",
    default=True,
    help="Scaffold the okf/ knowledge plane (default: enabled)",
)
@click.option(
    "--bare",
    is_flag=True,
    help="Initialize runtime state only; skip the tracked okf/ scaffold",
)
@click.option(
    "--project-name",
    default=None,
    help="Project name recorded in the scaffold manifest (default: directory name)",
)
@click.option(
    "--mcp/--no-mcp",
    default=True,
    help="Discover and wire supported MCP client configurations (default: enabled)",
)
@click.option(
    "--hitl/--no-hitl",
    default=False,
    help="Install an opt-in HITL intercept for actions marked requires_approval=true",
)
def init(
    workspace: str | None,
    producer_id: str | None,
    apply: bool,
    clients: tuple[str, ...],
    okf: bool,
    bare: bool,
    project_name: str | None,
    mcp: bool,
    hitl: bool,
) -> InitResult:
    """Auto-discover MCP clients, wire agentbus, and scaffold okf/ (idempotent)."""
    try:
        ws = _cli_workspace(workspace, create=apply)
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc
    pid = _producer_id(producer_id)

    scaffold_okf = okf and not bare
    if scaffold_okf:
        # Preflight the knowledge plane before ``apply_init`` can rewrite MCP client
        # configuration: a scaffold conflict or unsafe path must abort with nothing
        # mutated, rather than leaving clients wired to a half-bootstrapped workspace.
        try:
            scaffold_workspace(ws, project_name=project_name, check_only=True)
        except _workspace_error() as exc:
            hint = (
                "\nRe-run with --no-okf (or --bare) to wire MCP clients without the "
                "okf/ scaffold."
                if exc.code == "scaffold_conflict"
                else ""
            )
            raise click.ClickException(f"{exc.code}: {exc.message}{hint}") from exc

    if hitl:
        try:
            load_config(ws)
        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
            raise click.ClickException(
                f"invalid_intercept_config: {exc}"
            ) from exc

    try:
        result = apply_init(
            ws,
            producer_id=pid,
            dry_run=not apply,
            clients=list(clients) if clients else None,
            discover_clients=mcp,
        )
    except _invalid_mcp_config_error() as exc:
        raise click.ClickException(f"invalid_mcp_config: {exc}") from exc
    click.echo(format_init_summary(result))
    if not mcp:
        click.echo("MCP discovery: skipped (--no-mcp)")

    ignore: dict[str, Any] | None = None
    if not scaffold_okf:
        label = "--bare" if bare else "--no-okf"
        if not apply:
            suffix = "; dry-run, no runtime ignore written" if bare else ""
            click.echo(f"Knowledge plane: skipped ({label}){suffix}")
            return result
        ignore = _apply_runtime_ignore(
            ws, bare=True, phase="MCP configuration completed"
        )
        target = ignore["scope"] or "no git work tree; runtime ignore not written"
        click.echo(f"Knowledge plane: skipped ({label}) -> {target}")
        _finalize_init_runtime(ws, apply=apply, hitl=hitl)
        return result

    try:
        scaffold = scaffold_workspace(
            ws, project_name=project_name, check_only=not apply
        )
    except _workspace_error() as exc:
        hint = (
            "\nRe-run with --no-okf (or --bare) to wire MCP clients without the "
            "okf/ scaffold."
            if exc.code == "scaffold_conflict"
            else ""
        )
        raise click.ClickException(f"{exc.code}: {exc.message}{hint}") from exc
    except OSError as exc:
        raise click.ClickException(
            "scaffold_write_failed: MCP configuration phase completed, but the "
            f"okf/ scaffold could not be written: {exc}"
        ) from exc

    if apply:
        ignore = _apply_runtime_ignore(
            ws,
            bare=False,
            phase="MCP configuration and okf/ scaffold completed",
        )
    verb = "would create" if not apply else "created"
    click.echo(
        f"Knowledge plane: {verb} {len(scaffold['created'])}, "
        f"unchanged {len(scaffold['unchanged'])} (template v{scaffold['template_version']})"
    )
    if scaffold["managed"]:
        click.echo(
            "Knowledge plane: preserved distilled "
            + ", ".join(scaffold["managed"])
        )
    if scaffold.get("preserved"):
        click.echo(
            "Knowledge plane: preserved pre-existing "
            + ", ".join(scaffold["preserved"])
        )
    if ignore is not None and ignore["path"]:
        state = "added" if ignore["applied"] else "already present"
        click.echo(f"Runtime ignore: .agentbus/ {state} in {ignore['path']}")
    _finalize_init_runtime(ws, apply=apply, hitl=hitl)
    return result


@main.command()
@click.option(
    "--workspace", default=None, help="Workspace root (default: git root or cwd)"
)
@click.option("--topic", default=None, help="Filter by topic")
@click.option("--interval", default=1.0, show_default=True, help="Refresh seconds")
@click.option("--once", is_flag=True, help="Print snapshot and exit")
@click.option("--plain", is_flag=True, help="Plain/rich poll loop (no Textual TUI)")
@click.option("--retention-days", default=7, show_default=True)
@click.option(
    "--tz",
    "--timezone",
    default=None,
    envvar="AGENTBUS_TZ",
    help="Display timezone (e.g. UTC, local, Asia/Tokyo, America/New_York)",
)
def monitor(
    workspace: str | None,
    topic: str | None,
    interval: float,
    once: bool,
    plain: bool,
    retention_days: int,
    tz: str | None,
) -> None:
    """Mission-control TUI (Textual) or tail events.db."""
    try:
        ws = _cli_workspace(workspace)
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc
    run_monitor(
        ws,
        topic=topic,
        interval=interval,
        once=once,
        plain=plain,
        retention_days=retention_days,
        tz=tz,
    )


@click.command(name="agentbus-monitor")
@click.option(
    "--workspace", default=None, help="Workspace root (default: git root or cwd)"
)
@click.option("--topic", default=None, help="Filter by topic")
@click.option("--interval", default=1.0, show_default=True, help="Refresh seconds")
@click.option("--once", is_flag=True, help="Print snapshot and exit")
@click.option("--plain", is_flag=True, help="Plain/rich poll loop (no Textual TUI)")
@click.option("--retention-days", default=7, show_default=True)
@click.option(
    "--tz",
    "--timezone",
    default=None,
    envvar="AGENTBUS_TZ",
    help="Display timezone (e.g. UTC, local, Asia/Tokyo, America/New_York)",
)
def monitor_standalone(
    workspace: str | None,
    topic: str | None,
    interval: float,
    once: bool,
    plain: bool,
    retention_days: int,
    tz: str | None,
) -> None:
    """Standalone entrypoint for AgentBus God View Monitor."""
    try:
        ws = _cli_workspace(workspace)
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc
    run_monitor(
        ws,
        topic=topic,
        interval=interval,
        once=once,
        plain=plain,
        retention_days=retention_days,
        tz=tz,
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
    click.echo(
        json.dumps(
            {
                "path": str(ws / ".agentbus" / "intercepts.json"),
                "rules": config_data.to_dict()["rules"],
            }
        )
    )


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
@click.option(
    "--batch-id",
    default=None,
    help="Scope the proof to a batch (F4). Combine with --max-uses.",
)
@click.option(
    "--max-uses",
    type=int,
    default=1,
    show_default=True,
    help="Number of publishes this proof authorizes (1 = legacy single-use; "
    "ceiling 100). Batch-scoped proofs expire like single-use ones.",
)
def droid_mint(
    workspace: str,
    mission_id: str | None,
    ttl_minutes: int,
    batch_id: str | None,
    max_uses: int,
) -> None:
    """Mint a short-lived droid_proof for qa_droid role publishes."""
    ws = _cli_workspace(workspace)
    try:
        click.echo(
            json.dumps(
                mint_droid_proof(
                    ws,
                    mission_id=mission_id,
                    ttl_minutes=ttl_minutes,
                    batch_id=batch_id,
                    max_uses=max_uses,
                )
            )
        )
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc


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
    except _identity_error() as exc:
        raise click.ClickException(str(exc)) from exc
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
    except (ValueError, _forbidden_error()) as exc:
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
                store.reject_event(
                    event_id, reviewer_id=rid, reason=reason, auth_token=None
                )
            )
        )
    except (ValueError, _forbidden_error()) as exc:
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
@click.option(
    "--schema-file", type=click.Path(exists=True, dir_okay=False), required=True
)
@click.option("--version", default="1.0", show_default=True)
def schema_register(workspace: str, topic: str, schema_file: str, version: str) -> None:
    """Register a topic JSON Schema from a file."""
    schema_obj = json.loads(Path(schema_file).read_text(encoding="utf-8"))
    try:
        click.echo(
            json.dumps(
                register_schema(
                    _cli_workspace(workspace), topic, schema_obj, version=version
                )
            )
        )
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc


@schema.command("list")
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
def schema_list(workspace: str) -> None:
    """List registered pluggable topic schemas."""
    click.echo(json.dumps(list_schemas(_cli_workspace(workspace))))


@main.command("topic-list")
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
def topic_list(workspace: str) -> None:
    """List publishable topics: built-ins plus registered custom schemas (F2)."""
    from agentbus.schemas import KNOWN_TOPICS

    ws = _cli_workspace(workspace)
    try:
        custom = {
            entry["topic_name"]: entry.get("version", "1.0")
            for entry in list_schemas(ws)
        }
    except Exception:
        custom = {}
    payload = {
        "builtin": sorted(KNOWN_TOPICS.keys()),
        "dynamic": ["okf/status/<initiative> (pattern: okf/status/<name>)"],
        "custom_registered": dict(sorted(custom.items())),
        "hint": (
            "custom topics must be schema-registered before first publish: "
            "agentbus schema register --topic <topic> --schema-file <schema.json>"
        ),
    }
    click.echo(json.dumps(payload, indent=2))


@main.command()
@click.argument("trace_id")
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
@click.option("--retention-days", default=7, show_default=True)
def trace(workspace: str, trace_id: str, retention_days: int) -> None:
    """Render a hierarchical trace waterfall for a trace_id."""
    from agentbus.tracing import build_trace_tree, render_trace_tree

    store = _open_store(workspace, retention_days)
    try:
        try:
            events = store.fetch_trace_events(trace_id)
            roots = build_trace_tree(events)
            click.echo(render_trace_tree(trace_id, roots))
        except _identity_error() as exc:
            raise click.ClickException(str(exc)) from exc
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


@main.command("watch")
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
@click.option("--no-fs", is_flag=True, help="Disable filesystem watcher")
@click.option("--no-shell", is_flag=True, help="Disable process watcher")
@click.option("--poll-interval", type=float, default=2.0, show_default=True)
@click.option(
    "--debounce",
    "debounce_ms",
    type=int,
    default=400,
    show_default=True,
    help="FS debounce ms",
)
@click.option("--dry-run", is_flag=True, help="Log only; do not publish")
@click.option(
    "--duration", type=float, default=0, help="Exit after N seconds (0=forever)"
)
def watch_cmd(
    workspace: str | None,
    no_fs: bool,
    no_shell: bool,
    poll_interval: float,
    debounce_ms: int,
    dry_run: bool,
    duration: float,
) -> None:
    """God View OS watcher — publish system/fs and system/shell (requires [obs])."""
    ws = _cli_workspace(workspace)
    try:
        count = run_watch(
            ws,
            enable_fs=not no_fs,
            enable_shell=not no_shell,
            poll_interval=poll_interval,
            debounce_ms=debounce_ms,
            dry_run=dry_run,
            duration=duration,
        )
    except RuntimeError as exc:
        raise click.ClickException(str(exc)) from exc
    except FileNotFoundError as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(json.dumps({"published": count, "workspace": str(ws)}), err=True)


@main.command("tail")
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
@click.option(
    "--agents",
    default="all",
    show_default=True,
    help="Comma-separated agent ids (hermes,grok,claude,...) or all",
)
@click.option(
    "--list", "list_only", is_flag=True, help="Show path map presence and exit"
)
@click.option(
    "--publish",
    "do_publish",
    is_flag=True,
    help="Also publish lines to system/monologue (privacy-sensitive)",
)
@click.option(
    "--lines", type=int, default=15, show_default=True, help="Initial tail window"
)
@click.option(
    "--duration", type=float, default=0, help="Exit after N seconds (0=forever)"
)
def tail_cmd(
    workspace: str | None,
    agents: str,
    list_only: bool,
    do_publish: bool,
    lines: int,
    duration: float,
) -> None:
    """God View monologue tailer — multiplex agent session logs."""
    agent_list = (
        None
        if agents.strip().lower() == "all"
        else [a.strip() for a in agents.split(",") if a.strip()]
    )
    ws = None
    if do_publish or workspace:
        ws = _cli_workspace(workspace)
    try:
        code = run_tail(
            agents=agent_list,
            list_only=list_only,
            publish=do_publish,
            workspace=ws,
            lines=lines,
            duration=duration,
        )
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc
    if code:
        raise SystemExit(code)


@main.command("up")
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
@click.option(
    "-d",
    "--detach",
    is_flag=True,
    help="Run services in background only (no monitor TUI)",
)
@click.option(
    "--init",
    "do_init",
    is_flag=True,
    help="Write example .agentbus/swarm.yaml if missing",
)
@click.option(
    "--no-monitor",
    is_flag=True,
    help="Do not launch monitor even without --detach",
)
def up_cmd(
    workspace: str | None,
    detach: bool,
    do_init: bool,
    no_monitor: bool,
) -> None:
    """Start swarm services from .agentbus/swarm.yaml (Compose-style)."""
    ws = _cli_workspace(workspace)
    if do_init:
        path = write_example_swarm(ws)
        click.echo(json.dumps({"wrote": str(path)}))
    try:
        result = swarm_up(
            ws,
            detach=detach or no_monitor,
            run_monitor=not detach and not no_monitor,
        )
    except FileNotFoundError as exc:
        msg = str(exc)
        if "swarm.yaml" in msg or "missing" in msg.lower():
            raise click.ClickException(
                f"{msg}\nHint: agentbus up --init  # write example swarm.yaml"
            ) from exc
        # Binary not on PATH / command not found
        raise click.ClickException(f"failed to start service: {msg}") from exc
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc
    except OSError as exc:
        raise click.ClickException(f"failed to start service: {exc}") from exc
    click.echo(json.dumps(result, indent=2 if detach or no_monitor else None))


@main.command("down")
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
def down_cmd(workspace: str | None) -> None:
    """Stop all swarm services managed by agentbus up."""
    ws = _cli_workspace(workspace)
    results = stop_all(ws)
    click.echo(json.dumps({"stopped": results, "workspace": str(ws)}))


@main.command("ps")
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
@click.option("--json", "as_json", is_flag=True, help="Machine-readable output")
def ps_cmd(workspace: str | None, as_json: bool) -> None:
    """List running swarm services (PID + uptime)."""
    ws = _cli_workspace(workspace)
    rows = list_processes(ws)
    if as_json:
        click.echo(json.dumps({"services": rows, "workspace": str(ws)}))
        return
    if not rows:
        click.echo("No running swarm services.")
        return
    click.echo(f"{'NAME':<16} {'PID':<8} {'UPTIME':<10} COMMAND")
    for r in rows:
        click.echo(
            f"{r['name']:<16} {str(r['pid']):<8} {r['uptime']:<10} {r['command']}"
        )


@main.command("logs")
@click.argument("service_name")
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
@click.option("-f", "--follow", is_flag=True, help="Follow log output")
@click.option("-n", "--lines", type=int, default=50, show_default=True)
def logs_cmd(
    service_name: str,
    workspace: str | None,
    follow: bool,
    lines: int,
) -> None:
    """Show stdout/stderr logs for a swarm service."""
    ws = _cli_workspace(workspace)
    code = tail_service_logs(ws, service_name, follow=follow, lines=lines)
    if code:
        raise SystemExit(code)


@main.command("swarm-config")
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
def swarm_config_cmd(workspace: str | None) -> None:
    """Show resolved swarm.yaml path and parsed service names."""
    ws = _cli_workspace(workspace)
    path = swarm_yaml_path(ws)
    try:
        cfg = load_swarm_config(ws)
        click.echo(
            json.dumps(
                {
                    "path": str(path),
                    "version": cfg.version,
                    "services": list(cfg.services.keys()),
                }
            )
        )
    except FileNotFoundError:
        click.echo(json.dumps({"path": str(path), "exists": False}))
    except ValueError as exc:
        raise click.ClickException(f"invalid swarm.yaml: {exc}") from exc


@main.group()
def worker() -> None:
    """Wake plane — classical non-LLM worker (Go binary; PRD v0.12)."""


@worker.command("up")
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
@click.option("--config", "config_path", default=None, help="Path to worker.yaml")
@click.option("--to", default="grok", help="Preset target if config missing")
def worker_up(workspace: str | None, config_path: str | None, to: str) -> None:
    """Start wake worker (exec's agentbus-go-worker; never loads an LLM)."""
    ws = _cli_workspace(workspace)
    args = ["--cmd", "up", "--to", to]
    if config_path:
        args.extend(["--config", config_path])
    _run_go_worker(ws, *args)


@worker.command("once")
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
@click.option("--config", "config_path", default=None)
@click.option("--to", default="grok")
def worker_once(workspace: str | None, config_path: str | None, to: str) -> None:
    """Single drain of matching handoffs (CI/debug)."""
    ws = _cli_workspace(workspace)
    args = ["--cmd", "once", "--to", to]
    if config_path:
        args.extend(["--config", config_path])
    _run_go_worker(ws, *args)


@worker.command("sleep")
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
def worker_sleep(workspace: str | None) -> None:
    """Pause dispatch (stand-down); holds cursor."""
    _run_go_worker(_cli_workspace(workspace), "--cmd", "sleep")


@worker.command("wake")
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
@click.option(
    "--skip-backlog/--drain",
    default=False,
    help="Fast-forward cursor vs process backlog (default: drain — process backlog)",
)
def worker_wake(workspace: str | None, skip_backlog: bool) -> None:
    """Resume dispatch after sleep (default: drain backlog; max_event_age drops stale)."""
    args = ["--cmd", "wake"]
    if skip_backlog:
        args.append("--skip-backlog")
    else:
        args.append("--drain")
    _run_go_worker(_cli_workspace(workspace), *args)


@worker.command("status")
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
def worker_status(workspace: str | None) -> None:
    """JSON worker status (cursor, sleeping, counters)."""
    _run_go_worker(_cli_workspace(workspace), "--cmd", "status")


@worker.command("init")
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
@click.option("--to", default="grok", help="Implementer identity for preset filters")
def worker_init(workspace: str | None, to: str) -> None:
    """Write .agentbus/worker.yaml implementer preset."""
    _run_go_worker(_cli_workspace(workspace), "--cmd", "init", "--to", to)


@main.command("wake-ingress")
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
@click.option(
    "--runtime",
    type=click.Choice(["hermes", "factory"], case_sensitive=False),
    required=True,
    help="Queue namespace + default port (hermes=18787, factory=18788)",
)
@click.option("--host", default="127.0.0.1", show_default=True)
@click.option("--port", type=int, default=None, help="Override default port")
@click.option(
    "--token",
    default=None,
    envvar="AGENTBUS_WEBHOOK_TOKEN",
    help="Runtime-bound ingress capability (mandatory in protected/strict mode)",
)
@click.option(
    "--dev",
    is_flag=True,
    help="Allow tokenless localhost ingress for explicit local dogfood only.",
)
def wake_ingress_cmd(
    workspace: str | None,
    runtime: str,
    host: str,
    port: int | None,
    token: str | None,
    dev: bool,
) -> None:
    """Mode A localhost ingress: POST /agentbus/wake → JSONL queue (no LLM)."""
    from agentbus.wake_ingress import run_ingress

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        run_ingress(
            _cli_workspace(workspace),
            runtime=runtime.lower(),
            host=host,
            port=port,
            token=token,
            dev=dev,
        )
    except (ValueError, OSError) as exc:
        raise click.ClickException(str(exc)) from exc


@main.command("run")
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
@click.option(
    "--config",
    "config_path",
    required=True,
    type=click.Path(path_type=str),
    help="Path to runner.<id>.yaml (relative to workspace or absolute)",
)
@click.option(
    "--once",
    is_flag=True,
    help="Drain pending wake items once and exit (CI / dogfood)",
)
def run_cmd(workspace: str | None, config_path: str, once: bool) -> None:
    """Headless reason-plane runner (v0.15 Phase B) — TurnAdapter + dual intake."""
    from agentbus.runner import load_runner_config, run_loop

    ws = _cli_workspace(workspace)
    cfg_path = Path(config_path)
    if not cfg_path.is_absolute():
        candidate = ws / cfg_path
        cfg_path = candidate if candidate.is_file() else Path(config_path).resolve()
    try:
        cfg = load_runner_config(cfg_path)
        code = run_loop(ws, cfg, once=once)
    except (FileNotFoundError, ValueError, OSError) as exc:
        raise click.ClickException(str(exc)) from exc
    raise SystemExit(code)


@main.command("await")
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
@click.option(
    "--event-id",
    type=click.IntRange(min=1),
    default=None,
    envvar="AGENTBUS_WAKE_EVENT_ID",
    help="Origin wake event_id (drop file path). Env: AGENTBUS_WAKE_EVENT_ID.",
)
@click.option(
    "--expect-from",
    "expect_from",
    multiple=True,
    help="Allowed payload.from / producer_id values (repeatable). Primary predicate.",
)
@click.option(
    "--causation-id",
    type=int,
    default=None,
    help="Require matching causation_id on fulfilling event (primary predicate).",
)
@click.option(
    "--match",
    "summary_contains",
    default=None,
    help="Secondary filter: substring required in payload.summary.",
)
@click.option(
    "--action-type",
    default=None,
    help="Require a verified signed typed action (for privileged gates).",
)
@click.option(
    "--action-result",
    default=None,
    help="Require action.result (for example green or red).",
)
@click.option(
    "--topic",
    default="okf/handoff",
    show_default=True,
    help="Topic to match for fulfillment.",
)
@click.option(
    "--timeout-hours",
    type=float,
    default=None,
    help="Mandatory wait timeout in hours, max 24h (default 4h). Fractional "
    "values honored; ignored when --timeout-minutes is given.",
)
@click.option(
    "--timeout-minutes",
    type=float,
    default=None,
    help="Mandatory wait timeout in minutes (F5 convenience; takes precedence "
    "over --timeout-hours).",
)
@click.option(
    "--reason",
    default="await",
    show_default=True,
    help="Human reason stored on WaitRegistration.",
)
@click.option(
    "--producer-id",
    default=None,
    envvar="AGENTBUS_PRODUCER_ID",
    help="Waiting agent id (default: AGENTBUS_PRODUCER_ID or 'agent').",
)
@click.option(
    "--runner-id",
    default=None,
    envvar="AGENTBUS_RUNNER_ID",
    help="Runner id for suspend-ack keys (optional; runner fills if empty).",
)
@click.option(
    "--chain-key",
    default=None,
    envvar="AGENTBUS_CHAIN_KEY",
    help="Budget chain key (optional; runner fills from ChainBudget if empty).",
)
@click.option(
    "--wait-id",
    default=None,
    help="Optional wait id (default: generated w_<hex>).",
)
@click.option(
    "--register",
    is_flag=True,
    help="Also create WaitRegistration now (default: drop file only; runner registers).",
)
def await_cmd(
    workspace: str | None,
    event_id: int | None,
    expect_from: tuple[str, ...],
    causation_id: int | None,
    summary_contains: str | None,
    action_type: str | None,
    action_result: str | None,
    topic: str,
    timeout_hours: float | None,
    timeout_minutes: float | None,
    reason: str,
    producer_id: str | None,
    runner_id: str | None,
    chain_key: str | None,
    wait_id: str | None,
    register: bool,
) -> None:
    """Cooperative await (v0.16): write await.json and exit 75 (EX_TEMPFAIL).

    The headless runner maps exit 75 / await drop → TurnResult.status=suspended,
    publishes RUNNER_SUSPEND, and registers a durable wait. When the predicate
    matches (or timeout fires), a synthetic RESUME wake is delivered.
    """
    from agentbus.runner.types import AWAIT_EXIT_CODE
    from agentbus.runner.wait_store import (
        WaitPredicate,
        WaitStore,
        new_wait_id,
        timeout_hours_from,
        validate_agent_id,
        write_await_drop,
    )

    ws = _cli_workspace(workspace)
    if event_id is None:
        raise click.ClickException(
            "event-id required (flag --event-id or env AGENTBUS_WAKE_EVENT_ID)"
        )
    if not expect_from and causation_id is None:
        raise click.ClickException(
            "predicate requires --expect-from and/or --causation-id "
            "(summary --match alone is not allowed)"
        )
    if action_result is not None and action_type is None:
        raise click.ClickException("--action-result requires --action-type")
    if timeout_hours is None and timeout_minutes is None:
        # Preserve the documented default rather than a silent fallback.
        timeout_hours = 4.0

    hours = timeout_hours_from(timeout_minutes, timeout_hours)
    wid = wait_id or new_wait_id()
    raw_producer = (
        producer_id or os.environ.get("AGENTBUS_PRODUCER_ID") or "agent"
    ).strip()
    # Reject path-bearing / malformed identities before they reach filesystem
    # path construction or wait registration.
    try:
        producer = validate_agent_id(raw_producer)
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc
    from_any = [x.strip() for x in expect_from if x and str(x).strip()]
    predicate = {
        "causation_id": causation_id,
        "from_any": from_any,
        "summary_contains": summary_contains,
        "action_type": action_type,
        "action_result": action_result,
        "topic": topic or "okf/handoff",
    }
    drop: dict = {
        "wait_id": wid,
        "origin_event_id": int(event_id),
        "producer_id": producer,
        "runner_id": runner_id or "",
        "chain_key": chain_key or "",
        "timeout_hours": hours,
        "reason": reason,
        "predicate": predicate,
        "expect_from": from_any,
        "causation_id": causation_id,
        "match": summary_contains,
        "action_type": action_type,
        "action_result": action_result,
        "topic": topic,
    }
    path = write_await_drop(ws, int(event_id), drop)

    if register:
        WaitStore(ws).create(
            runner_id=str(runner_id or producer),
            producer_id=producer,
            chain_key=str(chain_key or event_id),
            origin_event_id=int(event_id),
            predicate=WaitPredicate.from_dict(predicate),
            reason=reason,
            timeout_hours=hours,
            wait_id=wid,
        )

    click.echo(
        json.dumps(
            {
                "wait_id": wid,
                "event_id": int(event_id),
                "await_path": str(path),
                "timeout_hours": hours,
                "exit_code": AWAIT_EXIT_CODE,
                "registered": bool(register),
            },
            indent=2,
        )
    )
    raise SystemExit(AWAIT_EXIT_CODE)


@main.command("doctor")
@click.option("--workspace", type=click.Path(file_okay=False), default=None)
@click.option("--json", "as_json", is_flag=True, help="Emit a machine-readable report.")
@click.option(
    "--strict", is_flag=True, help="Return non-zero for warnings as well as failures."
)
def doctor_cmd(workspace: str | None, as_json: bool, strict: bool) -> None:
    """Run read-only workspace checks plus isolated runtime probes."""
    from agentbus.doctor import run_doctor

    try:
        ws = _cli_workspace(workspace)
        report = run_doctor(ws)
    except (OSError, RuntimeError, ValueError) as exc:
        raise click.ClickException(str(exc)) from exc
    if as_json:
        click.echo(json.dumps(report.to_dict(), indent=2))
    else:
        click.echo(f"AgentBus Doctor: {report.overall_status}")
        for check in report.checks:
            click.echo(f"[{check.status:<4}] {check.name}: {check.message}")
    if report.overall_status == "FAIL" or (strict and report.overall_status == "WARN"):
        raise SystemExit(1)


@main.group("identity")
def identity_group() -> None:
    """Manage AgentID trust policy and audit/development identities."""


@identity_group.command("init")
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
@click.option(
    "--mode",
    type=click.Choice(["audit", "protected"], case_sensitive=False),
    default="audit",
    show_default=True,
)
@click.option(
    "--offline-root-pubkey",
    type=click.Path(path_type=Path, dir_okay=False, exists=True),
    default=None,
    help="Initialize public trust state without creating an online root key.",
)
def identity_init(
    workspace: str | None, mode: str, offline_root_pubkey: Path | None
) -> None:
    """Initialize a root-signed workspace identity policy."""
    ws = _cli_workspace(workspace)
    try:
        if offline_root_pubkey is not None:
            state = initialize_offline_identity(ws, offline_root_pubkey)
            click.echo(
                json.dumps(
                    {
                        **state,
                        "mode": "pending_offline_policy",
                        "strict_ready": False,
                    }
                )
            )
            return
        state = bootstrap_workspace_identity(ws, mode=mode.lower())
    except _identity_error() as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(
        json.dumps(
            {
                "workspace_id": state.workspace_id,
                "mode": state.mode,
                "policy_version": state.policy_version,
                "strict_ready": False,
                "custody": "local_audit_only",
            }
        )
    )


@identity_group.group("root")
def identity_root_group() -> None:
    """Create root material for storage outside the online workspace."""


@identity_root_group.command("generate")
@click.option(
    "--private-key",
    required=True,
    type=click.Path(path_type=Path, dir_okay=False),
)
@click.option(
    "--descriptor",
    required=True,
    type=click.Path(path_type=Path, dir_okay=False),
)
def identity_root_generate(private_key: Path, descriptor: Path) -> None:
    """Generate an offline root private key and public descriptor."""
    try:
        result = generate_offline_root(private_key, descriptor)
    except _identity_error() as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(json.dumps(result, indent=2))


@identity_group.group("key")
def identity_key_group() -> None:
    """Generate peer signing keys and public enrollment requests."""


@identity_key_group.command("generate")
@click.argument("producer_id")
@click.option("--principal", required=True)
@click.option(
    "--private-key",
    required=True,
    type=click.Path(path_type=Path, dir_okay=False),
)
@click.option(
    "--request",
    required=True,
    type=click.Path(path_type=Path, dir_okay=False),
)
@click.option("--capability", multiple=True, default=("message",))
@click.option("--topic", multiple=True, default=("okf/handoff",))
def identity_key_generate(
    producer_id: str,
    principal: str,
    private_key: Path,
    request: Path,
    capability: tuple[str, ...],
    topic: tuple[str, ...],
) -> None:
    """Generate one isolated peer key and its public enrollment request."""
    try:
        result = generate_enrollment_request(
            producer_id,
            principal,
            private_key,
            request,
            capabilities=capability,
            topics=topic,
        )
    except _identity_error() as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(json.dumps(result, indent=2))


@identity_group.command("export-policy-request")
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
@click.option(
    "--output",
    required=True,
    type=click.Path(path_type=Path, dir_okay=False),
)
@click.option(
    "--mode",
    type=click.Choice(["audit", "protected", "strict"]),
    default="audit",
)
@click.option(
    "--reference-monitor",
    type=click.Choice(["local_audit", "isolated_broker"]),
    default="isolated_broker",
)
@click.option(
    "--enrollment",
    multiple=True,
    type=click.Path(path_type=Path, dir_okay=False, exists=True),
)
@click.option("--revoke-key", "revoke_keys", multiple=True)
def identity_export_policy_request(
    workspace: str | None,
    output: Path,
    mode: str,
    reference_monitor: str,
    enrollment: tuple[Path, ...],
    revoke_keys: tuple[str, ...],
) -> None:
    """Export a canonical policy/registry request for offline signing."""
    try:
        result = export_policy_request(
            _cli_workspace(workspace),
            output,
            mode=mode,
            reference_monitor=reference_monitor,
            enrollment_paths=enrollment,
            revoke_key_ids=revoke_keys,
        )
    except _identity_error() as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(json.dumps(result, indent=2))


@identity_group.group("ceremony")
def identity_ceremony_group() -> None:
    """Sign policy requests on an offline administrator machine."""


@identity_ceremony_group.command("sign")
@click.option(
    "--root-key",
    required=True,
    type=click.Path(path_type=Path, dir_okay=False, exists=True),
)
@click.option(
    "--request",
    "request_path",
    required=True,
    type=click.Path(path_type=Path, dir_okay=False, exists=True),
)
@click.option(
    "--output",
    required=True,
    type=click.Path(path_type=Path, dir_okay=False),
)
def identity_ceremony_sign(
    root_key: Path, request_path: Path, output: Path
) -> None:
    """Sign one canonical request with the offline root."""
    try:
        result = sign_policy_request(request_path, root_key, output)
    except _identity_error() as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(json.dumps(result, indent=2))


@identity_group.command("import-signed-policy")
@click.argument(
    "bundle", type=click.Path(path_type=Path, dir_okay=False, exists=True)
)
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
def identity_import_signed_policy(bundle: Path, workspace: str | None) -> None:
    """Verify and install a detached root-signed policy bundle."""
    try:
        result = import_signed_policy(_cli_workspace(workspace), bundle)
    except _identity_error() as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(json.dumps(result, indent=2))


@identity_group.command("enroll")
@click.argument("producer_id")
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
@click.option("--capability", multiple=True, default=("message",))
@click.option("--topic", multiple=True, default=("okf/handoff",))
def identity_enroll(
    producer_id: str,
    workspace: str | None,
    capability: tuple[str, ...],
    topic: tuple[str, ...],
) -> None:
    """Enroll one producer key in the local audit registry."""
    ws = _cli_workspace(workspace)
    try:
        key_id = enroll_identity(ws, producer_id, capabilities=capability, topics=topic)
        state = load_trust_state(ws)
    except _identity_error() as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(
        json.dumps(
            {
                "producer_id": producer_id,
                "key_id": key_id,
                "registry_version": state.registry_version,
            }
        )
    )


@identity_group.command("list")
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
def identity_list(workspace: str | None) -> None:
    """List verified policy state and enrolled public identities."""
    try:
        state = load_trust_state(_cli_workspace(workspace))
    except _identity_error() as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(
        json.dumps(
            {
                "workspace_id": state.workspace_id,
                "mode": state.mode,
                "policy_version": state.policy_version,
                "registry_version": state.registry_version,
                "keys": state.registry.get("keys", []),
                "revoked_key_ids": state.registry.get("revoked_key_ids", []),
            },
            indent=2,
        )
    )


@identity_group.command("delegate")
@click.argument("parent_producer_id")
@click.argument("child_id")
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
@click.option("--capability", multiple=True, default=("message",))
@click.option("--topic", multiple=True, default=("okf/handoff",))
@click.option("--ttl-seconds", type=click.IntRange(min=1, max=3600), default=900)
def identity_delegate(
    parent_producer_id: str,
    child_id: str,
    workspace: str | None,
    capability: tuple[str, ...],
    topic: tuple[str, ...],
    ttl_seconds: int,
) -> None:
    """Issue one non-transitive, least-authority child identity."""
    ws = _cli_workspace(workspace)
    try:
        producer_id, key_id = delegate_identity(
            ws,
            parent_producer_id,
            child_id,
            capabilities=capability,
            topics=topic,
            ttl_seconds=ttl_seconds,
        )
    except _identity_error() as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(
        json.dumps(
            {
                "producer_id": producer_id,
                "key_id": key_id,
                "delegated_by": parent_producer_id,
                "ttl_seconds": ttl_seconds,
                "max_delegation_depth": 0,
            }
        )
    )


@identity_group.command("mode")
@click.argument("mode", type=click.Choice(["audit", "protected", "strict"]))
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
def identity_mode(mode: str, workspace: str | None) -> None:
    """Raise the root-signed workspace identity mode."""
    try:
        state = set_policy_mode(_cli_workspace(workspace), mode)
    except _identity_error() as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(
        json.dumps(
            {
                "mode": state.mode,
                "policy_version": state.policy_version,
                "strict_ready": False,
            }
        )
    )


@identity_group.command("reference-monitor")
@click.argument("monitor", type=click.Choice(["local_audit", "isolated_broker"]))
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
@click.option(
    "--isolation-attested",
    is_flag=True,
    help="Acknowledge an externally established distinct-principal/container boundary.",
)
def identity_reference_monitor(
    monitor: str, workspace: str | None, isolation_attested: bool
) -> None:
    """Bind the signed policy to a monitor class; does not make doctor strict-ready."""
    try:
        state = configure_reference_monitor(
            _cli_workspace(workspace),
            monitor,
            isolation_attested=isolation_attested,
        )
    except _identity_error() as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(
        json.dumps(
            {
                "reference_monitor": state.policy["reference_monitor"],
                "policy_version": state.policy_version,
                "strict_ready": False,
            }
        )
    )


@identity_group.command("record-recovery")
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
@click.option("--reason", required=True)
def identity_record_recovery(workspace: str | None, reason: str) -> None:
    """Record a root-signed prospective break-glass recovery boundary."""
    ws = _cli_workspace(workspace)
    store = EventStore(ws, auto_prune=False)
    try:
        effective_after = store.latest_event_id()
    finally:
        store.close()
    try:
        event = record_break_glass_recovery(
            ws,
            reason=reason,
            effective_after_event_id=effective_after,
        )
    except _identity_error() as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(json.dumps(event))


@identity_group.command("rotate")
@click.argument("producer_id")
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
@click.option("--grace-seconds", type=click.IntRange(min=0), default=300)
def identity_rotate(
    producer_id: str, workspace: str | None, grace_seconds: int
) -> None:
    """Rotate one producer key with a bounded verification grace window."""
    try:
        key_id = rotate_identity(
            _cli_workspace(workspace), producer_id, grace_seconds=grace_seconds
        )
    except _identity_error() as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(json.dumps({"producer_id": producer_id, "key_id": key_id}))


@identity_group.command("revoke")
@click.argument("key_id")
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
def identity_revoke(key_id: str, workspace: str | None) -> None:
    """Immediately revoke a producer key in the root-signed registry."""
    try:
        state = revoke_identity_key(_cli_workspace(workspace), key_id)
    except _identity_error() as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(
        json.dumps(
            {
                "key_id": key_id,
                "revoked": True,
                "registry_version": state.registry_version,
            }
        )
    )


@identity_group.command("issue-wake-capability")
@click.argument("runtime")
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
def identity_issue_wake_capability(runtime: str, workspace: str | None) -> None:
    """Issue a root-policy-bound capability for one wake runtime."""
    try:
        path = issue_wake_capability(_cli_workspace(workspace), runtime)
    except _identity_error() as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(json.dumps({"runtime": runtime, "capability_file": str(path)}))


@identity_group.command("verify-event")
@click.argument("event_id", type=click.IntRange(min=1))
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
def identity_verify_event(event_id: int, workspace: str | None) -> None:
    """Recompute one stored event's cryptographic verification result."""
    store = EventStore(_cli_workspace(workspace), auto_prune=False)
    try:
        result = store.verify_event(event_id)
    finally:
        store.close()
    click.echo(
        json.dumps(
            {
                "event_id": event_id,
                "verified": result.verified,
                "producer_id": result.producer_id,
                "key_id": result.key_id,
                "policy_version": result.policy_version,
                "reason": result.reason,
            }
        )
    )
    if not result.verified:
        raise SystemExit(1)


@main.group("broker")
def broker_group() -> None:
    """Run the isolated AgentID reference-monitor broker."""


@broker_group.command("run")
@click.option("--workspace", default=None, envvar="AGENTBUS_WORKSPACE")
@click.option(
    "--socket",
    "socket_path",
    type=click.Path(path_type=Path, dir_okay=False),
    default="/run/agentbus/agentbus.sock",
    show_default=True,
)
@click.option(
    "--force",
    is_flag=True,
    help="Reclaim an owner-matched stale socket after proving no broker accepts it.",
)
def broker_run(workspace: str | None, socket_path: Path, force: bool) -> None:
    """Serve authenticated, length-prefixed requests over a Unix socket."""
    if os.name != "posix":
        raise click.ClickException("strict broker is supported on POSIX hosts only")
    from agentbus.broker import run_broker

    try:
        run_broker(_cli_workspace(workspace), socket_path, force=force)
    except (_identity_error(), OSError, ValueError) as exc:
        raise click.ClickException(str(exc)) from exc


if __name__ == "__main__":
    main()
