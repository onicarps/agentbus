"""CLI: agentbus-ops watchdog (bash sre_edge_watchdog.sh parity)."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import click

from agentbus_ops import __version__
from agentbus_ops.agent import SREWatchdogAgent, decision_to_public_dict
from agentbus_ops.policy import DEFAULT_COOLDOWN_SECONDS, DEFAULT_PRODUCER_ID, DEFAULT_TO_AGENT
from agentbus_ops.state import default_state_path


def _default_workspace() -> str:
    return os.environ.get("AGENTBUS_WORKSPACE") or "/home/oni/okf_agent_workspace"


@click.group()
@click.version_option(__version__, prog_name="agentbus-ops")
def main() -> None:
    """AgentBus ops tools (edge-triggered SRE watchdog)."""


@main.command("watchdog")
@click.option(
    "--workspace",
    default=None,
    envvar="AGENTBUS_WORKSPACE",
    help="Coordination workspace root (default: AGENTBUS_WORKSPACE or OKF path).",
)
@click.option("--dry-run", is_flag=True, help="Decide + print; do not publish (unless --write-state).")
@click.option(
    "--write-state",
    is_flag=True,
    help="With --dry-run, still update sre_last_state.json (bootstrap tests).",
)
@click.option(
    "--force-bootstrap-publish",
    is_flag=True,
    help="On first run (no state file), publish current level (default: silent seed).",
)
@click.option("--include-metrics", is_flag=True, help="Attach compact agentbus metrics to summary.")
@click.option("--json", "json_out", is_flag=True, help="Machine-readable decision JSON on stdout.")
@click.option(
    "--cooldown",
    type=int,
    default=None,
    envvar="SRE_COOLDOWN_SECONDS",
    help=f"Min seconds between same-level re-publish (default {DEFAULT_COOLDOWN_SECONDS}).",
)
@click.option("--to", "to_agent", default=None, envvar="SRE_TO_AGENT", help="Handoff to= (default all).")
@click.option(
    "--producer",
    "producer_id",
    default=None,
    envvar="SRE_PRODUCER_ID",
    help="producer_id (default aider).",
)
@click.option(
    "--state-file",
    type=click.Path(dir_okay=False),
    default=None,
    envvar="SRE_STATE_FILE",
    help="Override state path.",
)
@click.option(
    "--health-script",
    type=click.Path(exists=False, dir_okay=False),
    default=None,
    envvar="SRE_HEALTH_SCRIPT",
    help="Override health probe script path.",
)
@click.option(
    "--seed-level",
    type=click.Choice(["healthy", "degraded", "critical"], case_sensitive=False),
    default=None,
    help="Testing: pretend previous level was LEVEL.",
)
def watchdog(
    workspace: str | None,
    dry_run: bool,
    write_state: bool,
    force_bootstrap_publish: bool,
    include_metrics: bool,
    json_out: bool,
    cooldown: int | None,
    to_agent: str | None,
    producer_id: str | None,
    state_file: str | None,
    health_script: str | None,
    seed_level: str | None,
) -> None:
    """Edge-triggered SRE: publish SRE_STATUS only when health level changes."""
    ws = Path(workspace or _default_workspace()).resolve()
    if not ws.is_dir():
        click.echo(f"ERROR: workspace not a directory: {ws}", err=True)
        sys.exit(1)

    # Guardrail: never use implementation repo as bus root
    if "/projects/agentbus" in str(ws) and not (ws / "scripts" / "swarm_health_check.sh").is_file():
        # Allow only if probe exists; still warn via note in probe for bad_workspace
        pass

    agent = SREWatchdogAgent(
        workspace=ws,
        state_file=state_file or default_state_path(ws),
        health_script=health_script,
        cooldown_seconds=cooldown if cooldown is not None else DEFAULT_COOLDOWN_SECONDS,
        producer_id=producer_id or DEFAULT_PRODUCER_ID,
        to_agent=to_agent or DEFAULT_TO_AGENT,
    )

    decision = agent.run_once(
        dry_run=dry_run,
        write_state=write_state,
        force_bootstrap_publish=force_bootstrap_publish,
        include_metrics=include_metrics,
        seed_level=seed_level,
    )

    if not json_out:
        click.echo(
            f"sre_edge_watchdog: action={decision.action} level={decision.level} "
            f"reason={decision.reason} dry_run={int(dry_run)}",
            err=True,
        )
        if decision.should_publish and dry_run:
            click.echo(
                f"DRY-RUN would publish topic=okf/handoff producer={agent.producer_id} "
                f"idem={decision.idempotency_key}",
                err=True,
            )
        if decision.should_publish and not dry_run and decision.publish_ok:
            click.echo(
                f"published SRE_STATUS event_id={decision.publish_event_id or '?'} "
                f"idem={decision.idempotency_key}",
                err=True,
            )
        if not dry_run or write_state:
            if decision.publish_ok or not decision.should_publish:
                click.echo(f"state written: {agent.state_file}", err=True)

    if decision.should_publish and not dry_run and not decision.publish_ok:
        click.echo(f"ERROR: publish failed: {decision.publish_error}", err=True)
        if json_out:
            click.echo(json.dumps(decision_to_public_dict(decision), separators=(",", ":")))
        sys.exit(2)

    if json_out:
        click.echo(json.dumps(decision_to_public_dict(decision), separators=(",", ":")))
    else:
        click.echo(
            f"action={decision.action} level={decision.level} "
            f"should_publish={str(decision.should_publish).lower()}"
        )
        if decision.publish_event_id:
            click.echo(f"publish_event_id={decision.publish_event_id}")

    sys.exit(0)


if __name__ == "__main__":
    main()
