#!/usr/bin/env python3
"""Dispatch a comprehensive pre-push QA mission to Factory via AgentBus.

Grok (engineer) runs this whenever code may ship. Factory owns execution.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

# Prefer in-tree package when developing
_REPO = Path(__file__).resolve().parents[1]
if str(_REPO / "src") not in sys.path:
    sys.path.insert(0, str(_REPO / "src"))

from agentbus.store import EventStore  # noqa: E402


def _run(cmd: list[str], cwd: Path | None = None) -> str:
    try:
        out = subprocess.run(
            cmd,
            cwd=cwd,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        return (out.stdout or out.stderr or "").strip()
    except (OSError, subprocess.TimeoutExpired) as exc:
        return f"(unavailable: {exc})"


def git_snapshot(repo: Path) -> dict[str, str]:
    if not (repo / ".git").exists() and not (repo / ".git").is_file():
        # may be symlink repo
        pass
    head = _run(["git", "rev-parse", "--short", "HEAD"], cwd=repo)
    branch = _run(["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=repo)
    stat = _run(["git", "diff", "--stat", "HEAD"], cwd=repo)
    status = _run(["git", "status", "-sb"], cwd=repo)
    dirty = _run(["git", "status", "--porcelain"], cwd=repo)
    return {
        "git_head": head or "unknown",
        "branch": branch or "unknown",
        "diff_stat": stat[:2000] if stat else "(clean or unavailable)",
        "status_sb": status[:500],
        "dirty": "yes" if dirty else "no",
    }


def render_mission(
    template: Path,
    *,
    initiative: str,
    repo: Path,
    mission_id: str,
    snap: dict[str, str],
    title: str,
    extra: str,
) -> Path:
    text = template.read_text(encoding="utf-8")
    filled = (
        text.replace("{{initiative}}", initiative)
        .replace("{{repo}}", str(repo))
        .replace("{{git_head}}", snap["git_head"])
        .replace("{{diff_stat}}", snap["diff_stat"])
        .replace("{{mission_id}}", mission_id)
        .replace("{{dispatch_event_id}}", "PENDING_PUBLISH")
    )
    out_dir = Path.home() / "okf_agent_workspace" / "initiatives" / initiative / "missions"
    # Prefer workspace from AGENTBUS_WORKSPACE
    ws = Path(os.environ.get("AGENTBUS_WORKSPACE", Path.home() / "okf_agent_workspace"))
    out_dir = ws / "initiatives" / initiative / "missions"
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    safe = "".join(c if c.isalnum() or c in "-_" else "_" for c in title)[:40]
    out = out_dir / f"mission_qa_{safe}_{stamp}.md"
    header = (
        f"---\ntype: Mission\ntitle: {title}\nmission_id: {mission_id}\n"
        f"status: dispatched\nrequester: grok\nexecutor: factory\n---\n\n"
        f"# {title}\n\n"
        f"**mission_id:** `{mission_id}`  \n"
        f"**repo:** `{repo}`  \n"
        f"**branch/HEAD:** `{snap['branch']} @ {snap['git_head']}`  \n"
        f"**dirty tree:** {snap['dirty']}  \n"
        f"**dispatched_at:** {stamp}  \n\n"
        f"## git status\n```\n{snap['status_sb']}\n```\n\n"
        f"## diff --stat\n```\n{snap['diff_stat']}\n```\n\n"
    )
    if extra:
        header += f"## Extra context\n\n{extra}\n\n"
    header += "---\n\n## Checklist (from template)\n\n"
    out.write_text(header + filled, encoding="utf-8")
    return out


def main() -> int:
    p = argparse.ArgumentParser(description="Dispatch Factory QA mission on AgentBus")
    p.add_argument("--workspace", default=os.environ.get("AGENTBUS_WORKSPACE", str(Path.home() / "okf_agent_workspace")))
    p.add_argument("--initiative", default="agentbus")
    p.add_argument("--repo", default=None, help="Git repo path (default: projects/agentbus under workspace)")
    p.add_argument("--title", required=True)
    p.add_argument(
        "--mission",
        default=None,
        help="Template path (default: initiatives/<init>/missions/mission_qa_prepush_template.md)",
    )
    p.add_argument("--extra", default="")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--producer-id", default="grok")
    args = p.parse_args()

    ws = Path(args.workspace).expanduser().resolve()
    repo = Path(args.repo).expanduser().resolve() if args.repo else (ws / "projects" / "agentbus")
    template = (
        Path(args.mission).expanduser()
        if args.mission
        else ws / "initiatives" / args.initiative / "missions" / "mission_qa_prepush_template.md"
    )
    if not template.is_file():
        print(f"template not found: {template}", file=sys.stderr)
        return 2

    mission_id = f"qa-{uuid.uuid4().hex[:12]}"
    snap = git_snapshot(repo)
    mission_path = render_mission(
        template,
        initiative=args.initiative,
        repo=repo,
        mission_id=mission_id,
        snap=snap,
        title=args.title,
        extra=args.extra,
    )
    # relative link for OKF
    try:
        rel_mission = "/" + str(mission_path.relative_to(ws))
    except ValueError:
        rel_mission = str(mission_path)

    summary = (
        f"FACTORY_QA_MISSION: {args.title} | mission_id={mission_id} | "
        f"repo={repo.name}@{snap['git_head']} dirty={snap['dirty']} | "
        f"Run full pre-push QA per mission file; reply QA_VERDICT GREEN|RED with causation_id. "
        f"Spawn droids as needed (droid_proof). Grok will not self-QA."
    )
    if len(summary) > 1900:
        summary = summary[:1900]

    payload = {
        "from": "grok",
        "to": "factory",
        "summary": summary,
        "initiative": args.initiative,
        "links": [
            rel_mission,
            "/runbooks/dispatch-factory-qa.md",
            "/runbooks/swarm-session.md",
            "/agents/factory.md",
        ],
    }

    print(json.dumps({"mission_id": mission_id, "mission_path": str(mission_path), "payload": payload}, indent=2))

    if args.dry_run:
        return 0

    store = EventStore(ws)
    try:
        ev, dup = store.publish(
            topic="okf/handoff",
            producer_id=args.producer_id,
            schema_version="1.0",
            payload=payload,
        )
    finally:
        store.close()

    # Patch mission file with dispatch event id
    text = mission_path.read_text(encoding="utf-8")
    mission_path.write_text(
        text.replace("PENDING_PUBLISH", str(ev.event_id)).replace(
            "{{dispatch_event_id}}", str(ev.event_id)
        ),
        encoding="utf-8",
    )

    print(
        json.dumps(
            {
                "published": True,
                "event_id": ev.event_id,
                "duplicate": dup,
                "mission_id": mission_id,
                "mission_path": str(mission_path),
                "to": "factory",
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
