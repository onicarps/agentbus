#!/usr/bin/env python3
"""Slack Socket Mode ↔ AgentBus bridge (optional ops script).

Agy strategy (2026-07-22, decision agy-slack-bridge-strategy):
  - producer_id: slack  (role bridge — Aider adds to roles.yaml)
  - channel meta via links: ["slack://{channel}/{ts}"]  (no schema change)
  - inbound default to: agy (never swarm); parse @agent / /ask agent
  - idempotency_key: slack:{channel}:{ts}
  - outbound only to: slack; strict ops-noise suppress
  - cold start: seek bus head (status.latest_event_id); cursor file
  - optional: not enabled by default in product package

Env:
  SLACK_BOT_TOKEN, SLACK_APP_TOKEN  — required to run live
  AGENTBUS_WORKSPACE               — OKF coordination root (default host path)
  SLACK_DEFAULT_CHANNEL            — optional fallback for outbound without links
  SLACK_BRIDGE_POLL_SECONDS        — poll interval (default 2)
  SLACK_BRIDGE_DRY_RUN             — if "1", print publishes/posts instead of CLI/API

Do NOT start against a live bus until:
  1) .agentbus/roles.yaml has producers.slack: bridge
  2) worker from-lists include slack
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Constants (aligned with Hermes Telegram standing orders / prompt_common)
# ---------------------------------------------------------------------------

OPS_SUMMARY_PREFIXES: tuple[str, ...] = (
    "RUNNER_ACK",
    "RUNNER_ERROR",
    "RUNNER_SUSPEND",
    "NO-OP",
    "TERMINAL_IDLE",
    "CHAIN_BREAK",
    "SUPPRESS ACK",
)

KNOWN_AGENTS: frozenset[str] = frozenset(
    {
        "agy",
        "grok",
        "factory",
        "hermes",
        "aider",
        "human",
        "swarm",  # recognized but never used as default ingress target
    }
)

DEFAULT_TO = "agy"
PRODUCER_ID = "slack"
HANDOFF_TOPIC = "okf/handoff"
SUMMARY_MAX = 2000
CURSOR_FILENAME = "slack_bridge.cursor"
SLACK_URI_RE = re.compile(r"^slack://([^/]+)/(.+)$")

# Target routing: @agent | /ask agent | agent:  (leading)
TARGET_RE = re.compile(
    r"^\s*(?:@|/ask\s+|/)(?P<a1>[a-z][a-z0-9_-]*)\b"
    r"|(?P<a2>[a-z][a-z0-9_-]*):\s+",
    re.IGNORECASE,
)

# Subtypes that are not fresh human chat (edits, joins, bot echoes, …)
IGNORED_MESSAGE_SUBTYPES: frozenset[str] = frozenset(
    {
        "bot_message",
        "message_changed",
        "message_deleted",
        "channel_join",
        "channel_leave",
        "channel_topic",
        "channel_purpose",
        "channel_name",
        "channel_archive",
        "channel_unarchive",
        "group_join",
        "group_leave",
        "file_share",
        "file_comment",
        "file_mention",
        "pinned_item",
        "unpinned_item",
        "ekm_access_denied",
        "channel_posting_permissions",
        "thread_broadcast",
    }
)


# ---------------------------------------------------------------------------
# Pure helpers (unit-tested without Slack or live bus)
# ---------------------------------------------------------------------------


def is_ops_noise_summary(summary: str | None) -> bool:
    """True if handoff summary is companion/ops noise (do not post to Slack)."""
    text = (summary or "").strip()
    if not text:
        return False
    upper = text.upper()
    for prefix in OPS_SUMMARY_PREFIXES:
        if upper.startswith(prefix.upper()):
            return True
    return False


def truncate_summary(text: str, max_len: int = SUMMARY_MAX) -> str:
    text = (text or "").strip()
    if not text:
        return "(empty)"
    if len(text) <= max_len:
        return text
    # Leave room for ellipsis marker
    return text[: max_len - 3].rstrip() + "..."


def slack_uri(channel: str, ts: str) -> str:
    return f"slack://{channel}/{ts}"


def parse_slack_uri(uri: str) -> tuple[str, str] | None:
    m = SLACK_URI_RE.match((uri or "").strip())
    if not m:
        return None
    return m.group(1), m.group(2)


def channel_from_links(links: list[str] | None) -> tuple[str, str] | None:
    """Return (channel, ts) from the first slack:// link, if any."""
    for link in links or []:
        parsed = parse_slack_uri(str(link))
        if parsed:
            return parsed
    return None


def parse_target_agent(text: str, default_to: str = DEFAULT_TO) -> tuple[str, str]:
    """Parse routing from Slack text.

    Returns (to_agent, cleaned_summary).
    Never returns swarm as the *default* when no target is specified.
    Explicit `@swarm` / `swarm:` is still allowed if the human asks.
    """
    raw = text or ""
    m = TARGET_RE.match(raw)
    if not m:
        return default_to, raw.strip()

    agent = (m.group("a1") or m.group("a2") or "").lower()
    if agent not in KNOWN_AGENTS:
        # Unknown @mention — keep text, default triage
        return default_to, raw.strip()

    cleaned = raw[m.end() :].strip()
    if not cleaned:
        cleaned = raw.strip()
    return agent, cleaned


def build_inbound_payload(
    *,
    text: str,
    channel: str,
    ts: str,
    user: str = "user",
    default_to: str = DEFAULT_TO,
    initiative: str | None = "agentbus",
) -> dict[str, Any]:
    """Build schema-valid okf/handoff payload for Slack → bus.

    Channel metadata lives only in links (additionalProperties: false).
    """
    to_agent, cleaned = parse_target_agent(text, default_to=default_to)
    # Safety: chat ingress must not stampede the swarm unless explicit
    if to_agent == "swarm" and default_to != "swarm":
        # Explicit @swarm is intentional; leave it. Implicit never happens
        # because parse_target_agent only returns swarm when matched.
        pass

    # Context engineering: explicit A2A reply directive so agents do not
    # guess routing (Slack is primary UI; do not default to Hermes).
    summary = truncate_summary(
        f"[slack:{user}] {cleaned}\n"
        f"(SYSTEM: Reply directly to 'slack' on the bus)"
    )
    payload: dict[str, Any] = {
        "from": PRODUCER_ID,
        "to": to_agent,
        "summary": summary,
        "links": [slack_uri(channel, ts)],
    }
    if initiative:
        payload["initiative"] = initiative
    return payload


def inbound_idempotency_key(channel: str, ts: str) -> str:
    return f"slack:{channel}:{ts}"


def should_accept_slack_message(event: dict[str, Any]) -> bool:
    """Filter Slack message events before bus publish."""
    if event.get("bot_id"):
        return False
    if event.get("subtype") in IGNORED_MESSAGE_SUBTYPES:
        return False
    # Hidden / system
    if event.get("hidden"):
        return False
    text = (event.get("text") or "").strip()
    if not text:
        return False
    if not event.get("channel") or not event.get("ts"):
        return False
    return True


def should_post_outbound(payload: dict[str, Any]) -> bool:
    """Outbound: only explicit to=slack, never ops noise."""
    if (payload.get("to") or "").strip().lower() != "slack":
        return False
    summary = payload.get("summary") or ""
    if summary.startswith("RUNNER_ACK") and "out=" in summary:
        return True
    if is_ops_noise_summary(summary):
        return False
    return True


def format_outbound_message(payload: dict[str, Any]) -> str:
    sender = (payload.get("from") or "system").strip()
    summary = (payload.get("summary") or "").strip()
    if summary.startswith("RUNNER_ACK") and "out=" in summary:
        summary = summary.split("out=", 1)[1].strip()
    return f"*{sender}*\n{summary}"


# ---------------------------------------------------------------------------
# Cursor + agentbus CLI helpers
# ---------------------------------------------------------------------------


def workspace_root(workspace: str | Path | None = None) -> Path:
    raw = workspace or os.environ.get(
        "AGENTBUS_WORKSPACE", "/home/oni/okf_agent_workspace"
    )
    return Path(raw).expanduser().resolve()


def cursor_path(workspace: Path) -> Path:
    return workspace / ".agentbus" / CURSOR_FILENAME


def load_cursor(workspace: Path) -> int | None:
    path = cursor_path(workspace)
    if not path.is_file():
        return None
    try:
        text = path.read_text(encoding="utf-8").strip()
        if not text:
            return None
        return int(text.split()[0])
    except (ValueError, OSError):
        return None


def save_cursor(workspace: Path, event_id: int) -> None:
    path = cursor_path(workspace)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"{int(event_id)}\n", encoding="utf-8")


def run_agentbus(
    args: list[str],
    *,
    workspace: Path,
    timeout: float = 15.0,
) -> subprocess.CompletedProcess[str]:
    cmd = ["agentbus", *args, "--workspace", str(workspace)]
    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )


def seek_bus_head(workspace: Path) -> int:
    """True bus head via `agentbus status` (never poll --limit 1 for head).

    `poll --limit 1` returns the oldest page's latest_id, which cold-starts
    history replay. status.latest_event_id is the global max.
    """
    res = run_agentbus(["status"], workspace=workspace, timeout=15.0)
    if res.returncode != 0:
        raise RuntimeError(
            f"agentbus status failed rc={res.returncode}: "
            f"{(res.stderr or res.stdout or '').strip()[:500]}"
        )
    data = json.loads(res.stdout)
    head = int(data.get("latest_event_id") or 0)
    return head


def resolve_start_cursor(workspace: Path) -> int:
    """Load durable cursor or seek head — never cold-replay history."""
    existing = load_cursor(workspace)
    if existing is not None:
        return existing
    head = seek_bus_head(workspace)
    save_cursor(workspace, head)
    return head


def publish_handoff(
    *,
    workspace: Path,
    payload: dict[str, Any],
    idempotency_key: str,
    dry_run: bool = False,
) -> dict[str, Any]:
    if dry_run:
        return {
            "dry_run": True,
            "payload": payload,
            "idempotency_key": idempotency_key,
        }
    res = run_agentbus(
        [
            "publish",
            "--topic",
            HANDOFF_TOPIC,
            "--producer-id",
            PRODUCER_ID,
            "--payload",
            json.dumps(payload),
            "--idempotency-key",
            idempotency_key,
        ],
        workspace=workspace,
        timeout=20.0,
    )
    if res.returncode != 0:
        err = (res.stderr or res.stdout or "").strip()
        raise RuntimeError(f"publish failed rc={res.returncode}: {err[:800]}")
    try:
        return json.loads(res.stdout) if res.stdout.strip() else {"ok": True}
    except json.JSONDecodeError:
        return {"ok": True, "raw": res.stdout[:200]}


def poll_handoffs(
    *,
    workspace: Path,
    since_id: int,
    limit: int = 50,
) -> tuple[list[dict[str, Any]], int, bool]:
    res = run_agentbus(
        [
            "poll",
            "--topic",
            HANDOFF_TOPIC,
            "--since-id",
            str(since_id),
            "--limit",
            str(limit),
        ],
        workspace=workspace,
        timeout=20.0,
    )
    if res.returncode != 0:
        raise RuntimeError(
            f"poll failed rc={res.returncode}: "
            f"{(res.stderr or res.stdout or '').strip()[:500]}"
        )
    data = json.loads(res.stdout)
    events = data.get("events") or []
    latest_id = int(data.get("latest_id") or since_id)
    has_more = bool(data.get("has_more"))
    return events, latest_id, has_more


def normalize_payload(payload: Any) -> dict[str, Any]:
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, str):
        try:
            parsed = json.loads(payload)
            return parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


# ---------------------------------------------------------------------------
# Live bridge (requires slack_bolt + tokens)
# ---------------------------------------------------------------------------


def _require_workspace(workspace: Path) -> None:
    agentbus_dir = workspace / ".agentbus"
    if not agentbus_dir.is_dir():
        print(
            f"Error: {agentbus_dir} missing — refuse to run "
            f"(set AGENTBUS_WORKSPACE to OKF coordination root).",
            file=sys.stderr,
        )
        sys.exit(2)


def build_bolt_app(
    *,
    workspace: Path,
    bot_token: str,
    dry_run: bool,
    default_channel: str | None,
):
    from slack_bolt import App

    app = App(token=bot_token)

    @app.event("message")
    def handle_message_events(body, logger, say):  # type: ignore[no-untyped-def]
        event = body.get("event") or {}
        if not should_accept_slack_message(event):
            return

        text = event.get("text") or ""
        user = event.get("user") or "user"
        channel = event["channel"]
        ts = event["ts"]

        payload = build_inbound_payload(
            text=text, channel=channel, ts=ts, user=user
        )
        key = inbound_idempotency_key(channel, ts)
        print(
            f"[Slack→Bus] {user} ch={channel} ts={ts} "
            f"to={payload['to']}: {payload['summary'][:120]}"
        )
        try:
            publish_handoff(
                workspace=workspace,
                payload=payload,
                idempotency_key=key,
                dry_run=dry_run,
            )
            if not dry_run:
                try:
                    app.client.reactions_add(
                        channel=channel, timestamp=ts, name="robot_face"
                    )
                except Exception as react_err:  # noqa: BLE001
                    logger.warning("reaction failed: %s", react_err)
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to publish to agentbus: %s", exc)
            try:
                say("❌ Failed to reach the swarm bus.")
            except Exception:  # noqa: BLE001
                pass

    def poll_loop() -> None:
        print("[AgentBus] Outbound poll loop started...")
        try:
            last_id = resolve_start_cursor(workspace)
            print(f"[AgentBus] cursor start last_id={last_id}")
        except Exception as exc:  # noqa: BLE001
            print(f"[AgentBus] FATAL cursor init: {exc}", file=sys.stderr)
            return

        poll_s = float(os.environ.get("SLACK_BRIDGE_POLL_SECONDS", "2"))
        while True:
            try:
                events, _page_latest, has_more = poll_handoffs(
                    workspace=workspace, since_id=last_id
                )
                for ev in events:
                    eid = int(ev.get("event_id") or 0)
                    payload = normalize_payload(ev.get("payload"))
                    if should_post_outbound(payload):
                        ch_ts = channel_from_links(payload.get("links"))
                        channel = ch_ts[0] if ch_ts else default_channel
                        msg = format_outbound_message(payload)
                        if not channel:
                            print(
                                f"[Warn] to=slack event {eid} has no "
                                f"slack:// link and no SLACK_DEFAULT_CHANNEL: "
                                f"{msg[:160]}"
                            )
                        elif dry_run:
                            print(f"[dry-run Slack post] ch={channel} {msg[:200]}")
                        else:
                            try:
                                kwargs: dict[str, Any] = {
                                    "channel": channel,
                                    "text": msg,
                                }
                                if ch_ts:
                                    # Thread under the original human message
                                    kwargs["thread_ts"] = ch_ts[1]
                                app.client.chat_postMessage(**kwargs)
                                print(f"[Bus→Slack] posted event {eid} → {channel}")
                            except Exception as post_err:  # noqa: BLE001
                                print(f"[Slack Error] post event {eid}: {post_err}")
                    if eid > last_id:
                        last_id = eid
                        save_cursor(workspace, last_id)
                # Drain catch-up without sleeping if has_more
                if has_more and events:
                    continue
            except Exception as exc:  # noqa: BLE001
                print(f"[AgentBus] Poll error: {exc}")
            time.sleep(poll_s)

    return app, poll_loop


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Slack Socket Mode ↔ AgentBus bridge (optional ops script)"
    )
    parser.add_argument(
        "--workspace",
        default=os.environ.get(
            "AGENTBUS_WORKSPACE", "/home/oni/okf_agent_workspace"
        ),
        help="OKF coordination workspace root",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=os.environ.get("SLACK_BRIDGE_DRY_RUN") == "1",
        help="Do not publish/post; print actions",
    )
    parser.add_argument(
        "--seek-head-only",
        action="store_true",
        help="Write cursor to bus head and exit (no Socket Mode)",
    )
    args = parser.parse_args(argv)

    workspace = workspace_root(args.workspace)
    _require_workspace(workspace)

    if args.seek_head_only:
        head = seek_bus_head(workspace)
        save_cursor(workspace, head)
        print(f"cursor={head} written to {cursor_path(workspace)}")
        return 0

    bot_token = os.environ.get("SLACK_BOT_TOKEN")
    app_token = os.environ.get("SLACK_APP_TOKEN")
    if not bot_token or not app_token:
        print(
            "Error: set SLACK_BOT_TOKEN and SLACK_APP_TOKEN "
            "(or use --seek-head-only / import helpers for tests).",
            file=sys.stderr,
        )
        return 1

    default_channel = os.environ.get("SLACK_DEFAULT_CHANNEL") or None
    dry_run = bool(args.dry_run)

    try:
        from slack_bolt.adapter.socket_mode import SocketModeHandler
    except ImportError:
        print(
            "Error: slack_bolt not installed. "
            "pip install slack-bolt  (optional ops dependency)",
            file=sys.stderr,
        )
        return 1

    app, poll_loop = build_bolt_app(
        workspace=workspace,
        bot_token=bot_token,
        dry_run=dry_run,
        default_channel=default_channel,
    )
    poller = threading.Thread(target=poll_loop, daemon=True, name="agentbus-poll")
    poller.start()

    print(
        f"Starting Slack Socket Mode Bridge "
        f"(workspace={workspace}, dry_run={dry_run}, producer={PRODUCER_ID})..."
    )
    handler = SocketModeHandler(app, app_token)
    handler.start()
    return 0


if __name__ == "__main__":
    sys.exit(main())
