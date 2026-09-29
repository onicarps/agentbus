#!/usr/bin/env python3
"""Slack Socket Mode ↔ AgentBus bridge (optional ops script).

Agy strategy (2026-07-22, decision agy-slack-bridge-strategy):
  - producer_id: slack  (role bridge — Aider adds to roles.yaml)
  - channel meta via links: ["slack://{channel}/{ts}"]  (no schema change)
  - inbound always to: agy; preserve @agent / /ask agent as triage context
  - idempotency_key: slack:{channel}:{ts}
  - outbound only from: agy to: slack; strict ops-noise suppress
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
import logging
import os
import re
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any


logger = logging.getLogger(__name__)

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

DEFAULT_TO = "agy"
PRODUCER_ID = "slack"
HANDOFF_TOPIC = "okf/handoff"
SUMMARY_MAX = 2000
CURSOR_FILENAME = "slack_bridge.cursor"
SLACK_URI_RE = re.compile(r"^slack://([^/]+)/(.+)$")

# Slack app_mention text usually starts with <@UBOTID> before @agent routing.
BOT_MENTION_RE = re.compile(r"^(?:\s*<@[A-Za-z0-9]+>)+")

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


def correlation_ts(event: dict[str, Any]) -> str:
    """Prefer thread root ts so replies stay in the human conversation."""
    return str(event.get("thread_ts") or event.get("ts") or "")


def strip_leading_bot_mentions(text: str) -> str:
    """Remove leading Slack ``<@U…>`` tokens (app_mention prefix) before routing."""
    raw = text or ""
    stripped = BOT_MENTION_RE.sub("", raw)
    return stripped.lstrip() if stripped != raw else raw


def parse_target_agent(text: str, default_to: str = DEFAULT_TO) -> tuple[str, str]:
    """Return the exclusive Agy route while preserving the human request.

    Slack is the human interface and Agy is its sole handler. A user mention
    such as ``@codex`` is context for Agy, never a bus destination.
    ``default_to`` remains for API compatibility and is deliberately ignored.
    """
    del default_to
    return DEFAULT_TO, strip_leading_bot_mentions(text or "").strip()


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
    ``ts`` should be ``thread_ts or ts`` so outbound threads under the root.
    """
    del default_to
    to_agent, cleaned = parse_target_agent(text)

    # The Slack context stays on the causal chain for Agy's eventual
    # synthesized reply; it must never authorize a direct worker response.
    # Reserve budget so the SYSTEM directive is never truncated away.
    uri = slack_uri(channel, ts)
    system = (
        "(SYSTEM: Substance replies must route to 'agy' on the bus with links "
        "preserved. Only Agy communicates directly to Slack.)"
    )
    user_part = f"[slack:{user}] {cleaned}".strip()
    reserved = len(system) + 1  # newline
    body_budget = max(32, SUMMARY_MAX - reserved)
    user_part = truncate_summary(user_part, max_len=body_budget)
    summary = f"{user_part}\n{system}"
    if len(summary) > SUMMARY_MAX:
        summary = truncate_summary(summary, max_len=SUMMARY_MAX)
    payload: dict[str, Any] = {
        "from": PRODUCER_ID,
        "to": to_agent,
        "summary": summary,
        "links": [uri],
    }
    if initiative:
        payload["initiative"] = initiative
    return payload


def resolve_slack_channel_ts(
    payload: dict[str, Any],
    *,
    causation_id: int | None = None,
    fetch_event: Any | None = None,
    max_hops: int = 20,
) -> tuple[str, str] | None:
    """Resolve (channel, ts) from payload.links or causation-chain backfill.

    Soft links: agents / companion ACK often drop ``links``. Walk ancestors via
    ``fetch_event(event_id) -> event dict | None`` until a ``slack://`` link is
    found (P0 fix pack 2026-07-22).
    """
    ch_ts = channel_from_links(payload.get("links") if isinstance(payload, dict) else None)
    if ch_ts:
        return ch_ts
    if fetch_event is None or causation_id is None:
        return None
    try:
        cur: int | None = int(causation_id)
    except (TypeError, ValueError):
        return None
    seen: set[int] = set()
    hops = 0
    while cur is not None and hops < max_hops and cur not in seen:
        seen.add(cur)
        hops += 1
        try:
            ev = fetch_event(cur)
        except Exception:  # noqa: BLE001 — best-effort backfill
            break
        if not ev:
            break
        if not isinstance(ev, dict):
            break
        raw_pl = ev.get("payload")
        if isinstance(raw_pl, str):
            try:
                raw_pl = json.loads(raw_pl)
            except json.JSONDecodeError:
                raw_pl = {}
        pl = raw_pl if isinstance(raw_pl, dict) else {}
        ch_ts = channel_from_links(pl.get("links"))
        if ch_ts:
            return ch_ts
        next_c = ev.get("causation_id")
        try:
            cur = int(next_c) if next_c is not None else None
        except (TypeError, ValueError):
            cur = None
    return None


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
    """Forward only Agy's human-facing Slack reply, never ops noise."""
    if (payload.get("to") or "").strip().lower() != "slack":
        return False
    producer = (payload.get("from") or "").strip().lower()
    if producer != "agy":
        logger.warning("suppressing unauthorized Slack egress from producer=%r", producer)
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
    @app.event("app_mention")
    def handle_message_events(body, logger, say):  # type: ignore[no-untyped-def]
        event = body.get("event") or {}
        if not should_accept_slack_message(event):
            return

        text = event.get("text") or ""
        user = event.get("user") or "user"
        channel = event["channel"]
        # Prefer thread root so multi-reply threads stay coherent.
        ts = correlation_ts(event) or event["ts"]
        # Idempotency still keys on the specific message ts (not thread root)
        # so app_mention + message dual-delivery for the same message collapses.
        msg_ts = str(event["ts"])

        payload = build_inbound_payload(
            text=text, channel=channel, ts=ts, user=user
        )
        key = inbound_idempotency_key(channel, msg_ts)
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

        # Optional EventStore for causation-chain slack:// backfill (P0).
        store = None
        fetch_event = None
        try:
            from agentbus.store import EventStore

            store = EventStore(workspace)

            def fetch_event(event_id: int):  # type: ignore[no-redef]
                ev = store.get_event(int(event_id))
                return ev.to_dict() if ev is not None else None

        except Exception as store_exc:  # noqa: BLE001
            print(
                f"[AgentBus] links backfill disabled (store open failed): {store_exc}"
            )

        poll_s = float(os.environ.get("SLACK_BRIDGE_POLL_SECONDS", "2"))
        try:
            while True:
                try:
                    events, _page_latest, has_more = poll_handoffs(
                        workspace=workspace, since_id=last_id
                    )
                    for ev in events:
                        eid = int(ev.get("event_id") or 0)
                        payload = normalize_payload(ev.get("payload"))
                        if should_post_outbound(payload):
                            causation = ev.get("causation_id")
                            try:
                                causation_id = (
                                    int(causation) if causation is not None else None
                                )
                            except (TypeError, ValueError):
                                causation_id = None
                            ch_ts = resolve_slack_channel_ts(
                                payload,
                                causation_id=causation_id,
                                fetch_event=fetch_event,
                            )
                            channel = ch_ts[0] if ch_ts else default_channel
                            msg = format_outbound_message(payload)
                            if not channel:
                                print(
                                    f"[Warn] to=slack event {eid} has no "
                                    f"slack:// link and no SLACK_DEFAULT_CHANNEL: "
                                    f"{msg[:160]}"
                                )
                            elif dry_run:
                                print(
                                    f"[dry-run Slack post] ch={channel} {msg[:200]}"
                                )
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
                                    print(
                                        f"[Bus→Slack] posted event {eid} → {channel}"
                                    )
                                except Exception as post_err:  # noqa: BLE001
                                    print(
                                        f"[Slack Error] post event {eid}: {post_err}"
                                    )
                        if eid > last_id:
                            last_id = eid
                            save_cursor(workspace, last_id)
                    # Drain catch-up without sleeping if has_more
                    if has_more and events:
                        continue
                except Exception as exc:  # noqa: BLE001
                    print(f"[AgentBus] Poll error: {exc}")
                time.sleep(poll_s)
        finally:
            if store is not None:
                try:
                    store.close()
                except Exception:  # noqa: BLE001
                    pass

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
