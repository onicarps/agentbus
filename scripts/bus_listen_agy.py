#!/usr/bin/env python3
"""Poll AgentBus for Agy→Grok handoffs; print only new actionable tasks (one line each)."""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

WS = Path(os.environ.get("AGENTBUS_WORKSPACE", "/home/oni/okf_agent_workspace/projects/agentbus"))
CURSOR = WS / ".agentbus" / "grok_listen.cursor"
POLL_SEC = float(os.environ.get("GROK_BUS_POLL_SEC", "20"))


def run_agentbus(*args: str) -> str:
    env = os.environ.copy()
    env["AGENTBUS_WORKSPACE"] = str(WS)
    env.setdefault("AGENTBUS_PRODUCER_ID", "grok")
    return subprocess.check_output(["agentbus", *args], env=env, text=True)


def load_cursor() -> int:
    try:
        return int(CURSOR.read_text().strip())
    except Exception:
        return 0


def save_cursor(n: int) -> None:
    CURSOR.parent.mkdir(parents=True, exist_ok=True)
    CURSOR.write_text(str(n) + "\n")


def is_actionable(payload: dict) -> bool:
    fr = (payload.get("from") or "").lower()
    to = (payload.get("to") or "").lower()
    if fr != "agy":
        return False
    if to in ("grok", "swarm", "*", "all", "implementer"):
        return True
    # also catch "to": "grok,hermes" style
    if "grok" in to:
        return True
    return False


def main() -> None:
    # one-shot mode if --once
    once = "--once" in sys.argv
    while True:
        try:
            since = load_cursor()
            raw = run_agentbus(
                "poll",
                "--topic",
                "okf/handoff",
                "--since-id",
                str(since),
                "--limit",
                "50",
            )
            data = json.loads(raw)
            events = data.get("events") or []
            max_id = since
            for e in sorted(events, key=lambda x: int(x.get("event_id") or 0)):
                eid = int(e.get("event_id") or 0)
                if eid > max_id:
                    max_id = eid
                p = e.get("payload") or {}
                if isinstance(p, str):
                    try:
                        p = json.loads(p)
                    except Exception:
                        p = {"summary": p}
                if not isinstance(p, dict):
                    continue
                if not is_actionable(p):
                    continue
                summary = (p.get("summary") or "").replace("\n", " ").strip()
                # single line event for monitor notifications
                print(
                    f"AGY_TASK event_id={eid} from={p.get('from')} to={p.get('to')} "
                    f"summary={summary}",
                    flush=True,
                )
            if max_id > since:
                save_cursor(max_id)
        except Exception as exc:
            print(f"LISTEN_ERROR {exc}", flush=True)
        if once:
            break
        time.sleep(POLL_SEC)


if __name__ == "__main__":
    main()
