"""Dual intake: webhook JSONL queue + classical WAKE file."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterator

from agentbus.runner.types import WakeEnvelope


def load_done_ids(path: Path) -> set[int]:
    if not path.is_file():
        return set()
    done: set[int] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            done.add(int(line))
        except ValueError:
            continue
    return done


def append_done_id(path: Path, event_id: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(f"{event_id}\n")
        fh.flush()


def _payload_fields(payload: dict[str, Any] | None) -> tuple[str, str, str]:
    p = payload if isinstance(payload, dict) else {}
    return (
        str(p.get("from") or ""),
        str(p.get("to") or ""),
        str(p.get("summary") or ""),
    )


def envelope_from_queue_record(rec: dict[str, Any]) -> WakeEnvelope | None:
    event_id = rec.get("event_id")
    if event_id is None and isinstance(rec.get("raw"), dict):
        event_id = rec["raw"].get("event_id")
    try:
        eid = int(event_id)
    except (TypeError, ValueError):
        return None

    raw_wake = rec.get("raw") if isinstance(rec.get("raw"), dict) else {}
    payload = rec.get("payload")
    if not isinstance(payload, dict):
        payload = raw_wake.get("payload") if isinstance(raw_wake.get("payload"), dict) else {}
    if not isinstance(payload, dict):
        payload = {}

    frm, to, summary = _payload_fields(payload)
    if not frm:
        frm = str(rec.get("from") or "")
    if not to:
        to = str(rec.get("to") or "")
    if not summary:
        summary = str(rec.get("summary") or "")

    topic = str(rec.get("topic") or raw_wake.get("topic") or "okf/handoff")
    causation = rec.get("causation_id")
    if causation is None:
        causation = raw_wake.get("causation_id")
    try:
        causation_id = int(causation) if causation is not None else None
    except (TypeError, ValueError):
        causation_id = None

    trace = rec.get("trace_id") or raw_wake.get("trace_id")
    return WakeEnvelope(
        event_id=eid,
        topic=topic,
        from_agent=frm,
        to=to,
        summary=summary,
        payload=payload,
        source="webhook_queue",
        raw=rec,
        causation_id=causation_id,
        trace_id=str(trace) if trace else None,
    )


def envelope_from_wake_file(data: dict[str, Any]) -> WakeEnvelope | None:
    try:
        eid = int(data.get("event_id"))
    except (TypeError, ValueError):
        return None
    payload = data.get("payload") if isinstance(data.get("payload"), dict) else {}
    frm, to, summary = _payload_fields(payload)
    causation = data.get("causation_id")
    try:
        causation_id = int(causation) if causation is not None else None
    except (TypeError, ValueError):
        causation_id = None
    # v0.16 synthetic resume wakes tag source=resume in the wake body
    src = str(data.get("source") or "wake_file")
    if src not in ("wake_file", "webhook_queue", "resume"):
        src = "wake_file"
    return WakeEnvelope(
        event_id=eid,
        topic=str(data.get("topic") or "okf/handoff"),
        from_agent=frm,
        to=to,
        summary=summary,
        payload=payload,
        source=src,
        raw=data,
        causation_id=causation_id,
        trace_id=str(data["trace_id"]) if data.get("trace_id") else None,
    )


def iter_queue_envelopes(queue_path: Path, done: set[int]) -> Iterator[WakeEnvelope]:
    if not queue_path.is_file():
        return
    with queue_path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(rec, dict):
                continue
            env = envelope_from_queue_record(rec)
            if env is None or env.event_id in done:
                continue
            yield env


def read_wake_file(path: Path, done: set[int]) -> WakeEnvelope | None:
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(data, dict):
        return None
    env = envelope_from_wake_file(data)
    if env is None or env.event_id in done:
        return None
    return env
