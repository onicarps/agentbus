"""Monologue tailer — multiplex agent reasoning logs; optional system/monologue publish."""

from __future__ import annotations

import hashlib
import json
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

from agentbus.schemas import set_validation_workspace
from agentbus.store import EventStore
from agentbus.wiretap import redact_value

# Static path registry (productized from AGENT_LOG_PATHS research).
AGENT_LOG_REGISTRY: dict[str, list[dict[str, str]]] = {
    "aider": [
        {"glob": "~/.aider.chat.history.md", "format": "markdown"},
        {"glob": ".aider.chat.history.md", "format": "markdown"},
    ],
    "claude": [
        {"glob": "~/.claude/projects/**/*.jsonl", "format": "jsonl"},
    ],
    "cursor": [
        {"glob": "~/.cursor/projects/*/agent-transcripts/*.jsonl", "format": "jsonl"},
    ],
    "hermes": [
        {"glob": "~/.hermes/sessions/*.jsonl", "format": "jsonl"},
        {"glob": "~/.hermes/.hermes_history", "format": "history"},
    ],
    "grok": [
        {"glob": "~/.grok/logs/unified.jsonl", "format": "jsonl"},
    ],
    "codex": [
        {"glob": "~/.codex/sessions/**/*.jsonl", "format": "jsonl"},
    ],
}


@dataclass
class LogSource:
    agent: str
    path: Path
    format: str


def expand_registry(
    agents: list[str] | None = None,
    *,
    home: Path | None = None,
    cwd: Path | None = None,
) -> list[LogSource]:
    home = home or Path.home()
    cwd = cwd or Path.cwd()
    wanted = set(agents) if agents else set(AGENT_LOG_REGISTRY)
    sources: list[LogSource] = []
    for agent, entries in AGENT_LOG_REGISTRY.items():
        if agent not in wanted:
            continue
        for entry in entries:
            pattern = entry["glob"]
            fmt = entry.get("format", "jsonl")
            if pattern.startswith("~/"):
                base = home / pattern[2:]
            elif pattern.startswith("/"):
                base = Path(pattern)
            else:
                base = cwd / pattern
            # Expand globs
            if any(ch in str(base) for ch in "*?["):
                # pathlib glob from first non-glob parent
                parts = base.parts
                non_glob = []
                for i, part in enumerate(parts):
                    if any(ch in part for ch in "*?["):
                        parent = Path(*non_glob) if non_glob else Path("/")
                        rest = str(Path(*parts[i:]))
                        try:
                            matches = sorted(parent.glob(rest), key=lambda p: p.stat().st_mtime, reverse=True)
                        except Exception:
                            matches = []
                        for m in matches[:8]:
                            if m.is_file():
                                sources.append(LogSource(agent=agent, path=m, format=fmt))
                        break
                    non_glob.append(part)
            else:
                if base.is_file():
                    sources.append(LogSource(agent=agent, path=base, format=fmt))
    return sources


def list_agent_logs(
    agents: list[str] | None = None,
    *,
    home: Path | None = None,
    cwd: Path | None = None,
) -> list[dict[str, Any]]:
    home = home or Path.home()
    cwd = cwd or Path.cwd()
    wanted = agents or list(AGENT_LOG_REGISTRY)
    rows: list[dict[str, Any]] = []
    present = expand_registry(wanted, home=home, cwd=cwd)
    by_agent: dict[str, list[LogSource]] = {}
    for s in present:
        by_agent.setdefault(s.agent, []).append(s)
    for agent in wanted:
        paths = by_agent.get(agent, [])
        rows.append(
            {
                "agent": agent,
                "present": bool(paths),
                "paths": [str(p.path) for p in paths],
                "registry": AGENT_LOG_REGISTRY.get(agent, []),
            }
        )
    return rows


def parse_line(agent: str, line: str, fmt: str) -> dict[str, Any] | None:
    line = line.rstrip("\n")
    if not line.strip():
        return None
    role = "unknown"
    text = line
    if fmt == "jsonl":
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            return {"agent": agent, "role": "raw", "text": line[:2000]}
        if isinstance(obj, dict):
            if "msg" in obj:
                role = str(obj.get("src") or obj.get("lvl") or "log")
                text = str(obj.get("msg", ""))
            elif "role" in obj or "content" in obj:
                role = str(obj.get("role", "message"))
                content = obj.get("content") or obj.get("text") or obj.get("message") or ""
                if isinstance(content, list):
                    parts = []
                    for block in content:
                        if isinstance(block, dict):
                            parts.append(str(block.get("text") or block.get("content") or block))
                        else:
                            parts.append(str(block))
                    text = " ".join(parts)
                else:
                    text = str(content)
            else:
                text = json.dumps(obj, ensure_ascii=False)[:2000]
        else:
            text = str(obj)[:2000]
    elif fmt == "markdown":
        role = "markdown"
        text = line
    elif fmt == "history":
        role = "history"
        text = line
    text = str(text)[:2000]
    return {"agent": agent, "role": role, "text": text}


def _safe_source_path(source_path: str, workspace: Path | None = None) -> str:
    """Home-abbreviated or workspace-relative path — never raw absolute home paths."""
    path = Path(source_path)
    try:
        resolved = path.resolve()
    except OSError:
        resolved = path
    if workspace is not None:
        try:
            return str(resolved.relative_to(workspace.resolve()))
        except ValueError:
            pass
    home = Path.home()
    try:
        return "~/" + str(resolved.relative_to(home))
    except ValueError:
        # Outside home: keep basename only to avoid leaking dir structure
        return resolved.name


def _monologue_idempotency_key(
    *, agent: str, source_path: str, role: str, text: str, byte_offset: int | None = None
) -> str:
    """Deterministic key so restarts do not re-publish the same backlog lines."""
    material = f"{agent}|{source_path}|{role}|{byte_offset if byte_offset is not None else ''}|{text}"
    digest = hashlib.sha256(material.encode("utf-8", errors="replace")).hexdigest()[:24]
    return f"mono-{digest}"


def _publish_monologue(
    store: EventStore,
    *,
    agent: str,
    role: str,
    text: str,
    source_path: str,
    workspace: Path | None = None,
    byte_offset: int | None = None,
) -> None:
    safe_path = _safe_source_path(source_path, workspace=workspace)
    payload = redact_value(
        {
            "agent": agent,
            "role": role,
            "text": text,
            "source_path": safe_path,
            "observer": "swarm-tail",
        }
    )
    store.publish(
        topic="system/monologue",
        producer_id="swarm-tail",
        schema_version="1.0",
        payload=payload,
        skip_rbac=True,
        idempotency_key=_monologue_idempotency_key(
            agent=agent,
            source_path=safe_path,
            role=role,
            text=text,
            byte_offset=byte_offset,
        ),
    )


def follow_sources(
    sources: list[LogSource],
    *,
    initial_lines: int = 15,
    publish: bool = False,
    workspace: Path | None = None,
    poll_interval: float = 0.5,
    duration: float = 0,
) -> Iterator[dict[str, Any]]:
    """Yield parsed monologue lines; optionally publish to bus."""
    store: EventStore | None = None
    if publish:
        if workspace is None:
            raise ValueError("workspace required when publish=True")
        set_validation_workspace(workspace)
        store = EventStore(workspace)

    # Open files and seek to end (after optional initial tail)
    handles: list[tuple[LogSource, Any, int]] = []
    try:
        for src in sources:
            try:
                fp = open(src.path, "r", encoding="utf-8", errors="replace")
            except OSError:
                continue
            if initial_lines > 0:
                # read last N lines
                try:
                    lines = fp.readlines()
                    # approximate offsets for idempotency of backlog lines
                    total = sum(len(ln.encode("utf-8", errors="replace")) for ln in lines)
                    start_idx = max(0, len(lines) - initial_lines)
                    offset = sum(
                        len(ln.encode("utf-8", errors="replace")) for ln in lines[:start_idx]
                    )
                    for line in lines[start_idx:]:
                        parsed = parse_line(src.agent, line, src.format)
                        if parsed:
                            safe = _safe_source_path(str(src.path), workspace=workspace)
                            parsed["source_path"] = safe
                            if store is not None:
                                _publish_monologue(
                                    store,
                                    agent=parsed["agent"],
                                    role=parsed["role"],
                                    text=parsed["text"],
                                    source_path=str(src.path),
                                    workspace=workspace,
                                    byte_offset=offset,
                                )
                            yield parsed
                        offset += len(line.encode("utf-8", errors="replace"))
                    # stay at end
                    fp.seek(0, os.SEEK_END)
                    _ = total  # silence unused if empty file
                except Exception:
                    fp.seek(0, os.SEEK_END)
            else:
                fp.seek(0, os.SEEK_END)
            handles.append((src, fp, 0))

        start = time.monotonic()
        while True:
            if duration and (time.monotonic() - start) >= duration:
                break
            any_data = False
            for src, fp, _ in handles:
                pos = fp.tell()
                line = fp.readline()
                while line:
                    any_data = True
                    parsed = parse_line(src.agent, line, src.format)
                    if parsed:
                        safe = _safe_source_path(str(src.path), workspace=workspace)
                        parsed["source_path"] = safe
                        if store is not None:
                            _publish_monologue(
                                store,
                                agent=parsed["agent"],
                                role=parsed["role"],
                                text=parsed["text"],
                                source_path=str(src.path),
                                workspace=workspace,
                                byte_offset=pos,
                            )
                        yield parsed
                    pos = fp.tell()
                    line = fp.readline()
            if not any_data:
                time.sleep(poll_interval)
    finally:
        for _, fp, _ in handles:
            try:
                fp.close()
            except Exception:
                pass
        if store is not None:
            store.close()


def run_tail(
    *,
    agents: list[str] | None = None,
    list_only: bool = False,
    publish: bool = False,
    workspace: Path | None = None,
    lines: int = 15,
    duration: float = 0,
) -> int:
    if list_only:
        rows = list_agent_logs(agents)
        print(json.dumps(rows, indent=2))
        return 0

    sources = expand_registry(agents)
    if not sources:
        print("No agent log files found for selection.", file=sys.stderr)
        return 1

    for entry in follow_sources(
        sources,
        initial_lines=lines,
        publish=publish,
        workspace=workspace,
        duration=duration if duration > 0 else 0,
    ):
        agent = entry.get("agent", "?")
        role = entry.get("role", "")
        text = entry.get("text", "")
        print(f"[{agent}/{role}] {text}")
        sys.stdout.flush()
        # If duration==0, follow forever until KeyboardInterrupt
        if duration == 0 and False:  # keep linter calm; loop controlled inside follow
            break
    return 0
