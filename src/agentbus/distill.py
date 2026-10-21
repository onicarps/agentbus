"""Deterministic materialization of AgentBus ``okf/handoff`` events into OKF v0.2.

Handoffs are written once as addressable concepts under
``okf/handoffs/YYYY/MM/<event_id:020d>-<source_hash>.md`` and indexed by a bounded
``okf/log.md`` (``LOG_LIMIT`` entries) so the durable index stays small while history
remains readable. A durable high-water mark under ``.agentbus/`` keeps re-runs
idempotent, and a per-output lock under ``.agentbus/`` serializes concurrent writers to
the same plane.
"""

from __future__ import annotations

import collections
import contextlib
import hashlib
import html
import importlib
import json
import os
import re
import textwrap
from pathlib import Path
from types import ModuleType
from typing import Any, Iterable

import yaml  # type: ignore[import-untyped]

from agentbus.scaffold import (
    WorkspaceError,
    fsync_directory,
    set_fd_mode,
    stage_write,
)
from agentbus.store import (
    EventStore,
    STATUS_PUBLISHED,
    STATUS_REJECTED,
    STATUS_TIMEOUT_FAILED,
)


def _optional_module(name: str) -> ModuleType | None:
    """Import a platform-specific module, returning ``None`` when unavailable."""
    try:
        return importlib.import_module(name)
    except ImportError:  # pragma: no cover - platform dependent
        return None


# ``fcntl`` covers Linux/macOS/WSL; ``msvcrt`` provides Windows byte-range locks.
fcntl = _optional_module("fcntl")
msvcrt = _optional_module("msvcrt")

DISTILLER_VERSION = "1"
STATE_NAME = ".distill-state.json"
LOCK_NAME = ".distill.lock"
LOG_NAME = "log.md"
LOG_LIMIT = 50
HANDOFF_TOPIC = "okf/handoff"

__all__ = [
    "DISTILLER_VERSION",
    "LOG_LIMIT",
    "default_state_path",
    "distill_workspace",
    "event_uri",
    "load_durable_state",
    "validate_log_concepts",
    "verify_distilled_log_digest",
]


def event_uri(event_id: int) -> str:
    """Return the stable URI for one AgentBus event."""
    return f"agentbus://events/{int(event_id)}"


def _yaml_string(value: Any) -> str:
    return json.dumps(str(value), ensure_ascii=False)


_MD_SPECIAL = re.compile(r"([\\`*\[\]()#^!|<>])")


def _one_line(value: Any) -> str:
    collapsed = " ".join(str(value).split())
    return _MD_SPECIAL.sub(r"\\\1", html.escape(collapsed, quote=False))


def _source_id(event_id: int) -> str:
    return f"agentbus-{int(event_id)}"


def _source_hash(event: dict[str, Any]) -> str:
    """Stable short digest of the event identity and payload."""
    material = json.dumps(
        {
            "event_id": int(event["event_id"]),
            "topic": str(event["topic"]),
            "producer_id": str(event["producer_id"]),
            "timestamp": str(event["timestamp"]),
            "payload": event["payload"],
        },
        sort_keys=True,
        ensure_ascii=False,
        default=str,
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()[:12]


def _month_parts(timestamp: str) -> tuple[str, str]:
    if (
        len(timestamp) < 7
        or not timestamp[:4].isdigit()
        or timestamp[4] != "-"
        or not timestamp[5:7].isdigit()
    ):
        raise WorkspaceError(
            "invalid_event_timestamp", f"cannot partition event timestamp: {timestamp!r}"
        )
    year, month = timestamp[:4], timestamp[5:7]
    if not 1 <= int(month) <= 12:
        raise WorkspaceError(
            "invalid_event_timestamp", f"cannot partition event timestamp: {timestamp!r}"
        )
    return year, month


def _handoff_relative(event: dict[str, Any]) -> Path:
    year, month = _month_parts(str(event["timestamp"]))
    name = f"{int(event['event_id']):020d}-{_source_hash(event)}.md"
    return Path("handoffs") / year / month / name


def _render_handoff(event: dict[str, Any]) -> str:
    raw_payload = event.get("payload")
    payload = raw_payload if isinstance(raw_payload, dict) else {}
    event_id = int(event["event_id"])
    source_id = _source_id(event_id)
    uri = event_uri(event_id)
    summary_lines = str(payload.get("summary", "")).splitlines() or [""]
    quoted_summary = "\n".join(
        f"> {html.escape(line)}" if line else ">" for line in summary_lines
    )
    initiative = payload.get("initiative")
    initiative_line = (
        f"- **Initiative:** {_one_line(initiative)}\n" if initiative is not None else ""
    )
    raw_links = payload.get("links") or []
    if isinstance(raw_links, str):
        links = [raw_links]
    elif isinstance(raw_links, (list, tuple)):
        links = [str(link) for link in raw_links]
    else:
        links = []
    link_section = ""
    if links:
        link_section = (
            "\n## Links\n\n" + "\n".join(f"- {_one_line(link)}" for link in links) + "\n"
        )
    return (
        "---\n"
        "type: Handoff\n"
        f"title: {_yaml_string(f'AgentBus handoff {event_id}')}\n"
        "description: Deterministic materialization of an acknowledged AgentBus handoff.\n"
        "status: stable\n"
        "generated:\n"
        f"  by: {_yaml_string(str(event['producer_id']))}\n"
        f"  at: {_yaml_string(str(event['timestamp']))}\n"
        "distilled:\n"
        f"  by: agentbus/distill-v{DISTILLER_VERSION}\n"
        "sources:\n"
        f"  - id: {source_id}\n"
        f"    resource: {uri}\n"
        f"    title: {_yaml_string(f'AgentBus event {event_id}')}\n"
        "---\n\n"
        f"# Handoff {event_id}\n\n"
        f"- **From:** {_one_line(payload.get('from', ''))}\n"
        f"- **To:** {_one_line(payload.get('to', ''))}\n"
        f"- **Topic:** {_one_line(event['topic'])}\n"
        f"{initiative_line}"
        f"- **Source:** [{uri}]({uri})[^{source_id}]\n\n"
        "## Summary\n\n"
        f"{quoted_summary}\n"
        f"{link_section}\n"
        f"[^{source_id}]: Source declared in frontmatter.\n"
    )


def _entry(event: dict[str, Any], relative: Path) -> dict[str, Any]:
    raw_payload = event.get("payload")
    payload = raw_payload if isinstance(raw_payload, dict) else {}
    return {
        "event_id": int(event["event_id"]),
        "timestamp": str(event["timestamp"]),
        "from": str(payload.get("from", "")),
        "to": str(payload.get("to", "")),
        "summary": str(payload.get("summary", "")),
        "path": relative.as_posix(),
        "source_hash": _source_hash(event),
    }


def _sort_key(row: dict[str, Any]) -> tuple[str, int, str]:
    return (
        str(row.get("timestamp", "")),
        int(row.get("event_id", 0)),
        str(row.get("path", "")),
    )


def _render_log(entries: Iterable[dict[str, Any]]) -> str:
    rows = sorted(entries, key=_sort_key)[-LOG_LIMIT:]
    if not rows:
        raise WorkspaceError("distill_empty_log", "cannot render an empty distilled log")
    latest = rows[-1]

    # Detect duplicate event_ids (e.g. events generated across separate clones/databases)
    id_counts = collections.Counter(int(row["event_id"]) for row in rows)

    sources: list[str] = []
    body: list[str] = []
    footnotes: list[str] = []
    for row in reversed(rows):
        event_id = int(row["event_id"])
        if id_counts[event_id] > 1:
            source_hash = str(row.get("source_hash") or Path(row["path"]).stem.split("-")[-1])
            source_id = f"agentbus-{event_id}-{source_hash}"
        else:
            source_id = _source_id(event_id)
        uri = event_uri(event_id)
        sources.extend(
            [
                f"  - id: {source_id}",
                f"    resource: {uri}",
                f"    title: {_yaml_string(f'AgentBus event {event_id}')}",
            ]
        )
        body.append(
            f"- `{row['timestamp']}` — [#{event_id}]({row['path']}) "
            f"{_one_line(row['from'])} → {_one_line(row['to'])}: "
            f"{_one_line(row['summary'])}[^{source_id}]"
        )
        footnotes.append(f"[^{source_id}]: Source declared in frontmatter.")
    body_content = "\n# Recent handoffs\n\n" + "\n".join(body) + "\n\n" + "\n".join(footnotes) + "\n"

    entries_yaml = yaml.safe_dump(
        [
            {
                "event_id": int(row["event_id"]),
                "timestamp": str(row["timestamp"]),
                "from": str(row.get("from", "")),
                "to": str(row.get("to", "")),
                "summary": str(row.get("summary", "")),
                "path": str(row["path"]),
            }
            for row in rows
        ],
        sort_keys=False,
        allow_unicode=True,
    )
    indented_entries = textwrap.indent(entries_yaml, "  ")

    header = (
        "---\n"
        "type: Handoff Log\n"
        "title: Recent distilled handoffs\n"
        f"description: Bounded deterministic index through AgentBus event {int(latest['event_id'])}.\n"
        "status: stable\n"
        "generated:\n"
        f"  by: agentbus/distill-v{DISTILLER_VERSION}\n"
        f"  at: {_yaml_string(latest['timestamp'])}\n"
    )
    rest = (
        "entries:\n"
        + indented_entries
        + "sources:\n"
        + "\n".join(sources)
        + "\n---\n"
        + body_content
    )
    unsigned = header + rest
    digest = hashlib.sha256(unsigned.encode("utf-8")).hexdigest()
    return header + f"distilled_sha256: {digest}\n" + rest


def _valid_recovered_row(item: Any) -> dict[str, Any] | None:
    if not isinstance(item, dict):
        return None
    event_id = item.get("event_id")
    if isinstance(event_id, bool) or not isinstance(event_id, int) or event_id <= 0:
        return None
    if not all(isinstance(item.get(k), str) for k in ("timestamp", "path")):
        return None
    path = Path(item["path"])
    if path.is_absolute() or ".." in path.parts or path.parts[:1] != ("handoffs",):
        return None
    return {
        "event_id": event_id,
        "timestamp": item["timestamp"],
        "from": str(item.get("from", "")),
        "to": str(item.get("to", "")),
        "summary": str(item.get("summary", "")),
        "path": path.as_posix(),
        "source_hash": str(item.get("source_hash") or path.stem.split("-")[-1]),
    }


def _validate_concept_file(out: Path, row: dict[str, Any]) -> dict[str, Any] | None:
    """Validate that a recovered row points to an authentic, existing Handoff concept."""
    relative = str(row.get("path", ""))
    target = out / relative
    try:
        _assert_safe_output(out, target)
    except WorkspaceError:
        return None
    if not target.is_file() or target.is_symlink():
        return None

    parts = target.stem.split("-")
    if len(parts) < 2:
        return None
    source_hash = parts[-1]
    if row.get("source_hash") and row["source_hash"] != source_hash:
        return None

    try:
        text = target.read_text(encoding="utf-8")
    except OSError:
        return None

    if "type: Handoff" not in text:
        return None
    event_id = int(row["event_id"])
    if f"resource: {event_uri(event_id)}" not in text:
        return None

    summary_match = re.search(r"## Summary\s*\n\n(>.*(?:\n>.*)*)", text)
    if summary_match:
        summary_lines = [
            line[2:] if line.startswith("> ") else (line[1:] if line.startswith(">") else line)
            for line in summary_match.group(1).splitlines()
        ]
        authentic_summary = html.unescape("\n".join(summary_lines).strip())
    else:
        authentic_summary = str(row.get("summary", ""))

    # If row provides a summary, it must match the authentic summary from the concept file
    if row.get("summary") and _one_line(row["summary"]) != _one_line(authentic_summary):
        return None

    from_match = re.search(r"^- \*\*From:\*\* (.*)$", text, re.MULTILINE)
    to_match = re.search(r"^- \*\*To:\*\* (.*)$", text, re.MULTILINE)
    date_match = re.search(r"^\s*at:\s*[\"']?([^\"'\n]+)[\"']?$", text, re.MULTILINE)

    def _unescape(value: str) -> str:
        return re.sub(r"\\([\\`*\[\]()#^!|<>])", r"\1", html.unescape(value))

    return {
        "event_id": event_id,
        "timestamp": date_match.group(1) if date_match else str(row["timestamp"]),
        "from": _unescape(from_match.group(1)) if from_match else str(row.get("from", "")),
        "to": _unescape(to_match.group(1)) if to_match else str(row.get("to", "")),
        "summary": authentic_summary,
        "path": relative,
        "source_hash": source_hash,
    }


def _recover_log_entries_from_text(text: str) -> list[dict[str, Any]]:
    if "type: Handoff Log" not in text:
        return []
    if f"agentbus/distill-v{DISTILLER_VERSION}" not in text:
        return []
    parts = text.split("\n---\n", 1)
    if len(parts) != 2:
        return []
    frontmatter, body = parts[0], parts[1]

    # 1. Structured entries in YAML frontmatter
    try:
        data = yaml.safe_load(frontmatter)
        if isinstance(data, dict):
            raw_entries = data.get("entries")
            if isinstance(raw_entries, list):
                valid: list[dict[str, Any]] = []
                for item in raw_entries:
                    row = _valid_recovered_row(item)
                    if row is not None:
                        valid.append(row)
                if valid:
                    return valid
    except Exception:
        pass

    # 2. Markdown line parsing fallback
    entries: list[dict[str, Any]] = []
    pattern = re.compile(
        r"^- `(?P<timestamp>[^`]+)` — \[#(?P<event_id>\d+)\]\((?P<path>[^)]+)\) (?P<from>.*?) → (?P<to>.*?): (?P<summary>.*?)\[\^(?P<source_id>[^\]]+)\]$"
    )
    for line in body.splitlines():
        match = pattern.match(line.strip())
        if match:
            d = match.groupdict()
            clean_summary = re.sub(
                r"\\([\\`*\[\]()#^!|<>])", r"\1", html.unescape(d["summary"])
            )
            clean_from = re.sub(
                r"\\([\\`*\[\]()#^!|<>])", r"\1", html.unescape(d["from"])
            )
            clean_to = re.sub(
                r"\\([\\`*\[\]()#^!|<>])", r"\1", html.unescape(d["to"])
            )
            row = _valid_recovered_row(
                {
                    "event_id": int(d["event_id"]),
                    "timestamp": d["timestamp"],
                    "from": clean_from,
                    "to": clean_to,
                    "summary": clean_summary,
                    "path": d["path"],
                }
            )
            if row is not None:
                entries.append(row)
    return entries


def _recover_log_entries(log_path: Path) -> list[dict[str, Any]]:
    """Recover entry records from a committed distilled log index."""
    if not log_path.is_file() or log_path.is_symlink():
        return []
    try:
        text = log_path.read_text(encoding="utf-8")
    except OSError:
        return []
    return _recover_log_entries_from_text(text)


def validate_log_concepts(output_root: Path, text: str) -> bool:
    """Verify that every entry in ``text`` corresponds to an authentic on-disk Handoff concept."""
    entries = _recover_log_entries_from_text(text)
    if not entries:
        return True
    return all(_validate_concept_file(output_root, r) is not None for r in entries)


_DIGEST_LINE = re.compile(r"^distilled_sha256:\s*([a-f0-9]{64})\s*\n", re.MULTILINE)


def verify_distilled_log_digest(text: str) -> bool:
    """Return whether ``text`` matches its embedded distilled log self-consistency digest.

    The distiller embeds a SHA-256 digest covering the full document except the
    ``distilled_sha256`` line itself into the frontmatter. This acts as consistency
    metadata allowing fresh checkouts (which lack local ``.agentbus/`` state) to
    confirm that a committed log index was rendered by the distiller and has not been
    unintentionally modified by hand.
    """
    if "type: Handoff Log" not in text:
        return False
    if f"agentbus/distill-v{DISTILLER_VERSION}" not in text:
        return False
    frontmatter_end = text.find("\n---\n")
    if frontmatter_end < 0:
        return False
    match = _DIGEST_LINE.search(text, 0, frontmatter_end + 1)
    if not match:
        return False
    unsigned = text[: match.start()] + text[match.end():]
    return hashlib.sha256(unsigned.encode("utf-8")).hexdigest() == match.group(1)


def _default_state() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "distiller_version": DISTILLER_VERSION,
        "high_water_event_id": 0,
        "entries": [],
    }


def _load_state(
    path: Path,
    *,
    expected_output: Path | None = None,
    workspace: Path | None = None,
) -> dict[str, Any]:
    if not path.exists() and not path.is_symlink():
        return _default_state()
    if path.is_symlink() or not path.is_file():
        raise WorkspaceError("unsafe_distill_path", f"refusing distill state path: {path}")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise WorkspaceError(
            "invalid_distill_state", f"cannot read {path}: {exc}"
        ) from exc
    if not isinstance(value, dict) or value.get("schema_version") != 1:
        raise WorkspaceError(
            "invalid_distill_state", f"unsupported distill state in {path}"
        )
    version = value.get("distiller_version")
    if version != DISTILLER_VERSION:
        raise WorkspaceError(
            "invalid_distill_state",
            f"unsupported distiller_version in {path}: {version!r}",
        )
    bound_output = value.get("output_root")
    if bound_output is not None:
        if not isinstance(bound_output, str) or not bound_output.strip():
            raise WorkspaceError(
                "invalid_distill_state", "output_root must be a non-empty string"
            )
        if expected_output is not None:
            candidate = (
                (workspace / bound_output).resolve()
                if (workspace is not None and not Path(bound_output).is_absolute())
                else Path(bound_output)
            )
            if candidate != expected_output:
                is_default_plane = (
                    expected_output == _default_output_root(workspace)
                    if workspace is not None
                    else False
                )
                is_canonical_state = (
                    path == workspace / ".agentbus" / STATE_NAME
                    if workspace is not None
                    else False
                )
                if is_canonical_state and is_default_plane and candidate.name == expected_output.name:
                    pass
                else:
                    raise WorkspaceError(
                        "distill_state_mismatch",
                        f"state {path} is bound to output {bound_output}, not {expected_output}",
                    )
    high_water = value.get("high_water_event_id")
    entries = value.get("entries")
    if isinstance(high_water, bool) or not isinstance(high_water, int) or high_water < 0:
        raise WorkspaceError(
            "invalid_distill_state", "high_water_event_id must be a non-negative integer"
        )
    if not isinstance(entries, list) or not all(isinstance(row, dict) for row in entries):
        raise WorkspaceError("invalid_distill_state", "entries must be a list of objects")
    for row in entries:
        event_id = row.get("event_id")
        relative = row.get("path")
        if isinstance(event_id, bool) or not isinstance(event_id, int) or event_id <= 0:
            raise WorkspaceError(
                "invalid_distill_state", "entry event_id must be a positive integer"
            )
        if not all(
            isinstance(row.get(key), str)
            for key in ("timestamp", "from", "to", "summary")
        ):
            raise WorkspaceError(
                "invalid_distill_state", "entry metadata must contain strings"
            )
        if not isinstance(relative, str):
            raise WorkspaceError("invalid_distill_state", "entry path must be a string")
        candidate = Path(relative)
        if candidate.is_absolute() or ".." in candidate.parts:
            raise WorkspaceError(
                "invalid_distill_state", f"unsafe entry path: {relative!r}"
            )
    return value


def _assert_safe_output(root: Path, target: Path) -> None:
    try:
        relative = target.relative_to(root)
    except ValueError as exc:
        raise WorkspaceError(
            "unsafe_distill_path", f"distill target escapes output root: {target}"
        ) from exc
    current = root
    for part in relative.parts[:-1]:
        current = current / part
        if current.is_symlink():
            raise WorkspaceError(
                "unsafe_distill_path", f"refusing symlinked output path: {current}"
            )
        if current.exists() and not current.is_dir():
            raise WorkspaceError(
                "unsafe_distill_path", f"output parent is not a directory: {current}"
            )
    if target.is_symlink():
        raise WorkspaceError(
            "unsafe_distill_path", f"refusing symlinked output file: {target}"
        )


def _atomic_replace(target: Path, text: str) -> None:
    """Stage ``text`` beside ``target`` and swap it in atomically."""
    if target.parent.is_symlink() or target.is_symlink():
        raise WorkspaceError(
            "unsafe_distill_path", f"refusing symlinked output path: {target}"
        )
    temporary = stage_write(target, text)
    try:
        if target.is_symlink():
            raise WorkspaceError(
                "unsafe_distill_path", f"refusing symlinked output file: {target}"
            )
        os.replace(temporary, target)
    except Exception:
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise
    fsync_directory(target.parent)


def _lock_descriptor(descriptor: int) -> None:
    """Take an exclusive advisory lock on ``descriptor``."""
    if fcntl is not None:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        return
    if msvcrt is None:
        raise WorkspaceError(
            "distill_lock_unavailable",
            "no file locking backend is available on this platform",
        )
    os.lseek(descriptor, 0, os.SEEK_SET)
    try:
        msvcrt.locking(descriptor, msvcrt.LK_LOCK, 1)
    except OSError as exc:
        raise WorkspaceError(
            "distill_lock_unavailable", f"timed out waiting for distill lock: {exc}"
        ) from exc


def _unlock_descriptor(descriptor: int) -> None:
    """Release the advisory lock held on ``descriptor``, if any."""
    if fcntl is not None:
        with contextlib.suppress(OSError):
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        return
    if msvcrt is None:
        return
    with contextlib.suppress(OSError):
        os.lseek(descriptor, 0, os.SEEK_SET)
        msvcrt.locking(descriptor, msvcrt.LK_UNLCK, 1)


@contextlib.contextmanager
def _distill_lock(path: Path):
    """Serialize distillation writers bound to one output plane."""
    if path.is_symlink():
        raise WorkspaceError(
            "unsafe_distill_lock", f"refusing symlinked distill lock: {path}"
        )
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags, 0o600)
    except OSError as exc:
        raise WorkspaceError(
            "distill_lock_unavailable", f"cannot open {path}: {exc}"
        ) from exc
    try:
        set_fd_mode(descriptor, 0o600)
        _lock_descriptor(descriptor)
        yield
    finally:
        _unlock_descriptor(descriptor)
        os.close(descriptor)


def _log_owned(
    target: Path,
    *,
    state: dict[str, Any],
    workspace: Path,
    output_root: Path,
) -> bool:
    """Return whether ``target`` is safe for the distiller to manage.

    Safe targets are absent files, initial scaffold placeholders (matching the
    scaffold manifest SHA-256 for default plane), or existing distilled indexes
    (matching either the committed or pending journal SHA-256, or matching
    ``_render_log(state['entries'])``). Any other file (such as a hand-written
    or hand-edited index) is protected against silent overwrite.
    """
    if not target.exists():
        return True
    if not target.is_file() or target.is_symlink():
        return False
    try:
        text = target.read_text(encoding="utf-8")
    except OSError:
        return False
    if "type: Handoff Log" not in text:
        return False

    # 1. Authentic distilled log: matches embedded consistency digest AND all listed
    # handoff concepts exist on disk with matching authentic provenance.
    if verify_distilled_log_digest(text) and validate_log_concepts(output_root, text):
        return True

    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
    recorded_digests = {
        state.get("log_sha256"),
        state.get("pending_log_sha256"),
    } - {None}
    if digest in recorded_digests:
        return True

    # 1. Distilled index: matches _render_log for the loaded prior entries
    prior_entries = state.get("entries", [])
    if prior_entries:
        try:
            if text == _render_log(prior_entries):
                return True
        except Exception:
            pass

    # 2. Scaffold placeholder: check default-plane manifest SHA-256
    manifest_path = workspace / "okf" / ".scaffold-manifest.json"
    if output_root == _default_output_root(workspace) and manifest_path.is_file():
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            expected = manifest.get("files", {}).get("okf/log.md", {}).get("sha256")
            if expected and digest == expected:
                return True
        except (OSError, ValueError, KeyError, TypeError):
            pass

    return False



def _collect(
    store: EventStore, *, high_water: int, limit: int, maintain: bool = True
) -> tuple[list[dict[str, Any]], int]:
    """Read handoffs after ``high_water`` in ascending id order without passing pending handoffs."""
    cursor = int(high_water)
    if maintain:
        store.expire_pending()
        store.expire_sla_breaches()

    terminal_statuses = (
        STATUS_PUBLISHED,
        STATUS_REJECTED,
        STATUS_TIMEOUT_FAILED,
    )
    # Stop at the first non-terminal status within one stable snapshot
    store._conn.execute("BEGIN")
    try:
        row = store._conn.execute(
            "SELECT MIN(event_id) AS min_non_terminal FROM events "
            "WHERE topic = ? AND event_id > ? "
            "AND (status IS NULL OR status NOT IN (?, ?, ?))",
            (HANDOFF_TOPIC, cursor, *terminal_statuses),
        ).fetchone()
        min_non_terminal = (
            int(row["min_non_terminal"])
            if row and row["min_non_terminal"] is not None
            else None
        )

        query = "SELECT * FROM events WHERE topic = ? AND event_id > ?"
        params: list[Any] = [HANDOFF_TOPIC, cursor]
        if min_non_terminal is not None:
            query += " AND event_id < ?"
            params.append(min_non_terminal)
        query += " ORDER BY event_id ASC LIMIT ?"
        params.append(limit)
        rows = store._conn.execute(query, params).fetchall()
    finally:
        store._conn.commit()

    events = [
        event.to_dict() for event in store._authoritative_events_from_rows(rows)
    ]
    # Filter for published handoffs with valid dict payload
    valid_events = [
        e
        for e in events
        if e.get("status") == STATUS_PUBLISHED and isinstance(e.get("payload"), dict)
    ]
    # Only advance cursor past events that have reached an explicit terminal status
    terminal_rows = [r for r in rows if r["status"] in terminal_statuses]
    new_cursor = int(terminal_rows[-1]["event_id"]) if terminal_rows else cursor
    return valid_events, new_cursor


def _output_root(workspace: Path, output_root: str | os.PathLike[str] | None) -> Path:
    if output_root is None:
        candidate = workspace / "okf"
    else:
        candidate = Path(output_root).expanduser()
        if not candidate.is_absolute():
            candidate = workspace / candidate
    if candidate.is_symlink():
        raise WorkspaceError(
            "unsafe_distill_path", f"refusing symlinked output root: {candidate}"
        )
    return candidate.resolve()


def _default_output_root(workspace: Path) -> Path:
    return (workspace / "okf").resolve()


def _plane_key(workspace: Path, output_root: Path) -> str:
    try:
        return output_root.relative_to(workspace.resolve()).as_posix()
    except ValueError:
        return output_root.as_posix()


def _state_path(workspace: Path, output_root: Path) -> Path:
    """Durable high-water state, isolated per resolved output root.

    The default ``okf/`` plane keeps the canonical ``.agentbus/.distill-state.json``.
    A custom ``--out`` gets its own digest-suffixed state file so materializing into
    one output root can neither hide nor contaminate another one's progress.
    """
    state_root = workspace / ".agentbus"
    if output_root == _default_output_root(workspace):
        return state_root / STATE_NAME
    digest = hashlib.sha256(_plane_key(workspace, output_root).encode("utf-8")).hexdigest()[:12]
    return state_root / f".distill-state.{digest}.json"


def default_state_path(workspace: str | os.PathLike[str]) -> Path:
    """Return the durable state path for a workspace's default ``okf/`` plane."""
    root = Path(workspace).expanduser()
    return _state_path(root, _default_output_root(root))


def _lock_path(workspace: Path, output_root: Path) -> Path:
    """Serialization lock, isolated per resolved output root.

    Mirrors :func:`_state_path`: the default ``okf/`` plane keeps the canonical
    ``.agentbus/.distill.lock`` while a custom ``--out`` gets its own digest-suffixed
    lock so writers to distinct planes never block or interleave on one another.
    """
    state_root = workspace / ".agentbus"
    if output_root == _default_output_root(workspace):
        return state_root / LOCK_NAME
    digest = hashlib.sha256(_plane_key(workspace, output_root).encode("utf-8")).hexdigest()[:12]
    return state_root / f".distill.{digest}.lock"


def load_durable_state(path: Path) -> dict[str, Any] | None:
    """Return validated durable state, or ``None`` when no state file exists.

    Raises :class:`WorkspaceError` when the path exists but is unsafe or invalid, so
    callers can tell "never distilled" apart from "cannot trust this state".
    """
    if not path.exists() and not path.is_symlink():
        return None
    return _load_state(path)


def _resolve_workspace(workspace: str | os.PathLike[str]) -> Path:
    root = Path(workspace).expanduser()
    if not root.is_dir():
        raise WorkspaceError(
            "distill_workspace_missing", f"workspace is not a directory: {root}"
        )
    return root.resolve()


def distill_workspace(
    workspace: str | os.PathLike[str],
    *,
    output_root: str | os.PathLike[str] | None = None,
    stdout: bool = False,
    limit: int = 100,
    dry_run: bool = False,
    reset: bool = False,
) -> dict[str, Any]:
    """Materialize unpublished ``okf/handoff`` events into the OKF knowledge plane.

    ``stdout`` and ``dry_run`` compute the same documents without writing files or
    advancing the high-water mark. ``reset`` rewinds the mark to zero so every
    handoff is reconsidered (existing identical files stay untouched).
    """
    if isinstance(limit, bool) or not 1 <= int(limit) <= 1000:
        raise WorkspaceError(
            "invalid_distill_limit", "distill limit must be between 1 and 1000"
        )
    root = _resolve_workspace(workspace)
    state_root = root / ".agentbus"
    if state_root.is_symlink():
        raise WorkspaceError(
            "unsafe_distill_path", f"refusing symlinked runtime directory: {state_root}"
        )
    out = _output_root(root, output_root)
    state_path = _state_path(root, out)

    def _run() -> dict[str, Any]:
        existing_state = (
            _load_state(state_path, expected_output=out, workspace=root)
            if state_path.exists()
            else _default_state()
        )
        state = _default_state() if reset else existing_state
        high_water = int(state["high_water_event_id"])
        store = EventStore(root, auto_prune=False)
        try:
            selected, new_high_water = _collect(
                store,
                high_water=high_water,
                limit=int(limit),
                maintain=not (stdout or dry_run),
            )
        finally:
            store.close()

        documents: list[tuple[Path, str, dict[str, Any]]] = []
        for event in selected:
            relative = _handoff_relative(event)
            documents.append((relative, _render_handoff(event), _entry(event, relative)))

        if stdout:
            return {
                "workspace": str(root),
                "high_water_event_id": new_high_water,
                "count": len(documents),
                "documents": [text for _relative, text, _row in documents],
            }
        if dry_run:
            return {
                "workspace": str(root),
                "output_root": str(out),
                "high_water_event_id": new_high_water,
                "count": len(documents),
                "created": [relative.as_posix() for relative, _t, _r in documents],
                "unchanged": [],
                "dry_run": True,
                "entries": [row for _rel, _text, row in documents],
                "documents": [text for _rel, text, _row in documents],
            }

        try:
            out.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise WorkspaceError(
                "distill_output_unavailable", f"cannot create {out}: {exc}"
            ) from exc
        if out.is_symlink():
            raise WorkspaceError(
                "unsafe_distill_path", f"refusing symlinked output root: {out}"
            )

        conflicts: list[str] = []
        created: list[str] = []
        unchanged: list[str] = []
        for relative, text, _row in documents:
            target = out / relative
            _assert_safe_output(out, target)
            if target.exists():
                if not target.is_file() or target.read_text(encoding="utf-8") != text:
                    conflicts.append(relative.as_posix())
                else:
                    unchanged.append(relative.as_posix())
            else:
                created.append(relative.as_posix())
        if conflicts:
            raise WorkspaceError(
                "distill_conflict",
                "refusing to overwrite non-identical distilled handoffs: "
                + ", ".join(conflicts),
            )

        prior_map: dict[str, dict[str, Any]] = {}
        for row in existing_state.get("entries", []):
            if isinstance(row, dict) and "path" in row:
                prior_map[str(row["path"])] = row

        # If local state is missing/empty, or to preserve committed history across clones/pulls,
        # seed prior entries from the on-disk index if it is an authentic distilled log.
        log_target = out / LOG_NAME
        _assert_safe_output(out, log_target)
        # To preserve committed history across clones and git pulls, merge prior entries
        # from the committed log index, validating each row against authentic concept files on disk.
        if (
            log_target.is_file()
            and not log_target.is_symlink()
            and _log_owned(log_target, state=existing_state, workspace=root, output_root=out)
        ):
            for raw_row in _recover_log_entries(log_target):
                validated = _validate_concept_file(out, raw_row)
                if validated is not None and validated["path"] not in prior_map:
                    prior_map[validated["path"]] = validated

        combined = dict(prior_map)
        for _relative, _text, row in documents:
            combined[str(row["path"])] = row
        entries = sorted(combined.values(), key=_sort_key)[-LOG_LIMIT:]
        if not documents:
            # Nothing re-rendered: keep state consistent with the on-disk index.
            entries = sorted(prior_map.values(), key=_sort_key)[-LOG_LIMIT:]

        new_log = _render_log(entries) if documents else None
        new_log_sha = (
            hashlib.sha256(new_log.encode("utf-8")).hexdigest()
            if new_log is not None
            else None
        )

        already_target = (
            bool(documents)
            and log_target.is_file()
            and not log_target.is_symlink()
            and log_target.read_text(encoding="utf-8") == new_log
        )
        if documents and not already_target and not _log_owned(
            log_target,
            state=existing_state,
            workspace=root,
            output_root=out,
        ):
            raise WorkspaceError(
                "distill_conflict",
                f"refusing to overwrite non-distilled index: {log_target}",
            )

        for relative, text, _row in documents:
            if relative.as_posix() in created:
                _atomic_replace(out / relative, text)

        state_root.mkdir(parents=True, exist_ok=True)
        try:
            stored_output = out.relative_to(root).as_posix()
        except ValueError:
            stored_output = out.as_posix()
        effective_high_water = (
            new_high_water
            if reset
            else max(new_high_water, int(existing_state.get("high_water_event_id", 0)))
        )

        if documents and new_log is not None:
            # Journal intended log digest before replacing the on-disk index
            journal_state = dict(existing_state)
            journal_state["pending_log_sha256"] = new_log_sha
            _atomic_replace(
                state_path,
                json.dumps(journal_state, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
            )
            _atomic_replace(log_target, new_log)

        updated_state = {
            "schema_version": 1,
            "distiller_version": DISTILLER_VERSION,
            "output_root": stored_output,
            "high_water_event_id": effective_high_water,
            "entries": entries,
            "log_sha256": new_log_sha or existing_state.get("log_sha256"),
        }
        _atomic_replace(
            state_path,
            json.dumps(updated_state, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        )
        return {
            "workspace": str(root),
            "output_root": str(out),
            "high_water_event_id": effective_high_water,
            "count": len(documents),
            "created": created,
            "unchanged": unchanged,
            "state": str(state_path),
            "entries": entries,
        }

    if stdout or dry_run:
        return _run()
    state_root.mkdir(parents=True, exist_ok=True)
    with _distill_lock(_lock_path(root, out)):
        return _run()
