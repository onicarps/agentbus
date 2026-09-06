"""Distributed trace IDs and span lineage for agent handoffs."""

from __future__ import annotations

import secrets
from typing import Any


def generate_span_id() -> str:
    return f"span-{secrets.token_hex(8)}"


def normalize_trace_id(trace_id: str | None) -> str | None:
    if trace_id is None:
        return None
    tid = trace_id.strip()
    if not tid:
        return None
    if len(tid) > 128:
        raise ValueError("invalid_trace_id: max length 128")
    return tid


def normalize_parent_span_id(parent_span_id: str | None) -> str | None:
    if parent_span_id is None:
        return None
    sid = parent_span_id.strip()
    if not sid:
        return None
    if len(sid) > 128:
        raise ValueError("invalid_parent_span_id: max length 128")
    return sid


def build_trace_tree(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return root nodes with nested 'children' for waterfall rendering.

    Lossless and cycle-safe guarantees:
    - events are deduplicated by ``event_id`` (callers may merge fetches);
    - no event silently vanishes: missing/looping lineage demotes a node to a
      root instead of dropping it (previously cycles and duplicate span_ids
      made events disappear from the waterfall);
    - duplicate ``span_id`` claims keep the earliest event as the span owner
      and preserve later events as its children.
    """
    seen_ids: set[Any] = set()
    unique: list[dict[str, Any]] = []
    for e in sorted(events, key=lambda e: e.get("event_id", 0)):
        eid = e.get("event_id")
        if eid is not None:
            if eid in seen_ids:
                continue
            seen_ids.add(eid)
        unique.append(e)

    by_span: dict[str, dict[str, Any]] = {}
    duplicates_by_span: dict[str, list[dict[str, Any]]] = {}
    spanless: list[dict[str, Any]] = []
    for e in unique:
        node = {**e, "children": []}
        span = e.get("span_id")
        if not span:
            spanless.append(node)
        elif span in by_span:
            duplicates_by_span.setdefault(span, []).append(node)
        else:
            by_span[span] = node

    def _chain_reaches(start_key: str, target_key: str) -> bool:
        """True if the parent chain from ``start_key`` reaches ``target_key``."""
        seen: set[str] = set()
        cur: str | None = start_key
        while cur and cur in by_span and cur not in seen:
            seen.add(cur)
            if cur == target_key:
                return True
            cur = by_span[cur].get("parent_span_id")
        return False

    roots: list[dict[str, Any]] = list(spanless)
    for node in by_span.values():
        parent = node.get("parent_span_id")
        if (
            parent
            and parent != node.get("span_id")
            and parent in by_span
            and not _chain_reaches(parent, node["span_id"])
        ):
            by_span[parent]["children"].append(node)
        else:
            # Missing parent, self-loop, or parent-cycle: render as a root so
            # the event is never silently dropped from the waterfall.
            roots.append(node)
    for span, dups in duplicates_by_span.items():
        by_span[span]["children"].extend(dups)

    def sort_tree(nodes: list[dict[str, Any]]) -> None:
        nodes.sort(key=lambda n: n.get("event_id", 0))
        for node in nodes:
            sort_tree(node["children"])

    sort_tree(roots)
    return roots


def render_trace_tree(trace_id: str, roots: list[dict[str, Any]]) -> str:
    from rich.console import Console
    from rich.tree import Tree

    console = Console(record=True)
    title = f"trace {trace_id}"
    if not roots:
        tree = Tree(title)
        tree.add("[dim]no events[/dim]")
    else:
        tree = Tree(f"[bold]{title}[/bold] ({len(roots)} root span(s))")
        for root in roots:
            _add_span_branch(tree, root)
    console.print(tree)
    return console.export_text()


def _add_span_branch(parent: Any, node: dict[str, Any]) -> None:
    label = _span_label(node)
    branch = parent.add(label)
    for child in node.get("children", []):
        _add_span_branch(branch, child)


def _span_label(node: dict[str, Any]) -> str:
    payload = node.get("payload") or {}
    who = payload.get("from") or node.get("producer_id", "?")
    summary = payload.get("summary", "")
    if len(summary) > 80:
        summary = summary[:77] + "..."
    span = node.get("span_id", "?")
    return f"[cyan]{who}[/cyan] [dim]{span}[/dim] — {summary or '(no summary)'}"


def format_trace_tree_plain(trace_id: str, roots: list[dict[str, Any]]) -> str:
    """ASCII trace waterfall for TUI / non-rich consumers."""

    def walk(node: dict[str, Any], indent: int = 0) -> list[str]:
        payload = node.get("payload") or {}
        who = payload.get("from") or node.get("producer_id", "?")
        summary = payload.get("summary", "")
        if len(summary) > 60:
            summary = summary[:57] + "..."
        span = node.get("span_id", "?")
        prefix = "  " * indent
        lines = [f"{prefix}├─ {who} ({span}) — {summary or '(no summary)'}"]
        for child in node.get("children", []):
            lines.extend(walk(child, indent + 1))
        return lines

    if not roots:
        return f"trace {trace_id}\n  (no events)"
    body: list[str] = [f"trace {trace_id} ({len(roots)} root span(s))"]
    for root in roots:
        body.extend(walk(root))
    return "\n".join(body)