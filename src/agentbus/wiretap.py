"""MCP wiretap — redact secrets and emit system/mcp observability events."""

from __future__ import annotations

import json
import re
import time
from pathlib import Path
from typing import Any, Callable

# Keys (case-insensitive) stripped from wiretap payloads before publish/log.
SENSITIVE_KEY_RE = re.compile(
    r"(auth_token|token|authorization|api[_-]?key|secret|password|passwd|bearer"
    r"|AGENTBUS_TOKEN|AGENTBUS_EXPECTED_TOKEN)",
    re.IGNORECASE,
)
REDACTED = "***REDACTED***"


def redact_value(obj: Any) -> Any:
    """Recursively redact sensitive keys from nested dicts/lists."""
    if isinstance(obj, dict):
        out: dict[str, Any] = {}
        for key, value in obj.items():
            if SENSITIVE_KEY_RE.search(str(key)):
                out[key] = REDACTED
            else:
                out[key] = redact_value(value)
        return out
    if isinstance(obj, list):
        return [redact_value(item) for item in obj]
    if isinstance(obj, str):
        # Mask obvious bearer tokens / long secrets in free text
        if len(obj) >= 32 and re.fullmatch(r"[A-Za-z0-9_\-+/=]{32,}", obj):
            return REDACTED
        return obj
    return obj


def summarize_result(result: Any, max_len: int = 240) -> str:
    try:
        if isinstance(result, str):
            text = result
        else:
            text = json.dumps(result, ensure_ascii=False, default=str)
    except Exception:
        text = repr(result)
    if len(text) > max_len:
        return text[: max_len - 3] + "..."
    return text


def emit_system_mcp(
    store: Any,
    *,
    tool: str,
    arguments: dict[str, Any] | None,
    latency_ms: float,
    result_summary: str | None = None,
    error: str | None = None,
    client: str | None = None,
    direction: str = "tools/call",
    wiretap_log: Path | None = None,
) -> int | None:
    """Publish a system/mcp event (skip_rbac). Returns event_id or None."""
    payload: dict[str, Any] = {
        "method": direction,
        "tool": tool,
        "arguments": redact_value(arguments or {}),
        "latency_ms": round(latency_ms, 2),
        "direction": "c2s",
        "observer": "wiretap",
    }
    if client:
        payload["client"] = client
    if result_summary is not None:
        payload["result_summary"] = result_summary
    if error is not None:
        payload["error"] = error[:500]

    if wiretap_log is not None:
        wiretap_log.parent.mkdir(parents=True, exist_ok=True)
        with open(wiretap_log, "a", encoding="utf-8") as fp:
            fp.write(json.dumps(payload, ensure_ascii=False, default=str) + "\n")

    try:
        event, _ = store.publish(
            topic="system/mcp",
            producer_id="wiretap",
            schema_version="1.0",
            payload=payload,
            skip_rbac=True,
            idempotency_key=None,
        )
        return event.event_id
    except Exception:
        return None


def instrument_call(
    store: Any,
    tool: str,
    arguments: dict[str, Any],
    fn: Callable[[], Any],
    *,
    wiretap_log: Path | None = None,
    client: str | None = None,
) -> Any:
    """Time a tool call, emit system/mcp, return result (or re-raise)."""
    t0 = time.perf_counter()
    try:
        result = fn()
        latency_ms = (time.perf_counter() - t0) * 1000.0
        emit_system_mcp(
            store,
            tool=tool,
            arguments=arguments,
            latency_ms=latency_ms,
            result_summary=summarize_result(result),
            client=client,
            wiretap_log=wiretap_log,
        )
        return result
    except Exception as exc:
        latency_ms = (time.perf_counter() - t0) * 1000.0
        emit_system_mcp(
            store,
            tool=tool,
            arguments=arguments,
            latency_ms=latency_ms,
            error=str(exc),
            client=client,
            wiretap_log=wiretap_log,
        )
        raise
