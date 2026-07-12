"""Topic payload JSON Schema validation."""

from __future__ import annotations

import re
from pathlib import Path

from jsonschema import Draft202012Validator

TOPIC_PATTERN = re.compile(r"^[a-z][a-z0-9._/-]*$")
_WORKSPACE: Path | None = None


def set_validation_workspace(workspace: Path | None) -> None:
    """Bind workspace for pluggable topic_schemas lookup (CLI/MCP startup)."""
    global _WORKSPACE
    _WORKSPACE = workspace.resolve() if workspace else None


def validate_topic_name(topic: str) -> None:
    if not topic or len(topic) > 128 or not TOPIC_PATTERN.match(topic):
        raise ValueError(f"invalid_topic: {topic}")

DEAD_LETTER_TOPIC = "okf/dead-letter"

KNOWN_TOPICS: dict[str, dict] = {
    DEAD_LETTER_TOPIC: {
        "type": "object",
        "required": ["reason", "original_event_id", "original_event", "summary"],
        "additionalProperties": False,
        "properties": {
            "reason": {"enum": ["SLA_BREACH"]},
            "original_event_id": {"type": "integer", "minimum": 1},
            "original_event": {"type": "object"},
            "summary": {"type": "string", "minLength": 1, "maxLength": 2000},
        },
    },
    "okf/approval": {
        "type": "object",
        "required": ["event_id", "approver", "decision"],
        "additionalProperties": False,
        "properties": {
            "event_id": {"type": "integer", "minimum": 1},
            "approver": {"type": "string", "pattern": r"^[a-z][a-z0-9_-]*$"},
            "decision": {"enum": ["approve", "reject"]},
            "reason": {"type": "string", "maxLength": 2000},
        },
    },
    "okf/handoff": {
        "type": "object",
        "required": ["from", "to", "summary"],
        "additionalProperties": False,
        "properties": {
            "from": {"type": "string", "pattern": r"^[a-z][a-z0-9_-]*$"},
            "to": {"type": "string", "pattern": r"^[a-z][a-z0-9_*,*-]+$"},
            "summary": {"type": "string", "minLength": 1, "maxLength": 2000},
            "links": {
                "type": "array",
                "items": {"type": "string"},
                "maxItems": 10,
            },
            "initiative": {"type": "string"},
            "droid_proof": {"type": "string", "minLength": 8, "maxLength": 256},
            "tool": {
                "type": "string",
                "minLength": 1,
                "maxLength": 256,
                "description": "Optional MCP/tool name for mcpsafe policy checks",
            },
            "tool_name": {
                "type": "string",
                "minLength": 1,
                "maxLength": 256,
            },
            "mcp_tool": {
                "type": "string",
                "minLength": 1,
                "maxLength": 256,
            },
            "artifacts": {
                "type": "array",
                "maxItems": 10,
                "items": {
                    "type": "object",
                    "required": ["type", "name", "content"],
                    "additionalProperties": False,
                    "properties": {
                        "type": {
                            "enum": ["git_diff", "file_content", "error_trace"],
                        },
                        "name": {"type": "string", "minLength": 1, "maxLength": 256},
                        "content": {"type": "string"},
                    },
                },
            },
        },
    },
    # God View observability topics (v0.9) — loose schemas for high-volume streams
    "system/mcp": {
        "type": "object",
        "required": ["tool"],
        "additionalProperties": True,
        "properties": {
            "method": {"type": "string"},
            "tool": {"type": "string", "minLength": 1},
            "arguments": {"type": "object"},
            "result_summary": {"type": "string"},
            "error": {"type": "string"},
            "latency_ms": {"type": "number"},
            "client": {"type": "string"},
            "direction": {"type": "string"},
            "observer": {"type": "string"},
        },
    },
    "system/fs": {
        "type": "object",
        "required": ["event", "path"],
        "additionalProperties": True,
        "properties": {
            "event": {"type": "string"},
            "path": {"type": "string"},
            "is_directory": {"type": "boolean"},
            "dest_path": {"type": "string"},
            "abs_path": {"type": "string"},
            "observer": {"type": "string"},
        },
    },
    "system/shell": {
        "type": "object",
        "required": ["event", "pid"],
        "additionalProperties": True,
        "properties": {
            "event": {"type": "string"},
            "pid": {"type": "integer"},
            "ppid": {"type": ["integer", "null"]},
            "name": {"type": "string"},
            "cmdline": {"type": "array", "items": {"type": "string"}},
            "cwd": {"type": "string"},
            "username": {"type": "string"},
            "under_workspace": {"type": "boolean"},
            "observer": {"type": "string"},
        },
    },
    "system/monologue": {
        "type": "object",
        "required": ["agent", "text"],
        "additionalProperties": True,
        "properties": {
            "agent": {"type": "string"},
            "role": {"type": "string"},
            "text": {"type": "string"},
            "source_path": {"type": "string"},
            "observer": {"type": "string"},
        },
    },
}

SYSTEM_TOPICS = frozenset(
    {"system/mcp", "system/fs", "system/shell", "system/monologue"}
)

# okf/status/<initiative> — dynamic suffix
STATUS_TOPIC_PATTERN = re.compile(r"^okf/status/[a-z][a-z0-9-]*$")
STATUS_PAYLOAD_SCHEMA = {
    "type": "object",
    "required": ["state", "message"],
    "additionalProperties": False,
    "properties": {
        "state": {"enum": ["idle", "active", "blocked", "complete"]},
        "message": {"type": "string", "maxLength": 500},
    },
}


def validate_topic(topic: str, *, workspace: Path | None = None) -> None:
    validate_topic_name(topic)
    if topic in KNOWN_TOPICS or STATUS_TOPIC_PATTERN.match(topic):
        return
    ws = workspace or _WORKSPACE
    if ws is not None:
        from agentbus.schema_registry import load_schema

        if load_schema(ws, topic) is not None:
            return
    raise ValueError(f"unknown_topic: {topic}")


def _resolve_schema(topic: str, workspace: Path | None = None) -> dict:
    if STATUS_TOPIC_PATTERN.match(topic):
        return STATUS_PAYLOAD_SCHEMA
    if topic in KNOWN_TOPICS:
        return KNOWN_TOPICS[topic]
    ws = workspace or _WORKSPACE
    if ws is not None:
        from agentbus.schema_registry import load_schema

        custom = load_schema(ws, topic)
        if custom is not None:
            return custom
    raise ValueError(f"unknown_topic: {topic}")


def normalize_handoff_payload(payload: dict) -> dict:
    """Coerce common agent mistakes before schema validation."""
    normalized = dict(payload)
    if "summary" not in normalized and "content" in normalized:
        normalized["summary"] = normalized.pop("content")
    if "to" in normalized and "," in normalized["to"]:
        normalized["to"] = "all"
    normalized.pop("idempotency_key", None)
    for key in (
        "architecture",
        "deliverables",
        "mission_id",
        "next_phase",
        "status",
        "title",
        "link",
    ):
        normalized.pop(key, None)
    return normalized


def validate_payload(
    topic: str, payload: dict, *, workspace: Path | None = None
) -> dict:
    """Validate and return normalized payload (may coerce common agent mistakes)."""
    validate_topic(topic, workspace=workspace)
    if topic == "okf/handoff":
        payload = normalize_handoff_payload(payload)
    schema = _resolve_schema(topic, workspace=workspace)
    validator = Draft202012Validator(schema)
    errors = sorted(validator.iter_errors(payload), key=lambda e: e.path)
    if errors:
        raise ValueError(f"invalid_payload: {errors[0].message}")
    return payload