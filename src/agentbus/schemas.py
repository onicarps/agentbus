"""Topic payload JSON Schema validation."""

from __future__ import annotations

import re

from jsonschema import Draft202012Validator

TOPIC_PATTERN = re.compile(r"^[a-z][a-z0-9._/-]*$")

KNOWN_TOPICS: dict[str, dict] = {
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
        },
    },
}

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


def validate_topic(topic: str) -> None:
    if not topic or len(topic) > 128 or not TOPIC_PATTERN.match(topic):
        raise ValueError(f"invalid_topic: {topic}")
    if topic in KNOWN_TOPICS or STATUS_TOPIC_PATTERN.match(topic):
        return
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


def validate_payload(topic: str, payload: dict) -> dict:
    """Validate and return normalized payload (may coerce common agent mistakes)."""
    validate_topic(topic)
    if topic == "okf/handoff":
        payload = normalize_handoff_payload(payload)
    if STATUS_TOPIC_PATTERN.match(topic):
        schema = STATUS_PAYLOAD_SCHEMA
    else:
        schema = KNOWN_TOPICS[topic]
    validator = Draft202012Validator(schema)
    errors = sorted(validator.iter_errors(payload), key=lambda e: e.path)
    if errors:
        raise ValueError(f"invalid_payload: {errors[0].message}")
    return payload