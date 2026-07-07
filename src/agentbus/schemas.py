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
            "from": {"type": "string", "pattern": r"^[a-z][a-z0-9-]*$"},
            "to": {"type": "string", "pattern": r"^[a-z][a-z0-9-*]+$"},
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


def validate_payload(topic: str, payload: dict) -> None:
    validate_topic(topic)
    if STATUS_TOPIC_PATTERN.match(topic):
        schema = STATUS_PAYLOAD_SCHEMA
    else:
        schema = KNOWN_TOPICS[topic]
    validator = Draft202012Validator(schema)
    errors = sorted(validator.iter_errors(payload), key=lambda e: e.path)
    if errors:
        raise ValueError(f"invalid_payload: {errors[0].message}")