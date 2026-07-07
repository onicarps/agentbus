"""HITL intercept rule configuration."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path


DEFAULT_TTL_MINUTES = 60


@dataclass
class InterceptRule:
    topic: str
    contains: str
    ttl_minutes: int = DEFAULT_TTL_MINUTES

    def to_dict(self) -> dict:
        return {
            "topic": self.topic,
            "contains": self.contains,
            "ttl_minutes": self.ttl_minutes,
        }

    @classmethod
    def from_dict(cls, data: dict) -> InterceptRule:
        return cls(
            topic=data["topic"],
            contains=data["contains"],
            ttl_minutes=int(data.get("ttl_minutes", DEFAULT_TTL_MINUTES)),
        )


@dataclass
class InterceptConfig:
    rules: list[InterceptRule] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {"rules": [r.to_dict() for r in self.rules]}

    @classmethod
    def from_dict(cls, data: dict) -> InterceptConfig:
        return cls(rules=[InterceptRule.from_dict(r) for r in data.get("rules", [])])


def config_path(workspace: Path) -> Path:
    return workspace / ".agentbus" / "intercepts.json"


def load_config(workspace: Path) -> InterceptConfig:
    path = config_path(workspace)
    if not path.exists():
        return InterceptConfig()
    return InterceptConfig.from_dict(json.loads(path.read_text(encoding="utf-8")))


def save_config(workspace: Path, config: InterceptConfig) -> Path:
    path = config_path(workspace)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(config.to_dict(), indent=2) + "\n", encoding="utf-8")
    return path


def add_rule(workspace: Path, rule: InterceptRule) -> InterceptConfig:
    config = load_config(workspace)
    config.rules = [r for r in config.rules if not (r.topic == rule.topic and r.contains == rule.contains)]
    config.rules.append(rule)
    save_config(workspace, config)
    return config


def match_rule(workspace: Path, topic: str, payload: dict) -> InterceptRule | None:
    """Return first matching intercept rule for topic + payload text, else None."""
    haystack = json.dumps(payload, ensure_ascii=False).lower()
    for rule in load_config(workspace).rules:
        if rule.topic != topic:
            continue
        if rule.contains.lower() in haystack:
            return rule
    return None