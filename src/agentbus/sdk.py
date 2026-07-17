"""Code-first Python SDK for pluggable topic schemas."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, TypeVar

from agentbus.schema_registry import register_schema
from agentbus.store import EventStore

T = TypeVar("T")


class AgentBus:
    """Workspace-scoped bus with Pydantic topic registration."""

    def __init__(self, workspace: str | Path | None = None) -> None:
        import os

        ws = workspace or os.environ.get("AGENTBUS_WORKSPACE", Path.cwd())
        self.workspace = Path(ws).resolve()
        self._topics: dict[str, type] = {}

    def topic(self, name: str) -> Callable[[type[T]], type[T]]:
        def decorator(cls: type[T]) -> type[T]:
            try:
                from pydantic import BaseModel
            except ImportError as exc:
                raise ImportError(
                    "pydantic required for agentbus.sdk — pip install 'okf-agentbus[sdk]'"
                ) from exc
            if not isinstance(cls, type) or not issubclass(cls, BaseModel):
                raise TypeError("@bus.topic requires a Pydantic BaseModel subclass")
            schema = cls.model_json_schema()
            register_schema(self.workspace, name, schema)
            self._topics[name] = cls
            cls.__agentbus_topic__ = name  # type: ignore[attr-defined]
            return cls

        return decorator

    def publish(
        self,
        model: Any,
        *,
        producer_id: str | None = None,
        schema_version: str = "1.0",
        **kwargs: Any,
    ) -> dict:
        import os

        topic = getattr(model.__class__, "__agentbus_topic__", None)
        if not topic:
            raise ValueError("model_not_registered: decorate class with @bus.topic")
        pid = producer_id or os.environ.get("AGENTBUS_PRODUCER_ID", "")
        if not pid:
            raise ValueError("producer_id required (arg or AGENTBUS_PRODUCER_ID)")
        payload = model.model_dump()
        store = EventStore(self.workspace)
        try:
            from agentbus.schemas import validate_payload

            payload = validate_payload(topic, payload, producer_id=pid)
            event, duplicate = store.publish(
                topic=topic,
                producer_id=pid,
                schema_version=schema_version,
                payload=payload,
                **kwargs,
            )
            return {
                "event_id": event.event_id,
                "topic": event.topic,
                "duplicate": duplicate,
            }
        finally:
            store.close()