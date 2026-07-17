"""TurnAdapter protocol and registry."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol

from agentbus.runner.types import TurnResult, WakeEnvelope


class TurnAdapter(Protocol):
    def start_turn(
        self, wake: WakeEnvelope, *, budget_remaining: int
    ) -> TurnResult: ...


def get_adapter(
    adapter_type: str,
    *,
    workspace: Path | None = None,
    options: dict[str, Any] | None = None,
) -> TurnAdapter:
    kind = (adapter_type or "echo").strip().lower()
    if kind == "echo":
        from agentbus.runner.adapters.echo import EchoAdapter

        return EchoAdapter()
    if kind == "hermes":
        from agentbus.runner.adapters.hermes import HermesAdapter

        if workspace is None:
            raise ValueError("hermes adapter requires workspace")
        return HermesAdapter(workspace=workspace, options=options)
    if kind == "factory":
        from agentbus.runner.adapters.factory import FactoryAdapter

        if workspace is None:
            raise ValueError("factory adapter requires workspace")
        return FactoryAdapter(workspace=workspace, options=options)
    if kind == "grok":
        from agentbus.runner.adapters.grok import GrokAdapter

        if workspace is None:
            raise ValueError("grok adapter requires workspace")
        return GrokAdapter(workspace=workspace, options=options)
    if kind == "agy":
        from agentbus.runner.adapters.agy import AgyAdapter

        if workspace is None:
            raise ValueError("agy adapter requires workspace")
        return AgyAdapter(workspace=workspace, options=options)
    raise ValueError(
        f"unknown adapter type {adapter_type!r} "
        f"(supported: echo, hermes, factory, grok, agy)"
    )
