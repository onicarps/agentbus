"""Runner YAML config loader."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass
class IntakeConfig:
    mode: str  # webhook_queue | wake_file
    runtime: str | None = None
    queue_path: str | None = None
    done_path: str | None = None
    wake_file: str | None = None


@dataclass
class AdapterConfig:
    type: str = "echo"
    options: dict[str, Any] = field(default_factory=dict)


@dataclass
class BudgetConfig:
    max_turns_per_chain: int = 10
    max_event_age_hours: int = 24


@dataclass
class RunnerConfig:
    version: str
    runner_id: str
    producer_id: str
    intake: IntakeConfig
    adapter: AdapterConfig
    accept_to: list[str] = field(default_factory=list)
    allow_broadcast: bool = False
    budget: BudgetConfig = field(default_factory=BudgetConfig)
    poll_interval_ms: int = 1000
    runs_dir: str = ".agentbus/runs"
    path: Path | None = None

    def resolve(self, workspace: Path, rel: str | None) -> Path | None:
        if not rel:
            return None
        p = Path(rel)
        if p.is_absolute():
            return p
        return (workspace / p).resolve()


def load_runner_config(path: Path) -> RunnerConfig:
    path = path.resolve()
    if not path.is_file():
        raise FileNotFoundError(f"runner config not found: {path}")
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        raise ValueError("runner config root must be a mapping")

    runner_id = str(raw.get("runner_id") or "").strip()
    producer_id = str(raw.get("producer_id") or "").strip()
    if not runner_id:
        raise ValueError("runner_id required")
    if not producer_id:
        raise ValueError("producer_id required")

    intake_raw = raw.get("intake") or {}
    if not isinstance(intake_raw, dict):
        raise ValueError("intake must be a mapping")
    mode = str(intake_raw.get("mode") or "").strip()
    if mode not in {"webhook_queue", "wake_file"}:
        raise ValueError("intake.mode must be webhook_queue or wake_file")
    runtime = intake_raw.get("runtime")
    if mode == "webhook_queue" and not runtime and not intake_raw.get("queue_path"):
        raise ValueError("intake.runtime or intake.queue_path required for webhook_queue")

    adapter_raw = raw.get("adapter") or {}
    if not isinstance(adapter_raw, dict):
        adapter_raw = {"type": str(adapter_raw)}
    adapter_type = str(adapter_raw.get("type") or "echo").strip()
    # All adapter keys except type become options (command, timeout, dry_run, …)
    adapter_options = {
        str(k): v for k, v in adapter_raw.items() if k != "type"
    }

    budget_raw = raw.get("budget") or {}
    if not isinstance(budget_raw, dict):
        budget_raw = {}

    accept_to = raw.get("accept_to") or []
    if not isinstance(accept_to, list) or not accept_to:
        raise ValueError("accept_to must be a non-empty list")

    return RunnerConfig(
        version=str(raw.get("version") or "1.0"),
        runner_id=runner_id,
        producer_id=producer_id,
        intake=IntakeConfig(
            mode=mode,
            runtime=str(runtime).strip() if runtime else None,
            queue_path=intake_raw.get("queue_path"),
            done_path=intake_raw.get("done_path"),
            wake_file=intake_raw.get("wake_file"),
        ),
        adapter=AdapterConfig(type=adapter_type, options=adapter_options),
        accept_to=[str(x) for x in accept_to],
        allow_broadcast=bool(raw.get("allow_broadcast", False)),
        budget=BudgetConfig(
            max_turns_per_chain=int(
                budget_raw.get("max_turns_per_chain", 10)
            ),
            max_event_age_hours=int(budget_raw.get("max_event_age_hours", 24)),
        ),
        poll_interval_ms=int(raw.get("poll_interval_ms", 1000)),
        runs_dir=str(raw.get("runs_dir") or ".agentbus/runs"),
        path=path,
    )


def default_queue_path(workspace: Path, runtime: str) -> Path:
    return workspace / ".agentbus" / "ingress" / f"{runtime}_wake_queue.jsonl"


def default_done_path(workspace: Path, runtime: str) -> Path:
    return workspace / ".agentbus" / "ingress" / f"{runtime}_wake_done.ids"


def runner_done_path(workspace: Path, runner_id: str) -> Path:
    return workspace / ".agentbus" / f"runner.{runner_id}.done.ids"


def runner_state_path(workspace: Path, runner_id: str) -> Path:
    return workspace / ".agentbus" / f"runner.{runner_id}.state.json"
