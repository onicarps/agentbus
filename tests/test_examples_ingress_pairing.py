"""Example configs must pair webhook workers with webhook_queue runners (#682)."""

from __future__ import annotations

from pathlib import Path

import yaml

from agentbus.runner.config import load_runner_config

REPO = Path(__file__).resolve().parents[1]
EXAMPLES = REPO / "examples"


def test_example_runner_factory_uses_webhook_queue():
    cfg = load_runner_config(EXAMPLES / "runner.factory.yaml")
    assert cfg.intake.mode == "webhook_queue"
    assert cfg.intake.runtime == "factory"
    assert cfg.producer_id == "factory"


def test_example_runner_hermes_uses_webhook_queue():
    cfg = load_runner_config(EXAMPLES / "runner.hermes.yaml")
    assert cfg.intake.mode == "webhook_queue"
    assert cfg.intake.runtime == "hermes"


def test_example_swarm_documents_ingress_pairing():
    text = (EXAMPLES / "swarm.yaml").read_text(encoding="utf-8")
    assert "Ingress pairing" in text or "ingress" in text.lower()
    assert "webhook_queue" in text
    # hermes-runner stays off by default (no consumer → ingress must stay off)
    raw = yaml.safe_load(text)
    hermes = raw["services"]["hermes-runner"]
    assert hermes.get("enabled") is False
