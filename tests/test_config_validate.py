"""Tests for agentbus validate-config (ingress ↔ runner pairing #682)."""

from __future__ import annotations

import json
from pathlib import Path

import yaml
from click.testing import CliRunner

from agentbus.cli import main
from agentbus.config_validate import validate_workspace_config

REPO = Path(__file__).resolve().parents[1]
EXAMPLES = REPO / "examples"


def _write_swarm(ws: Path, services: dict) -> None:
    adir = ws / ".agentbus"
    adir.mkdir(parents=True, exist_ok=True)
    (adir / "swarm.yaml").write_text(
        yaml.safe_dump({"version": "1.0", "services": services}, sort_keys=False),
        encoding="utf-8",
    )


def _write_runner(
    ws: Path,
    name: str,
    *,
    producer_id: str,
    mode: str,
    runtime: str | None = None,
    accept_to: list[str] | None = None,
) -> Path:
    adir = ws / ".agentbus"
    adir.mkdir(parents=True, exist_ok=True)
    path = adir / name
    intake: dict = {"mode": mode}
    if runtime:
        intake["runtime"] = runtime
    if mode == "wake_file":
        intake["wake_file"] = f".agentbus/WAKE.{producer_id}.json"
    raw = {
        "version": "1.0",
        "runner_id": f"{producer_id}-runner-1",
        "producer_id": producer_id,
        "intake": intake,
        "adapter": {"type": "echo"},
        "accept_to": accept_to or [producer_id],
        "budget": {"max_turns_per_chain": 10, "max_event_age_hours": 24},
    }
    path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
    return path


def _write_worker(
    ws: Path,
    filename: str,
    *,
    producer_id: str,
    wake_mode: str = "file",
    webhook_url: str | None = None,
) -> Path:
    adir = ws / ".agentbus"
    adir.mkdir(parents=True, exist_ok=True)
    path = adir / filename
    raw: dict = {
        "version": "1.0",
        "worker_id": f"{producer_id}-1",
        "producer_id": producer_id,
        "wake_mode": wake_mode,
        "subscribe": [{"topic": "okf/handoff"}],
    }
    if webhook_url:
        raw["webhook_url"] = webhook_url
    path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
    return path


def _write_roles(ws: Path, producers: dict[str, str], roles: dict | None = None) -> None:
    adir = ws / ".agentbus"
    adir.mkdir(parents=True, exist_ok=True)
    payload = {
        "roles": roles
        or {
            "engineer": {"can_publish_topics": ["okf/handoff"]},
            "qa": {"can_publish_topics": ["okf/handoff"]},
        },
        "producers": producers,
    }
    (adir / "roles.yaml").write_text(
        yaml.safe_dump(payload, sort_keys=False), encoding="utf-8"
    )


def test_validate_ok_paired_webhook_triad(tmp_path: Path):
    """factory: ingress on + runner webhook_queue + worker webhook → OK."""
    _write_runner(
        tmp_path,
        "runner.factory.yaml",
        producer_id="factory",
        mode="webhook_queue",
        runtime="factory",
    )
    _write_worker(
        tmp_path,
        "worker.factory.yaml",
        producer_id="factory",
        wake_mode="webhook",
        webhook_url="http://127.0.0.1:18788/agentbus/wake",
    )
    _write_roles(tmp_path, {"factory": "qa"})
    _write_swarm(
        tmp_path,
        {
            "factory-wake-ingress": {
                "command": "agentbus wake-ingress --runtime factory --port 18788",
            },
            "factory-runner": {
                "command": "agentbus run --config .agentbus/runner.factory.yaml",
            },
            "factory-wake": {
                "command": "agentbus worker up --config .agentbus/worker.factory.yaml",
            },
        },
    )
    report = validate_workspace_config(tmp_path)
    assert report.ok, report.to_dict()
    assert not report.errors
    codes = {f.code for f in report.findings}
    assert "INGRESS_WITHOUT_RUNNER" not in codes


def test_validate_error_ingress_without_runner(tmp_path: Path):
    """#682 class: ingress on, matching runner disabled."""
    _write_runner(
        tmp_path,
        "runner.hermes.yaml",
        producer_id="hermes",
        mode="webhook_queue",
        runtime="hermes",
    )
    _write_swarm(
        tmp_path,
        {
            "hermes-wake-ingress": {
                "command": "agentbus wake-ingress --runtime hermes --port 18787",
            },
            "hermes-runner": {
                "enabled": False,
                "command": "agentbus run --config .agentbus/runner.hermes.yaml",
            },
            "watch": {"command": "agentbus watch --no-shell"},
        },
    )
    report = validate_workspace_config(tmp_path)
    assert not report.ok
    assert any(f.code == "INGRESS_WITHOUT_RUNNER" for f in report.errors)


def test_validate_error_ingress_runner_mode_mismatch(tmp_path: Path):
    """Ingress on but runner uses wake_file → hard error."""
    _write_runner(
        tmp_path,
        "runner.factory.yaml",
        producer_id="factory",
        mode="wake_file",
        runtime="factory",
    )
    _write_swarm(
        tmp_path,
        {
            "factory-wake-ingress": {
                "command": "agentbus wake-ingress --runtime factory",
            },
            "factory-runner": {
                "command": "agentbus run --config .agentbus/runner.factory.yaml",
            },
        },
    )
    report = validate_workspace_config(tmp_path)
    assert not report.ok
    assert any(f.code == "INGRESS_RUNNER_MODE_MISMATCH" for f in report.errors)


def test_validate_error_worker_webhook_without_triad(tmp_path: Path):
    """Worker wake_mode=webhook without ingress/runner queue."""
    _write_runner(
        tmp_path,
        "runner.factory.yaml",
        producer_id="factory",
        mode="wake_file",
        runtime="factory",
    )
    _write_worker(
        tmp_path,
        "worker.factory.yaml",
        producer_id="factory",
        wake_mode="webhook",
        webhook_url="http://127.0.0.1:18788/agentbus/wake",
    )
    _write_swarm(
        tmp_path,
        {
            "factory-wake-ingress": {
                "enabled": False,
                "command": "agentbus wake-ingress --runtime factory",
            },
            "factory-runner": {
                "command": "agentbus run --config .agentbus/runner.factory.yaml",
            },
        },
    )
    report = validate_workspace_config(tmp_path)
    assert not report.ok
    codes = {f.code for f in report.errors}
    assert "WORKER_WEBHOOK_WITHOUT_INGRESS" in codes
    assert "WORKER_WEBHOOK_WITHOUT_RUNNER_QUEUE" in codes


def test_validate_warn_runner_webhook_without_ingress(tmp_path: Path):
    """Enabled webhook_queue runner with ingress off → warning, still ok."""
    _write_runner(
        tmp_path,
        "runner.factory.yaml",
        producer_id="factory",
        mode="webhook_queue",
        runtime="factory",
    )
    _write_swarm(
        tmp_path,
        {
            "factory-wake-ingress": {
                "enabled": False,
                "command": "agentbus wake-ingress --runtime factory",
            },
            "factory-runner": {
                "command": "agentbus run --config .agentbus/runner.factory.yaml",
            },
        },
    )
    report = validate_workspace_config(tmp_path)
    assert report.ok
    assert any(f.code == "RUNNER_WEBHOOK_WITHOUT_INGRESS" for f in report.warnings)


def test_validate_warn_stale_queue(tmp_path: Path):
    _write_runner(
        tmp_path,
        "runner.hermes.yaml",
        producer_id="hermes",
        mode="wake_file",
        runtime="hermes",
    )
    qdir = tmp_path / ".agentbus" / "ingress"
    qdir.mkdir(parents=True)
    (qdir / "hermes_wake_queue.jsonl").write_text(
        '{"event_id": 1}\n', encoding="utf-8"
    )
    _write_swarm(
        tmp_path,
        {
            "hermes-wake-ingress": {
                "enabled": False,
                "command": "agentbus wake-ingress --runtime hermes",
            },
            "hermes-runner": {
                "command": "agentbus run --config .agentbus/runner.hermes.yaml",
            },
        },
    )
    report = validate_workspace_config(tmp_path)
    assert report.ok
    assert any(f.code == "STALE_QUEUE_INGRESS_OFF" for f in report.warnings)


def test_validate_rbac_role_missing(tmp_path: Path):
    _write_runner(
        tmp_path,
        "runner.grok.yaml",
        producer_id="grok",
        mode="wake_file",
    )
    _write_roles(
        tmp_path,
        producers={"grok": "nonexistent_role"},
        roles={"engineer": {"can_publish_topics": ["okf/handoff"]}},
    )
    _write_swarm(
        tmp_path,
        {
            "grok-runner": {
                "command": "agentbus run --config .agentbus/runner.grok.yaml",
            },
        },
    )
    report = validate_workspace_config(tmp_path)
    assert not report.ok
    assert any(f.code == "RBAC_ROLE_MISSING" for f in report.errors)


def test_validate_missing_swarm(tmp_path: Path):
    report = validate_workspace_config(tmp_path)
    assert not report.ok
    assert any(f.code == "SWARM_MISSING" for f in report.errors)


def test_cli_validate_config_json(tmp_path: Path):
    _write_runner(
        tmp_path,
        "runner.factory.yaml",
        producer_id="factory",
        mode="webhook_queue",
        runtime="factory",
    )
    _write_worker(
        tmp_path,
        "worker.factory.yaml",
        producer_id="factory",
        wake_mode="webhook",
        webhook_url="http://127.0.0.1:18788/agentbus/wake",
    )
    _write_roles(tmp_path, {"factory": "qa"})
    _write_swarm(
        tmp_path,
        {
            "factory-wake-ingress": {
                "command": "agentbus wake-ingress --runtime factory",
            },
            "factory-runner": {
                "command": "agentbus run --config .agentbus/runner.factory.yaml",
            },
        },
    )
    runner = CliRunner()
    result = runner.invoke(
        main, ["validate-config", "--workspace", str(tmp_path)]
    )
    assert result.exit_code == 0, result.output
    data = json.loads(result.output)
    assert data["ok"] is True
    assert data["error_count"] == 0


def test_cli_validate_config_fails_on_mismatch(tmp_path: Path):
    _write_runner(
        tmp_path,
        "runner.hermes.yaml",
        producer_id="hermes",
        mode="webhook_queue",
        runtime="hermes",
    )
    _write_swarm(
        tmp_path,
        {
            "hermes-wake-ingress": {
                "command": "agentbus wake-ingress --runtime hermes",
            },
            "hermes-runner": {
                "enabled": False,
                "command": "agentbus run --config .agentbus/runner.hermes.yaml",
            },
            "watch": {"command": "agentbus watch --no-shell"},
        },
    )
    runner = CliRunner()
    result = runner.invoke(
        main, ["validate-config", "--workspace", str(tmp_path)]
    )
    assert result.exit_code == 1
    data = json.loads(result.output)
    assert data["ok"] is False
    assert data["error_count"] >= 1


def test_cli_validate_config_strict_warnings(tmp_path: Path):
    _write_runner(
        tmp_path,
        "runner.factory.yaml",
        producer_id="factory",
        mode="webhook_queue",
        runtime="factory",
    )
    _write_swarm(
        tmp_path,
        {
            "factory-wake-ingress": {
                "enabled": False,
                "command": "agentbus wake-ingress --runtime factory",
            },
            "factory-runner": {
                "command": "agentbus run --config .agentbus/runner.factory.yaml",
            },
        },
    )
    runner = CliRunner()
    normal = runner.invoke(
        main, ["validate-config", "--workspace", str(tmp_path)]
    )
    assert normal.exit_code == 0, normal.output
    strict = runner.invoke(
        main, ["validate-config", "--workspace", str(tmp_path), "--strict"]
    )
    assert strict.exit_code == 1


def test_cli_validate_config_text(tmp_path: Path):
    _write_swarm(
        tmp_path,
        {
            "watch": {"command": "agentbus watch --no-shell"},
        },
    )
    runner = CliRunner()
    result = runner.invoke(
        main, ["validate-config", "--workspace", str(tmp_path), "--text"]
    )
    assert result.exit_code == 0
    assert "validate-config: OK" in result.output


def test_examples_swarm_validates_when_wired(tmp_path: Path):
    """examples/swarm.yaml + example runners: no hard errors when ingress off."""
    # Copy example swarm + runners into temp workspace shape
    adir = tmp_path / ".agentbus"
    adir.mkdir()
    swarm_raw = yaml.safe_load((EXAMPLES / "swarm.yaml").read_text(encoding="utf-8"))
    # Point runner configs at .agentbus copies
    for name, defn in list(swarm_raw["services"].items()):
        if not isinstance(defn, dict):
            continue
        cmd = defn.get("command") or ""
        if "runner." in cmd:
            # rewrite to local
            defn["command"] = cmd.replace("examples/", ".agentbus/")
    (adir / "swarm.yaml").write_text(
        yaml.safe_dump(swarm_raw, sort_keys=False), encoding="utf-8"
    )
    for runner_name in (
        "runner.factory.yaml",
        "runner.hermes.yaml",
        "runner.grok.yaml",
        "runner.agy.yaml",
        "runner.aider.yaml",
    ):
        src = EXAMPLES / runner_name
        if src.is_file():
            (adir / runner_name).write_text(src.read_text(encoding="utf-8"), encoding="utf-8")

    report = validate_workspace_config(tmp_path)
    # examples leave all runners disabled → may warn SWARM_ALL_DISABLED; no errors
    assert report.ok, report.to_dict()
    assert not report.errors
