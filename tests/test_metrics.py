"""Tests for agentbus metrics aggregate (status + SLA + ingress queues)."""

from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from threading import Thread

import yaml
from click.testing import CliRunner

from agentbus.cli import main
from agentbus.metrics import collect_workspace_metrics, format_metrics_text
from agentbus.schemas import DEAD_LETTER_TOPIC
from agentbus.store import EventStore

REPO = Path(__file__).resolve().parents[1]


def _write_swarm(ws: Path, services: dict) -> None:
    adir = ws / ".agentbus"
    adir.mkdir(parents=True, exist_ok=True)
    (adir / "swarm.yaml").write_text(
        yaml.safe_dump({"version": "1.0", "services": services}, sort_keys=False),
        encoding="utf-8",
    )


def _write_queue(
    ws: Path,
    runtime: str,
    event_ids: list[int],
    *,
    done_ids: list[int] | None = None,
) -> None:
    ingress = ws / ".agentbus" / "ingress"
    ingress.mkdir(parents=True, exist_ok=True)
    qpath = ingress / f"{runtime}_wake_queue.jsonl"
    lines = []
    for eid in event_ids:
        rec = {
            "event_id": eid,
            "topic": "okf/handoff",
            "from": "grok",
            "to": runtime,
            "summary": f"wake {eid}",
            "payload": {"from": "grok", "to": runtime, "summary": f"wake {eid}"},
        }
        lines.append(json.dumps(rec))
    qpath.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
    if done_ids is not None:
        dpath = ingress / f"{runtime}_wake_done.ids"
        dpath.write_text(
            "\n".join(str(i) for i in done_ids) + ("\n" if done_ids else ""),
            encoding="utf-8",
        )


def _seed_bus(ws: Path) -> EventStore:
    store = EventStore(ws, retention_days=7)
    store.publish(
        topic="okf/handoff",
        producer_id="agy",
        schema_version="1.0",
        payload={"from": "agy", "to": "grok", "summary": "hello"},
    )
    return store


def test_metrics_status_and_empty_ingress(tmp_path: Path):
    _write_swarm(
        tmp_path,
        {
            "watch": {"command": "agentbus watch --no-shell"},
        },
    )
    store = _seed_bus(tmp_path)
    try:
        report = collect_workspace_metrics(
            tmp_path, probe_health=False, include_waits=False, store=store
        )
    finally:
        store.close()
    assert report.ok
    d = report.to_dict()
    assert d["status"]["event_count"] >= 1
    assert d["status"]["latest_event_id"] >= 1
    assert d["sla"]["active_count"] == 0
    assert d["sla"]["dead_letter"]["total"] == 0
    assert d["ingress"] == []
    assert "waits" not in d


def test_metrics_undrained_vs_line_count(tmp_path: Path):
    """queue line_count is historical; undrained is true backlog."""
    _write_swarm(
        tmp_path,
        {
            "factory-wake-ingress": {
                "command": (
                    "agentbus wake-ingress --runtime factory "
                    "--host 127.0.0.1 --port 18788"
                ),
            },
        },
    )
    # 5 enqueued, 3 done → undrained=2, line_count=5
    _write_queue(tmp_path, "factory", [10, 11, 12, 13, 14], done_ids=[10, 11, 12])
    store = _seed_bus(tmp_path)
    try:
        report = collect_workspace_metrics(
            tmp_path, probe_health=False, include_waits=False, store=store
        )
    finally:
        store.close()
    assert report.ok
    assert len(report.ingress) == 1
    ing = report.ingress[0]
    assert ing["service"] == "factory-wake-ingress"
    assert ing["runtime"] == "factory"
    assert ing["enabled"] is True
    q = ing["queue"]
    assert q["line_count"] == 5
    assert q["undrained"] == 2
    assert q["done_count"] == 3
    assert set(q["undrained_sample"]) == {13, 14}
    assert ing["note"] == "health_probe_skipped"


def test_metrics_disabled_ingress_noted(tmp_path: Path):
    _write_swarm(
        tmp_path,
        {
            "watch": {"command": "agentbus watch --no-shell"},
            "hermes-wake-ingress": {
                "enabled": False,
                "command": (
                    "agentbus wake-ingress --runtime hermes "
                    "--host 127.0.0.1 --port 18787"
                ),
            },
        },
    )
    _write_queue(tmp_path, "hermes", [1, 2], done_ids=[1])
    store = _seed_bus(tmp_path)
    try:
        report = collect_workspace_metrics(
            tmp_path, probe_health=True, include_waits=False, store=store
        )
    finally:
        store.close()
    hermes = next(i for i in report.ingress if i["runtime"] == "hermes")
    assert hermes["enabled"] is False
    assert hermes["note"] == "disabled_by_config"
    assert hermes["health"] is None  # no probe when disabled
    assert hermes["queue"]["undrained"] == 1


def test_metrics_health_probe_success(tmp_path: Path):
    class _Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            body = json.dumps(
                {
                    "ok": True,
                    "runtime": "factory",
                    "queue_depth": 3,
                }
            ).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, fmt: str, *args) -> None:  # noqa: ARG002
            return

    server = HTTPServer(("127.0.0.1", 0), _Handler)
    port = server.server_address[1]
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        _write_swarm(
            tmp_path,
            {
                "factory-wake-ingress": {
                    "command": (
                        f"agentbus wake-ingress --runtime factory "
                        f"--host 127.0.0.1 --port {port}"
                    ),
                },
            },
        )
        _write_queue(tmp_path, "factory", [1, 2, 3], done_ids=[1, 2, 3])
        store = _seed_bus(tmp_path)
        try:
            report = collect_workspace_metrics(
                tmp_path,
                probe_health=True,
                include_waits=False,
                store=store,
                health_timeout_s=2.0,
            )
        finally:
            store.close()
        ing = report.ingress[0]
        assert ing["health"]["reachable"] is True
        assert ing["health"]["ok"] is True
        assert ing["health"]["queue_depth"] == 3
        assert ing["note"] is None
    finally:
        server.shutdown()


def test_metrics_dead_letter_by_reason(tmp_path: Path):
    _write_swarm(tmp_path, {"watch": {"command": "agentbus watch --no-shell"}})
    store = EventStore(tmp_path, retention_days=7)
    try:
        orig, _ = store.publish(
            topic="okf/handoff",
            producer_id="agy",
            schema_version="1.0",
            payload={"from": "agy", "to": "grok", "summary": "with sla"},
            sla_timeout_minutes=60,
        )
        store.publish(
            topic=DEAD_LETTER_TOPIC,
            producer_id="agentbus",
            schema_version="1.0",
            payload={
                "reason": "SLA_BREACH",
                "original_event_id": orig.event_id,
                "original_event": {
                    "event_id": orig.event_id,
                    "topic": "okf/handoff",
                },
                "summary": f"SLA_BREACH for event {orig.event_id}",
            },
        )
        store.publish(
            topic=DEAD_LETTER_TOPIC,
            producer_id="agentbus",
            schema_version="1.0",
            payload={
                "reason": "WAIT_TIMEOUT",
                "original_event_id": orig.event_id,
                "original_event": {
                    "event_id": orig.event_id,
                    "topic": "okf/handoff",
                },
                "summary": "WAIT_TIMEOUT for wait w_test",
            },
        )
        report = collect_workspace_metrics(
            tmp_path, probe_health=False, include_waits=False, store=store
        )
    finally:
        store.close()
    dl = report.sla["dead_letter"]
    assert dl["total"] == 2
    assert dl["by_reason"]["SLA_BREACH"] == 1
    assert dl["by_reason"]["WAIT_TIMEOUT"] == 1
    assert len(dl["recent"]) == 2


def test_metrics_waits_open_count(tmp_path: Path):
    from agentbus.runner.wait_store import (
        WaitPredicate,
        WaitRegistration,
        WaitStore,
        utc_now_iso,
    )

    _write_swarm(tmp_path, {"watch": {"command": "agentbus watch --no-shell"}})
    ws = WaitStore(tmp_path)
    ws.save(
        WaitRegistration(
            wait_id="w_testpending01",
            runner_id="grok-runner-1",
            producer_id="grok",
            chain_key="1",
            origin_event_id=1,
            suspended_at=utc_now_iso(),
            timeout_at=utc_now_iso(),
            reason="await factory",
            predicate=WaitPredicate(from_any=["factory"], causation_id=1),
            status="pending",
        )
    )
    ws.save(
        WaitRegistration(
            wait_id="w_testdone00001",
            runner_id="grok-runner-1",
            producer_id="grok",
            chain_key="2",
            origin_event_id=2,
            suspended_at=utc_now_iso(),
            timeout_at=utc_now_iso(),
            reason="done",
            predicate=WaitPredicate(from_any=["factory"], causation_id=2),
            status="fulfilled",
            fulfilled_by=99,
        )
    )
    store = _seed_bus(tmp_path)
    try:
        report = collect_workspace_metrics(
            tmp_path, probe_health=False, include_waits=True, store=store
        )
    finally:
        store.close()
    assert report.waits is not None
    assert report.waits["open_count"] == 1
    assert report.waits["by_status"]["pending"] == 1
    assert report.waits["by_status"]["fulfilled"] == 1
    assert report.waits["open"][0]["wait_id"] == "w_testpending01"


def test_format_metrics_text(tmp_path: Path):
    _write_swarm(
        tmp_path,
        {
            "factory-wake-ingress": {
                "enabled": False,
                "command": "agentbus wake-ingress --runtime factory --port 18788",
            },
        },
    )
    _write_queue(tmp_path, "factory", [1], done_ids=[])
    store = _seed_bus(tmp_path)
    try:
        report = collect_workspace_metrics(
            tmp_path, probe_health=False, include_waits=False, store=store
        )
    finally:
        store.close()
    text = format_metrics_text(report)
    assert "metrics: OK" in text
    assert "status: events=" in text
    assert "factory-wake-ingress" in text
    assert "undrained=1" in text
    assert "note=disabled_by_config" in text


def test_cli_metrics_json(tmp_path: Path):
    _write_swarm(tmp_path, {"watch": {"command": "agentbus watch --no-shell"}})
    store = _seed_bus(tmp_path)
    store.close()
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "metrics",
            "--workspace",
            str(tmp_path),
            "--no-health",
            "--no-waits",
        ],
    )
    assert result.exit_code == 0, result.output
    data = json.loads(result.output)
    assert data["ok"] is True
    assert data["status"]["event_count"] >= 1
    assert "waits" not in data


def test_cli_metrics_text(tmp_path: Path):
    _write_swarm(tmp_path, {"watch": {"command": "agentbus watch --no-shell"}})
    store = _seed_bus(tmp_path)
    store.close()
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "metrics",
            "--workspace",
            str(tmp_path),
            "--text",
            "--no-health",
            "--no-waits",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "metrics: OK" in result.output
    assert "status: events=" in result.output
