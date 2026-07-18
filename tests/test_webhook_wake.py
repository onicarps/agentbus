import threading
import json
import subprocess
import os
import shutil
from pathlib import Path
from http.server import HTTPServer, BaseHTTPRequestHandler

import pytest
import yaml

from agentbus.store import EventStore


def _ensure_go_worker() -> Path:
    """Build go-core/bin/agentbus-go-worker if missing (CI / clean checkouts)."""
    go_core = Path(__file__).parent.parent / "go-core"
    binary = go_core / "bin" / "agentbus-go-worker"
    if binary.is_file() and os.access(binary, os.X_OK):
        return binary
    go = shutil.which("go")
    if not go:
        pytest.skip("go toolchain not available to build agentbus-go-worker")
    binary.parent.mkdir(parents=True, exist_ok=True)
    result = subprocess.run(
        [go, "build", "-o", str(binary), "./cmd/agentbus-go-worker"],
        cwd=go_core,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        pytest.fail(f"go build failed: {result.stderr}")
    return binary


class MockServerRequestHandler(BaseHTTPRequestHandler):
    received_requests = []
    
    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        MockServerRequestHandler.received_requests.append(json.loads(post_data))
        
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(b'{"status": "ok"}')
        
    def log_message(self, format, *args):
        pass # silent


@pytest.fixture
def mock_server():
    MockServerRequestHandler.received_requests = []
    server = HTTPServer(('127.0.0.1', 0), MockServerRequestHandler)
    port = server.server_port
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{port}"
    server.shutdown()
    server.server_close()
    thread.join(timeout=1)


def test_webhook_wake(mock_server, tmp_path):
    workspace = tmp_path
    ab_dir = workspace / ".agentbus"
    ab_dir.mkdir()
    
    # Write worker.yaml
    worker_cfg = {
        "version": "1.0",
        "worker_id": "test-webhook-worker",
        "role": "implementer",
        "producer_id": "grok",
        "cursor_path": ".agentbus/worker.cursor",
        "state_path": ".agentbus/worker.state.json",
        "wake_mode": "webhook",
        "webhook_url": mock_server,
        "subscribe": [
            {
                "topic": "okf/handoff",
                "from": ["agy"],
                "to": ["grok", "implementer"]
            }
        ],
        "watch": {
            "mode": "poll",
            "poll_fallback_ms": 100,
            "paths": [".agentbus/events.db"]
        },
        "on_task": [
            {"write": {"path": ".agentbus/WAKE.json"}}
        ],
        "budget": {
            "max_dispatches_per_hour": 60,
            "max_concurrent_exec": 1,
            "require_wake_after_sleep": True
        }
    }
    
    (ab_dir / "worker.yaml").write_text(yaml.safe_dump(worker_cfg))
    
    # Initialize store and publish event
    store = EventStore(workspace)
    payload = {"from": "agy", "to": "grok", "summary": "test webhook payload"}
    store.publish(
        topic="okf/handoff",
        producer_id="agy",
        schema_version="1.0",
        payload=payload,
    )
    store.close()

    binary = _ensure_go_worker()
    go_core_dir = binary.parent.parent
    cmd = [str(binary), "-cmd", "once"]
    env = os.environ.copy()
    env["AGENTBUS_WORKSPACE"] = str(workspace)

    result = subprocess.run(cmd, cwd=go_core_dir, env=env, capture_output=True, text=True)
    
    # Verify webhook received
    assert result.returncode == 0, f"Worker failed: {result.stderr}"
    
    assert len(MockServerRequestHandler.received_requests) == 1
    req = MockServerRequestHandler.received_requests[0]
    assert req["topic"] == "okf/handoff"
    assert req["payload"]["summary"] == "test webhook payload"
    assert req["worker_id"] == "test-webhook-worker"

    # File wake still written (webhook is additive)
    wake_path = ab_dir / "WAKE.json"
    assert wake_path.is_file(), "WAKE.json should still be written in webhook mode"
    wake = json.loads(wake_path.read_text())
    assert wake["event_id"] == req["event_id"]
    assert wake["payload"]["summary"] == "test webhook payload"


def test_webhook_sends_idempotency_and_token(mock_server, tmp_path, monkeypatch):
    """D1: Idempotency-Key + token headers on POST."""
    monkeypatch.setenv("AGENTBUS_WEBHOOK_TOKEN", "dogfood-token")
    workspace = tmp_path
    ab_dir = workspace / ".agentbus"
    ab_dir.mkdir()
    worker_cfg = {
        "version": "1.0",
        "worker_id": "test-webhook-worker",
        "role": "implementer",
        "producer_id": "grok",
        "cursor_path": ".agentbus/worker.cursor",
        "state_path": ".agentbus/worker.state.json",
        "wake_mode": "webhook",
        "webhook_url": mock_server,
        "subscribe": [
            {"topic": "okf/handoff", "from": ["agy"], "to": ["grok", "implementer"]}
        ],
        "watch": {"mode": "poll", "poll_fallback_ms": 100, "paths": [".agentbus/events.db"]},
        "on_task": [{"write": {"path": ".agentbus/WAKE.json"}}],
        "budget": {"max_dispatches_per_hour": 60, "max_concurrent_exec": 1},
    }
    (ab_dir / "worker.yaml").write_text(yaml.safe_dump(worker_cfg))
    store = EventStore(workspace)
    store.publish(
        topic="okf/handoff",
        producer_id="agy",
        schema_version="1.0",
        payload={"from": "agy", "to": "grok", "summary": "token header check"},
    )
    store.close()

    # Capture headers via custom handler — extend mock by reading last request only for body;
    # re-run with instrumentation: monkeypatch BaseHTTPRequestHandler is heavy; assert via
    # second mock that records headers.
    recorded = []

    class H(BaseHTTPRequestHandler):
        def do_POST(self):
            length = int(self.headers["Content-Length"])
            body = self.rfile.read(length)
            recorded.append(
                {
                    "body": json.loads(body),
                    "idem": self.headers.get("Idempotency-Key"),
                    "token": self.headers.get("X-AgentBus-Token"),
                    "auth": self.headers.get("Authorization"),
                }
            )
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'{}')

        def log_message(self, *a):
            pass

    srv = HTTPServer(("127.0.0.1", 0), H)
    port = srv.server_port
    th = threading.Thread(target=srv.serve_forever, daemon=True)
    th.start()
    try:
        worker_cfg["webhook_url"] = f"http://127.0.0.1:{port}"
        (ab_dir / "worker.yaml").write_text(yaml.safe_dump(worker_cfg))
        # reset cursor so once re-processes? worker already consumed — re-publish
        store = EventStore(workspace)
        store.publish(
            topic="okf/handoff",
            producer_id="agy",
            schema_version="1.0",
            payload={"from": "agy", "to": "grok", "summary": "token header check 2"},
        )
        store.close()
        binary = _ensure_go_worker()
        go_core_dir = binary.parent.parent
        cmd = [str(binary), "-cmd", "once"]
        env = os.environ.copy()
        env["AGENTBUS_WORKSPACE"] = str(workspace)
        env["AGENTBUS_WEBHOOK_TOKEN"] = "dogfood-token"
        result = subprocess.run(cmd, cwd=go_core_dir, env=env, capture_output=True, text=True)
        assert result.returncode == 0, result.stderr
        assert recorded, "no webhook received"
        last = recorded[-1]
        assert last["idem"] and ":" in last["idem"]
        assert last["token"] == "dogfood-token"
        assert last["auth"] == "Bearer dogfood-token"
    finally:
        srv.shutdown()
        srv.server_close()
        th.join(timeout=1)
