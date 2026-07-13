import threading
import json
import time
import subprocess
import os
from pathlib import Path
from http.server import HTTPServer, BaseHTTPRequestHandler

import pytest
import yaml

from agentbus.store import EventStore


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
                "from": ["*"],
                "to": ["*"]
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
    payload = {"from": "grok", "to": "agy", "summary": "test webhook payload"}
    store.publish(
        topic="okf/handoff",
        producer_id="grok",
        schema_version="1.0",
        payload=payload,
    )
    store.close()
    
    # Build and run worker with `go run` or compiled binary
    go_core_dir = Path(__file__).parent.parent / "go-core"
    
    cmd = ["go", "run", "./cmd/worker", "once"]
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
