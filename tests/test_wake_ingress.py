"""Mode A wake-ingress unit tests."""

from __future__ import annotations

import json
import threading
import urllib.error
import urllib.request
from http.client import HTTPConnection

import pytest

from agentbus.wake_ingress import WakeIngressServer


@pytest.fixture
def ingress(tmp_path):
    server = WakeIngressServer(
        "127.0.0.1",
        0,
        workspace=tmp_path,
        runtime="hermes",
        token=None,
    )
    port = server.server_address[1]
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    yield server, port
    server.shutdown()
    server.store.close()
    server.server_close()
    t.join(timeout=2)


def _post(port: int, body: dict, headers: dict | None = None, path: str = "/agentbus/wake"):
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}",
        data=data,
        method="POST",
        headers={"Content-Type": "application/json", **(headers or {})},
    )
    with urllib.request.urlopen(req, timeout=3) as resp:
        return resp.status, json.loads(resp.read().decode())


def test_enqueue_and_dedupe(ingress, tmp_path):
    server, port = ingress
    env = {
        "schema_version": "1.0",
        "worker_id": "hermes-1",
        "event_id": 42,
        "topic": "okf/handoff",
        "payload": {"from": "agy", "to": "hermes", "summary": "do ops"},
        "hint": {"ack_with_causation_id": "42"},
    }
    st, body = _post(port, env)
    assert st == 200
    assert body["ok"] is True
    assert body["deduped"] is False
    assert body["event_id"] == 42

    st2, body2 = _post(port, env)
    assert body2["deduped"] is True

    q = tmp_path / ".agentbus" / "ingress" / "hermes_wake_queue.jsonl"
    lines = [ln for ln in q.read_text().splitlines() if ln.strip()]
    assert len(lines) == 1
    rec = json.loads(lines[0])
    assert rec["event_id"] == 42
    assert rec["summary"] == "do ops"


def test_token_required(tmp_path):
    server = WakeIngressServer(
        "127.0.0.1",
        0,
        workspace=tmp_path,
        runtime="factory",
        token="secret",
    )
    port = server.server_address[1]
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        env = {"event_id": 1, "payload": {"from": "a", "to": "b", "summary": "x"}}
        with pytest.raises(urllib.error.HTTPError) as ei:
            _post(port, env)
        assert ei.value.code == 401
        st, body = _post(port, env, headers={"X-AgentBus-Token": "secret"})
        assert st == 200 and body["ok"]
    finally:
        server.shutdown()
        server.store.close()
        server.server_close()
        t.join(timeout=2)


def test_health(ingress):
    server, port = ingress
    conn = HTTPConnection("127.0.0.1", port, timeout=3)
    conn.request("GET", "/agentbus/wake/health")
    resp = conn.getresponse()
    data = json.loads(resp.read().decode())
    assert resp.status == 200
    assert data["ok"] is True
    assert data["runtime"] == "hermes"
    conn.close()
