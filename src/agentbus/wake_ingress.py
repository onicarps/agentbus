"""Mode A wake ingress — localhost HTTP queue for Hermes/Factory (v0.13).

POST /agentbus/wake → dedupe → append JSONL queue. No LLM in the hot path.
Spec: initiatives/agentbus/decisions/webhook-bridge-hermes-factory-spec-2026-07-16.md
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from agentbus.workspace_guard import assert_workspace_supported

log = logging.getLogger("agentbus.wake_ingress")

MAX_BODY = 256 * 1024
PATH_WAKE = "/agentbus/wake"
PATH_HEALTH = "/agentbus/wake/health"

DEFAULT_PORTS = {
    "hermes": 18787,
    "factory": 18788,
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class IngressStore:
    """Dedupe seen event_ids + append-only queue JSONL."""

    def __init__(self, workspace: Path, runtime: str) -> None:
        self.workspace = workspace
        self.runtime = runtime
        self.dir = workspace / ".agentbus" / "ingress"
        self.dir.mkdir(parents=True, exist_ok=True)
        self.queue_path = self.dir / f"{runtime}_wake_queue.jsonl"
        self.db_path = self.dir / f"{runtime}_seen.db"
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS seen ("
            "event_id INTEGER PRIMARY KEY, received_at TEXT NOT NULL)"
        )
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def seen(self, event_id: int) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM seen WHERE event_id=?", (event_id,)
            ).fetchone()
            return row is not None

    def mark_and_enqueue(self, event_id: int, record: dict[str, Any]) -> bool:
        """Return True if newly enqueued, False if deduped."""
        with self._lock:
            if self._conn.execute(
                "SELECT 1 FROM seen WHERE event_id=?", (event_id,)
            ).fetchone():
                return False
            self._conn.execute(
                "INSERT INTO seen(event_id, received_at) VALUES(?, ?)",
                (event_id, _utc_now()),
            )
            self._conn.commit()
            with self.queue_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(record, ensure_ascii=False) + "\n")
            return True

    def queue_depth(self) -> int:
        if not self.queue_path.is_file():
            return 0
        n = 0
        with self.queue_path.open(encoding="utf-8") as fh:
            for line in fh:
                if line.strip():
                    n += 1
        return n


def _client_is_loopback(addr: str | None) -> bool:
    if not addr:
        return False
    return addr in {"127.0.0.1", "::1", "localhost"} or addr.startswith("127.")


class WakeIngressHandler(BaseHTTPRequestHandler):
    server: "WakeIngressServer"  # type: ignore[assignment]

    def log_message(self, fmt: str, *args: Any) -> None:
        log.info("%s - %s", self.address_string(), fmt % args)

    def _json(self, code: int, body: dict[str, Any]) -> None:
        raw = json.dumps(body).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        if path == PATH_HEALTH:
            store = self.server.store
            self._json(
                200,
                {
                    "ok": True,
                    "runtime": self.server.runtime,
                    "workspace": str(self.server.workspace),
                    "queue_depth": store.queue_depth(),
                    "token_required": bool(self.server.token),
                },
            )
            return
        self._json(404, {"ok": False, "error": "not_found"})

    def do_POST(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        if path != PATH_WAKE:
            self._json(404, {"ok": False, "error": "not_found"})
            return

        client = self.client_address[0] if self.client_address else ""
        if not _client_is_loopback(client):
            self._json(403, {"ok": False, "error": "loopback_only"})
            return

        if self.server.token:
            got = self.headers.get("X-AgentBus-Token") or ""
            auth = self.headers.get("Authorization") or ""
            if auth.lower().startswith("bearer "):
                got = auth[7:].strip() or got
            if got != self.server.token:
                self._json(401, {"ok": False, "error": "unauthorized"})
                return

        length = int(self.headers.get("Content-Length") or "0")
        if length <= 0 or length > MAX_BODY:
            self._json(413 if length > MAX_BODY else 400, {"ok": False, "error": "bad_body"})
            return
        raw = self.rfile.read(length)
        try:
            envelope = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            self._json(400, {"ok": False, "error": "invalid_json"})
            return

        event_id = envelope.get("event_id")
        if not isinstance(event_id, int) or event_id < 1:
            # header fallback
            try:
                event_id = int(self.headers.get("X-AgentBus-Event-Id") or "0")
            except ValueError:
                event_id = 0
        if event_id < 1:
            self._json(400, {"ok": False, "error": "missing_event_id"})
            return

        payload = envelope.get("payload") if isinstance(envelope.get("payload"), dict) else {}
        record = {
            "received_at": _utc_now(),
            "event_id": event_id,
            "runtime": self.server.runtime,
            "from": payload.get("from"),
            "to": payload.get("to"),
            "summary": payload.get("summary"),
            "links": payload.get("links") or [],
            "worker_id": envelope.get("worker_id"),
            "topic": envelope.get("topic"),
            "raw": envelope,
        }
        new = self.server.store.mark_and_enqueue(event_id, record)
        self._json(
            200,
            {"ok": True, "event_id": event_id, "deduped": not new},
        )


class WakeIngressServer(ThreadingHTTPServer):
    def __init__(
        self,
        host: str,
        port: int,
        *,
        workspace: Path,
        runtime: str,
        token: str | None,
    ) -> None:
        super().__init__((host, port), WakeIngressHandler)
        self.workspace = workspace
        self.runtime = runtime
        self.token = token or ""
        self.store = IngressStore(workspace, runtime)


def run_ingress(
    workspace: Path,
    *,
    runtime: str,
    host: str = "127.0.0.1",
    port: int | None = None,
    token: str | None = None,
) -> None:
    runtime = runtime.strip().lower()
    if runtime not in DEFAULT_PORTS and port is None:
        raise ValueError(f"unknown runtime {runtime!r}; pass --port")
    workspace = assert_workspace_supported(workspace)
    port = port or DEFAULT_PORTS.get(runtime) or 18787
    token = token if token is not None else os.environ.get("AGENTBUS_WEBHOOK_TOKEN", "")

    if host not in {"127.0.0.1", "::1", "localhost"}:
        log.warning(
            "wake-ingress binding %s — prefer 127.0.0.1 (WEBHOOK_SPEC_GO)",
            host,
        )
    if not token:
        log.warning(
            "WARNING: wake-ingress runtime=%s has NO shared token "
            "(localhost dogfood only). Set AGENTBUS_WEBHOOK_TOKEN or --token "
            "for multi-user hosts.",
            runtime,
        )

    server = WakeIngressServer(
        host, port, workspace=workspace, runtime=runtime, token=token or None
    )
    log.info(
        "wake-ingress listening http://%s:%s%s runtime=%s workspace=%s queue=%s",
        host,
        port,
        PATH_WAKE,
        runtime,
        workspace,
        server.store.queue_path,
    )
    try:
        server.serve_forever()
    finally:
        server.store.close()
        server.server_close()
