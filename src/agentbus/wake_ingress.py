"""Mode A wake ingress — localhost HTTP queue for Hermes/Factory (v0.13).

POST /agentbus/wake → dedupe → append JSONL queue. No LLM in the hot path.
Spec: initiatives/agentbus/decisions/webhook-bridge-hermes-factory-spec-2026-07-16.md
"""

from __future__ import annotations

import json
import logging
import os
import secrets
import sqlite3
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlparse, urlsplit

from agentbus.workspace_guard import assert_workspace_supported
from agentbus.identity import (
    configured as identity_configured,
    load_trust_state,
    verify_wake_capability,
)
from agentbus.store import EventStore

log = logging.getLogger("agentbus.wake_ingress")

MAX_BODY = 256 * 1024
PATH_WAKE = "/agentbus/wake"
PATH_HEALTH = "/agentbus/wake/health"
LOOPBACK_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})

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


def _header_hostname(value: str | None) -> str | None:
    if not value:
        return None
    try:
        hostname = urlsplit(f"//{value}").hostname
    except ValueError:
        return None
    return hostname.rstrip(".").lower() if hostname else None


def _normalized_origin(value: str) -> tuple[str, str, int] | None:
    try:
        parsed = urlsplit(value)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            return None
        if parsed.username or parsed.password or parsed.path not in {"", "/"}:
            return None
        if parsed.query or parsed.fragment:
            return None
        hostname = parsed.hostname.rstrip(".").lower()
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        return parsed.scheme, hostname, port
    except ValueError:
        return None


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
                    "token_required": bool(self.server.token) or not self.server.dev,
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

        if _header_hostname(self.headers.get("Host")) not in LOOPBACK_HOSTS:
            self._json(403, {"ok": False, "error": "invalid_host"})
            return
        origin = self.headers.get("Origin")
        if origin is not None and (
            _normalized_origin(origin) not in self.server.allowed_origins
        ):
            self._json(403, {"ok": False, "error": "invalid_origin"})
            return
        if (self.headers.get("Sec-Fetch-Site") or "").strip().lower() == "cross-site":
            self._json(403, {"ok": False, "error": "cross_site_request"})
            return
        content_type = (self.headers.get("Content-Type") or "").split(";", 1)[
            0
        ].strip().lower()
        if content_type != "application/json":
            self._json(415, {"ok": False, "error": "json_content_type_required"})
            return

        if self.server.protected and not self.server.token:
            self._json(503, {"ok": False, "error": "runtime_capability_required"})
            return
        if not self.server.token and not self.server.dev:
            self._json(503, {"ok": False, "error": "runtime_capability_required"})
            return
        if self.server.token:
            got = self.headers.get("X-AgentBus-Token") or ""
            auth = self.headers.get("Authorization") or ""
            if auth.lower().startswith("bearer "):
                got = auth[7:].strip() or got
            if not secrets.compare_digest(got, self.server.token):
                self._json(401, {"ok": False, "error": "unauthorized"})
                return

        length = int(self.headers.get("Content-Length") or "0")
        if length <= 0 or length > MAX_BODY:
            self._json(
                413 if length > MAX_BODY else 400, {"ok": False, "error": "bad_body"}
            )
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

        if self.server.protected:
            event, verification = self.server.event_store.get_verified_event(event_id)
            if event is None:
                self._json(404, {"ok": False, "error": "event_not_found"})
                return
            if not verification.verified:
                self._json(
                    403,
                    {
                        "ok": False,
                        "error": "event_unverified",
                        "reason": verification.reason,
                    },
                )
                return
            envelope = event.to_dict()

        payload = (
            envelope.get("payload") if isinstance(envelope.get("payload"), dict) else {}
        )
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
        dev: bool = False,
        allowed_origins: Iterable[str] | None = None,
    ) -> None:
        super().__init__((host, port), WakeIngressHandler)
        self.workspace = workspace
        self.runtime = runtime
        self.token = token or ""
        self.dev = dev
        self.store = IngressStore(workspace, runtime)
        configured_origins = (
            list(allowed_origins)
            if allowed_origins is not None
            else [
                item.strip()
                for item in os.environ.get("AGENTBUS_WAKE_ALLOWED_ORIGINS", "").split(",")
                if item.strip()
            ]
        )
        default_origins = {
            ("http", hostname, self.server_port)
            for hostname in LOOPBACK_HOSTS
        }
        self.allowed_origins = default_origins | {
            normalized
            for origin in configured_origins
            if (normalized := _normalized_origin(origin)) is not None
        }
        state = load_trust_state(workspace) if identity_configured(workspace) else None
        self.protected = state is not None and state.mode in {"protected", "strict"}
        if self.protected and (
            state is None or not verify_wake_capability(state, runtime, self.token)
        ):
            self.store.close()
            super().server_close()
            raise ValueError(
                f"invalid or unregistered runtime capability for {runtime!r}"
            )
        self.event_store = EventStore(workspace, auto_prune=False)

    def server_close(self) -> None:
        self.event_store.close()
        super().server_close()


def run_ingress(
    workspace: Path,
    *,
    runtime: str,
    host: str = "127.0.0.1",
    port: int | None = None,
    token: str | None = None,
    dev: bool = False,
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
    state = load_trust_state(workspace) if identity_configured(workspace) else None
    protected = state is not None and state.mode in {"protected", "strict"}
    if protected and not token:
        raise ValueError(
            "runtime capability required in protected/strict identity mode; "
            "pass --token or AGENTBUS_WEBHOOK_TOKEN"
        )
    if not token and not dev:
        raise ValueError(
            "wake-ingress token required; pass --token or use --dev for local dogfood"
        )
    if not token:
        log.warning(
            "WARNING: wake-ingress runtime=%s has NO shared token "
            "(localhost dogfood only). Set AGENTBUS_WEBHOOK_TOKEN or --token "
            "for multi-user hosts.",
            runtime,
        )

    server = WakeIngressServer(
        host,
        port,
        workspace=workspace,
        runtime=runtime,
        token=token or None,
        dev=dev,
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
