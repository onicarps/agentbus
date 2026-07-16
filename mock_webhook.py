"""Local webhook sink for agentbus wake_mode=webhook dogfooding.

Listens on localhost:8000 and appends each POST body to webhook_received.jsonl
plus the latest payload to webhook_received.json.
"""

from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parent
LATEST = ROOT / "webhook_received.json"
LOG = ROOT / "webhook_received.jsonl"
HOST, PORT = "127.0.0.1", 8000


class WebhookHandler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:
        n = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(n)
        text = body.decode("utf-8", errors="replace")
        LATEST.write_text(text + "\n", encoding="utf-8")
        with LOG.open("a", encoding="utf-8") as f:
            f.write(text.rstrip("\n") + "\n")
        try:
            data = json.loads(text)
            print(
                f"wake event_id={data.get('event_id')} "
                f"topic={data.get('topic')} "
                f"to={data.get('payload', {}).get('to')}",
                flush=True,
            )
        except json.JSONDecodeError:
            print(f"wake raw_bytes={len(body)}", flush=True)
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b"OK")

    def log_message(self, fmt: str, *args) -> None:
        # Keep stdout for wake lines only.
        pass


if __name__ == "__main__":
    server = HTTPServer((HOST, PORT), WebhookHandler)
    print(f"mock webhook listening on http://{HOST}:{PORT}/my-agent-webhook", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("stopped", flush=True)
