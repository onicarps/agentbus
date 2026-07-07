#!/usr/bin/env python3
"""AgentBus Phase 5 — Factory/MCP integration validation.

Tests Agy event-56 criteria via MCP stdio ONLY for lock operations.
Run: python tests/factory_validate.py
CI:  pytest tests/test_factory_mcp_validation.py -m integration
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class MCPClient:
    """MCP stdio client — no agentbus imports for lock operations."""

    def __init__(self, workspace: Path, *, token: str | None = None, label: str = "client"):
        self._proc: subprocess.Popen | None = None
        self._next_id = 0
        self._workspace = workspace
        self._token = token
        self._label = label

    def start(self) -> None:
        env = os.environ.copy()
        env.pop("AGENTBUS_TOKEN", None)
        if self._token:
            env["AGENTBUS_TOKEN"] = self._token
        self._proc = subprocess.Popen(
            [sys.executable, "-m", "agentbus.cli", "serve", "--workspace", str(self._workspace)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            bufsize=0,
        )
        init_msg = {
            "jsonrpc": "2.0",
            "id": self._next_id,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": f"factory-validator-{self._label}", "version": "0.2.2"},
            },
        }
        self._next_id += 1
        self._send(init_msg)
        resp = self._read_response()
        if not resp or "error" in resp:
            raise RuntimeError(f"[{self._label}] MCP init failed: {resp}")

    def _send(self, msg: dict) -> None:
        assert self._proc and self._proc.stdin
        self._proc.stdin.write((json.dumps(msg) + "\n").encode())
        self._proc.stdin.flush()

    def _read_response(self) -> dict | None:
        assert self._proc and self._proc.stdout
        line = self._proc.stdout.readline()
        if not line:
            return None
        return json.loads(line.decode())

    def call_tool(self, name: str, arguments: dict) -> dict:
        msg = {
            "jsonrpc": "2.0",
            "id": self._next_id,
            "method": "tools/call",
            "params": {"name": name, "arguments": arguments},
        }
        self._next_id += 1
        self._send(msg)
        resp = self._read_response()
        if resp is None:
            return {"error": "NO_RESPONSE"}
        if "error" in resp:
            return {"error": resp["error"]}
        content = resp.get("result", {}).get("content", [])
        if content and isinstance(content, list):
            text = content[0].get("text", "{}") if isinstance(content[0], dict) else "{}"
            try:
                return json.loads(text)
            except json.JSONDecodeError:
                return {"raw_text": text}
        return resp.get("result", {})

    def stop(self) -> None:
        if self._proc and self._proc.poll() is None:
            self._proc.terminate()
            try:
                self._proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._proc.kill()


class TestResult:
    def __init__(self) -> None:
        self.passed = 0
        self.failed = 0
        self.results: list[tuple[str, str, str]] = []

    def record(self, name: str, passed: bool, detail: str = "") -> None:
        status = "PASS" if passed else "FAIL"
        if passed:
            self.passed += 1
        else:
            self.failed += 1
        print(f"  {'+' if passed else '-'} [{status}] {name}" + (f" -- {detail}" if detail else ""))
        self.results.append((status, name, detail))


def _ensure_token(workspace: Path) -> str:
    subprocess.run(
        [sys.executable, "-m", "agentbus.cli", "token", "ensure", "--workspace", str(workspace), "--quiet"],
        check=True,
        capture_output=True,
    )
    return (workspace / ".agentbus" / "token").read_text().strip()


def run_validation(workspace: Path | None = None) -> TestResult:
    """Run all Agy event-56 scenarios. Uses temp workspace if none provided."""
    results = TestResult()
    tmp = None
    if workspace is None:
        tmp = tempfile.TemporaryDirectory(prefix="agentbus-factory-")
        workspace = Path(tmp.name)

    token = _ensure_token(workspace)
    resource = str(workspace / "okf" / "test-resource-phase5-validation")
    (workspace / "okf").mkdir(parents=True, exist_ok=True)

    client = MCPClient(workspace, token=token, label="primary")
    client.start()

    try:
        # Scenario 1: Droid A acquires
        r1 = client.call_tool(
            "agentbus_lock_acquire",
            {"resource": resource, "ttl_seconds": 300, "owner_id": "droid-a"},
        )
        results.record("Droid A acquires lock", r1.get("acquired") is True, f"lease_id={r1.get('lease_id')}")
        if not r1.get("acquired"):
            return results
        lease_a = r1["lease_id"]

        # Scenario 2: Droid B rejected
        r2 = client.call_tool(
            "agentbus_lock_acquire",
            {"resource": resource, "ttl_seconds": 300, "owner_id": "droid-b"},
        )
        results.record(
            "Droid B acquire rejected",
            r2.get("acquired") is False,
            f"current_owner={r2.get('current_owner')}",
        )

        # Scenario 3: Heartbeat renew
        r3 = client.call_tool(
            "agentbus_lock_renew",
            {"resource": resource, "lease_id": lease_a, "owner_id": "droid-a"},
        )
        results.record("Droid A heartbeat", r3.get("renewed") is True, f"expires_at={r3.get('expires_at')}")

        # Scenario 4: Real TTL expiry (short TTL, no release shortcut)
        client.call_tool(
            "agentbus_lock_release",
            {"resource": resource, "lease_id": lease_a, "owner_id": "droid-a"},
        )
        r4a = client.call_tool(
            "agentbus_lock_acquire",
            {"resource": resource, "ttl_seconds": 2, "owner_id": "droid-a"},
        )
        results.record("Droid A short-TTL acquire", r4a.get("acquired") is True, "ttl=2s")
        if r4a.get("acquired"):
            time.sleep(2.5)
            st = client.call_tool("agentbus_lock_status", {"resource": resource})
            results.record("TTL expired (no heartbeat)", st.get("locked") is False, json.dumps(st))
            r4b = client.call_tool(
                "agentbus_lock_acquire",
                {"resource": resource, "ttl_seconds": 300, "owner_id": "droid-b"},
            )
            results.record(
                "Droid B acquires after TTL expiry",
                r4b.get("acquired") is True,
                f"lease_id={r4b.get('lease_id')}",
            )
            if r4b.get("acquired"):
                client.call_tool(
                    "agentbus_lock_release",
                    {
                        "resource": resource,
                        "lease_id": r4b["lease_id"],
                        "owner_id": "droid-b",
                    },
                )

        # Scenario 5: Auth enforcement via MCP (wrong auth_token)
        r5 = client.call_tool(
            "agentbus_lock_acquire",
            {
                "resource": resource,
                "ttl_seconds": 300,
                "owner_id": "droid-c",
                "auth_token": "definitely-wrong-token",
            },
        )
        rejected = r5.get("acquired") is not True or "error" in r5 or "raw_text" in r5
        results.record("Auth enforcement (wrong token)", rejected, json.dumps(r5)[:120])

        # Scenario 6: Outside workspace
        r6 = client.call_tool(
            "agentbus_lock_acquire",
            {"resource": "/tmp/outside-resource", "ttl_seconds": 300, "owner_id": "droid-d"},
        )
        outside_rejected = r6.get("acquired") is not True or "raw_text" in r6
        results.record("Outside workspace rejected", outside_rejected, json.dumps(r6)[:120])

        # Scenario 7: 3+ concurrent droids hammering same resource
        hammer_resource = str(workspace / "okf" / "hammer-target")
        (workspace / "okf").mkdir(exist_ok=True)
        errors: list[str] = []
        acquired_count = {"n": 0}
        lock = threading.Lock()

        def hammer(owner: str) -> None:
            c = MCPClient(workspace, token=token, label=owner)
            try:
                c.start()
                for _ in range(5):
                    r = c.call_tool(
                        "agentbus_lock_acquire",
                        {"resource": hammer_resource, "ttl_seconds": 60, "owner_id": owner},
                    )
                    if r.get("acquired"):
                        with lock:
                            acquired_count["n"] += 1
                        c.call_tool(
                            "agentbus_lock_release",
                            {
                                "resource": hammer_resource,
                                "lease_id": r["lease_id"],
                                "owner_id": owner,
                            },
                        )
                    time.sleep(0.05)
            except Exception as exc:
                errors.append(f"{owner}:{exc}")
            finally:
                c.stop()

        threads = [threading.Thread(target=hammer, args=(f"droid-{i}",)) for i in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        results.record(
            "3+ concurrent droids hammer (no deadlock)",
            len(errors) == 0 and acquired_count["n"] >= 1,
            f"acquires={acquired_count['n']}, errors={errors or 'none'}",
        )

    finally:
        client.stop()
        if tmp:
            tmp.cleanup()

    return results


def main() -> None:
    print("=" * 60)
    print("AgentBus Phase 5 — Factory/MCP Integration Validation")
    print("=" * 60)
    ws_override = os.environ.get("AGENTBUS_VALIDATION_WORKSPACE")
    workspace = Path(ws_override) if ws_override else None
    results = run_validation(workspace)
    print(f"\nPASSED: {results.passed}  FAILED: {results.failed}")
    if results.failed:
        print("STATUS: FAILED")
        sys.exit(1)
    print("STATUS: GREEN")
    sys.exit(0)


if __name__ == "__main__":
    main()