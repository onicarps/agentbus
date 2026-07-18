"""MCP stdio round-trip — proves client capability at protocol level."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

ROOT = Path(__file__).resolve().parents[1]
VENV_PYTHON = ROOT / ".venv" / "bin" / "python"
@pytest.fixture
def server_params(tmp_path):
    ws = tmp_path / "ws"
    ws.mkdir()
    agentbus_bin = ROOT / ".venv" / "bin" / "agentbus"
    if not agentbus_bin.exists():
        pytest.skip("agentbus not installed — run pip install -e '.[dev]'")
    params = StdioServerParameters(
        command=str(agentbus_bin),
        args=["serve", "--workspace", str(ws)],
        env={
            "AGENTBUS_PRODUCER_ID": "pytest",
        },
    )
    return ws, params


@pytest.mark.asyncio
async def test_mcp_publish_poll_roundtrip(server_params):
    ws, params = server_params
    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()

            tools = await session.list_tools()
            names = {t.name for t in tools.tools}
            assert "agentbus_publish" in names
            assert "agentbus_poll" in names
            assert "agentbus_status" in names
            assert "agentbus_lock_acquire" in names
            assert "agentbus_lock_release" in names
            assert "agentbus_lock_renew" in names
            assert "agentbus_lock_status" in names

            payload = {
                "from": "grok",
                "to": "agy",
                "summary": "MCP stdio round-trip test",
                "initiative": "agentbus",
            }
            pub = await session.call_tool(
                "agentbus_publish",
                {
                    "topic": "okf/handoff",
                    "payload": payload,
                    "schema_version": "1.0",
                },
            )
            pub_data = json.loads(pub.content[0].text)
            assert pub_data["event_id"] == 1
            assert pub_data["duplicate"] is False

            poll = await session.call_tool(
                "agentbus_poll",
                {"topic": "okf/handoff", "since_id": 0},
            )
            poll_data = json.loads(poll.content[0].text)
            assert len(poll_data["events"]) == 1
            assert poll_data["events"][0]["payload"]["summary"] == payload["summary"]

            status = await session.call_tool("agentbus_status", {})
            status_data = json.loads(status.content[0].text)
            assert status_data["event_count"] == 1
            assert "okf/handoff" in status_data["topics"]

            resource = str(ws / "shared.md")
            lock = await session.call_tool(
                "agentbus_lock_acquire",
                {"resource": resource, "owner_id": "pytest"},
            )
            lock_data = json.loads(lock.content[0].text)
            assert lock_data["acquired"] is True
            lease_id = lock_data["lease_id"]

            lock_status = await session.call_tool(
                "agentbus_lock_status",
                {"resource": resource},
            )
            assert json.loads(lock_status.content[0].text)["locked"] is True

            release = await session.call_tool(
                "agentbus_lock_release",
                {
                    "resource": resource,
                    "lease_id": lease_id,
                    "owner_id": "pytest",
                },
            )
            assert json.loads(release.content[0].text)["released"] is True