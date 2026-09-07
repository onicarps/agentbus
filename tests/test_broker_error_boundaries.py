"""Fail-closed broker behavior for unsupported CLI and MCP operations."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner
from mcp.server.mcpserver.exceptions import ToolError

import agentbus.server as server
from agentbus.cli import main
from agentbus.client import BrokerTransport, BrokerTransportError


@pytest.mark.parametrize(
    ("method", "args", "expected"),
    [
        ("fetch_trace_events", ("trace-1",), "fetch_trace_events"),
        ("fetch_unprojected_handoffs", (), "fetch_unprojected_handoffs"),
        ("set_mcpsafe", (object(),), "set_mcpsafe"),
    ],
)
def test_broker_transport_exposes_stable_unsupported_codes(
    tmp_path: Path,
    method: str,
    args: tuple[Any, ...],
    expected: str,
) -> None:
    transport = BrokerTransport(tmp_path, tmp_path / "missing.sock")

    with pytest.raises(
        BrokerTransportError,
        match=rf"^broker_operation_unsupported:{expected}$",
    ):
        getattr(transport, method)(*args)


@pytest.mark.parametrize(
    ("args", "expected"),
    [
        (("sla", "list"), "broker_operation_unsupported:list_active_slas"),
        (("sla", "clear", "1"), "broker_operation_unsupported:clear_sla"),
        (("review",), "broker_operation_unsupported:review_pending"),
        (("trace", "trace-1"), "broker_operation_unsupported:fetch_trace_events"),
        (
            ("project-log", "--dry-run"),
            "broker_operation_unsupported:fetch_unprojected_handoffs",
        ),
    ],
)
def test_cli_unsupported_broker_operations_are_clean_errors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    args: tuple[str, ...],
    expected: str,
) -> None:
    monkeypatch.setenv("AGENTBUS_BROKER_SOCKET", str(tmp_path / "missing.sock"))
    command = list(args)
    if command[0] == "sla":
        command[1:1] = ["--workspace", str(tmp_path)]
    else:
        command.extend(("--workspace", str(tmp_path)))

    result = CliRunner().invoke(main, command)

    assert result.exit_code == 1
    assert f"Error: {expected}" in result.output
    assert "Traceback" not in result.output


def test_cli_mcpsafe_configuration_is_cleanly_refused_over_broker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lock = tmp_path / ".mcpsafe.lock"
    lock.write_text("{}", encoding="utf-8")
    monkeypatch.setenv("AGENTBUS_AUTH", "off")
    monkeypatch.setenv("AGENTBUS_BROKER_SOCKET", str(tmp_path / "missing.sock"))

    result = CliRunner().invoke(
        main,
        [
            "publish",
            "--workspace",
            str(tmp_path),
            "--topic",
            "okf/handoff",
            "--payload",
            json.dumps({"from": "codex", "to": "agy", "summary": "candidate"}),
            "--producer-id",
            "codex",
            "--enable-mcpsafe",
            "--mcpsafe-lock",
            str(lock),
        ],
    )

    assert result.exit_code == 1
    assert "Error: broker_operation_unsupported:set_mcpsafe" in result.output
    assert "Traceback" not in result.output


@pytest.mark.parametrize(
    ("tool", "kwargs", "expected"),
    [
        ("agentbus_review", {}, "broker_operation_unsupported:review_pending"),
        (
            "agentbus_lock_acquire",
            {"resource": "/tmp/a", "owner_id": "codex"},
            "broker_lease_operations_unsupported",
        ),
        (
            "agentbus_lock_release",
            {"resource": "/tmp/a", "lease_id": "lease-1", "owner_id": "codex"},
            "broker_lease_operations_unsupported",
        ),
        (
            "agentbus_lock_renew",
            {"resource": "/tmp/a", "lease_id": "lease-1", "owner_id": "codex"},
            "broker_lease_operations_unsupported",
        ),
        (
            "agentbus_lock_status",
            {"resource": "/tmp/a"},
            "broker_lease_operations_unsupported",
        ),
    ],
)
def test_mcp_unsupported_broker_operations_raise_tool_errors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    tool: str,
    kwargs: dict[str, Any],
    expected: str,
) -> None:
    transport = BrokerTransport(tmp_path, tmp_path / "missing.sock")
    monkeypatch.setattr(server, "_store", transport)
    monkeypatch.setattr(server, "_lease_store", None)
    monkeypatch.setattr(server, "_workspace", tmp_path)

    with pytest.raises(ToolError, match=rf"^{expected}$"):
        getattr(server, tool)(**kwargs)
