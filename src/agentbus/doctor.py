"""Read-only diagnostics for an AgentBus workspace and isolated runtime probes."""

from __future__ import annotations

import asyncio
import importlib.metadata
import json
import os
import platform
import shutil
import sqlite3
import struct
import sys
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import yaml
from jsonschema import Draft202012Validator

from agentbus.bin_resolve import resolve_go_binary
from agentbus.rbac import RbacConfig
from agentbus.swarm import _pid_alive, state_path
from agentbus.workspace_guard import diagnose_workspace


@dataclass
class DiagnosticCheck:
    name: str
    status: str
    message: str
    details: dict[str, Any] | None = None


@dataclass
class DoctorReport:
    workspace: str
    overall_status: str
    checks: list[DiagnosticCheck]

    def to_dict(self) -> dict[str, Any]:
        return {
            "workspace": self.workspace,
            "overall_status": self.overall_status,
            "checks": [asdict(check) for check in self.checks],
        }


def check_workspace(workspace: Path) -> DiagnosticCheck:
    if not workspace.is_dir():
        return DiagnosticCheck("workspace", "FAIL", f"workspace does not exist: {workspace}")
    ok, reason = diagnose_workspace(workspace)
    if not ok:
        return DiagnosticCheck("workspace", "FAIL", reason, {"path": str(workspace)})
    if not os.access(workspace, os.R_OK | os.W_OK | os.X_OK):
        return DiagnosticCheck("workspace", "FAIL", "workspace is not readable and writable")
    marker = workspace / ".agentbus" / "workspace"
    if marker.is_file():
        configured = marker.read_text(encoding="utf-8").strip()
        if configured and Path(configured).expanduser().resolve() != workspace.resolve():
            return DiagnosticCheck(
                "workspace", "FAIL", "workspace marker resolves to a different directory",
                {"marker": configured, "resolved": str(workspace.resolve())},
            )
    return DiagnosticCheck("workspace", "OK", "workspace resolution and filesystem are usable")


def check_database(workspace: Path) -> DiagnosticCheck:
    db = workspace / ".agentbus" / "events.db"
    if not db.is_file():
        return DiagnosticCheck("database", "WARN", "events.db is not initialized", {"path": str(db)})
    try:
        conn = sqlite3.connect(
            f"{db.resolve().as_uri()}?mode=ro&immutable=1", uri=True, timeout=5
        )
        integrity = [row[0] for row in conn.execute("PRAGMA integrity_check").fetchall()]
        header = db.read_bytes()[:20]
        journal = "WAL" if header[18:20] == b"\x02\x02" else "ROLLBACK"
        try:
            configured_busy = int(os.environ.get("AGENTBUS_SQLITE_BUSY_TIMEOUT") or "0")
        except ValueError:
            configured_busy = 0
        busy = configured_busy if configured_busy > 0 else (10000 if os.name == "nt" else 5000)
        tables = {
            row[0] for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        columns = {row[1] for row in conn.execute("PRAGMA table_info(events)").fetchall()}
        conn.close()
    except (OSError, sqlite3.Error) as exc:
        return DiagnosticCheck("database", "FAIL", f"database inspection failed: {exc}")
    required_columns = {
        "event_id", "topic", "producer_id", "timestamp", "schema_version", "payload",
        "status", "trace_id", "span_id", "parent_span_id",
    }
    missing = sorted(required_columns - columns)
    expected_journal = "ROLLBACK" if os.name == "nt" else "WAL"
    details = {
        "integrity": integrity,
        "journal_mode": journal,
        "busy_timeout_ms": busy,
        "tables": sorted(tables),
        "missing_event_columns": missing,
    }
    if integrity != ["ok"] or "events" not in tables or missing:
        return DiagnosticCheck("database", "FAIL", "integrity or schema/migration check failed", details)
    if journal != expected_journal or busy < (10000 if os.name == "nt" else 5000):
        return DiagnosticCheck("database", "WARN", "SQLite runtime settings differ from defaults", details)
    return DiagnosticCheck("database", "OK", "database integrity, schema, journal and timeout verified", details)


def check_rbac(workspace: Path) -> DiagnosticCheck:
    path = workspace / ".agentbus" / "roles.yaml"
    if not path.is_file():
        return DiagnosticCheck("rbac", "WARN", "roles.yaml is absent; no producer role map is active")
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise ValueError("root must be a mapping")
        for key in ("roles", "producers", "token_roles"):
            if key in raw and not isinstance(raw[key], dict):
                raise ValueError(f"{key} must be a mapping")
        config = RbacConfig.from_dict(raw)
        unknown = sorted(
            {role for role in [*config.producers.values(), *config.token_roles.values()] if role not in config.roles}
        )
        for name, role in config.roles.items():
            if not name or not all(isinstance(topic, str) for topic in role.can_publish_topics):
                raise ValueError(f"role {name!r} has invalid publish topics")
        if unknown:
            raise ValueError(f"producer/token mappings reference unknown roles: {', '.join(unknown)}")
    except (OSError, yaml.YAMLError, TypeError, ValueError) as exc:
        return DiagnosticCheck("rbac", "FAIL", f"RBAC configuration invalid: {exc}")
    return DiagnosticCheck(
        "rbac", "OK", "RBAC syntax and role references verified",
        {"roles": sorted(config.roles), "producers": sorted(config.producers)},
    )


def check_schema_registry(workspace: Path) -> DiagnosticCheck:
    db = workspace / ".agentbus" / "events.db"
    if not db.is_file():
        return DiagnosticCheck("schema_registry", "WARN", "schema registry is not initialized")
    try:
        conn = sqlite3.connect(
            f"{db.resolve().as_uri()}?mode=ro&immutable=1", uri=True, timeout=5
        )
        present = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='topic_schemas'"
        ).fetchone()
        if not present:
            conn.close()
            return DiagnosticCheck("schema_registry", "WARN", "topic_schemas table is absent")
        rows = conn.execute("SELECT topic_name, json_schema FROM topic_schemas").fetchall()
        conn.close()
        for topic, raw_schema in rows:
            Draft202012Validator.check_schema(json.loads(raw_schema))
            if not isinstance(topic, str) or not topic:
                raise ValueError("empty topic name")
    except Exception as exc:
        return DiagnosticCheck("schema_registry", "FAIL", f"schema registry invalid: {exc}")
    return DiagnosticCheck("schema_registry", "OK", f"validated {len(rows)} registered schema(s)")


def _binary_arch(path: Path) -> str | None:
    data = path.read_bytes()[:64]
    if data[:4] == b"\x7fELF" and len(data) >= 20:
        endian = "<" if data[5] == 1 else ">"
        return {62: "x86_64", 183: "arm64"}.get(struct.unpack(endian + "H", data[18:20])[0])
    if data[:2] == b"MZ" and len(data) >= 64:
        pe_offset = struct.unpack("<I", data[60:64])[0]
        with path.open("rb") as stream:
            stream.seek(pe_offset + 4)
            return {0x8664: "x86_64", 0xAA64: "arm64"}.get(struct.unpack("<H", stream.read(2))[0])
    magic = data[:4]
    if magic in {b"\xcf\xfa\xed\xfe", b"\xfe\xed\xfa\xcf"} and len(data) >= 8:
        endian = "<" if magic == b"\xcf\xfa\xed\xfe" else ">"
        return {0x01000007: "x86_64", 0x0100000C: "arm64"}.get(struct.unpack(endian + "I", data[4:8])[0])
    return None


def check_go_binaries() -> DiagnosticCheck:
    expected = platform.machine().lower()
    expected = "x86_64" if expected in {"amd64", "x86_64"} else "arm64" if expected in {"aarch64", "arm64"} else expected
    details: dict[str, Any] = {}
    missing: list[str] = []
    failures: list[str] = []
    unknown_arch: list[str] = []
    for name, env_var in (("agentbus-go-worker", "AGENTBUS_GO_WORKER"), ("agentbus-go-serve", "AGENTBUS_GO_SERVE")):
        try:
            path = resolve_go_binary(name, env_var=env_var)
            arch = _binary_arch(path)
            details[name] = {"path": str(path), "architecture": arch, "executable": os.access(path, os.X_OK)}
            if not os.access(path, os.X_OK) or (arch is not None and arch != expected):
                failures.append(name)
            elif arch is None:
                unknown_arch.append(name)
        except (FileNotFoundError, OSError) as exc:
            missing.append(f"{name}: {exc}")
    if failures:
        return DiagnosticCheck("go_binaries", "FAIL", f"unusable or wrong-architecture binaries: {', '.join(failures)}", details)
    if missing:
        return DiagnosticCheck("go_binaries", "WARN", "optional Go helpers not found", {**details, "missing": missing})
    if unknown_arch:
        return DiagnosticCheck(
            "go_binaries", "WARN",
            f"could not identify binary architecture: {', '.join(unknown_arch)}",
            details,
        )
    return DiagnosticCheck("go_binaries", "OK", "Go helpers are executable and match the host architecture", details)


def check_process_state(workspace: Path) -> DiagnosticCheck:
    path = state_path(workspace)
    if not path.is_file():
        return DiagnosticCheck("process_state", "OK", "no managed process state recorded")
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        services = data.get("services", {})
        if not isinstance(services, dict):
            raise ValueError("services must be a mapping")
        stale = sorted(
            name for name, record in services.items()
            if not isinstance(record, dict) or not _pid_alive(int(record.get("pid") or 0))
        )
    except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
        return DiagnosticCheck("process_state", "FAIL", f"process metadata invalid: {exc}")
    if stale:
        return DiagnosticCheck("process_state", "WARN", f"stale process records: {', '.join(stale)}", {"state": str(path)})
    return DiagnosticCheck("process_state", "OK", f"verified {len(services)} live process record(s)")


def check_disk(workspace: Path) -> DiagnosticCheck:
    try:
        usage = shutil.disk_usage(workspace)
        free_mb = usage.free // (1024 * 1024)
    except OSError as exc:
        return DiagnosticCheck("disk", "FAIL", f"disk inspection failed: {exc}")
    status = "FAIL" if free_mb < 200 else "WARN" if free_mb < 1000 else "OK"
    return DiagnosticCheck("disk", status, f"{free_mb} MiB free", {"free_bytes": usage.free})


def check_isolated_publish_poll() -> DiagnosticCheck:
    from agentbus.store import EventStore

    try:
        with tempfile.TemporaryDirectory(prefix="agentbus-doctor-") as directory:
            store = EventStore(Path(directory))
            event, duplicate = store.publish(
                topic="okf/handoff", producer_id="doctor", schema_version="1.0",
                payload={"from": "doctor", "to": "doctor", "summary": "isolated diagnostic"},
                skip_rbac=True,
            )
            result = store.poll("okf/handoff")
            store.close()
            if duplicate or [item["event_id"] for item in result["events"]] != [event.event_id]:
                raise RuntimeError("round trip returned unexpected events")
    except Exception as exc:
        return DiagnosticCheck("isolated_publish_poll", "FAIL", f"isolated round trip failed: {exc}")
    return DiagnosticCheck("isolated_publish_poll", "OK", "isolated publish/poll round trip passed")


async def _mcp_probe() -> tuple[int, str]:
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client

    with tempfile.TemporaryDirectory(prefix="agentbus-mcp-doctor-") as directory:
        env = os.environ.copy()
        env["AGENTBUS_PRODUCER_ID"] = "doctor"
        params = StdioServerParameters(
            command=sys.executable,
            args=["-m", "agentbus.cli", "--quiet", "serve", "--workspace", directory],
            env=env,
        )
        async with asyncio.timeout(10):
            async with stdio_client(params) as (read, write):
                async with ClientSession(read, write) as session:
                    await session.initialize()
                    tools = await session.list_tools()
                    names = {tool.name for tool in tools.tools}
                    required = {"agentbus_publish", "agentbus_poll", "agentbus_status"}
                    if not required <= names:
                        raise RuntimeError(f"missing tools: {sorted(required - names)}")
                    return len(names), "protocol initialization and tools/list succeeded"


def check_mcp_stdio() -> DiagnosticCheck:
    try:
        count, message = asyncio.run(_mcp_probe())
    except Exception as exc:
        return DiagnosticCheck(
            "mcp_stdio", "FAIL",
            f"MCP stdio probe failed ({type(exc).__name__}): {exc}",
        )
    return DiagnosticCheck("mcp_stdio", "OK", message, {"tool_count": count})


def check_versions() -> DiagnosticCheck:
    packages = {}
    for name in ("okf-agentbus", "mcp", "click", "jsonschema"):
        try:
            packages[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            packages[name] = "not-installed"
    return DiagnosticCheck(
        "versions", "OK", "runtime versions collected (environment values are not emitted)",
        {"python": platform.python_version(), "platform": platform.platform(), "packages": packages},
    )


def run_doctor(workspace: Path) -> DoctorReport:
    checks = [
        check_workspace(workspace), check_database(workspace), check_rbac(workspace),
        check_schema_registry(workspace), check_go_binaries(), check_process_state(workspace),
        check_disk(workspace), check_isolated_publish_poll(), check_mcp_stdio(), check_versions(),
    ]
    overall = "FAIL" if any(c.status == "FAIL" for c in checks) else "WARN" if any(c.status == "WARN" for c in checks) else "OK"
    return DoctorReport(str(workspace.resolve()), overall, checks)
