"""Swarm orchestration — agentbus up/down/ps/logs (v0.10.0).

Reads ``.agentbus/swarm.yaml`` (Compose-style), spawns background services with
cross-OS process groups so Ctrl+C / ``agentbus down`` does not orphan agents.
"""

from __future__ import annotations

import json
import os
import shlex
import signal
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, IO

import yaml

SWARM_FILENAME = "swarm.yaml"
STATE_FILENAME = "swarm.state.json"
LOG_DIRNAME = "logs"

# Windows creation flag (avoid importing subprocess.CREATE_* on POSIX type-checkers)
_CREATE_NEW_PROCESS_GROUP = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0x00000200)
_IS_WINDOWS = sys.platform == "win32"


@dataclass
class ServiceSpec:
    name: str
    command: str
    env: dict[str, str] = field(default_factory=dict)
    cwd: str | None = None
    enabled: bool = True


@dataclass
class SwarmConfig:
    version: str
    services: dict[str, ServiceSpec]
    path: Path


def swarm_dir(workspace: Path) -> Path:
    return workspace.resolve() / ".agentbus"


def swarm_yaml_path(workspace: Path) -> Path:
    return swarm_dir(workspace) / SWARM_FILENAME


def state_path(workspace: Path) -> Path:
    return swarm_dir(workspace) / STATE_FILENAME


def logs_dir(workspace: Path) -> Path:
    return swarm_dir(workspace) / LOG_DIRNAME


def load_swarm_config(workspace: Path) -> SwarmConfig:
    path = swarm_yaml_path(workspace)
    if not path.is_file():
        raise FileNotFoundError(
            f"missing {path} — create a swarm.yaml (see docs / examples/swarm.yaml)"
        )
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        raise ValueError("swarm.yaml root must be a mapping")
    version = str(raw.get("version") or "1.0")
    services_raw = raw.get("services") or {}
    if not isinstance(services_raw, dict) or not services_raw:
        raise ValueError("swarm.yaml must define a non-empty 'services' mapping")
    services: dict[str, ServiceSpec] = {}
    for name, defn in services_raw.items():
        if not isinstance(name, str) or not name.strip():
            raise ValueError(f"invalid service name: {name!r}")
        # Prevent path traversal into log/state paths
        if any(ch in name for ch in ("/", "\\", "..")) or name in {".", ".."}:
            raise ValueError(f"invalid service name (path-unsafe): {name!r}")
        if isinstance(defn, str):
            command = defn
            env: dict[str, str] = {}
            cwd = None
            enabled = True
        elif isinstance(defn, dict):
            # enabled: false → defined but not started by `agentbus up` (v0.15 Phase F)
            enabled = bool(defn.get("enabled", True))
            command = defn.get("command")
            if not command or not isinstance(command, str):
                raise ValueError(f"service '{name}' requires string 'command'")
            env = {str(k): str(v) for k, v in (defn.get("env") or {}).items()}
            cwd = defn.get("cwd")
            if cwd is not None:
                cwd = str(cwd)
        else:
            raise ValueError(f"service '{name}' must be a string command or mapping")
        services[name] = ServiceSpec(
            name=name, command=command, env=env, cwd=cwd, enabled=enabled
        )
    if not any(s.enabled for s in services.values()):
        raise ValueError(
            "swarm.yaml has no enabled services "
            "(all entries set enabled: false — flip at least one to true)"
        )
    return SwarmConfig(version=version, services=services, path=path)


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _read_state(workspace: Path) -> dict[str, Any]:
    path = state_path(workspace)
    if not path.is_file():
        return {"workspace": str(workspace.resolve()), "services": {}, "updated_at": None}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {"workspace": str(workspace.resolve()), "services": {}, "updated_at": None}
    if not isinstance(data, dict):
        return {"workspace": str(workspace.resolve()), "services": {}, "updated_at": None}
    data.setdefault("services", {})
    return data


def _write_state(workspace: Path, state: dict[str, Any]) -> None:
    path = state_path(workspace)
    path.parent.mkdir(parents=True, exist_ok=True)
    state["workspace"] = str(workspace.resolve())
    state["updated_at"] = _now_iso()
    path.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    if not _IS_WINDOWS:
        # Prefer /proc when present (Linux) so zombies (state Z) count as dead.
        # Fall back to os.kill(0) on macOS/BSD where /proc is unavailable.
        stat_path = Path(f"/proc/{pid}/stat")
        if stat_path.is_file():
            try:
                raw = stat_path.read_text(encoding="utf-8", errors="replace")
                rparen = raw.rfind(")")
                if rparen != -1 and len(raw) > rparen + 2:
                    state = raw[rparen + 2]
                    if state in {"Z", "X"}:  # zombie / dead
                        return False
                return True
            except OSError:
                pass
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _parse_command(command: str) -> list[str]:
    if _IS_WINDOWS:
        # posix=False keeps outer quotes on tokens — strip them for Popen
        tokens = shlex.split(command, posix=False)
        cleaned: list[str] = []
        for tok in tokens:
            if len(tok) >= 2 and tok[0] == tok[-1] and tok[0] in {'"', "'"}:
                cleaned.append(tok[1:-1])
            else:
                cleaned.append(tok)
        return cleaned
    return shlex.split(command)


def _popen_kwargs() -> dict[str, Any]:
    kwargs: dict[str, Any] = {}
    if _IS_WINDOWS:
        kwargs["creationflags"] = _CREATE_NEW_PROCESS_GROUP
    else:
        # New process group so killpg can wipe the tree
        kwargs["start_new_session"] = True
    return kwargs


def _service_log_paths(workspace: Path, name: str) -> tuple[Path, Path]:
    d = logs_dir(workspace)
    d.mkdir(parents=True, exist_ok=True)
    return d / f"{name}.stdout.log", d / f"{name}.stderr.log"


def start_service(
    workspace: Path,
    spec: ServiceSpec,
    *,
    extra_env: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Spawn one service; return state record."""
    argv = _parse_command(spec.command)
    if not argv:
        raise ValueError(f"empty command for service '{spec.name}'")

    env = os.environ.copy()
    env["AGENTBUS_WORKSPACE"] = str(workspace.resolve())
    env.update(spec.env)
    if extra_env:
        env.update(extra_env)

    # Automatically prepend the current venv's bin directory to PATH
    import sys
    venv_bin = os.path.join(sys.prefix, "bin")
    if os.path.isdir(venv_bin):
        env["PATH"] = f"{venv_bin}{os.pathsep}{env.get('PATH', '')}"

    cwd = workspace.resolve()
    if spec.cwd:
        cwd = (workspace / spec.cwd).resolve() if not Path(spec.cwd).is_absolute() else Path(spec.cwd)

    out_path, err_path = _service_log_paths(workspace, spec.name)
    for log_path in (out_path, err_path):
        try:
            if not log_path.exists():
                log_path.touch()
            os.chmod(log_path, 0o600)
        except OSError:
            pass
    out_fp = open(out_path, "a", encoding="utf-8")  # noqa: SIM115
    err_fp = open(err_path, "a", encoding="utf-8")  # noqa: SIM115
    try:
        proc = subprocess.Popen(
            argv,
            cwd=str(cwd),
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=out_fp,
            stderr=err_fp,
            **_popen_kwargs(),
        )
    except Exception:
        out_fp.close()
        err_fp.close()
        raise
    # Parent keeps files open only for child inherit; close our handles
    out_fp.close()
    err_fp.close()

    return {
        "name": spec.name,
        "pid": proc.pid,
        "pgid": proc.pid if not _IS_WINDOWS else None,
        "command": spec.command,
        "argv": argv,
        "started_at": _now_iso(),
        "stdout_log": str(out_path),
        "stderr_log": str(err_path),
        "cwd": str(cwd),
    }


def stop_pid(pid: int, *, timeout: float = 5.0) -> str:
    """Gracefully stop a process / process group. Returns status string."""
    if not _pid_alive(pid):
        return "already_dead"

    def _sig_tree(sig: int | None, *, force: bool = False) -> None:
        if _IS_WINDOWS:
            if not force:
                try:
                    os.kill(pid, signal.CTRL_BREAK_EVENT)  # type: ignore[attr-defined]
                    return
                except (AttributeError, OSError):
                    pass
            subprocess.run(
                ["taskkill", "/PID", str(pid), "/T", "/F"],
                capture_output=True,
                check=False,
            )
            return
        assert sig is not None
        try:
            os.killpg(pid, sig)
        except ProcessLookupError:
            raise
        except OSError:
            os.kill(pid, sig)

    try:
        _sig_tree(signal.SIGTERM, force=False)
    except ProcessLookupError:
        return "already_dead"

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not _pid_alive(pid):
            return "terminated"
        time.sleep(0.05)

    try:
        # SIGKILL is POSIX-only; Windows force path uses taskkill /F
        if _IS_WINDOWS:
            _sig_tree(None, force=True)
        else:
            _sig_tree(signal.SIGKILL, force=True)
    except ProcessLookupError:
        return "terminated"
    # brief wait for reaper
    for _ in range(20):
        if not _pid_alive(pid):
            return "killed"
        time.sleep(0.05)
    return "killed"


def stop_all(workspace: Path) -> list[dict[str, Any]]:
    state = _read_state(workspace)
    results: list[dict[str, Any]] = []
    services = dict(state.get("services") or {})
    for name, rec in services.items():
        pid = int(rec.get("pid") or 0)
        status = stop_pid(pid) if pid else "no_pid"
        results.append({"name": name, "pid": pid, "status": status})
    state["services"] = {}
    _write_state(workspace, state)
    return results


def prune_dead(workspace: Path) -> dict[str, Any]:
    state = _read_state(workspace)
    alive: dict[str, Any] = {}
    for name, rec in (state.get("services") or {}).items():
        pid = int(rec.get("pid") or 0)
        if pid and _pid_alive(pid):
            alive[name] = rec
    state["services"] = alive
    _write_state(workspace, state)
    return state


def list_processes(workspace: Path) -> list[dict[str, Any]]:
    state = prune_dead(workspace)
    rows: list[dict[str, Any]] = []
    now = datetime.now(timezone.utc)
    for name, rec in sorted((state.get("services") or {}).items()):
        started = rec.get("started_at")
        uptime = ""
        if started:
            try:
                ts = datetime.fromisoformat(started.replace("Z", "+00:00"))
                secs = int((now - ts).total_seconds())
                uptime = f"{secs // 3600:02d}:{(secs % 3600) // 60:02d}:{secs % 60:02d}"
            except ValueError:
                uptime = "?"
        rows.append(
            {
                "name": name,
                "pid": rec.get("pid"),
                "uptime": uptime,
                "command": rec.get("command", ""),
                "started_at": started,
            }
        )
    return rows


def swarm_up(
    workspace: Path,
    *,
    detach: bool = False,
    config: SwarmConfig | None = None,
    run_monitor: bool = True,
) -> dict[str, Any]:
    """Start all services. If not detach and run_monitor, block in monitor until exit."""
    cfg = config or load_swarm_config(workspace)
    # Stop any prior managed services for clean restart
    stop_all(workspace)

    state = _read_state(workspace)
    started: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for name, spec in cfg.services.items():
        if not spec.enabled:
            skipped.append({"name": name, "reason": "enabled:false"})
            continue
        rec = start_service(workspace, spec)
        state.setdefault("services", {})[name] = rec
        started.append(rec)
    state["config_path"] = str(cfg.path)
    state["version"] = cfg.version
    _write_state(workspace, state)

    result: dict[str, Any] = {
        "started": [{"name": r["name"], "pid": r["pid"]} for r in started],
        "skipped": skipped,
        "detach": detach,
        "state": str(state_path(workspace)),
    }

    if detach or not run_monitor:
        return result

    # Foreground monitor; on exit, tear down children.
    # Do NOT install custom SIGINT/SIGTERM handlers here — they steal Ctrl+C
    # from Textual and hang the TUI in raw mode (Agy #188). Textual exits via
    # KeyboardInterrupt / normal app quit; finally always stop_all().
    try:
        from agentbus.devex import run_monitor as _run_monitor

        _run_monitor(workspace, topic=None, interval=1.0, once=False, plain=False)
    except KeyboardInterrupt:
        pass
    finally:
        stop_all(workspace)
        result["shutdown"] = "ok"
    return result


def tail_service_logs(
    workspace: Path,
    service_name: str,
    *,
    follow: bool = False,
    lines: int = 50,
    stream: IO[str] | None = None,
) -> int:
    """Print service logs; return 0 ok, 1 missing."""
    out = stream or sys.stdout
    state = _read_state(workspace)
    rec = (state.get("services") or {}).get(service_name)
    # Logs may exist even if process dead
    out_path, err_path = _service_log_paths(workspace, service_name)
    if rec:
        out_path = Path(rec.get("stdout_log") or out_path)
        err_path = Path(rec.get("stderr_log") or err_path)

    if not out_path.is_file() and not err_path.is_file():
        print(f"no logs for service '{service_name}'", file=sys.stderr)
        return 1

    def _read_tail(path: Path, n: int) -> list[str]:
        if not path.is_file():
            return []
        try:
            data = path.read_text(encoding="utf-8", errors="replace").splitlines()
            return data[-n:] if n > 0 else data
        except OSError:
            return []

    for line in _read_tail(out_path, lines):
        print(f"[stdout] {line}", file=out)
    for line in _read_tail(err_path, lines):
        print(f"[stderr] {line}", file=out)

    if not follow:
        return 0

    # Follow both files
    fps: list[tuple[str, Any]] = []
    for label, path in (("stdout", out_path), ("stderr", err_path)):
        if path.is_file():
            fp = open(path, "r", encoding="utf-8", errors="replace")  # noqa: SIM115
            fp.seek(0, os.SEEK_END)
            fps.append((label, fp))
    try:
        while True:
            any_data = False
            for label, fp in fps:
                line = fp.readline()
                while line:
                    any_data = True
                    print(f"[{label}] {line.rstrip()}", file=out)
                    out.flush()
                    line = fp.readline()
            if not any_data:
                time.sleep(0.3)
    except KeyboardInterrupt:
        return 0
    finally:
        for _, fp in fps:
            try:
                fp.close()
            except OSError:
                pass


def write_example_swarm(workspace: Path, *, force: bool = False) -> Path:
    """Write a starter swarm.yaml if missing."""
    path = swarm_yaml_path(workspace)
    if path.is_file() and not force:
        return path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        """# AgentBus swarm — declarative multi-service DX
# v0.15: headless runners support enabled: false (skipped by agentbus up)
version: "1.0"
services:
  watch:
    command: "agentbus watch --no-shell"
  # --- v0.15 headless reason-plane (opt-in) ---
  # Requires runner configs under .agentbus/runner.*.yaml
  # hermes-runner:
  #   enabled: false
  #   command: "agentbus run --config .agentbus/runner.hermes.yaml"
  #   env:
  #     AGENTBUS_PRODUCER_ID: "hermes"
  # factory-runner:
  #   enabled: false
  #   command: "agentbus run --config .agentbus/runner.factory.yaml"
  #   env:
  #     AGENTBUS_PRODUCER_ID: "factory"
  # grok-runner:
  #   enabled: false
  #   command: "agentbus run --config .agentbus/runner.grok.yaml"
  #   env:
  #     AGENTBUS_PRODUCER_ID: "grok"
  # agy-runner:
  #   enabled: false
  #   command: "agentbus run --config .agentbus/runner.agy.yaml"
  #   env:
  #     AGENTBUS_PRODUCER_ID: "agy"
""",
        encoding="utf-8",
    )
    return path
