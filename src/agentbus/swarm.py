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
import tempfile
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
    # Replace the state file atomically.  A supervisor interruption must not
    # leave a valid-looking but empty file that makes `ps` forget live services.
    tmp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as tmp:
            tmp_path = Path(tmp.name)
            tmp.write(json.dumps(state, indent=2) + "\n")
            tmp.flush()
            os.fsync(tmp.fileno())
        try:
            os.chmod(tmp_path, 0o600)
        except OSError:
            pass
        os.replace(tmp_path, path)
        tmp_path = None
    finally:
        if tmp_path is not None:
            try:
                tmp_path.unlink()
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


def _proc_environment(pid: int) -> dict[str, str]:
    """Read a Linux process environment without requiring optional psutil."""
    try:
        raw = Path(f"/proc/{pid}/environ").read_bytes()
    except (OSError, PermissionError):
        return {}
    result: dict[str, str] = {}
    for item in raw.split(b"\0"):
        if b"=" not in item:
            continue
        key, value = item.split(b"=", 1)
        try:
            result[key.decode()] = value.decode()
        except UnicodeDecodeError:
            continue
    return result


def _proc_command(pid: int) -> list[str]:
    try:
        raw = Path(f"/proc/{pid}/cmdline").read_bytes()
    except (OSError, PermissionError):
        return []
    return [part.decode(errors="replace") for part in raw.split(b"\0") if part]


def _proc_started_at(pid: int) -> str | None:
    """Return a process start timestamp on Linux, if available."""
    try:
        raw = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8", errors="replace")
        rparen = raw.rfind(")")
        if rparen == -1:
            return None
        fields = raw[rparen + 2 :].split()
        # fields starts at proc stat field 3; starttime is field 22.
        start_ticks = int(fields[19])
        clk_tck = os.sysconf("SC_CLK_TCK")
        boot_time = None
        for line in Path("/proc/stat").read_text(encoding="utf-8").splitlines():
            if line.startswith("btime "):
                boot_time = int(line.split()[1])
                break
        if boot_time is None or clk_tck <= 0:
            return None
        return datetime.fromtimestamp(
            boot_time + (start_ticks / clk_tck), tz=timezone.utc
        ).strftime("%Y-%m-%dT%H:%M:%SZ")
    except (OSError, ValueError, IndexError, TypeError):
        return None


def _config_argument(argv: list[str]) -> str | None:
    for index, token in enumerate(argv):
        if token == "--config" and index + 1 < len(argv):
            return argv[index + 1]
        if token.startswith("--config="):
            return token.split("=", 1)[1]
    return None


def _command_matches(actual: list[str], expected: list[str]) -> bool:
    """Match a service command inside interpreter/wrapper-prefixed argv."""
    if not actual or not expected:
        return False
    for start in range(len(actual) - len(expected) + 1):
        candidate = actual[start : start + len(expected)]
        if candidate == expected:
            return True
        # A config may say `agentbus ...`, while the process has the absolute
        # venv script path.  Only relax the executable token comparison.
        if (
            Path(candidate[0]).name == Path(expected[0]).name
            and candidate[1:] == expected[1:]
        ):
            return True
    return False


def _service_match_score(
    env: dict[str, str], actual: list[str], spec: ServiceSpec
) -> int:
    """Return a confidence score for assigning a process to a service."""
    if env.get("AGENTBUS_SWARM_SERVICE") == spec.name:
        return 100
    expected = _parse_command(spec.command)
    if _command_matches(actual, expected):
        return 90
    # Older launchers did not set AGENTBUS_SWARM_SERVICE and the Go worker
    # replaces `agentbus worker up` in argv.  Its workspace-scoped config path
    # plus producer identity is still a strong, unambiguous service key.
    expected_config = _config_argument(expected)
    actual_config = _config_argument(actual)
    if (
        expected_config
        and expected_config == actual_config
        and spec.env
        and all(env.get(key) == value for key, value in spec.env.items())
    ):
        return 80
    return 0


def discover_processes(workspace: Path, config: SwarmConfig) -> dict[str, dict[str, Any]]:
    """Recover live services whose supervisor state was lost or went stale.

    `agentbus up` marks children with the workspace and service name.  The
    workspace match prevents cross-workspace adoption; command/config matching
    preserves compatibility with processes started before the service marker
    existed.  On non-/proc platforms the durable state file remains the source
    of truth.
    """
    if _IS_WINDOWS or not Path("/proc").is_dir():
        return {}

    root = str(workspace.resolve())
    processes: list[tuple[int, dict[str, str], list[str]]] = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        pid = int(entry.name)
        env = _proc_environment(pid)
        if env.get("AGENTBUS_WORKSPACE") != root:
            continue
        actual = _proc_command(pid)
        if actual:
            processes.append((pid, env, actual))

    recovered: dict[str, dict[str, Any]] = {}
    used_pids: set[int] = set()
    for name, spec in config.services.items():
        if not spec.enabled:
            continue
        best: tuple[int, int, dict[str, str], list[str]] | None = None
        for pid, env, actual in processes:
            if pid in used_pids:
                continue
            score = _service_match_score(env, actual, spec)
            candidate = (score, -pid, env, actual)
            if score and (best is None or candidate[:2] > best[:2]):
                best = candidate
        if best is None:
            continue
        _, _, _, actual = best
        used_pids.add(-best[1])
        recovered[name] = {
            "name": name,
            "pid": -best[1],
            "pgid": -best[1] if not _IS_WINDOWS else None,
            "command": spec.command,
            "argv": actual,
            "started_at": _proc_started_at(-best[1]),
            "stdout_log": str(logs_dir(workspace) / f"{name}.stdout.log"),
            "stderr_log": str(logs_dir(workspace) / f"{name}.stderr.log"),
            "cwd": root,
            "discovered": True,
        }
    return recovered


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
    # Keep a durable identity on the child so `ps` can recover it after the
    # supervisor is restarted or its state file is rebuilt.
    env["AGENTBUS_WORKSPACE"] = str(workspace.resolve())
    env["AGENTBUS_SWARM_SERVICE"] = spec.name

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
    # Prune dead from existing state, then discover any running swarm children
    # so that untracked/orphan processes are terminated even if state was lost.
    state = prune_dead(workspace)
    try:
        config = load_swarm_config(workspace)
    except (FileNotFoundError, OSError, ValueError):
        config = None
    if config is not None:
        recovered = discover_processes(workspace, config)
        services = state.setdefault("services", {})
        for name, record in recovered.items():
            if name not in services:
                services[name] = record

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
    try:
        config = load_swarm_config(workspace)
    except (FileNotFoundError, OSError, ValueError):
        config = None
    if config is not None:
        recovered = discover_processes(workspace, config)
        services = state.setdefault("services", {})
        changed = False
        for name, record in recovered.items():
            current = services.get(name)
            current_pid = int(current.get("pid") or 0) if isinstance(current, dict) else 0
            if not current_pid or not _pid_alive(current_pid):
                services[name] = record
                changed = True
        if changed:
            _write_state(workspace, state)
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
    try:
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
    except Exception:
        # Roll back partially started services so none are left orphaned
        for rec in started:
            pid = int(rec.get("pid") or 0)
            if pid:
                stop_pid(pid)
        state["services"] = {}
        _write_state(workspace, state)
        raise

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
