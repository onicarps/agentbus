"""OS watcher daemon — passive FS + shell process → system/fs + system/shell."""

from __future__ import annotations

import os
import signal
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Any

from agentbus.schemas import set_validation_workspace
from agentbus.store import EventStore
from agentbus.wiretap import redact_value

# Directory segment names ignored case-insensitively (Windows can surface
# alternate casing; Path.parts alone is not enough for feedback loops).
IGNORED_DIR_NAMES = frozenset(
    {
        ".git",
        ".venv",
        "venv",
        "node_modules",
        "__pycache__",
        ".pytest_cache",
        ".mypy_cache",
        ".ruff_cache",
        ".tox",
        "dist",
        "build",
        ".eggs",
        ".agentbus",  # critical: events.db lives here — must never re-enter
    }
)

# Exact basenames (lower) that commonly participate in bus/TUI feedback loops.
IGNORED_BASENAMES = frozenset(
    {
        "log.md",
        "textual.log",
        "events.db",
        "events.db-journal",
        "events.db-wal",
        "events.db-shm",
        "project-log.json",
        "token",
        "wiretap.jsonl",
    }
)

IGNORED_SUFFIXES = (
    ".log",
    ".db",
    ".db-journal",
    ".db-wal",
    ".db-shm",
    ".tmp",
    ".swp",
    "~",
)

SHELL_NAMES = {
    "bash",
    "sh",
    "zsh",
    "fish",
    "dash",
    "python",
    "python3",
    "node",
    "npm",
    "npx",
    "pip",
    "pip3",
    "uv",
    "cargo",
    "go",
    "make",
    "git",
    "pytest",
    "docker",
    "curl",
    "rg",
    "tmux",
    "agentbus",
    "aider",
    "claude",
    "grok",
    "hermes",
}


def _normalize_path_str(path: str) -> str:
    """Normalize OS path so Windows ``\\`` and mixed separators compare safely."""
    if not path:
        return ""
    # os.path.normpath handles drive letters / .. ; force forward slashes for parts
    return os.path.normpath(path).replace("\\", "/")


def _should_ignore(path: str) -> bool:
    """Return True if this FS path must not emit system/fs (anti-feedback).

    Handles:
    - case-insensitive ``.agentbus`` / ``.AGENTBUS`` (Windows)
    - path separators ``\\`` vs ``/``
    - ``log.md`` / ``*.log`` projection loops from project-log / TUI
    - SQLite/db files written by the bus itself
    """
    norm = _normalize_path_str(path)
    if not norm:
        return True

    # Split on / after normpath so Windows paths never depend on Path.parts alone
    parts = [p for p in norm.split("/") if p and p != "."]
    lower_parts = [p.lower() for p in parts]

    if any(p in IGNORED_DIR_NAMES for p in lower_parts):
        return True

    basename = lower_parts[-1] if lower_parts else ""
    if basename in IGNORED_BASENAMES:
        return True
    if any(basename.endswith(suf) for suf in IGNORED_SUFFIXES):
        return True
    # Hidden / swap noise (keep .env.example-style allow-list empty for safety)
    if basename.startswith(".") and basename not in {".env.example"}:
        return True
    return False


class BusPublisher:
    def __init__(self, workspace: Path, dry_run: bool = False) -> None:
        self.workspace = workspace.resolve()
        self.dry_run = dry_run
        self._lock = threading.Lock()
        self.published = 0
        self._store: EventStore | None = None
        if not dry_run:
            set_validation_workspace(self.workspace)
            self._store = EventStore(self.workspace)

    def close(self) -> None:
        if self._store is not None:
            self._store.close()
            self._store = None

    def publish(self, topic: str, payload: dict[str, Any], *, producer_id: str = "os-watcher") -> int | None:
        if self.dry_run:
            self.published += 1
            print(f"[dry-run] {topic} {payload}", file=sys.stderr)
            return None
        assert self._store is not None
        with self._lock:
            try:
                event, _ = self._store.publish(
                    topic=topic,
                    producer_id=producer_id,
                    schema_version="1.0",
                    payload=payload,
                    skip_rbac=True,
                    idempotency_key=f"watch-{uuid.uuid4().hex[:16]}",
                )
                self.published += 1
                return event.event_id
            except Exception as exc:
                print(f"publish fail: {exc}", file=sys.stderr)
                return None


def run_watch(
    workspace: Path,
    *,
    enable_fs: bool = True,
    enable_shell: bool = True,
    poll_interval: float = 2.0,
    debounce_ms: int = 400,
    dry_run: bool = False,
    duration: float = 0,
) -> int:
    """Run OS watcher until interrupted or duration elapses. Returns published count."""
    FileSystemEventHandler, Observer, psutil = _require_obs_full()

    workspace = workspace.resolve()
    if not workspace.is_dir():
        raise FileNotFoundError(f"workspace not found: {workspace}")

    publisher = BusPublisher(workspace, dry_run=dry_run)
    debounce_s = max(debounce_ms, 0) / 1000.0
    observer = None
    shell: ShellWatcher | None = None
    stop = threading.Event()

    def _shutdown(*_args: Any) -> None:
        stop.set()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    if enable_fs:
        handler = _make_fs_handler(
            FileSystemEventHandler, publisher, workspace, debounce_s
        )
        observer = Observer()
        observer.schedule(handler, str(workspace), recursive=True)
        observer.start()

    if enable_shell:
        shell = ShellWatcher(publisher, workspace, interval=poll_interval, psutil=psutil)
        shell.start()

    start = time.monotonic()
    try:
        while not stop.is_set():
            if duration and (time.monotonic() - start) >= duration:
                break
            stop.wait(0.5)
    finally:
        if observer:
            observer.stop()
            observer.join(timeout=3)
        if shell:
            shell.stop()
        count = publisher.published
        publisher.close()
    return count


def _require_obs_full() -> tuple[Any, Any, Any]:
    try:
        from watchdog.events import FileSystemEventHandler
        from watchdog.observers import Observer
    except ImportError as exc:
        raise RuntimeError(
            "watchdog required for agentbus watch — pip install 'okf-agentbus[obs]'"
        ) from exc
    try:
        import psutil
    except ImportError as exc:
        raise RuntimeError(
            "psutil required for agentbus watch — pip install 'okf-agentbus[obs]'"
        ) from exc
    return FileSystemEventHandler, Observer, psutil


def _make_fs_handler(
    base_cls: type,
    publisher: BusPublisher,
    workspace: Path,
    debounce_s: float,
) -> Any:
    class FSHandler(base_cls):  # type: ignore[misc,valid-type]
        def __init__(self) -> None:
            super().__init__()
            self.publisher = publisher
            self.workspace = workspace
            self.debounce_s = debounce_s
            self._last: dict[str, float] = {}
            self._lock = threading.Lock()

        def _rel(self, path: str) -> str:
            try:
                return str(Path(path).resolve().relative_to(self.workspace))
            except Exception:
                return path

        def _emit(
            self,
            event_type: str,
            src_path: str,
            is_directory: bool,
            dest_path: str | None = None,
        ) -> None:
            if _should_ignore(src_path):
                return
            if dest_path and _should_ignore(dest_path):
                return
            name = Path(src_path).name
            if name.endswith(("~", ".swp", ".tmp")):
                return
            if name.startswith(".") and name not in {".env.example"}:
                return

            key = f"{event_type}:{src_path}:{dest_path or ''}"
            now = time.monotonic()
            with self._lock:
                last = self._last.get(key, 0.0)
                if now - last < self.debounce_s:
                    return
                self._last[key] = now
                if len(self._last) > 500:
                    cutoff = now - 60
                    self._last = {k: v for k, v in self._last.items() if v > cutoff}

            payload: dict[str, Any] = {
                "event": event_type,
                "path": self._rel(src_path),
                "is_directory": is_directory,
                "observer": "watchdog",
            }
            if dest_path:
                payload["dest_path"] = self._rel(dest_path)
            self.publisher.publish("system/fs", payload)

        def on_created(self, event: Any) -> None:
            self._emit("created", event.src_path, event.is_directory)

        def on_modified(self, event: Any) -> None:
            if event.is_directory:
                return
            self._emit("modified", event.src_path, False)

        def on_deleted(self, event: Any) -> None:
            self._emit("deleted", event.src_path, event.is_directory)

        def on_moved(self, event: Any) -> None:
            dest = getattr(event, "dest_path", None)
            self._emit("moved", event.src_path, event.is_directory, dest_path=dest)

    return FSHandler()


def _cwd_under_workspace(cwd: str, workspace: Path) -> bool:
    """True if cwd is the workspace or a descendant (path containment, not prefix)."""
    if not cwd:
        return False
    try:
        Path(cwd).resolve().relative_to(workspace)
        return True
    except (OSError, ValueError):
        return False


class ShellWatcher:
    def __init__(
        self,
        publisher: BusPublisher,
        workspace: Path,
        interval: float = 2.0,
        *,
        psutil: Any,
    ) -> None:
        self.publisher = publisher
        self.workspace = workspace.resolve()
        self.interval = interval
        self.psutil = psutil
        self._seen: set[int] = set()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._seen = {p.pid for p in self.psutil.process_iter(["pid"])}
        self._thread = threading.Thread(target=self._loop, name="shell-watch", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=3)

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._scan()
            except Exception as exc:
                print(f"shell scan: {exc}", file=sys.stderr)
            self._stop.wait(self.interval)

    def _scan(self) -> None:
        psutil = self.psutil
        current: set[int] = set()
        for proc in psutil.process_iter(
            ["pid", "name", "cmdline", "cwd", "username", "create_time", "ppid"]
        ):
            try:
                info = proc.info
                pid = info["pid"]
                current.add(pid)
                if pid in self._seen:
                    continue
                cwd = info.get("cwd") or ""
                name = (info.get("name") or "").lower()
                cmdline = info.get("cmdline") or []
                under_ws = _cwd_under_workspace(cwd, self.workspace)
                interesting_name = any(name == n or name.startswith(n) for n in SHELL_NAMES)
                if not under_ws and not interesting_name:
                    continue
                if not under_ws:
                    if name not in {
                        "aider",
                        "claude",
                        "grok",
                        "hermes",
                        "agentbus",
                        "python",
                        "python3",
                    }:
                        continue
                    joined = " ".join(cmdline).lower()
                    if not any(
                        k in joined for k in ("agentbus", "aider", "claude", "hermes", "mcp")
                    ):
                        continue

                # Redact cmdline/cwd secrets before publish (tokens often appear on argv).
                safe_cwd = cwd
                try:
                    if under_ws and cwd:
                        safe_cwd = str(Path(cwd).resolve().relative_to(self.workspace))
                    elif cwd:
                        home = Path.home()
                        try:
                            safe_cwd = "~/" + str(Path(cwd).resolve().relative_to(home))
                        except ValueError:
                            safe_cwd = Path(cwd).name
                except (OSError, ValueError):
                    safe_cwd = Path(cwd).name if cwd else ""

                payload = redact_value(
                    {
                        "event": "process_start",
                        "pid": pid,
                        "ppid": info.get("ppid"),
                        "name": info.get("name"),
                        "cmdline": (cmdline or [])[:20],
                        "cwd": safe_cwd,
                        "username": info.get("username"),
                        "under_workspace": under_ws,
                        "observer": "psutil",
                    }
                )
                self.publisher.publish("system/shell", payload)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        self._seen |= current
        if len(self._seen) > 50_000:
            self._seen = current
