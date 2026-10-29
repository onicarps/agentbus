"""Unified first-run discovery, serialized setup, and safe client connection.

Design notes (read before changing):

* ``discover`` is strictly read-only.  It never creates a directory, token,
  marker, lock, database, or backup, even when paths are missing or malformed.
* Every mutating setup path takes an advisory ``.agentbus/init.lock``.  On
  POSIX the lock is ``flock``-based, so it is released automatically when the
  process exits or dies; the pid written into the file is informational only
  and is never used to decide whether the lock is stale.
* Client config changes are staged in the destination directory, fsynced, and
  installed with ``os.replace`` one file at a time.  This is **not** a
  cross-file atomic transaction: independent filesystems, crashes, and
  non-cooperating writers can still leave partial state.  On failure the
  transaction restores every file it changed and reports any rollback error.
* ``Configured`` means the file matches what AgentBus requires. ``Differs``
  means an existing AgentBus entry has different parameters. ``Awaiting
  Approval`` means the host client has not observed the change yet.
  ``Verified`` is never claimed for a client: a successful MCP ``initialize``
  + ``tools/list`` exchange only sets ``command_verified`` and proves the
  configured command speaks MCP, not that the host approved or connected.
* Backups are created with ``O_EXCL`` and a timestamp/PID stamp; an existing
  backup is never replaced. User-scope writes additionally take a global
  lock so distinct workspaces serialize access to shared client files.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import queue
import shutil
import stat
import subprocess
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

from agentbus.adapters.command import (
    McpCommand,
    McpCommandError,
    resolve_mcp_command,
)
from agentbus.adapters.manifest import (
    Installation,
    Manifest,
    build_entry,
    load_manifests,
)
from agentbus.adapters.probe import (
    ENTRY_CONFLICT,
    STATUS_ABSENT,
    STATUS_PRESENT,
    STATUS_UNKNOWN,
    ClientProbe,
    current_platform,
    probe_installations,
)
from agentbus.adapters.writer import (
    ConfigConflictError,
    Patch,
    entry_env_keys,
    plan_patch,
    redact_entry,
)
from agentbus.safe_file import read_regular_file

SETUP_LOCK_NAME = "init.lock"
USER_CONFIG_LOCK_NAME = "user-config.lock"
LOCK_TIMEOUT_SECONDS = 10.0
LOCK_POLL_SECONDS = 0.05

STATE_PLANNED = "planned"
STATE_CONFIGURED = "configured"
STATE_DIFFERS = "differs"
STATE_AWAITING = "awaiting_approval"
STATE_REFUSED = "refused"
STATE_SKIPPED = "skipped"
STATE_ERROR = "error"

ACTION_CREATE = "create"
ACTION_UPDATE = "update"
ACTION_UNCHANGED = "unchanged"
ACTION_REFUSE = "refuse"
ACTION_SKIP = "skip"
ACTION_ERROR = "error"

EXIT_OK = 0
EXIT_FAILED = 1
EXIT_USAGE = 2
EXIT_LOCK_TIMEOUT = 3
EXIT_VERIFY_FAILED = 4

#: Client families a ``--client`` selector may name. Installation ids
#: (``cursor-project``) are accepted too; anything else is a typo, and a typo
#: must fail closed instead of silently selecting nothing.
SUPPORTED_CLIENTS: tuple[str, ...] = ("claude", "codex", "cursor", "gemini", "vscode")


class SetupLockTimeout(RuntimeError):
    """Another setup process held the workspace lock past the deadline."""


class SetupLockSecurityError(RuntimeError):
    """The lock path cannot be safely opened."""


class ConcurrentEditError(RuntimeError):
    """A config file changed between planning and commit."""


class UnknownClientError(ValueError):
    """A ``--client`` selector names no supported client or installation."""

    def __init__(self, name: str, supported: Sequence[str] = SUPPORTED_CLIENTS):
        self.name = name
        self.supported = tuple(supported)
        super().__init__(
            f"Unrecognized client '{name}'. "
            f"Supported clients: {', '.join(self.supported)}."
        )


def known_client_ids(
    manifests: Sequence[Manifest] | None = None,
) -> tuple[str, ...]:
    """Client families plus every packaged installation id."""
    catalogue = load_manifests() if manifests is None else manifests
    known = set(SUPPORTED_CLIENTS)
    for manifest in catalogue:
        for installation in manifest.installations:
            known.add(installation.client)
            known.add(installation.installation_id)
    return tuple(sorted(known))


def validate_client_selection(
    clients: Iterable[str] | None,
    *,
    manifests: Sequence[Manifest] | None = None,
) -> None:
    """Fail closed on an unknown ``--client`` selector."""
    if not clients:
        return
    known = set(known_client_ids(manifests))
    for name in clients:
        if name not in known:
            raise UnknownClientError(name)


class TransactionRollbackConflictError(RuntimeError):
    """Rollback refused to overwrite content changed outside the transaction.

    Zero-data-loss invariant: when a file this transaction wrote no longer
    matches the bytes it wrote, the current content belongs to somebody else
    (an editor, a concurrent tool, or the operator). Rollback leaves that
    content untouched and reports where the pre-transaction backup lives so
    the operator can recover or reconcile manually.
    """

    def __init__(self, path: Path, backup: Path | None, *, detail: str = ""):
        self.path = Path(path)
        self.backup = Path(backup) if backup is not None else None
        if self.backup is not None:
            action = f"pre-transaction backup preserved at {self.backup}"
        else:
            action = "file left in place; nothing was deleted"
        message = (
            f"external modification detected on {self.path} during rollback; "
            f"{action}"
        )
        if detail:
            message = f"{message} ({detail})"
        super().__init__(message)


class SetupWriteError(RuntimeError):
    """A staged write failed and the transaction was rolled back."""

    def __init__(
        self,
        cause: BaseException,
        rollback_errors: Sequence[str],
        conflicts: Sequence[TransactionRollbackConflictError] = (),
    ):
        self.cause = cause
        self.rollback_errors = tuple(rollback_errors)
        self.rollback_conflicts = tuple(conflicts)
        detail = f"write failed: {cause}"
        if self.rollback_errors:
            detail += "; rollback incomplete: " + "; ".join(self.rollback_errors)
        else:
            detail += "; all files this operation changed were restored"
        super().__init__(detail)


# ---------------------------------------------------------------------------
# Setup lock (re-entrant within one process, cross-process advisory)
# ---------------------------------------------------------------------------


def _lock_file(workspace: Path) -> Path:
    return workspace.resolve() / ".agentbus" / SETUP_LOCK_NAME


class _LockHolder:
    """One acquired operating-system lock, shared by every reentrant handle.

    Depth, owner, and the descriptor live together in a single object so the
    counter cannot drift per instance: an out-of-order release from a nested
    handle decrements the same state the outer handle acquired.
    """

    __slots__ = ("fd", "owner", "depth", "waited")

    def __init__(self, fd: int, owner: int, *, waited: float):
        self.fd: int | None = fd
        self.owner: int | None = owner
        self.depth = 1
        self.waited = waited


class SetupLock:
    """Advisory setup lock for one workspace or one global config root.

    Reentrancy is permitted **only** from the owning thread. A second thread
    in the same process must contend for the operating-system lock exactly
    like a separate process, so concurrent first runs cannot overlap. The
    lock path is also used for the user-scope configuration lock, because a
    workspace lock cannot serialize writes from distinct workspaces.
    """

    _registry: dict[str, _LockHolder] = {}
    _registry_guard = threading.Lock()

    def __init__(
        self,
        workspace: Path | None = None,
        *,
        path: Path | None = None,
        timeout: float = LOCK_TIMEOUT_SECONDS,
        poll: float = LOCK_POLL_SECONDS,
    ):
        if path is None:
            if workspace is None:
                raise ValueError("SetupLock needs a workspace or an explicit path")
            path = _lock_file(workspace)
        self.path = Path(path).expanduser().absolute()
        self.timeout = max(0.0, float(timeout))
        self.poll = max(0.0, float(poll))
        self._holder: _LockHolder | None = None
        self._waited = 0.0

    @classmethod
    def at_path(
        cls,
        path: Path,
        *,
        timeout: float = LOCK_TIMEOUT_SECONDS,
        poll: float = LOCK_POLL_SECONDS,
    ) -> "SetupLock":
        return cls(path=path, timeout=timeout, poll=poll)

    @property
    def waited(self) -> float:
        return self._waited

    @property
    def owner_thread_id(self) -> int | None:
        holder = self._holder
        return holder.owner if holder is not None else None

    @property
    def depth(self) -> int:
        holder = self._holder
        return holder.depth if holder is not None else 0

    def _try_acquire_fd(self, fd: int) -> bool:
        if os.name == "nt":  # pragma: no cover - exercised on Windows only
            import msvcrt

            try:
                os.lseek(fd, 0, os.SEEK_SET)
                msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
            except OSError:
                return False
            return True
        import errno
        import fcntl

        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            if exc.errno in (errno.EAGAIN, errno.EWOULDBLOCK, errno.EACCES):
                return False
            raise SetupLockSecurityError(f"cannot lock {self.path}: {exc}") from exc
        return True

    def _unlock_fd(self, fd: int) -> None:
        if os.name == "nt":  # pragma: no cover - exercised on Windows only
            import msvcrt

            with contextlib.suppress(OSError):
                os.lseek(fd, 0, os.SEEK_SET)
                msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
            return
        import fcntl

        with contextlib.suppress(OSError):
            fcntl.flock(fd, fcntl.LOCK_UN)

    def acquire(self) -> "SetupLock":
        ident = threading.get_ident()
        key = str(self.path)
        with SetupLock._registry_guard:
            active = SetupLock._registry.get(key)
            if (
                active is not None
                and active.owner == ident
                and active.fd is not None
            ):
                # Same thread: reentrant, share the open descriptor and the
                # one depth counter that both handles release against.
                active.depth += 1
                self._holder = active
                self._waited = active.waited
                return self
            # A lock held by another thread must contend for the OS lock below
            # rather than short-circuit on this process's registry entry.
        if self.path.parent.is_symlink():
            raise SetupLockSecurityError("refusing symlinked lock directory")
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise SetupLockSecurityError(f"cannot create lock directory: {exc}") from exc
        if self.path.is_symlink():
            raise SetupLockSecurityError("refusing symlinked lock path")
        flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
        try:
            fd = os.open(self.path, flags, 0o600)
        except OSError as exc:
            raise SetupLockSecurityError(f"cannot open setup lock: {exc}") from exc
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            os.close(fd)
            raise SetupLockSecurityError("lock path is not a regular file")
        started = time.monotonic()
        try:
            while True:
                if self._try_acquire_fd(fd):
                    break
                if time.monotonic() - started >= self.timeout:
                    last_writer = self._read_holder()
                    detail = f"workspace setup lock busy: {self.path}"
                    if last_writer:
                        detail += f" (last writer pid {last_writer})"
                    raise SetupLockTimeout(detail)
                time.sleep(self.poll)
        except BaseException:
            # Exactly one close for the descriptor we opened: the timeout
            # branch raises instead of closing, so a failed acquire can never
            # close a descriptor twice (or close one it never owned).
            with contextlib.suppress(OSError):
                os.close(fd)
            raise
        waited = time.monotonic() - started
        holder = _LockHolder(fd, ident, waited=waited)
        self._holder = holder
        self._waited = waited
        with contextlib.suppress(OSError):
            os.ftruncate(fd, 0)
            os.write(fd, f"{os.getpid()}\n".encode("ascii"))
        with SetupLock._registry_guard:
            SetupLock._registry[key] = holder
        return self

    def _read_holder(self) -> str:
        try:
            return self.path.read_text(encoding="ascii").strip()
        except OSError:
            return ""

    def release(self) -> None:
        """Release one level of this handle's acquisition.

        Every ``acquire`` must be matched by one ``release``. The OS lock is
        dropped only when the shared depth counter reaches zero, so nested
        handles may be released in any order without freeing the lock early.
        A handle that never acquired (a timed-out contender) is a no-op.
        """
        holder = self._holder
        if holder is None:
            return
        ident = threading.get_ident()
        if holder.owner is not None and holder.owner != ident:
            # Only the owning thread may release; a non-owner call is a
            # programming error that must not corrupt the lock state.
            raise RuntimeError(
                f"setup lock {self.path} is owned by thread "
                f"{holder.owner}, not {ident}"
            )
        if holder.depth <= 0:
            return
        holder.depth -= 1
        if holder.depth > 0:
            return
        key = str(self.path)
        with SetupLock._registry_guard:
            if SetupLock._registry.get(key) is holder:
                SetupLock._registry.pop(key, None)
        fd = holder.fd
        holder.fd = None
        holder.owner = None
        self._holder = None
        if fd is not None:
            self._unlock_fd(fd)
            with contextlib.suppress(OSError):
                os.close(fd)

    def __enter__(self) -> "SetupLock":
        return self.acquire()

    def __exit__(self, *_exc: object) -> None:
        self.release()


def setup_lock(
    workspace: Path,
    *,
    timeout: float = LOCK_TIMEOUT_SECONDS,
    poll: float = LOCK_POLL_SECONDS,
) -> SetupLock:
    return SetupLock(workspace, timeout=timeout, poll=poll)


def global_config_lock(
    home: Path,
    *,
    timeout: float = LOCK_TIMEOUT_SECONDS,
    poll: float = LOCK_POLL_SECONDS,
) -> SetupLock:
    """Serializes user-scope config writes across independent workspaces."""
    return SetupLock.at_path(
        Path(home).expanduser().resolve() / ".agentbus" / USER_CONFIG_LOCK_NAME,
        timeout=timeout,
        poll=poll,
    )


# ---------------------------------------------------------------------------
# Staged multi-file writes with rollback
# ---------------------------------------------------------------------------


@dataclass
class _Staged:
    path: Path
    new_bytes: bytes
    original_bytes: bytes | None
    original_mode: int | None


@dataclass(frozen=True)
class TransactionOutcome:
    written: tuple[str, ...]
    backups: tuple[str, ...]
    backup_map: Mapping[str, str] = field(default_factory=dict)
    rollback_errors: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "written": list(self.written),
            "backups": list(self.backups),
            "backup_map": dict(self.backup_map),
            "rollback_errors": list(self.rollback_errors),
        }


def _mode_of(path: Path) -> int:
    try:
        return stat.S_IMODE(path.stat().st_mode)
    except OSError:
        return 0o600


def _escapes_workspace(path: Path, workspace: Path) -> bool:
    try:
        path.resolve().relative_to(workspace.resolve())
    except ValueError:
        return True
    return False


def _atomic_write(path: Path, data: bytes, mode: int) -> None:
    """Write ``data`` to ``path`` via a same-directory temp file + replace."""
    if path.is_symlink():
        path = path.resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = _mkstemp(path)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, mode)
        os.replace(temporary, path)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(temporary)
        raise


def _mkstemp(path: Path) -> tuple[int, str]:
    import tempfile

    return tempfile.mkstemp(
        dir=str(path.parent), prefix=f".{path.name}.", suffix=".agentbus-tmp"
    )


def _fsync_dir(path: Path) -> None:
    if os.name == "nt":  # pragma: no cover - Windows has no dir fsync
        return
    with contextlib.suppress(OSError):
        fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


def backup_path(path: Path, *, stamp: str | None = None) -> Path:
    """Collision-resistant backup name.

    With no ``stamp`` this is the legacy display form; callers that create a
    backup always pass a unique ``.agentbus.bak.<timestamp>.<pid>`` stamp and
    create it with ``O_EXCL`` so a concurrent writer's backup is never
    overwritten.
    """
    suffix = path.suffix + ".agentbus.bak"
    if stamp:
        suffix += f".{stamp}"
    return path.with_suffix(suffix)


def _create_backup(path: Path, data: bytes, mode: int) -> Path:
    """Create a new backup without ever replacing an existing one."""
    timestamp = time.strftime("%Y%m%dT%H%M%S", time.gmtime())
    for counter in range(1000):
        stamp = f"{timestamp}.{os.getpid()}.{counter}"
        candidate = backup_path(path, stamp=stamp)
        try:
            fd = os.open(
                candidate,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0),
                mode or 0o600,
            )
        except FileExistsError:
            continue
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            os.chmod(candidate, mode or 0o600)
        except BaseException:
            with contextlib.suppress(OSError):
                candidate.unlink()
            raise
        return candidate
    raise OSError(f"{path}: could not allocate a unique backup name")


class FileTransaction:
    """Stage several files, then install them one at a time with rollback."""

    def __init__(self) -> None:
        self._staged: list[_Staged] = []
        self._applied: list[_Staged] = []
        self._created_dirs: list[Path] = []
        #: SHA-256 of the exact bytes this transaction installed per path.
        self._written_hashes: dict[Path, str] = {}
        #: Pre-transaction backup created for each existing path.
        self._backups: dict[Path, Path] = {}
        #: Typed conflicts recorded when rollback preserved an outside edit.
        self._conflicts: list[TransactionRollbackConflictError] = []

    @property
    def rollback_conflicts(self) -> tuple[TransactionRollbackConflictError, ...]:
        return tuple(self._conflicts)

    def stage(
        self,
        path: Path,
        new_bytes: bytes,
        *,
        original_bytes: bytes | None,
    ) -> None:
        mode = _mode_of(path) if original_bytes is not None else 0o600
        self._staged.append(
            _Staged(
                path=path,
                new_bytes=new_bytes,
                original_bytes=original_bytes,
                original_mode=mode,
            )
        )

    def _ensure_parent(self, path: Path) -> None:
        missing: list[Path] = []
        current = path.parent
        while not current.exists():
            missing.append(current)
            if current.parent == current:
                break
            current = current.parent
        path.parent.mkdir(parents=True, exist_ok=True)
        self._created_dirs.extend(reversed(missing))

    def commit(self) -> TransactionOutcome:
        written: list[str] = []
        backups: list[str] = []
        backup_map: dict[str, str] = {}
        try:
            for item in self._staged:
                self._ensure_parent(item.path)
                current = None
                try:
                    current = read_regular_file(item.path)
                except FileNotFoundError:
                    current = None
                except OSError as exc:
                    raise ConcurrentEditError(
                        f"{item.path}: cannot re-read before write: {exc}"
                    ) from exc
                if current != item.original_bytes:
                    raise ConcurrentEditError(
                        f"{item.path}: changed by another writer after planning"
                    )
                if item.original_bytes is not None:
                    try:
                        backup = _create_backup(
                            item.path,
                            item.original_bytes,
                            item.original_mode or 0o600,
                        )
                    except OSError as exc:
                        raise ConcurrentEditError(
                            f"{item.path}: cannot create backup: {exc}"
                        ) from exc
                    backups.append(str(backup))
                    backup_map[str(item.path)] = str(backup)
                    self._backups[item.path] = backup
                _atomic_write(
                    item.path, item.new_bytes, item.original_mode or 0o600
                )
                self._written_hashes[item.path] = hashlib.sha256(
                    item.new_bytes
                ).hexdigest()
                _fsync_dir(item.path.resolve().parent)
                self._applied.append(item)
                written.append(str(item.path))
        except BaseException as exc:
            errors = tuple(self.rollback())
            raise SetupWriteError(exc, errors, self._conflicts) from exc
        return TransactionOutcome(
            tuple(written), tuple(backups), backup_map
        )

    def _written_digest(self, item: _Staged) -> str:
        return self._written_hashes.get(
            item.path, hashlib.sha256(item.new_bytes).hexdigest()
        )

    def _conflict(
        self, item: _Staged, *, detail: str
    ) -> TransactionRollbackConflictError:
        conflict = TransactionRollbackConflictError(
            item.path, self._backups.get(item.path), detail=detail
        )
        self._conflicts.append(conflict)
        return conflict

    def rollback(self) -> list[str]:
        """Restore what this transaction changed, never destroying outside work.

        For every applied file the current bytes are compared against the
        digest of the bytes this transaction wrote. A mismatch means an
        external writer intervened: the file is preserved as-is (restoring the
        backup would silently erase that work) and a typed conflict is
        recorded alongside the human-readable error.
        """
        errors: list[str] = []
        for item in reversed(self._applied):
            try:
                try:
                    current = read_regular_file(item.path)
                except FileNotFoundError:
                    current = None
                if current is not None and hashlib.sha256(
                    current
                ).hexdigest() != self._written_digest(item):
                    conflict = self._conflict(
                        item, detail="file no longer matches what AgentBus wrote"
                    )
                    errors.append(str(conflict))
                    continue
                if item.original_bytes is None:
                    if current is None:
                        continue
                    (item.path.resolve() if item.path.is_symlink() else item.path).unlink()
                else:
                    if current is None:
                        conflict = self._conflict(
                            item,
                            detail="file was deleted after AgentBus wrote it",
                        )
                        errors.append(str(conflict))
                        continue
                    _atomic_write(
                        item.path,
                        item.original_bytes,
                        item.original_mode or 0o600,
                    )
                _fsync_dir(item.path.resolve().parent)
            except OSError as exc:
                errors.append(f"{item.path}: rollback failed: {exc}")
        self._applied.clear()
        self._written_hashes.clear()
        for directory in reversed(self._created_dirs):
            with contextlib.suppress(OSError):
                directory.rmdir()
        self._created_dirs.clear()
        return errors


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DiscoveryReport:
    workspace: str
    platform: str
    producer_id: str
    command: dict[str, Any] | None
    command_error: dict[str, Any] | None
    probes: tuple[ClientProbe, ...]
    manifests: tuple[str, ...]
    warnings: tuple[str, ...] = ()
    #: The single resolved command object, reused by connect for planning and
    #: verification. Never serialized: ``command`` is its redacted report view.
    resolved_command: McpCommand | None = field(
        default=None, repr=False, compare=False
    )

    def to_dict(self) -> dict[str, Any]:
        return {
            "workspace": self.workspace,
            "platform": self.platform,
            "producer_id": self.producer_id,
            "command": self.command,
            "command_error": self.command_error,
            "manifests": list(self.manifests),
            "clients": [probe.to_dict() for probe in self.probes],
            "warnings": list(self.warnings),
        }

    def for_installation(self, installation_id: str) -> ClientProbe | None:
        for probe in self.probes:
            if probe.installation_id == installation_id:
                return probe
        return None


def default_producer_id(environ: Mapping[str, str] | None = None) -> str:
    env = os.environ if environ is None else environ
    return (env.get("AGENTBUS_PRODUCER_ID") or "").strip() or "local-user"


def _expected_entries(
    manifests: Sequence[Manifest],
    *,
    command: McpCommand | None,
    workspace: Path,
    producer_id: str,
) -> dict[str, dict[str, Any]]:
    if command is None:
        return {}
    return {
        installation.installation_id: build_entry(
            installation,
            argv=command.argv,
            workspace=workspace,
            producer_id=producer_id,
        )
        for manifest in manifests
        for installation in manifest.installations
    }


def discover(
    *,
    workspace: Path,
    clients: Iterable[str] | None = None,
    producer_id: str | None = None,
    home: Path | None = None,
    platform: str | None = None,
    environ: Mapping[str, str] | None = None,
    command: McpCommand | None = None,
    which: Callable[[str], str | None] = shutil.which,
    python: str | None = None,
    manifests: Sequence[Manifest] | None = None,
) -> DiscoveryReport:
    """Read-only discovery of MCP client installations."""
    resolved_workspace = Path(workspace).resolve()
    resolved_platform = current_platform() if platform is None else platform
    pid = producer_id or default_producer_id(environ)
    catalogue = tuple(load_manifests() if manifests is None else manifests)

    command_error: dict[str, Any] | None = None
    resolved_command = command
    if resolved_command is None:
        try:
            resolved_command = resolve_mcp_command(
                environ=environ, which=which, validate=True, python=python
            )
        except McpCommandError as exc:
            command_error = {
                "code": exc.code,
                "detail": str(exc),
                "hints": list(exc.hints),
            }

    expected = _expected_entries(
        catalogue,
        command=resolved_command,
        workspace=resolved_workspace,
        producer_id=pid,
    )
    probes = probe_installations(
        workspace=resolved_workspace,
        home=home,
        platform=resolved_platform,
        environ=environ,
        clients=clients,
        expected_entries=expected,
        manifests=catalogue,
        which=which,
    )
    warnings: list[str] = []
    if resolved_command is not None:
        warnings.extend(resolved_command.warnings)
    if command_error is not None:
        warnings.append(
            "no durable MCP command resolved: "
            f"{command_error['code']}; connect will refuse to write a broken entry"
        )

    return DiscoveryReport(
        workspace=str(resolved_workspace),
        platform=resolved_platform,
        producer_id=pid,
        command=(
            redact_entry(resolved_command.to_dict())
            if resolved_command
            else None
        ),
        command_error=command_error,
        probes=probes,
        manifests=tuple(manifest.manifest_id for manifest in catalogue),
        warnings=tuple(warnings),
        resolved_command=resolved_command,
    )


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------


@dataclass
class ConnectItem:
    installation_id: str
    title: str
    scope: str
    path: str | None
    action: str
    state: str
    reason: str
    configured: bool = False
    changed: bool = False
    backup: str | None = None
    approval: dict[str, str] = field(default_factory=dict)
    previous_entry: dict[str, Any] | None = None
    verification: dict[str, Any] | None = None
    command_verified: bool = False
    host_client_observed: bool = False

    def to_dict(self) -> dict[str, Any]:
        safe_entry = redact_entry(self.previous_entry)
        return {
            "installation": self.installation_id,
            "title": self.title,
            "scope": self.scope,
            "path": self.path,
            "action": self.action,
            "state": self.state,
            "configured": self.configured,
            "changed": self.changed,
            "reason": self.reason,
            "backup": self.backup,
            "approval": dict(self.approval),
            "previous_entry": safe_entry,
            "previous_env_keys": entry_env_keys(self.previous_entry),
            "verification": self.verification,
            "command_verified": self.command_verified,
            "host_client_observed": self.host_client_observed,
        }


@dataclass(frozen=True)
class FirstHandoff:
    installation_id: str
    recipient: str
    executed: bool
    prompt: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "installation": self.installation_id,
            "recipient": self.recipient,
            "executed": self.executed,
            "prompt": self.prompt,
        }


@dataclass(frozen=True)
class ConnectReport:
    workspace: str
    dry_run: bool
    platform: str
    items: tuple[ConnectItem, ...]
    command: dict[str, Any] | None
    command_error: dict[str, Any] | None
    verification: dict[str, Any]
    first_handoffs: tuple[FirstHandoff, ...]
    lock_wait_seconds: float
    exit_code: int
    errors: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "workspace": self.workspace,
            "dry_run": self.dry_run,
            "platform": self.platform,
            "command": self.command,
            "command_error": self.command_error,
            "items": [item.to_dict() for item in self.items],
            "verification": dict(self.verification),
            "first_handoffs": [handoff.to_dict() for handoff in self.first_handoffs],
            "lock_wait_seconds": round(self.lock_wait_seconds, 4),
            "exit_code": self.exit_code,
            "errors": list(self.errors),
            "guarantees": {
                "config_writes": (
                    "per-file atomic replace with rollback; not cross-file atomic"
                ),
                "lock": (
                    f".agentbus/{SETUP_LOCK_NAME} advisory flock; user-scope "
                    f"writes also take ~/.agentbus/{USER_CONFIG_LOCK_NAME}"
                ),
                "backups": (
                    "collision-resistant O_EXCL backup names; existing backups "
                    "are never replaced"
                ),
                "verification": (
                    "MCP stdio initialize + tools/list proves the command only; "
                    "host approval and client exchange are not observed"
                ),
            },
        }


def client_selected(probe: ClientProbe, wanted: set[str]) -> bool:
    return not wanted or probe.installation_id in wanted or probe.client in wanted


def auto_connect_allowed(probe: ClientProbe, installation: Installation, wanted: set[str]) -> bool:
    return installation.auto_connect or probe.installation_id in wanted


def _select(probes: Iterable[ClientProbe], *, clients: Iterable[str] | None) -> list[ClientProbe]:
    if clients is None:
        return list(probes)
    wanted = set(clients)
    return [
        probe
        for probe in probes
        if client_selected(probe, wanted)
    ]


def _scope_matches(probe: ClientProbe, scope: str) -> bool:
    return scope in ("all", "", None) or probe.scope == scope


def _refusal(probe: ClientProbe, reason: str) -> ConnectItem:
    return ConnectItem(
        installation_id=probe.installation_id,
        title=probe.title,
        scope=probe.scope,
        path=probe.config_path,
        action=ACTION_REFUSE,
        state=STATE_REFUSED,
        reason=reason,
        approval={
            "mode": probe.approval_mode,
            "guidance": probe.approval_guidance,
        },
    )


def _skip(probe: ClientProbe, reason: str) -> ConnectItem:
    return ConnectItem(
        installation_id=probe.installation_id,
        title=probe.title,
        scope=probe.scope,
        path=probe.config_path,
        action=ACTION_SKIP,
        state=STATE_SKIPPED,
        reason=reason,
        approval={
            "mode": probe.approval_mode,
            "guidance": probe.approval_guidance,
        },
    )


def _installations_by_probe(
    manifests: Sequence[Manifest],
) -> dict[str, Installation]:
    return {
        installation.installation_id: installation
        for manifest in manifests
        for installation in manifest.installations
    }


def _first_handoff_prompt(
    *, workspace: Path, producer_id: str, client: str, recipient: str = "codex"
) -> str:
    payload = json.dumps({"from": producer_id, "to": recipient, "summary": f"First handoff from {client} via MCP"})
    return (
        f"You are connected to the AgentBus workspace {workspace} as producer "
        f"'{producer_id}' through {client}. Complete this four-step first "
        f"handoff exactly as written:\n"
        f"1. Use the agentbus_publish MCP tool to publish one event on topic "
        f"'okf/handoff' with schema_version '1.0' and payload "
        f"{payload}. Record the "
        f"returned event_id as N.\n"
        f"2. Call agentbus_poll on topic 'okf/handoff' with since_id set to "
        f"N - 1 (poll returns only events with event_id > since_id, so N - 1 "
        f"includes event N). Confirm the event with event_id == N is returned.\n"
        f"3. Ask '{recipient}' to process that event and publish an "
        f"acknowledgement event with causation_id set to N.\n"
        f"4. Call agentbus_poll on topic 'okf/handoff' with since_id set to N "
        f"(NOT N - 1) and confirm an event whose causation_id == N is "
        f"returned. Report both event ids.\n"
        f"Do not claim success unless step 2 returns event N and step 4 "
        f"returns an acknowledgement with causation_id == N."
    )


def connect(
    *,
    workspace: Path,
    clients: Iterable[str] | None = None,
    scope: str = "all",
    apply: bool = False,
    allow_user_scope: bool = False,
    producer_id: str | None = None,
    home: Path | None = None,
    platform: str | None = None,
    environ: Mapping[str, str] | None = None,
    lock_timeout: float = LOCK_TIMEOUT_SECONDS,
    verify: bool = False,
    interactive: bool = False,
    json_output: bool = False,
    confirm: Callable[[str], bool] | None = None,
    command: McpCommand | None = None,
    which: Callable[[str], str | None] = shutil.which,
    python: str | None = None,
    manifests: Sequence[Manifest] | None = None,
    transaction_factory: Callable[[], FileTransaction] = FileTransaction,
) -> ConnectReport:
    """Preview or apply client configuration for detected installations."""
    resolved_workspace = Path(workspace).resolve()
    catalogue = tuple(load_manifests() if manifests is None else manifests)
    by_id = _installations_by_probe(catalogue)
    pid = producer_id or default_producer_id(environ)
    report = discover(
        workspace=resolved_workspace,
        clients=clients,
        producer_id=pid,
        home=home,
        platform=platform,
        environ=environ,
        command=command,
        which=which,
        python=python,
        manifests=catalogue,
    )

    errors: list[str] = []
    requested = _select(report.probes, clients=clients)
    selected = [
        probe
        for probe in requested
        if _scope_matches(probe, scope)
    ]

    items: list[ConnectItem] = []
    for name in clients or ():
        if not any(name in (probe.installation_id, probe.client) for probe in selected):
            excluded = next((probe for probe in requested if name in (probe.installation_id, probe.client)), None)
            if excluded is not None:
                items.append(_refusal(excluded, f"excluded_by_scope:{scope}"))
    candidates: list[tuple[ClientProbe, Installation]] = []

    if report.command is None:
        for probe in selected:
            items.append(
                _skip(probe, "client_not_detected")
                if probe.status == STATUS_ABSENT and probe.installation_id not in (clients or ())
                else _refusal(probe, "no_durable_mcp_command")
            )
        return ConnectReport(
            workspace=str(resolved_workspace),
            dry_run=not apply,
            platform=report.platform,
            items=tuple(items),
            command=None,
            command_error=report.command_error,
            verification={"performed": False, "detail": "no_command"},
            first_handoffs=(),
            lock_wait_seconds=0.0,
            exit_code=EXIT_FAILED if any(item.state == STATE_REFUSED for item in items) else EXIT_OK,
            errors=tuple(errors),
        )

    # Reuse the one command discovery resolved: probing, preview, apply, and
    # verification must all describe the same executable.
    resolved_command = report.resolved_command

    expected = _expected_entries(
        catalogue,
        command=resolved_command,
        workspace=resolved_workspace,
        producer_id=pid,
    )

    named_clients = set(clients) if clients else set()

    for probe in selected:
        named_exactly = probe.installation_id in named_clients
        if probe.status == STATUS_ABSENT:
            # An unnamed client that is not installed is not a failure; an
            # explicitly requested one is.
            items.append(
                _refusal(probe, "client_not_detected")
                if named_exactly
                else _skip(probe, "client_not_detected")
            )
            continue
        if probe.status == STATUS_UNKNOWN:
            items.append(
                _refusal(probe, probe.reason or "unknown_evidence")
                if named_exactly
                else _skip(probe, probe.reason or "unknown_evidence")
            )
            continue
        if probe.entry_state == ENTRY_CONFLICT:
            items.append(_refusal(probe, "conflicting_agentbus_entry"))
            continue
        installation = by_id.get(probe.installation_id)
        if installation is None:  # pragma: no cover - manifests are consistent
            items.append(_refusal(probe, "manifest_missing"))
            continue
        if not auto_connect_allowed(probe, installation, named_clients):
            # Declarative policy: bulk runs must not wire clients that opted
            # out (e.g. Codex project scope) unless explicitly targeted.
            items.append(_skip(probe, "auto_connect_disabled"))
            continue
        if probe.scope == "user" and apply and not allow_user_scope:
            authorized = False
            # A prompt is only legitimate on a real terminal with human
            # output; JSON consumers and pipes must fail closed instead.
            if interactive and not json_output and confirm is not None:
                authorized = confirm(
                    f"Write AgentBus server config to user-scope file "
                    f"{probe.config_path}?"
                )
            if not authorized:
                reason = "user_scope_requires_authorization"
                items.append(_refusal(probe, reason) if named_exactly else _skip(probe, reason))
                continue
        candidates.append((probe, installation))

    # Plan every candidate before writing anything (fail closed, no partials).
    plans: list[tuple[ClientProbe, Installation, Patch]] = []
    planning_failed = False
    if report.command is not None:
        for probe, installation in candidates:
            if probe.config_path is None:  # pragma: no cover - probe guarantees
                items.append(_refusal(probe, "config_path_unknown"))
                planning_failed = True
                continue
            path = Path(probe.config_path)
            if installation.scope == "project" and _escapes_workspace(path, resolved_workspace):
                items.append(_refusal(probe, "project_path_escapes_workspace"))
                planning_failed = True
                continue
            entry = expected[probe.installation_id]
            try:
                patch = plan_patch(installation, path=path, entry=entry)
            except ConfigConflictError as exc:
                items.append(_refusal(probe, str(exc)))
                errors.append(str(exc))
                planning_failed = True
                continue
            plans.append((probe, installation, patch))

    if planning_failed:
        planned_ids = {probe.installation_id for probe, _i, _p in plans}
        for probe, _installation in candidates:
            if probe.installation_id in planned_ids:
                items.append(_refusal(probe, "aborted_by_conflict"))
        return ConnectReport(
            workspace=str(resolved_workspace),
            dry_run=not apply,
            platform=report.platform,
            items=tuple(items),
            command=report.command,
            command_error=report.command_error,
            verification={"performed": False, "detail": "conflict"},
            first_handoffs=(),
            lock_wait_seconds=0.0,
            exit_code=EXIT_FAILED,
            errors=tuple(errors),
        )

    def lock_failure_item(probe: ClientProbe, patch: Patch, reason: str) -> ConnectItem:
        if patch.changed:
            return _refusal(probe, reason)
        return ConnectItem(
            installation_id=probe.installation_id, title=probe.title, scope=probe.scope,
            path=str(patch.path), action=ACTION_UNCHANGED, state=STATE_CONFIGURED,
            reason="unchanged", configured=True, changed=False,
            approval={"mode": probe.approval_mode, "guidance": probe.approval_guidance},
            previous_entry=redact_entry(patch.previous_entry),
        )

    lock_wait = 0.0
    outcome = TransactionOutcome((), ())
    if apply and any(patch.changed for _, _, patch in plans):
        lock = setup_lock(resolved_workspace, timeout=lock_timeout)
        try:
            lock.acquire()
        except (SetupLockTimeout, SetupLockSecurityError) as exc:
            security = isinstance(exc, SetupLockSecurityError)
            reason = "setup_lock_security" if security else "setup_lock_timeout"
            for probe, _installation, patch in plans:
                items.append(lock_failure_item(probe, patch, reason))
            return ConnectReport(
                workspace=str(resolved_workspace),
                dry_run=False,
                platform=report.platform,
                items=tuple(items),
                command=report.command,
                command_error=report.command_error,
                verification={"performed": False, "detail": reason},
                first_handoffs=(),
                lock_wait_seconds=0.0,
                exit_code=EXIT_FAILED if security else EXIT_LOCK_TIMEOUT,
                errors=(str(exc),),
            )
        lock_wait = lock.waited
        user_lock: SetupLock | None = None
        try:
            # A workspace lock cannot serialize a shared user-scope file, so
            # distinct workspaces additionally take the global config lock.
            if any(
                installation.scope == "user" and patch.changed
                for _probe, installation, patch in plans
            ):
                home_root = Path(home) if home is not None else Path.home()
                try:
                    user_lock = global_config_lock(
                        home_root, timeout=lock_timeout
                    )
                    user_lock.acquire()
                except (SetupLockTimeout, SetupLockSecurityError) as exc:
                    security = isinstance(exc, SetupLockSecurityError)
                    reason = "global_config_lock_security" if security else "global_config_lock_timeout"
                    for probe, _installation, patch in plans:
                        items.append(lock_failure_item(probe, patch, reason))
                    return ConnectReport(
                        workspace=str(resolved_workspace),
                        dry_run=False,
                        platform=report.platform,
                        items=tuple(items),
                        command=report.command,
                        command_error=report.command_error,
                        verification={
                            "performed": False,
                            "detail": reason,
                        },
                        first_handoffs=(),
                        lock_wait_seconds=lock_wait,
                        exit_code=EXIT_FAILED if security else EXIT_LOCK_TIMEOUT,
                        errors=(str(exc),),
                    )
            # Re-read under the lock: another cooperative writer may have won.
            replanned: list[tuple[ClientProbe, Installation, Patch]] = []
            replan_error: ConfigConflictError | None = None
            for probe, installation, _patch in plans:
                path = Path(probe.config_path or "")
                try:
                    if installation.scope == "project" and _escapes_workspace(path, resolved_workspace):
                        raise ConfigConflictError("project_path_escapes_workspace", str(path))
                    fresh = plan_patch(
                        installation,
                        path=path,
                        entry=expected[probe.installation_id],
                    )
                except ConfigConflictError as exc:
                    replan_error = exc
                    break
                replanned.append((probe, installation, fresh))

            if replan_error is not None:
                errors.append(str(replan_error))
                fresh_by_id = {probe.installation_id: patch for probe, _, patch in replanned}
                for probe, _installation, patch in plans:
                    fresh = fresh_by_id.get(probe.installation_id)
                    items.append(lock_failure_item(probe, fresh, str(replan_error)) if fresh is not None else _refusal(probe, str(replan_error)))
                return ConnectReport(
                    workspace=str(resolved_workspace),
                    dry_run=False,
                    platform=report.platform,
                    items=tuple(items),
                    command=report.command,
                    command_error=report.command_error,
                    verification={"performed": False, "detail": "conflict"},
                    first_handoffs=(),
                    lock_wait_seconds=lock_wait,
                    exit_code=EXIT_FAILED,
                    errors=tuple(errors),
                )
            plans = replanned

            transaction = transaction_factory()
            changed_plans = [p for p in plans if p[2].changed]
            if changed_plans:
                for _probe, _installation, patch in changed_plans:
                    assert patch.new_bytes is not None
                    transaction.stage(
                        patch.path,
                        patch.new_bytes,
                        original_bytes=patch.original_bytes,
                    )
                outcome = transaction.commit()
        except SetupWriteError as exc:
            errors.append(str(exc))
            for probe, _installation, _patch in plans:
                items.append(
                    ConnectItem(
                        installation_id=probe.installation_id,
                        title=probe.title,
                        scope=probe.scope,
                        path=probe.config_path,
                        action=ACTION_ERROR,
                        state=STATE_ERROR,
                        reason=str(exc.cause),
                        approval={
                            "mode": probe.approval_mode,
                            "guidance": probe.approval_guidance,
                        },
                    )
                )
            return ConnectReport(
                workspace=str(resolved_workspace),
                dry_run=False,
                platform=report.platform,
                items=tuple(items),
                command=report.command,
                command_error=report.command_error,
                verification={"performed": False, "detail": "write_failed"},
                first_handoffs=(),
                lock_wait_seconds=lock_wait,
                exit_code=EXIT_FAILED,
                errors=tuple(errors),
            )
        finally:
            if user_lock is not None:
                user_lock.release()
            lock.release()

    backups = {str(path) for path in outcome.backups}
    for probe, _installation, patch in plans:
        previous = redact_entry(patch.previous_entry)
        if not apply:
            action = ACTION_UNCHANGED if not patch.changed else (
                ACTION_CREATE if not patch.exists else ACTION_UPDATE
            )
            state = STATE_PLANNED if patch.changed else STATE_CONFIGURED
            if patch.changed and patch.previous_entry is not None:
                # An existing but divergent entry is reported as ``differs``,
                # never as configured.
                state = STATE_DIFFERS
            items.append(
                ConnectItem(
                    installation_id=probe.installation_id,
                    title=probe.title,
                    scope=probe.scope,
                    path=str(patch.path),
                    action=action,
                    state=state,
                    reason=patch.reason,
                    configured=not patch.changed,
                    changed=patch.changed,
                    approval={
                        "mode": probe.approval_mode,
                        "guidance": probe.approval_guidance,
                    },
                    previous_entry=previous,
                )
            )
            continue
        backup = outcome.backup_map.get(str(patch.path))
        items.append(
            ConnectItem(
                installation_id=probe.installation_id,
                title=probe.title,
                scope=probe.scope,
                path=str(patch.path),
                action=ACTION_UNCHANGED
                if not patch.changed
                else (ACTION_CREATE if not patch.exists else ACTION_UPDATE),
                state=STATE_CONFIGURED if not patch.changed else STATE_AWAITING,
                reason=patch.reason,
                configured=True,
                changed=patch.changed,
                backup=backup if backup in backups else None,
                approval={
                    "mode": probe.approval_mode,
                    "guidance": probe.approval_guidance,
                },
                previous_entry=previous,
            )
        )

    verification: dict[str, Any] = {
        "performed": False,
        "ok": False,
        "detail": "not_requested",
        "command_handshake_verified": False,
        "host_client_observed": False,
        "limitations": (
            "A successful handshake proves the configured command speaks MCP; "
            "it does not prove the host client approved or loaded the server."
        ),
    }
    if verify and not apply:
        verification = {
            "performed": False,
            "ok": False,
            "detail": "requires_apply",
            "command_handshake_verified": False,
            "host_client_observed": False,
            "hint": "re-run with --apply --verify to handshake the written config",
        }
    elif verify:
        verification = _run_verification(
            workspace=resolved_workspace,
            items=items,
            expected=expected,
            environ=environ,
        )

    handoffs = tuple(
        FirstHandoff(
            installation_id=item.installation_id,
            recipient="codex",
            executed=False,
            prompt=_first_handoff_prompt(
                workspace=resolved_workspace,
                producer_id=pid,
                client=item.installation_id,
            ),
        )
        for item in items
        if item.state in (STATE_CONFIGURED, STATE_AWAITING)
        and item.scope == "project"
    )

    failures = [
        item
        for item in items
        if item.state in (STATE_REFUSED, STATE_ERROR)
    ]
    exit_code = EXIT_OK
    if failures:
        exit_code = EXIT_FAILED
    if (
        verify and apply and not failures
        and verification.get("detail") != "nothing_to_verify"
        and not verification.get("ok", False)
    ):
        exit_code = EXIT_VERIFY_FAILED
    if not apply and not failures:
        exit_code = EXIT_OK

    return ConnectReport(
        workspace=str(resolved_workspace),
        dry_run=not apply,
        platform=report.platform,
        items=tuple(items),
        command=report.command,
        command_error=report.command_error,
        verification=verification,
        first_handoffs=handoffs,
        lock_wait_seconds=lock_wait,
        exit_code=exit_code,
        errors=tuple(errors),
    )


def _run_verification(
    *,
    workspace: Path,
    items: list[ConnectItem],
    expected: Mapping[str, Mapping[str, Any]],
    environ: Mapping[str, str] | None,
) -> dict[str, Any]:
    if not (workspace / ".agentbus" / "events.db").is_file():
        return {
            "performed": False,
            "detail": "workspace_not_initialized",
            "hint": "run 'agentbus init --apply' before --verify",
        }
    results: list[dict[str, Any]] = []
    for item in items:
        if item.state not in (STATE_CONFIGURED, STATE_AWAITING):
            continue
        entry = expected.get(item.installation_id)
        if not entry:
            continue
        argv = [str(entry["command"]), *[str(a) for a in entry.get("args", [])]]
        base = os.environ if environ is None else environ
        # Match the command probe: the host client will not inherit this
        # shell's PYTHONPATH, so a handshake that only works with it would
        # certify a command the client cannot start.
        env = {
            str(key): str(value)
            for key, value in base.items()
            if str(key).upper() != "PYTHONPATH"
        }
        env.update({str(k): str(v) for k, v in (entry.get("env") or {}).items()})
        result = verify_mcp_handshake(argv, env=env, workspace=workspace)
        results.append({"installation": item.installation_id, **result.to_dict()})
        # A successful stdio handshake proves the *command* speaks MCP. It is
        # not evidence that the host client approved, loaded, or connected the
        # server, so the item state stays ``awaiting_approval``.
        item.command_verified = result.ok
        item.host_client_observed = False
        item.verification = result.to_dict()
    if not results:
        return {"performed": False, "ok": False, "detail": "nothing_to_verify",
                "command_handshake_verified": False, "host_client_observed": False}
    ok = all(entry["ok"] for entry in results)
    return {
        "performed": True,
        "ok": ok,
        "detail": "mcp_stdio_handshake",
        "command_handshake_verified": ok,
        "host_client_observed": False,
        "results": results,
        "limitations": (
            "Verification launches the configured command and completes an MCP "
            "initialize + tools/list exchange. Host approval, restart, and the "
            "client's own connection are still unobserved."
        ),
    }


# ---------------------------------------------------------------------------
# MCP stdio handshake verification
# ---------------------------------------------------------------------------

_PROTOCOL_VERSION = "2025-06-18"


@dataclass(frozen=True)
class HandshakeResult:
    ok: bool
    server_name: str | None = None
    server_version: str | None = None
    protocol_version: str | None = None
    tool_count: int | None = None
    tool_names: tuple[str, ...] = ()
    error: str | None = None
    duration_ms: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "server_name": self.server_name,
            "server_version": self.server_version,
            "protocol_version": self.protocol_version,
            "tool_count": self.tool_count,
            "tool_names": list(self.tool_names),
            "error": self.error,
            "duration_ms": self.duration_ms,
        }


def _terminate(proc: subprocess.Popen[str]) -> None:
    if proc.poll() is None:
        with contextlib.suppress(OSError):
            if os.name == "posix":
                os.killpg(proc.pid, 15)
            else:  # pragma: no cover - Windows path
                proc.terminate()
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            with contextlib.suppress(OSError):
                if os.name == "posix":
                    os.killpg(proc.pid, 9)
                else:  # pragma: no cover - Windows path
                    proc.kill()
            with contextlib.suppress(Exception):
                proc.wait(timeout=3)


def verify_mcp_handshake(
    argv: Sequence[str],
    *,
    env: Mapping[str, str] | None = None,
    cwd: Path | None = None,
    workspace: Path | None = None,
    timeout: float = 20.0,
) -> HandshakeResult:
    """Speak MCP over stdio: ``initialize`` then ``tools/list``, then reap."""
    if not argv:
        return HandshakeResult(False, error="empty argv")
    started = time.monotonic()
    env_map = dict(os.environ if env is None else env)
    try:
        proc = subprocess.Popen(
            list(argv),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            encoding="utf-8",
            bufsize=1,
            env=env_map,
            cwd=str(cwd or workspace) if (cwd or workspace) else None,
            start_new_session=(os.name == "posix"),
        )
    except OSError as exc:
        return HandshakeResult(
            False,
            error=f"spawn failed: {exc}",
            duration_ms=int((time.monotonic() - started) * 1000),
        )

    lines: "queue.Queue[str | None]" = queue.Queue()

    def _pump() -> None:
        assert proc.stdout is not None
        try:
            for line in proc.stdout:
                lines.put(line)
        except (OSError, ValueError):
            pass
        finally:
            lines.put(None)

    pump = threading.Thread(target=_pump, name="agentbus-mcp-verify", daemon=True)
    pump.start()
    deadline = started + timeout

    def _send(message: Mapping[str, Any]) -> None:
        assert proc.stdin is not None
        proc.stdin.write(json.dumps(message) + "\n")
        proc.stdin.flush()

    def _await(request_id: int) -> dict[str, Any]:
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"no response to request {request_id}")
            try:
                line = lines.get(timeout=min(remaining, 0.5))
            except queue.Empty:
                continue
            if line is None:
                raise RuntimeError(
                    f"server exited before responding (code {proc.poll()})"
                )
            text = line.strip()
            if not text:
                continue
            try:
                message = json.loads(text)
            except json.JSONDecodeError:
                continue  # tolerate stray non-JSON output
            if not isinstance(message, dict):
                continue
            if message.get("id") != request_id:
                continue
            if "error" in message:
                raise RuntimeError(
                    f"MCP error {message['error'].get('code')}: "
                    f"{message['error'].get('message')}"
                )
            result = message.get("result")
            return result if isinstance(result, dict) else {}

    try:
        _send(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": _PROTOCOL_VERSION,
                    "capabilities": {},
                    "clientInfo": {"name": "agentbus-verify", "version": "1"},
                },
            }
        )
        init = _await(1)
        _send({"jsonrpc": "2.0", "method": "notifications/initialized"})
        _send({"jsonrpc": "2.0", "id": 2, "method": "tools/list"})
        listing = _await(2)
        tools = listing.get("tools") or []
        names = tuple(
            str(tool.get("name"))
            for tool in tools
            if isinstance(tool, dict) and tool.get("name")
        )
        server = init.get("serverInfo") or {}
        return HandshakeResult(
            True,
            server_name=server.get("name"),
            server_version=server.get("version"),
            protocol_version=init.get("protocolVersion"),
            tool_count=len(names),
            tool_names=names,
            duration_ms=int((time.monotonic() - started) * 1000),
        )
    except Exception as exc:  # noqa: BLE001 - report every failure as data
        return HandshakeResult(
            False,
            error=f"{type(exc).__name__}: {exc}",
            duration_ms=int((time.monotonic() - started) * 1000),
        )
    finally:
        _terminate(proc)
        pump.join(timeout=2)
        for stream in (proc.stdin, proc.stdout):
            if stream is not None:
                with contextlib.suppress(OSError, ValueError):
                    stream.close()


# ---------------------------------------------------------------------------
# Human-readable rendering
# ---------------------------------------------------------------------------


def format_discovery(report: DiscoveryReport) -> str:
    lines = [
        f"Workspace: {report.workspace}",
        f"Platform: {report.platform}",
        f"Producer: {report.producer_id}",
    ]
    if report.command:
        argv = [str(report.command["command"]), *map(str, report.command["args"])]
        lines.append(f"MCP command: {' '.join(argv)}")
        lines.append(f"  source: {report.command['source']}")
        for warning in report.command.get("warnings") or []:
            lines.append(f"  warning: {warning}")
    else:
        lines.append("MCP command: unresolved")
        if report.command_error:
            lines.append(f"  {report.command_error['code']}: {report.command_error['detail']}")
    for warning in report.warnings:
        lines.append(f"Warning: {warning}")
    lines.append("")
    present = [probe for probe in report.probes if probe.status == STATUS_PRESENT]
    unknown = [probe for probe in report.probes if probe.status == STATUS_UNKNOWN]
    absent = [probe for probe in report.probes if probe.status == STATUS_ABSENT]
    lines.append(
        f"Clients: {len(present)} present, {len(unknown)} unknown, "
        f"{len(absent)} absent"
    )
    for probe in report.probes:
        entry = probe.entry_state
        if probe.status == STATUS_PRESENT and probe.managed:
            entry = "agentbus entry " + entry
        lines.append(
            f"  {probe.status:<7} {probe.installation_id:<20} {probe.config_path}"
            f"  [{probe.reason}; {entry}]"
        )
    return "\n".join(lines)


def format_connect(report: ConnectReport) -> str:
    if report.dry_run:
        mode = "preview (no writes)"
    elif report.exit_code != EXIT_OK:
        mode = "finished with errors"
    elif any(item.action in (ACTION_CREATE, ACTION_UPDATE) for item in report.items):
        mode = "applied"
    else:
        mode = "finished (no changes)"
    lines = [f"Connect {mode} for {report.workspace}"]
    if report.command:
        lines.append(
            "Command: "
            + " ".join(
                [str(report.command["command"]), *map(str, report.command["args"])]
            )
        )
    if report.command_error:
        lines.append(f"Command error: {report.command_error['code']}")
    for item in report.items:
        detail = f"  {item.state:<16} {item.installation_id:<20} {item.path or '-'}"
        if item.action != ACTION_UNCHANGED:
            detail += f"  [{item.action}: {item.reason}]"
        lines.append(detail)
        if item.backup:
            lines.append(f"    backup: {item.backup}")
        if item.state == STATE_AWAITING:
            lines.append(
                "    next: restart/reload the client, then approve the server if prompted"
            )
        if item.state == STATE_DIFFERS:
            lines.append(
                "    note: existing agentbus entry differs from the expected "
                "configuration"
            )
        if item.command_verified and item.verification:
            lines.append(
                "    command: MCP handshake completed "
                f"({item.verification.get('tool_count')} tools); host client "
                "connection/approval is still unobserved"
            )
        if item.state == STATE_REFUSED:
            lines.append(f"    refused: {item.reason}")
    verification = report.verification
    if verification.get("performed"):
        lines.append(
            "Verification: handshake "
            + ("succeeded" if verification.get("ok") else "failed")
            + " (host client approval is still unobserved)"
        )
    elif verification.get("detail") not in (None, "not_requested"):
        lines.append(f"Verification: not performed ({verification.get('detail')})")
    if report.first_handoffs:
        lines.append("")
        lines.append("First-handoff prompts (NOT executed; copy into the client):")
        for handoff in report.first_handoffs:
            lines.append(f"  {handoff.installation_id}:")
            lines.append(f"    {handoff.prompt}")
    if report.errors:
        lines.append("")
        for error in report.errors:
            lines.append(f"Error: {error}")
    return "\n".join(lines)
