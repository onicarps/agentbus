"""Authenticated Unix-domain AgentBus broker server."""

from __future__ import annotations

import grp
import hashlib
import os
import pwd
import socket
import socketserver
import stat
import struct
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from agentbus.broker.protocol import (
    BrokerProtocolError,
    PROTOCOL_VERSION,
    recv_frame,
    send_frame,
    validate_request,
)
from agentbus.ceremony import import_signed_policy
from agentbus.identity import (
    IdentityError,
    configured as identity_configured,
    load_trust_state,
)
from agentbus.store import EventStore


_BIND_UMASK_LOCK = threading.Lock()
DEFAULT_READ_TIMEOUT_SECONDS = 30.0
DEFAULT_MAX_CONNECTIONS = 64


@dataclass(frozen=True)
class PeerCredentials:
    pid: int
    uid: int
    gid: int
    username: str
    groupname: str


def peer_credentials(conn: socket.socket) -> PeerCredentials:
    if not hasattr(socket, "SO_PEERCRED"):
        raise BrokerProtocolError("peer_credentials_unavailable")
    size = struct.calcsize("3i")
    pid, uid, gid = struct.unpack(
        "3i", conn.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, size)
    )
    try:
        username = pwd.getpwuid(uid).pw_name
        groupname = grp.getgrgid(gid).gr_name
    except KeyError as exc:
        raise BrokerProtocolError("peer_principal_unresolvable") from exc
    return PeerCredentials(pid, uid, gid, username, groupname)


class BrokerApplication:
    """Policy/authentication boundary around one serialized EventStore."""

    def __init__(self, workspace: Path, *, broker_uid: int | None = None) -> None:
        self.workspace = workspace.resolve()
        self.broker_uid = os.geteuid() if broker_uid is None else broker_uid
        self.store = EventStore(self.workspace, auto_prune=False)
        self._lock = threading.RLock()

    def close(self) -> None:
        self.store.close()

    def _binding(self, peer: PeerCredentials) -> tuple[str, dict]:
        if not identity_configured(self.workspace):
            raise IdentityError("identity_not_configured")
        state = load_trust_state(self.workspace, update_high_water=False)
        principals = state.policy.get("principals") or {}
        if not isinstance(principals, dict):
            raise IdentityError("invalid_principal_policy")
        matches = [
            (str(producer), binding)
            for producer, binding in principals.items()
            if isinstance(binding, dict)
            and binding.get("username") == peer.username
        ]
        if len(matches) != 1:
            raise IdentityError("peer_principal_not_authorized")
        return matches[0]

    @staticmethod
    def _require_fields(body: dict, allowed: set[str], required: set[str]) -> None:
        unknown = set(body) - allowed
        missing = required - set(body)
        if unknown or missing:
            raise BrokerProtocolError("invalid_broker_operation_fields")

    def handle(self, peer: PeerCredentials, operation: str, body: dict) -> Any:
        producer, _binding = self._binding(peer)
        with self._lock:
            if operation == "publish":
                allowed = {
                    "topic",
                    "producer_id",
                    "schema_version",
                    "payload",
                    "causation_id",
                    "idempotency_key",
                    "sla_timeout_minutes",
                    "trace_id",
                    "parent_span_id",
                    "identity_envelope",
                    "action",
                }
                self._require_fields(
                    body, allowed, {"topic", "producer_id", "schema_version", "payload"}
                )
                if body["producer_id"] != producer:
                    raise IdentityError("peer_producer_mismatch")
                event, duplicate = self.store.publish(
                    topic=body["topic"],
                    producer_id=producer,
                    schema_version=body["schema_version"],
                    payload=body["payload"],
                    causation_id=body.get("causation_id"),
                    idempotency_key=body.get("idempotency_key"),
                    sla_timeout_minutes=body.get("sla_timeout_minutes"),
                    trace_id=body.get("trace_id"),
                    parent_span_id=body.get("parent_span_id"),
                    identity_envelope=body.get("identity_envelope"),
                    action=body.get("action"),
                    auto_sign=False,
                )
                return {"event": event.to_dict(), "duplicate": duplicate}
            if operation == "poll":
                self._require_fields(body, {"topic", "since_id", "limit"}, {"topic"})
                return self.store.poll(
                    str(body["topic"]),
                    since_id=int(body.get("since_id", 0)),
                    limit=int(body.get("limit", 50)),
                )
            if operation == "get_event":
                self._require_fields(body, {"event_id"}, {"event_id"})
                event, verification = self.store.get_verified_event(
                    int(body["event_id"])
                )
                return {
                    "event": event.to_dict() if event is not None else None,
                    "verified": verification.verified,
                    "reason": verification.reason,
                }
            if operation == "verify_event":
                self._require_fields(body, {"event_id"}, {"event_id"})
                result = self.store.verify_event(int(body["event_id"]))
                return {
                    "verified": result.verified,
                    "producer_id": result.producer_id,
                    "key_id": result.key_id,
                    "policy_version": result.policy_version,
                    "reason": result.reason,
                }
            if operation == "status":
                self._require_fields(body, set(), set())
                return self.store.status(producer_id=producer)
            if operation == "wake_notify":
                self._require_fields(body, {"event_id"}, {"event_id"})
                event, verification = self.store.get_verified_event(
                    int(body["event_id"])
                )
                if event is None or not verification.verified:
                    raise IdentityError("wake_event_not_verified")
                return {"accepted": True, "event_id": event.event_id}
            if operation == "admin_import":
                self._require_fields(body, {"bundle_path"}, {"bundle_path"})
                if producer != "identity-admin":
                    raise IdentityError("identity_admin_principal_required")
                candidate = Path(str(body["bundle_path"])).resolve()
                staging = (self.workspace / ".agentbus" / "identity" / "incoming").resolve()
                if staging not in candidate.parents:
                    raise IdentityError("admin_bundle_outside_staging")
                return import_signed_policy(self.workspace, candidate)
        raise BrokerProtocolError("unsupported_broker_operation")

    def receipt(self, socket_path: Path) -> dict:
        """Return broker-local facts; ABUS-021-004 will validate them externally."""
        binary = Path(__file__).resolve()
        return {
            "protocol_version": PROTOCOL_VERSION,
            "workspace": str(self.workspace),
            "broker_uid": self.broker_uid,
            "socket_path": str(socket_path),
            "broker_module_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
        }


class _BrokerHandler(socketserver.BaseRequestHandler):
    server: "BrokerServer"

    def handle(self) -> None:
        request_id = "unknown"
        try:
            self.request.settimeout(self.server.read_timeout_seconds)
            peer = peer_credentials(self.request)
            frame = recv_frame(self.request)
            request_id, operation, body = validate_request(frame)
            result = self.server.application.handle(peer, operation, body)
            response = {
                "protocol_version": PROTOCOL_VERSION,
                "request_id": request_id,
                "ok": True,
                "result": result,
            }
        except Exception as exc:
            if isinstance(exc, (IdentityError, BrokerProtocolError, ValueError)):
                code = str(exc) or type(exc).__name__
            else:
                code = "broker_internal_error"
            response = {
                "protocol_version": PROTOCOL_VERSION,
                "request_id": request_id,
                "ok": False,
                "error": {"code": code[:256]},
            }
        try:
            send_frame(self.request, response)
        except OSError:
            return


class BrokerServer(socketserver.ThreadingUnixStreamServer):
    daemon_threads = True
    allow_reuse_address = False

    def __init__(
        self,
        socket_path: Path,
        application: BrokerApplication,
        *,
        force: bool = False,
        read_timeout_seconds: float = DEFAULT_READ_TIMEOUT_SECONDS,
        max_connections: int = DEFAULT_MAX_CONNECTIONS,
    ) -> None:
        if read_timeout_seconds <= 0:
            raise ValueError("broker_read_timeout_must_be_positive")
        if max_connections <= 0:
            raise ValueError("broker_max_connections_must_be_positive")
        # Resolve the containing directory separately. Resolving the complete path
        # would follow an attacker-planted symlink in the socket leaf.
        raw_path = Path(os.path.abspath(os.fspath(socket_path)))
        self.socket_path = raw_path.parent.resolve() / raw_path.name
        self.application = application
        self.socket_path.parent.mkdir(parents=True, mode=0o750, exist_ok=True)
        self._validate_socket_directory()
        self._prepare_socket_path(force=force)
        self.read_timeout_seconds = float(read_timeout_seconds)
        self.max_connections = int(max_connections)
        self._connection_slots = threading.BoundedSemaphore(self.max_connections)

        # Unix sockets are created from mode 0777. Apply 0117 at bind time so the
        # path is 0660 from its first observable instant, with no bind/chmod gap.
        with _BIND_UMASK_LOCK:
            previous_umask = os.umask(0o117)
            try:
                super().__init__(str(self.socket_path), _BrokerHandler)
            finally:
                os.umask(previous_umask)
        os.chmod(self.socket_path, 0o660)

    def _validate_socket_directory(self) -> None:
        try:
            parent_stat = os.lstat(self.socket_path.parent)
        except OSError as exc:
            raise IdentityError("broker_socket_directory_invalid") from exc
        if not stat.S_ISDIR(parent_stat.st_mode):
            raise IdentityError("broker_socket_directory_invalid")
        if parent_stat.st_uid != os.geteuid():
            raise IdentityError("broker_socket_directory_wrong_owner")
        if stat.S_IMODE(parent_stat.st_mode) & 0o022:
            raise IdentityError("broker_socket_directory_insecure_mode")

    def _prepare_socket_path(self, *, force: bool) -> None:
        if not os.path.lexists(self.socket_path):
            return
        try:
            before = os.lstat(self.socket_path)
        except OSError as exc:
            raise IdentityError("broker_socket_inspection_failed") from exc
        if not force:
            raise IdentityError("broker_socket_already_exists")
        if not stat.S_ISSOCK(before.st_mode):
            raise IdentityError("broker_socket_reclaim_not_socket")
        if before.st_uid != os.geteuid():
            raise IdentityError("broker_socket_reclaim_wrong_owner")

        probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        probe.settimeout(0.25)
        try:
            probe.connect(str(self.socket_path))
        except ConnectionRefusedError:
            pass
        except FileNotFoundError:
            return
        except OSError as exc:
            # Timeouts, permission errors, and other ambiguous outcomes must not
            # authorize unlinking a possibly live security boundary.
            raise IdentityError("broker_socket_reclaim_probe_failed") from exc
        else:
            raise IdentityError("broker_socket_in_use")
        finally:
            probe.close()

        try:
            after = os.lstat(self.socket_path)
        except FileNotFoundError:
            return
        except OSError as exc:
            raise IdentityError("broker_socket_inspection_failed") from exc
        if (
            not stat.S_ISSOCK(after.st_mode)
            or after.st_uid != os.geteuid()
            or (after.st_dev, after.st_ino) != (before.st_dev, before.st_ino)
        ):
            raise IdentityError("broker_socket_changed_during_reclaim")
        self.socket_path.unlink()

    def process_request(self, request: socket.socket, client_address: Any) -> None:
        if not self._connection_slots.acquire(blocking=False):
            request.close()
            return
        try:
            super().process_request(request, client_address)
        except Exception:
            self._connection_slots.release()
            raise

    def process_request_thread(
        self, request: socket.socket, client_address: Any
    ) -> None:
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._connection_slots.release()

    def server_close(self) -> None:
        try:
            super().server_close()
        finally:
            try:
                self.socket_path.unlink()
            except FileNotFoundError:
                pass
            self.application.close()


def run_broker(workspace: Path, socket_path: Path, *, force: bool = False) -> None:
    application = BrokerApplication(workspace)
    try:
        server = BrokerServer(socket_path, application, force=force)
    except Exception:
        application.close()
        raise
    try:
        server.serve_forever(poll_interval=0.25)
    finally:
        server.server_close()
