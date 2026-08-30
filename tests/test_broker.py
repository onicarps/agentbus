from __future__ import annotations

import json
import os
import pwd
import socket
import stat
import threading
import time
from pathlib import Path

import pytest

from agentbus.broker.protocol import (
    BrokerProtocolError,
    PROTOCOL_VERSION,
    encode_frame,
    recv_frame,
    send_frame,
)
from agentbus.broker.server import (
    BrokerApplication,
    BrokerServer,
    PeerCredentials,
)
from agentbus.ceremony import (
    export_policy_request,
    generate_enrollment_request,
    generate_offline_root,
    import_signed_policy,
    initialize_offline_identity,
    sign_policy_request,
)
from agentbus.identity import IdentityError, sign_event_envelope
from agentbus.rbac import ensure_default_roles


def _workspace(tmp_path: Path) -> tuple[Path, Path, str]:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    ensure_default_roles(workspace)
    root_key = tmp_path / "offline" / "root.pem"
    descriptor = tmp_path / "offline" / "root.json"
    generate_offline_root(root_key, descriptor)
    initialize_offline_identity(workspace, descriptor)

    username = pwd.getpwuid(os.geteuid()).pw_name
    peer_key = tmp_path / "peer" / "codex.pem"
    enrollment = tmp_path / "peer" / "codex.json"
    generate_enrollment_request(
        "codex",
        username,
        peer_key,
        enrollment,
        capabilities=("message", "implementation"),
    )
    request = tmp_path / "request.json"
    bundle = tmp_path / "offline" / "bundle.json"
    export_policy_request(workspace, request, enrollment_paths=(enrollment,))
    sign_policy_request(request, root_key, bundle)
    import_signed_policy(workspace, bundle)
    return workspace, peer_key, username


def _peer(username: str) -> PeerCredentials:
    entry = pwd.getpwnam(username)
    return PeerCredentials(os.getpid(), entry.pw_uid, entry.pw_gid, username, "users")


def test_broker_binds_kernel_principal_and_verified_signature(tmp_path: Path) -> None:
    workspace, peer_key, username = _workspace(tmp_path)
    app = BrokerApplication(workspace)
    payload = {"from": "codex", "to": "agy", "summary": "implementation ready"}
    envelope = sign_event_envelope(
        workspace,
        topic="okf/handoff",
        producer_id="codex",
        schema_version="1.0",
        payload=payload,
        action={"type": "implementation"},
        private_key_path=peer_key,
    )
    try:
        result = app.handle(
            _peer(username),
            "publish",
            {
                "topic": "okf/handoff",
                "producer_id": "codex",
                "schema_version": "1.0",
                "payload": payload,
                "identity_envelope": envelope,
                "action": {"type": "implementation"},
            },
        )
        assert result["duplicate"] is False
        assert result["event"]["producer_id"] == "codex"
        assert result["event"]["verification_status"] == "verified"
        fetched = app.handle(
            _peer(username), "get_event", {"event_id": result["event"]["event_id"]}
        )
        assert fetched["verified"] is True
    finally:
        app.close()


def test_broker_rejects_payload_identity_different_from_peer(tmp_path: Path) -> None:
    workspace, _peer_key, username = _workspace(tmp_path)
    app = BrokerApplication(workspace)
    try:
        with pytest.raises(IdentityError, match="peer_producer_mismatch"):
            app.handle(
                _peer(username),
                "publish",
                {
                    "topic": "okf/handoff",
                    "producer_id": "factory",
                    "schema_version": "1.0",
                    "payload": {
                        "from": "factory",
                        "to": "codex",
                        "summary": "forged verdict",
                    },
                },
            )
    finally:
        app.close()


def test_unregistered_peer_is_rejected_before_store_operation(tmp_path: Path) -> None:
    workspace, _peer_key, _username = _workspace(tmp_path)
    app = BrokerApplication(workspace)
    try:
        with pytest.raises(IdentityError, match="peer_principal_not_authorized"):
            app.handle(
                PeerCredentials(1, 424242, 424242, "agy-child", "agy-child"),
                "status",
                {},
            )
    finally:
        app.close()


def test_unix_server_uses_length_prefixed_request_response(tmp_path: Path) -> None:
    workspace, _peer_key, _username = _workspace(tmp_path)
    socket_path = tmp_path / "run" / "agentbus.sock"
    app = BrokerApplication(workspace)
    server = BrokerServer(socket_path, app)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.connect(str(socket_path))
            send_frame(
                client,
                {
                    "protocol_version": PROTOCOL_VERSION,
                    "request_id": "status-1",
                    "operation": "status",
                    "body": {},
                },
            )
            response = recv_frame(client)
        assert response["ok"] is True
        assert response["request_id"] == "status-1"
        assert response["result"]["workspace"] == str(workspace)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
    assert not socket_path.exists()


def test_protocol_rejects_duplicate_json_and_oversized_frames() -> None:
    left, right = socket.socketpair()
    try:
        raw = b'{"request_id":"a","request_id":"b"}'
        left.sendall(len(raw).to_bytes(4, "big") + raw)
        with pytest.raises(IdentityError, match="duplicate_json_key"):
            recv_frame(right)
    finally:
        left.close()
        right.close()

    with pytest.raises(BrokerProtocolError, match="frame_size"):
        encode_frame({"x": "y" * (4 * 1024 * 1024)})


def test_server_returns_stable_error_without_traceback(tmp_path: Path) -> None:
    workspace, _peer_key, _username = _workspace(tmp_path)
    socket_path = tmp_path / "run" / "agentbus.sock"
    server = BrokerServer(socket_path, BrokerApplication(workspace))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.connect(str(socket_path))
            send_frame(
                client,
                {
                    "protocol_version": PROTOCOL_VERSION,
                    "request_id": "bad-1",
                    "operation": "raw_sql",
                    "body": {"sql": "select * from events"},
                },
            )
            response = recv_frame(client)
        assert response == {
            "protocol_version": PROTOCOL_VERSION,
            "request_id": "bad-1",
            "ok": False,
            "error": {"code": "unsupported_broker_operation"},
        }
        assert "traceback" not in json.dumps(response).lower()
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_broker_socket_is_secure_at_bind_and_rejects_insecure_parent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace, _peer_key, _username = _workspace(tmp_path)
    socket_dir = tmp_path / "run"
    socket_path = socket_dir / "agentbus.sock"
    original_chmod = os.chmod
    observed_modes: list[int] = []

    def observe_chmod(
        path: os.PathLike[str] | str, mode: int, **kwargs: object
    ) -> None:
        if Path(path) == socket_path:
            observed_modes.append(stat.S_IMODE(os.lstat(path).st_mode))
        original_chmod(path, mode, **kwargs)

    monkeypatch.setattr(os, "chmod", observe_chmod)
    server = BrokerServer(socket_path, BrokerApplication(workspace))
    try:
        assert observed_modes == [0o660]
        assert stat.S_IMODE(os.lstat(socket_path).st_mode) == 0o660
    finally:
        server.server_close()

    insecure_dir = tmp_path / "insecure"
    insecure_dir.mkdir(mode=0o777)
    insecure_dir.chmod(0o777)
    app = BrokerApplication(workspace)
    with pytest.raises(IdentityError, match="broker_socket_directory_insecure_mode"):
        BrokerServer(insecure_dir / "agentbus.sock", app)
    app.close()


def test_broker_rejects_dangling_symlink_even_with_force(tmp_path: Path) -> None:
    workspace, _peer_key, _username = _workspace(tmp_path)
    socket_dir = tmp_path / "run"
    socket_dir.mkdir(mode=0o750)
    socket_path = socket_dir / "agentbus.sock"
    socket_path.symlink_to(tmp_path / "attacker" / "redirected.sock")

    for force in (False, True):
        app = BrokerApplication(workspace)
        expected = (
            "broker_socket_already_exists"
            if not force
            else "broker_socket_reclaim_not_socket"
        )
        with pytest.raises(IdentityError, match=expected):
            BrokerServer(socket_path, app, force=force)
        app.close()


def test_broker_force_reclaims_only_stale_owned_socket(tmp_path: Path) -> None:
    workspace, _peer_key, _username = _workspace(tmp_path)
    socket_path = tmp_path / "run" / "agentbus.sock"
    first = BrokerServer(socket_path, BrokerApplication(workspace))
    thread = threading.Thread(target=first.serve_forever, daemon=True)
    thread.start()
    try:
        app = BrokerApplication(workspace)
        with pytest.raises(IdentityError, match="broker_socket_in_use"):
            BrokerServer(socket_path, app, force=True)
        app.close()
    finally:
        first.shutdown()
        # Simulate an unclean exit by closing the descriptor without unlinking.
        socketserver_close = super(BrokerServer, first).server_close
        socketserver_close()
        first.application.close()
        thread.join(timeout=2)

    replacement = BrokerServer(
        socket_path, BrokerApplication(workspace), force=True
    )
    replacement.server_close()


def test_broker_bounds_idle_connections_and_times_them_out(tmp_path: Path) -> None:
    workspace, _peer_key, _username = _workspace(tmp_path)
    socket_path = tmp_path / "run" / "agentbus.sock"
    server = BrokerServer(
        socket_path,
        BrokerApplication(workspace),
        read_timeout_seconds=0.05,
        max_connections=2,
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    clients: list[socket.socket] = []
    try:
        for _ in range(8):
            client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            client.connect(str(socket_path))
            clients.append(client)
        time.sleep(0.15)
        assert server._connection_slots._value == 2
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.connect(str(socket_path))
            send_frame(
                client,
                {
                    "protocol_version": PROTOCOL_VERSION,
                    "request_id": "status-after-idle",
                    "operation": "status",
                    "body": {},
                },
            )
            assert recv_frame(client)["ok"] is True
    finally:
        for client in clients:
            client.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
