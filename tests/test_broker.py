from __future__ import annotations

import json
import os
import pwd
import socket
import threading
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

