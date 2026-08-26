"""Versioned, length-prefixed AgentBus broker protocol."""

from __future__ import annotations

import json
import socket
import struct
from typing import Any

from agentbus.identity import IdentityError, strict_json_loads, validate_jcs_value

PROTOCOL_VERSION = "1"
MAX_FRAME_BYTES = 4 * 1024 * 1024
HEADER = struct.Struct("!I")


class BrokerProtocolError(IdentityError):
    """A stable protocol-boundary error."""


def _read_exact(sock: socket.socket, size: int) -> bytes:
    chunks: list[bytes] = []
    remaining = size
    while remaining:
        chunk = sock.recv(remaining)
        if not chunk:
            raise BrokerProtocolError("truncated_broker_frame")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def recv_frame(sock: socket.socket, *, max_bytes: int = MAX_FRAME_BYTES) -> dict:
    raw_size = _read_exact(sock, HEADER.size)
    (size,) = HEADER.unpack(raw_size)
    if size < 2 or size > max_bytes:
        raise BrokerProtocolError("invalid_broker_frame_size")
    value = strict_json_loads(_read_exact(sock, size))
    if not isinstance(value, dict):
        raise BrokerProtocolError("broker_frame_must_be_object")
    return value


def encode_frame(value: dict[str, Any], *, max_bytes: int = MAX_FRAME_BYTES) -> bytes:
    validate_jcs_value(value)
    raw = json.dumps(
        value, ensure_ascii=False, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")
    if len(raw) < 2 or len(raw) > max_bytes:
        raise BrokerProtocolError("invalid_broker_frame_size")
    return HEADER.pack(len(raw)) + raw


def send_frame(sock: socket.socket, value: dict[str, Any]) -> None:
    sock.sendall(encode_frame(value))


def validate_request(value: dict[str, Any]) -> tuple[str, str, dict]:
    if set(value) != {"protocol_version", "request_id", "operation", "body"}:
        raise BrokerProtocolError("invalid_broker_request_fields")
    if value.get("protocol_version") != PROTOCOL_VERSION:
        raise BrokerProtocolError("unsupported_broker_protocol")
    request_id = value.get("request_id")
    operation = value.get("operation")
    body = value.get("body")
    if not isinstance(request_id, str) or not request_id or len(request_id) > 128:
        raise BrokerProtocolError("invalid_broker_request_id")
    if not isinstance(operation, str) or not operation or len(operation) > 64:
        raise BrokerProtocolError("invalid_broker_operation")
    if not isinstance(body, dict):
        raise BrokerProtocolError("invalid_broker_request_body")
    return request_id, operation, body

