"""Event transports for direct development stores and the isolated broker."""

from __future__ import annotations

import os
import socket
import stat
import uuid
from pathlib import Path
from typing import Any, Protocol

from agentbus.artifacts import extract_artifacts
from agentbus.broker.protocol import PROTOCOL_VERSION, recv_frame, send_frame
from agentbus.identity import (
    IdentityError,
    VerificationResult,
    configured as identity_configured,
    load_trust_state,
    sign_event_envelope,
)
from agentbus.store import Event, EventStore

DEFAULT_BROKER_SOCKET = Path("/run/agentbus/agentbus.sock")
DEFAULT_BROKER_TIMEOUT_SECONDS = 30.0


class BrokerTransportError(IdentityError):
    """Stable client-side broker transport failure."""


class EventTransport(Protocol):
    """Authoritative event operations shared by SDK, MCP, and CLI callers."""

    def publish(self, **kwargs: Any) -> tuple[Event, bool]: ...

    def poll(self, topic: str, since_id: int = 0, limit: int = 50) -> dict: ...

    def get_event(self, event_id: int) -> Event | None: ...

    def verify_event(self, event_id: int) -> VerificationResult: ...

    def status(self, producer_id: str | None = None) -> dict: ...

    def close(self) -> None: ...


def _event_from_dict(value: Any) -> Event:
    if not isinstance(value, dict):
        raise BrokerTransportError("invalid_broker_event")
    required = {
        "event_id",
        "topic",
        "producer_id",
        "timestamp",
        "schema_version",
        "payload",
        "causation_id",
        "idempotency_key",
        "status",
    }
    if not required.issubset(value):
        raise BrokerTransportError("invalid_broker_event")
    fields = Event.__dataclass_fields__
    return Event(**{name: value[name] for name in fields if name in value})


class BrokerTransport:
    """One-request-per-connection client for the framed Unix socket protocol."""

    def __init__(
        self,
        workspace: Path,
        socket_path: Path | str,
        *,
        timeout_seconds: float = DEFAULT_BROKER_TIMEOUT_SECONDS,
    ) -> None:
        if os.name != "posix" or not hasattr(socket, "AF_UNIX"):
            raise BrokerTransportError("broker_transport_unavailable_on_platform")
        if timeout_seconds <= 0:
            raise ValueError("broker_timeout_must_be_positive")
        self.workspace = workspace.resolve()
        self.socket_path = Path(socket_path)
        self.timeout_seconds = float(timeout_seconds)

    def request(self, operation: str, body: dict[str, Any]) -> Any:
        request_id = uuid.uuid4().hex
        request = {
            "protocol_version": PROTOCOL_VERSION,
            "request_id": request_id,
            "operation": operation,
            "body": body,
        }
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as conn:
                conn.settimeout(self.timeout_seconds)
                conn.connect(os.fspath(self.socket_path))
                send_frame(conn, request)
                response = recv_frame(conn)
        except (OSError, TimeoutError) as exc:
            raise BrokerTransportError("broker_unavailable") from exc

        if response.get("protocol_version") != PROTOCOL_VERSION:
            raise BrokerTransportError("invalid_broker_response_protocol")
        if response.get("request_id") != request_id:
            raise BrokerTransportError("broker_response_request_id_mismatch")
        ok = response.get("ok")
        if ok is True and set(response) == {
            "protocol_version",
            "request_id",
            "ok",
            "result",
        }:
            return response["result"]
        if ok is False and set(response) == {
            "protocol_version",
            "request_id",
            "ok",
            "error",
        }:
            error = response.get("error")
            code = error.get("code") if isinstance(error, dict) else None
            if isinstance(code, str) and code:
                raise BrokerTransportError(code)
        raise BrokerTransportError("invalid_broker_response")

    def publish(self, **kwargs: Any) -> tuple[Event, bool]:
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
            "auto_sign",
            "signing_key_path",
            # Direct-store-only compatibility arguments. They may not weaken
            # broker policy, so reject non-default uses below.
            "auth_token",
            "status",
            "pending_until",
            "skip_intercept",
            "skip_rbac",
        }
        if set(kwargs) - allowed:
            raise TypeError("unsupported_broker_publish_argument")
        for name in ("status", "pending_until"):
            if kwargs.get(name) is not None:
                raise BrokerTransportError("broker_publish_direct_override_refused")
        if kwargs.get("auth_token") is not None:
            raise BrokerTransportError("broker_publish_auth_token_refused")
        for name in ("skip_intercept", "skip_rbac"):
            if kwargs.get(name):
                raise BrokerTransportError("broker_publish_direct_override_refused")

        topic = str(kwargs["topic"])
        producer_id = str(kwargs["producer_id"])
        schema_version = str(kwargs.get("schema_version") or "1.0")
        payload = kwargs["payload"]
        if not isinstance(payload, dict):
            raise ValueError("invalid_broker_publish_payload")
        envelope = kwargs.get("identity_envelope")
        if envelope is None and kwargs.get("auto_sign", True):
            stored_payload, artifacts = extract_artifacts(payload)
            envelope = sign_event_envelope(
                self.workspace,
                topic=topic,
                producer_id=producer_id,
                schema_version=schema_version,
                payload=stored_payload,
                artifacts=artifacts,
                causation_id=kwargs.get("causation_id"),
                idempotency_key=kwargs.get("idempotency_key"),
                trace_id=kwargs.get("trace_id"),
                action=kwargs.get("action"),
                private_key_path=kwargs.get("signing_key_path"),
            )
        body = {
            "topic": topic,
            "producer_id": producer_id,
            "schema_version": schema_version,
            "payload": payload,
        }
        optional = (
            "causation_id",
            "idempotency_key",
            "sla_timeout_minutes",
            "trace_id",
            "parent_span_id",
            "action",
        )
        body.update({name: kwargs[name] for name in optional if kwargs.get(name) is not None})
        if envelope is not None:
            body["identity_envelope"] = envelope
        result = self.request("publish", body)
        if not isinstance(result, dict) or not isinstance(result.get("duplicate"), bool):
            raise BrokerTransportError("invalid_broker_publish_result")
        return _event_from_dict(result.get("event")), result["duplicate"]

    def poll(self, topic: str, since_id: int = 0, limit: int = 50) -> dict:
        result = self.request(
            "poll", {"topic": topic, "since_id": since_id, "limit": limit}
        )
        if not isinstance(result, dict):
            raise BrokerTransportError("invalid_broker_poll_result")
        return result

    def get_event(self, event_id: int) -> Event | None:
        result = self.request("get_event", {"event_id": event_id})
        if not isinstance(result, dict):
            raise BrokerTransportError("invalid_broker_get_event_result")
        event = result.get("event")
        return _event_from_dict(event) if event is not None else None

    def get_verified_event(self, event_id: int) -> tuple[Event | None, VerificationResult]:
        result = self.request("get_event", {"event_id": event_id})
        if not isinstance(result, dict) or not isinstance(result.get("verified"), bool):
            raise BrokerTransportError("invalid_broker_get_event_result")
        event_value = result.get("event")
        event = _event_from_dict(event_value) if event_value is not None else None
        return event, VerificationResult(
            result["verified"],
            event.producer_id if event is not None else None,
            None,
            None,
            result.get("reason"),
        )

    def verify_event(self, event_id: int) -> VerificationResult:
        result = self.request("verify_event", {"event_id": event_id})
        if not isinstance(result, dict) or not isinstance(result.get("verified"), bool):
            raise BrokerTransportError("invalid_broker_verify_result")
        return VerificationResult(
            result["verified"],
            result.get("producer_id"),
            result.get("key_id"),
            result.get("policy_version"),
            result.get("reason"),
        )

    def status(self, producer_id: str | None = None) -> dict:
        # The authenticated kernel principal determines producer identity.
        del producer_id
        result = self.request("status", {})
        if not isinstance(result, dict):
            raise BrokerTransportError("invalid_broker_status_result")
        return result

    def close(self) -> None:
        """Connections are request-scoped; retained for transport parity."""

    def _unsupported(self, operation: str, **kwargs: Any) -> Any:
        del kwargs
        raise BrokerTransportError(f"broker_operation_unsupported:{operation}")

    def review_pending(self, topic: str | None = None, limit: int = 50) -> Any:
        return self._unsupported("review_pending", topic=topic, limit=limit)

    def approve_event(self, event_id: int, **kwargs: Any) -> Any:
        return self._unsupported("approve_event", event_id=event_id, **kwargs)

    def reject_event(self, event_id: int, **kwargs: Any) -> Any:
        return self._unsupported("reject_event", event_id=event_id, **kwargs)

    def list_active_slas(self) -> Any:
        return self._unsupported("list_active_slas")

    def _clear_sla(self, event_id: int) -> Any:
        return self._unsupported("clear_sla", event_id=event_id)

    def project_handoffs(self, **kwargs: Any) -> Any:
        return self._unsupported("project_handoffs", **kwargs)

    def fetch_trace_events(self, trace_id: str) -> Any:
        return self._unsupported("fetch_trace_events", trace_id=trace_id)

    def fetch_unprojected_handoffs(self, limit: int = 100) -> Any:
        return self._unsupported("fetch_unprojected_handoffs", limit=limit)

    def set_mcpsafe(self, enforcer: Any) -> Any:
        return self._unsupported("set_mcpsafe", enforcer=enforcer)


def broker_socket_for_workspace(workspace: Path) -> Path | None:
    """Select the broker without ever treating failure as fallback permission."""
    configured_path = (os.environ.get("AGENTBUS_BROKER_SOCKET") or "").strip()
    if configured_path:
        return Path(configured_path)
    if identity_configured(workspace):
        state = load_trust_state(workspace, update_high_water=False)
        if state.mode == "strict":
            raise BrokerTransportError("broker_socket_required_in_strict_mode")
    try:
        mode = os.lstat(DEFAULT_BROKER_SOCKET).st_mode
    except OSError:
        return None
    return DEFAULT_BROKER_SOCKET if stat.S_ISSOCK(mode) else None


def open_event_transport(
    workspace: Path,
    *,
    retention_days: int = 7,
    auto_prune: bool = True,
) -> EventTransport:
    """Open the required broker transport or an audit/development store."""
    socket_path = broker_socket_for_workspace(workspace)
    if socket_path is not None:
        return BrokerTransport(workspace, socket_path)
    return EventStore(workspace, retention_days=retention_days, auto_prune=auto_prune)
