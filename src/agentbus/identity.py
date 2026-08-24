"""AgentID canonical envelopes, trust policy, and Ed25519 key lifecycle.

The event database is deliberately not trusted by this module.  Callers pass the
stored payload and hydrated artifacts back through :func:`verify_envelope` before
using a restricted event.  Strict enforcement belongs to the isolated broker;
the local key files implemented here are the audit/development signer boundary.
"""

from __future__ import annotations

import base64
import hashlib
import json
import math
import os
import secrets
import tempfile
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

import rfc8785
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)


IDENTITY_DIR = ".agentbus/identity"
ENVELOPE_VERSION = "1"
MODE_ORDER = {"audit": 0, "protected": 1, "strict": 2}
DEFAULT_RESTRICTED_TOPICS = ("okf/handoff", "okf/approval", "system/")
MAX_SAFE_INTEGER = (1 << 53) - 1
_UNSET = object()


class IdentityError(ValueError):
    """Base error for invalid AgentID state or envelopes."""


class IdentityNotConfigured(IdentityError):
    """The workspace has no AgentID trust root yet."""


class VerificationError(IdentityError):
    """Cryptographic or policy verification failed."""


@dataclass(frozen=True)
class TrustState:
    workspace_id: str
    mode: str
    policy_version: int
    registry_version: int
    restricted_topics: tuple[str, ...]
    policy: dict[str, Any]
    registry: dict[str, Any]


@dataclass(frozen=True)
class VerificationResult:
    verified: bool
    producer_id: str | None
    key_id: str | None
    policy_version: int | None
    reason: str | None = None


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _unb64(value: str) -> bytes:
    if not isinstance(value, str) or not value:
        raise VerificationError("invalid_base64url")
    try:
        return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
    except Exception as exc:
        raise VerificationError("invalid_base64url") from exc


def _reject_duplicate(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise IdentityError(f"duplicate_json_key: {key}")
        result[key] = value
    return result


def strict_json_loads(raw: str | bytes) -> Any:
    """Decode JSON without lossy or ambiguous inputs."""
    if isinstance(raw, bytes):
        try:
            raw = raw.decode("utf-8", errors="strict")
        except UnicodeDecodeError as exc:
            raise IdentityError("invalid_utf8") from exc
    try:
        value = json.loads(
            raw,
            object_pairs_hook=_reject_duplicate,
            parse_constant=lambda token: (_ for _ in ()).throw(
                IdentityError(f"non_finite_number: {token}")
            ),
        )
    except json.JSONDecodeError as exc:
        raise IdentityError(f"invalid_json: {exc.msg}") from exc
    validate_jcs_value(value)
    return value


def validate_jcs_value(value: Any, *, path: str = "$") -> None:
    """Enforce the common Python/Node/Go AgentID JSON input domain."""
    if value is None or isinstance(value, bool):
        return
    if isinstance(value, str):
        if unicodedata.normalize("NFC", value) != value:
            raise IdentityError(f"non_nfc_string: {path}")
        try:
            value.encode("utf-8", errors="strict")
        except UnicodeEncodeError as exc:
            raise IdentityError(f"invalid_unicode_scalar: {path}") from exc
        return
    if isinstance(value, int):
        if abs(value) > MAX_SAFE_INTEGER:
            raise IdentityError(f"unsafe_integer: {path}")
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise IdentityError(f"non_finite_number: {path}")
        return
    if isinstance(value, list):
        for index, child in enumerate(value):
            validate_jcs_value(child, path=f"{path}[{index}]")
        return
    if isinstance(value, dict):
        for key, child in value.items():
            if not isinstance(key, str):
                raise IdentityError(f"non_string_key: {path}")
            validate_jcs_value(key, path=f"{path}.<key>")
            validate_jcs_value(child, path=f"{path}.{key}")
        return
    raise IdentityError(f"unsupported_json_type: {path}={type(value).__name__}")


def canonical_bytes(value: Any) -> bytes:
    validate_jcs_value(value)
    try:
        return rfc8785.dumps(value)
    except (ValueError, TypeError) as exc:
        raise IdentityError(f"jcs_error: {exc}") from exc


def _sha256_jcs(value: Any) -> str:
    return "sha256:" + hashlib.sha256(canonical_bytes(value)).hexdigest()


def _identity_dir(workspace: Path) -> Path:
    # EventStore's Windows-concurrency test patches ``os.name`` after creating
    # a PosixPath. Re-instantiating it through Path would incorrectly select
    # WindowsPath on Linux; preserve an existing concrete path object.
    base = workspace if isinstance(workspace, Path) else Path(workspace)
    return base.resolve() / IDENTITY_DIR


def _atomic_json(path: Path, value: Any, *, mode: int = 0o644) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(tmp_name, mode)
        os.replace(tmp_name, path)
    finally:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass


def _private_bytes(key: Ed25519PrivateKey) -> bytes:
    return key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def _public_raw(key: Ed25519PublicKey) -> bytes:
    return key.public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    )


def _load_private(path: Path) -> Ed25519PrivateKey:
    try:
        key = serialization.load_pem_private_key(path.read_bytes(), password=None)
    except (OSError, ValueError, TypeError) as exc:
        raise IdentityError(f"invalid_private_key: {path}") from exc
    if not isinstance(key, Ed25519PrivateKey):
        raise IdentityError(f"not_ed25519_private_key: {path}")
    return key


def _signed_document(value: dict[str, Any], key: Ed25519PrivateKey) -> dict[str, Any]:
    return {"signed": value, "signature": _b64(key.sign(canonical_bytes(value)))}


def _verify_document(document: Any, key: Ed25519PublicKey, name: str) -> dict[str, Any]:
    if not isinstance(document, dict):
        raise VerificationError(f"invalid_{name}_document")
    signed = document.get("signed")
    signature = document.get("signature")
    if not isinstance(signed, dict) or not isinstance(signature, str):
        raise VerificationError(f"invalid_{name}_document")
    try:
        key.verify(_unb64(signature), canonical_bytes(signed))
    except InvalidSignature as exc:
        raise VerificationError(f"invalid_{name}_signature") from exc
    return signed


def bootstrap_workspace_identity(workspace: Path, *, mode: str = "audit") -> TrustState:
    """Create an audit/development trust root and empty registry.

    The generated root private key is intentionally labelled local/audit.  It is
    not sufficient for strict mode; strict promotion moves signing and monitor
    custody behind a distinct OS principal.
    """
    if mode not in MODE_ORDER:
        raise IdentityError(f"invalid_identity_mode: {mode}")
    root = _identity_dir(workspace)
    if (root / "trust-root.json").exists():
        raise IdentityError("identity_already_initialized")
    root.mkdir(parents=True, exist_ok=True)
    os.chmod(root, 0o700)
    (root / "private").mkdir(mode=0o700)

    private = Ed25519PrivateKey.generate()
    public = private.public_key()
    root_key_id = "workspace-root-v1"
    private_path = root / "private" / "workspace-root.pem"
    private_path.write_bytes(_private_bytes(private))
    os.chmod(private_path, 0o600)
    _atomic_json(
        root / "trust-root.json",
        {
            "algorithm": "Ed25519",
            "key_id": root_key_id,
            "public_key": _b64(_public_raw(public)),
            "custody": "local_audit_only",
        },
    )

    workspace_id = secrets.token_hex(16)
    registry = {
        "registry_version": "1",
        "workspace_id": workspace_id,
        "keys": [],
        "revoked_key_ids": [],
    }
    _atomic_json(root / "registry.json", _signed_document(registry, private))
    policy = {
        "policy_version": "1",
        "workspace_id": workspace_id,
        "mode": mode,
        "registry_version": "1",
        "registry_digest": _sha256_jcs(registry),
        "restricted_topics": list(DEFAULT_RESTRICTED_TOPICS),
        "replay_window_seconds": "300",
        "reference_monitor": "local_audit",
    }
    _atomic_json(root / "policy.json", _signed_document(policy, private))
    return load_trust_state(workspace, update_high_water=True)


def _load_root_public(root: Path) -> Ed25519PublicKey:
    try:
        data = strict_json_loads((root / "trust-root.json").read_bytes())
        if data.get("algorithm") != "Ed25519":
            raise VerificationError("unsupported_root_algorithm")
        return Ed25519PublicKey.from_public_bytes(_unb64(data["public_key"]))
    except OSError as exc:
        raise IdentityNotConfigured("identity_not_configured") from exc


def configured(workspace: Path) -> bool:
    return (_identity_dir(workspace) / "trust-root.json").is_file()


def load_trust_state(workspace: Path, *, update_high_water: bool = False) -> TrustState:
    root = _identity_dir(workspace)
    public = _load_root_public(root)
    try:
        policy_doc = strict_json_loads((root / "policy.json").read_bytes())
        registry_doc = strict_json_loads((root / "registry.json").read_bytes())
    except OSError as exc:
        raise VerificationError("identity_documents_missing") from exc
    policy = _verify_document(policy_doc, public, "policy")
    registry = _verify_document(registry_doc, public, "registry")
    if policy.get("workspace_id") != registry.get("workspace_id"):
        raise VerificationError("workspace_id_mismatch")
    if policy.get("registry_digest") != _sha256_jcs(registry):
        raise VerificationError("registry_digest_mismatch")
    try:
        policy_version = int(policy["policy_version"])
        registry_version = int(registry["registry_version"])
        required_registry = int(policy["registry_version"])
    except (KeyError, TypeError, ValueError) as exc:
        raise VerificationError("invalid_identity_version") from exc
    if registry_version != required_registry:
        raise VerificationError("registry_version_mismatch")
    mode = str(policy.get("mode") or "")
    if mode not in MODE_ORDER:
        raise VerificationError("invalid_identity_mode")

    high_path = root / "high-water.json"
    high = {"policy_version": 0, "registry_version": 0}
    if high_path.is_file():
        loaded = strict_json_loads(high_path.read_bytes())
        if isinstance(loaded, dict):
            high = loaded
    if policy_version < int(high.get("policy_version", 0)):
        raise VerificationError("policy_rollback")
    if registry_version < int(high.get("registry_version", 0)):
        raise VerificationError("registry_rollback")
    if update_high_water and (
        policy_version > int(high.get("policy_version", 0))
        or registry_version > int(high.get("registry_version", 0))
    ):
        _atomic_json(
            high_path,
            {"policy_version": policy_version, "registry_version": registry_version},
            mode=0o600,
        )
    restricted = policy.get("restricted_topics") or []
    if not isinstance(restricted, list) or not all(
        isinstance(x, str) for x in restricted
    ):
        raise VerificationError("invalid_restricted_topics")
    return TrustState(
        workspace_id=str(policy["workspace_id"]),
        mode=effective_mode(mode),
        policy_version=policy_version,
        registry_version=registry_version,
        restricted_topics=tuple(restricted),
        policy=policy,
        registry=registry,
    )


def effective_mode(policy_mode: str) -> str:
    requested = (os.environ.get("AGENTBUS_IDENTITY_MODE") or "").strip().lower()
    if requested not in MODE_ORDER:
        return policy_mode
    return requested if MODE_ORDER[requested] > MODE_ORDER[policy_mode] else policy_mode


def topic_restricted(topic: str, state: TrustState) -> bool:
    return any(
        topic == item or (item.endswith("/") and topic.startswith(item))
        for item in state.restricted_topics
    )


def enroll_identity(
    workspace: Path,
    producer_id: str,
    *,
    capabilities: Iterable[str] = ("message",),
    topics: Iterable[str] = ("okf/handoff",),
) -> str:
    if not producer_id or "/subagent/" in producer_id:
        raise IdentityError("invalid_producer_id")
    root = _identity_dir(workspace)
    state = load_trust_state(workspace, update_high_water=True)
    root_private = _load_private(root / "private" / "workspace-root.pem")
    if any(entry.get("producer_id") == producer_id for entry in state.registry["keys"]):
        raise IdentityError(f"producer_already_enrolled: {producer_id}")
    private = Ed25519PrivateKey.generate()
    key_id = f"{producer_id}-{secrets.token_hex(6)}"
    private_path = root / "private" / f"{key_id}.pem"
    private_path.write_bytes(_private_bytes(private))
    os.chmod(private_path, 0o600)
    registry = dict(state.registry)
    registry["registry_version"] = str(state.registry_version + 1)
    registry["keys"] = [
        *state.registry["keys"],
        {
            "key_id": key_id,
            "producer_id": producer_id,
            "algorithm": "Ed25519",
            "public_key": _b64(_public_raw(private.public_key())),
            "capabilities": sorted(set(capabilities)),
            "topics": sorted(set(topics)),
            "state": "active",
        },
    ]
    _write_registry_and_policy(root, state, registry, root_private)
    load_trust_state(workspace, update_high_water=True)
    return key_id


def _write_registry_and_policy(
    root: Path,
    state: TrustState,
    registry: dict[str, Any],
    root_private: Ed25519PrivateKey,
) -> None:
    _atomic_json(root / "registry.json", _signed_document(registry, root_private))
    policy = dict(state.policy)
    policy["policy_version"] = str(state.policy_version + 1)
    policy["registry_version"] = registry["registry_version"]
    policy["registry_digest"] = _sha256_jcs(registry)
    _atomic_json(root / "policy.json", _signed_document(policy, root_private))


def rotate_identity(
    workspace: Path, producer_id: str, *, grace_seconds: int = 300
) -> str:
    if grace_seconds < 0:
        raise IdentityError("invalid_rotation_grace")
    root = _identity_dir(workspace)
    state = load_trust_state(workspace, update_high_water=True)
    old = _active_key(state, producer_id)
    root_private = _load_private(root / "private" / "workspace-root.pem")
    private = Ed25519PrivateKey.generate()
    key_id = f"{producer_id}-{secrets.token_hex(6)}"
    private_path = root / "private" / f"{key_id}.pem"
    private_path.write_bytes(_private_bytes(private))
    os.chmod(private_path, 0o600)
    not_after = (
        (datetime.now(timezone.utc) + timedelta(seconds=grace_seconds))
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )
    keys: list[dict[str, Any]] = []
    for entry in state.registry["keys"]:
        updated = dict(entry)
        if entry.get("key_id") == old["key_id"]:
            updated["state"] = "retiring"
            updated["not_after"] = not_after
        keys.append(updated)
    keys.append(
        {
            "key_id": key_id,
            "producer_id": producer_id,
            "algorithm": "Ed25519",
            "public_key": _b64(_public_raw(private.public_key())),
            "capabilities": old.get("capabilities", []),
            "topics": old.get("topics", []),
            "state": "active",
        }
    )
    registry = dict(state.registry)
    registry["registry_version"] = str(state.registry_version + 1)
    registry["keys"] = keys
    _write_registry_and_policy(root, state, registry, root_private)
    load_trust_state(workspace, update_high_water=True)
    return key_id


def revoke_identity_key(workspace: Path, key_id: str) -> TrustState:
    root = _identity_dir(workspace)
    state = load_trust_state(workspace, update_high_water=True)
    if not any(entry.get("key_id") == key_id for entry in state.registry["keys"]):
        raise IdentityError(f"identity_key_not_found: {key_id}")
    root_private = _load_private(root / "private" / "workspace-root.pem")
    keys = []
    for entry in state.registry["keys"]:
        updated = dict(entry)
        if entry.get("key_id") == key_id:
            updated["state"] = "revoked"
        keys.append(updated)
    revoked = sorted(set([*state.registry.get("revoked_key_ids", []), key_id]))
    registry = dict(state.registry)
    registry["registry_version"] = str(state.registry_version + 1)
    registry["keys"] = keys
    registry["revoked_key_ids"] = revoked
    _write_registry_and_policy(root, state, registry, root_private)
    return load_trust_state(workspace, update_high_water=True)


def set_policy_mode(workspace: Path, mode: str) -> TrustState:
    if mode not in MODE_ORDER:
        raise IdentityError(f"invalid_identity_mode: {mode}")
    root = _identity_dir(workspace)
    state = load_trust_state(workspace, update_high_water=True)
    if MODE_ORDER[mode] < MODE_ORDER[state.policy["mode"]]:
        raise IdentityError("identity_mode_downgrade_refused")
    if mode == "strict" and state.policy.get("reference_monitor") == "local_audit":
        raise IdentityError("strict_requires_isolated_reference_monitor")
    private = _load_private(root / "private" / "workspace-root.pem")
    policy = dict(state.policy)
    policy["policy_version"] = str(state.policy_version + 1)
    policy["mode"] = mode
    _atomic_json(root / "policy.json", _signed_document(policy, private))
    return load_trust_state(workspace, update_high_water=True)


def issue_wake_capability(workspace: Path, runtime: str) -> Path:
    """Issue and policy-bind one runtime-specific wake capability."""
    if not runtime or any(
        ch not in "abcdefghijklmnopqrstuvwxyz0123456789-_" for ch in runtime
    ):
        raise IdentityError("invalid_runtime_id")
    root = _identity_dir(workspace)
    state = load_trust_state(workspace, update_high_water=True)
    private = _load_private(root / "private" / "workspace-root.pem")
    token = secrets.token_urlsafe(48)
    token_path = root / "private" / f"wake-{runtime}.token"
    token_path.write_text(token + "\n", encoding="utf-8")
    os.chmod(token_path, 0o600)
    policy = dict(state.policy)
    policy["policy_version"] = str(state.policy_version + 1)
    capabilities = dict(policy.get("wake_capabilities") or {})
    capabilities[runtime] = {
        "sha256": hashlib.sha256(token.encode()).hexdigest(),
        "scope": "notify_persisted_event_id",
    }
    policy["wake_capabilities"] = capabilities
    _atomic_json(root / "policy.json", _signed_document(policy, private))
    load_trust_state(workspace, update_high_water=True)
    return token_path


def verify_wake_capability(state: TrustState, runtime: str, token: str) -> bool:
    configured_capability = (state.policy.get("wake_capabilities") or {}).get(runtime)
    if not isinstance(configured_capability, dict):
        return False
    expected = configured_capability.get("sha256")
    if not isinstance(expected, str):
        return False
    actual = hashlib.sha256(token.encode()).hexdigest()
    return secrets.compare_digest(actual, expected)


def artifact_digests(artifacts: Iterable[dict[str, Any]]) -> list[dict[str, str]]:
    result: list[dict[str, str]] = []
    seen: set[str] = set()
    for artifact in artifacts:
        name = artifact.get("name")
        content = artifact.get("content")
        art_type = artifact.get("type")
        if (
            not isinstance(name, str)
            or not isinstance(content, str)
            or not isinstance(art_type, str)
        ):
            raise IdentityError("invalid_artifact_for_signing")
        normalized = unicodedata.normalize("NFC", name)
        if normalized != name or name in seen:
            raise IdentityError("duplicate_or_non_nfc_artifact_name")
        seen.add(name)
        raw = content.encode("utf-8", errors="strict")
        result.append(
            {
                "name": name,
                "type": art_type,
                "size": str(len(raw)),
                "sha256": hashlib.sha256(raw).hexdigest(),
            }
        )
    return sorted(result, key=lambda item: item["name"])


def _active_key(
    state: TrustState, producer_id: str, key_id: str | None = None
) -> dict[str, Any]:
    matches = [
        item
        for item in state.registry.get("keys", [])
        if item.get("producer_id") == producer_id
        and item.get("state") == "active"
        and (key_id is None or item.get("key_id") == key_id)
    ]
    if len(matches) != 1:
        raise IdentityError(f"active_identity_key_not_found: {producer_id}")
    return matches[0]


def _verification_key(
    state: TrustState, producer_id: str, key_id: str, timestamp: str
) -> dict[str, Any]:
    matches = [
        item
        for item in state.registry.get("keys", [])
        if item.get("producer_id") == producer_id and item.get("key_id") == key_id
    ]
    if len(matches) != 1:
        raise VerificationError("identity_key_not_found")
    entry = matches[0]
    if entry.get("state") == "revoked" or key_id in set(
        state.registry.get("revoked_key_ids") or []
    ):
        raise VerificationError("key_revoked")
    if entry.get("state") == "retiring" and timestamp > str(
        entry.get("not_after") or ""
    ):
        raise VerificationError("rotation_grace_expired")
    return entry


def sign_event_envelope(
    workspace: Path,
    *,
    topic: str,
    producer_id: str,
    schema_version: str,
    payload: dict[str, Any],
    artifacts: Iterable[dict[str, Any]] = (),
    causation_id: int | None = None,
    idempotency_key: str | None = None,
    trace_id: str | None = None,
    action: dict[str, Any] | None = None,
    timestamp: str | None = None,
    nonce: str | None = None,
) -> dict[str, Any]:
    state = load_trust_state(workspace, update_high_water=True)
    entry = _active_key(state, producer_id)
    if topic_restricted(topic, state) and payload.get("from") != producer_id:
        raise IdentityError("payload_from_producer_mismatch")
    allowed_topics = entry.get("topics") or []
    if not any(
        topic == item or (str(item).endswith("/") and topic.startswith(str(item)))
        for item in allowed_topics
    ):
        raise IdentityError(f"identity_topic_not_allowed: {topic}")
    chosen_action = action or payload.get("action") or {"type": "message"}
    action_type = chosen_action.get("type") if isinstance(chosen_action, dict) else None
    if action_type not in (entry.get("capabilities") or []):
        raise IdentityError(f"identity_capability_not_allowed: {action_type}")
    unsigned = {
        "envelope_version": ENVELOPE_VERSION,
        "workspace_id": state.workspace_id,
        "topic": topic,
        "producer_id": producer_id,
        "key_id": entry["key_id"],
        "policy_version": str(state.policy_version),
        "registry_version": str(state.registry_version),
        "schema_version": schema_version,
        "nonce": nonce or secrets.token_hex(16),
        "timestamp": timestamp
        or datetime.now(timezone.utc)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z"),
        "causation_id": str(causation_id) if causation_id is not None else None,
        "idempotency_key": idempotency_key,
        "trace_id": trace_id,
        "action": chosen_action,
        "payload": payload,
        "artifact_digests": artifact_digests(artifacts),
    }
    validate_jcs_value(unsigned)
    private = _load_private(
        _identity_dir(workspace) / "private" / f"{entry['key_id']}.pem"
    )
    return {
        "signed": unsigned,
        "signature": _b64(private.sign(canonical_bytes(unsigned))),
    }


def verify_envelope(
    workspace: Path,
    envelope: dict[str, Any],
    *,
    stored_payload: dict[str, Any],
    artifacts: Iterable[dict[str, Any]] = (),
    expected_topic: str | None = None,
    expected_producer: str | None = None,
    expected_schema_version: str | None = None,
    expected_causation_id: int | None | object = _UNSET,
    expected_idempotency_key: str | None | object = _UNSET,
    expected_trace_id: str | None | object = _UNSET,
) -> VerificationResult:
    try:
        state = load_trust_state(workspace, update_high_water=False)
        if not isinstance(envelope, dict):
            raise VerificationError("missing_identity_envelope")
        unsigned = envelope.get("signed")
        signature = envelope.get("signature")
        if not isinstance(unsigned, dict) or not isinstance(signature, str):
            raise VerificationError("invalid_identity_envelope")
        if unsigned.get("envelope_version") != ENVELOPE_VERSION:
            raise VerificationError("unsupported_envelope_version")
        if unsigned.get("workspace_id") != state.workspace_id:
            raise VerificationError("workspace_id_mismatch")
        if int(unsigned.get("policy_version", "-1")) > state.policy_version:
            raise VerificationError("future_policy_version")
        if int(unsigned.get("registry_version", "-1")) > state.registry_version:
            raise VerificationError("future_registry_version")
        producer = unsigned.get("producer_id")
        key_id = unsigned.get("key_id")
        if not isinstance(producer, str) or not isinstance(key_id, str):
            raise VerificationError("invalid_identity_binding")
        if expected_topic is not None and unsigned.get("topic") != expected_topic:
            raise VerificationError("topic_mismatch")
        if expected_producer is not None and producer != expected_producer:
            raise VerificationError("producer_mismatch")
        if (
            expected_schema_version is not None
            and unsigned.get("schema_version") != expected_schema_version
        ):
            raise VerificationError("schema_version_mismatch")
        if expected_causation_id is not _UNSET:
            signed_causation = unsigned.get("causation_id")
            wanted_causation = (
                str(expected_causation_id)
                if expected_causation_id is not None
                else None
            )
            if signed_causation != wanted_causation:
                raise VerificationError("causation_id_mismatch")
        if (
            expected_idempotency_key is not _UNSET
            and unsigned.get("idempotency_key") != expected_idempotency_key
        ):
            raise VerificationError("idempotency_key_mismatch")
        if (
            expected_trace_id is not _UNSET
            and unsigned.get("trace_id") != expected_trace_id
        ):
            raise VerificationError("trace_id_mismatch")
        if unsigned.get("payload") != stored_payload:
            raise VerificationError("payload_mismatch")
        if unsigned.get("artifact_digests") != artifact_digests(artifacts):
            raise VerificationError("artifact_digest_mismatch")
        if (
            topic_restricted(str(unsigned.get("topic") or ""), state)
            and stored_payload.get("from") != producer
        ):
            raise VerificationError("payload_from_producer_mismatch")
        if key_id in set(state.registry.get("revoked_key_ids") or []):
            raise VerificationError("key_revoked")
        timestamp = unsigned.get("timestamp")
        if not isinstance(timestamp, str):
            raise VerificationError("invalid_event_timestamp")
        try:
            datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        except ValueError as exc:
            raise VerificationError("invalid_event_timestamp") from exc
        entry = _verification_key(state, producer, key_id, timestamp)
        public = Ed25519PublicKey.from_public_bytes(_unb64(entry["public_key"]))
        try:
            public.verify(_unb64(signature), canonical_bytes(unsigned))
        except InvalidSignature as exc:
            raise VerificationError("invalid_event_signature") from exc
        return VerificationResult(
            True, producer, key_id, int(unsigned["policy_version"])
        )
    except (IdentityError, KeyError, TypeError, ValueError) as exc:
        return VerificationResult(False, None, None, None, str(exc))
