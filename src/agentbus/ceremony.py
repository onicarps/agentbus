"""Detached AgentID root ceremony helpers.

The offline functions in this module never need an AgentBus workspace.  Online
initialization persists only a public root descriptor; the private root is not
created or copied into ``.agentbus/identity``.
"""

from __future__ import annotations

import hashlib
import os
import secrets
from pathlib import Path
from typing import Any, Iterable

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)

from agentbus.identity import (
    DEFAULT_ACTION_PRODUCERS,
    DEFAULT_RESTRICTED_TOPICS,
    IdentityError,
    VerificationError,
    _atomic_json,
    _b64,
    _identity_dir,
    _load_private,
    _public_raw,
    _sha256_jcs,
    _signed_document,
    _unb64,
    _verify_document,
    canonical_bytes,
    strict_json_loads,
)

ROOT_DESCRIPTOR_TYPE = "agentbus-root-descriptor"
ENROLLMENT_TYPE = "agentbus-enrollment-request"
POLICY_REQUEST_TYPE = "agentbus-policy-request"
SIGNED_BUNDLE_TYPE = "agentbus-signed-policy-bundle"
CEREMONY_VERSION = "1"


def _read_json(path: Path) -> dict[str, Any]:
    value = strict_json_loads(path.read_bytes())
    if not isinstance(value, dict):
        raise IdentityError(f"invalid_json_object: {path}")
    return value


def _root_id(public_raw: bytes) -> str:
    return "sha256:" + hashlib.sha256(public_raw).hexdigest()


def generate_offline_root(private_key_path: Path, descriptor_path: Path) -> dict:
    """Generate an offline root and its public descriptor.

    Existing targets are never overwritten.  Callers are responsible for
    placing ``private_key_path`` on offline storage.
    """
    if private_key_path.exists() or descriptor_path.exists():
        raise IdentityError("offline_root_target_exists")
    private_key_path.parent.mkdir(parents=True, exist_ok=True)
    descriptor_path.parent.mkdir(parents=True, exist_ok=True)
    private = Ed25519PrivateKey.generate()
    raw_public = _public_raw(private.public_key())
    pem = private.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    fd = os.open(private_key_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as handle:
        handle.write(pem)
        handle.flush()
        os.fsync(handle.fileno())
    descriptor = {
        "document_type": ROOT_DESCRIPTOR_TYPE,
        "ceremony_version": CEREMONY_VERSION,
        "root_id": _root_id(raw_public),
        "algorithm": "Ed25519",
        "public_key": _b64(raw_public),
    }
    _atomic_json(descriptor_path, descriptor)
    return descriptor


def initialize_offline_identity(workspace: Path, descriptor_path: Path) -> dict:
    """Install a public root descriptor without creating online root authority."""
    descriptor = _read_json(descriptor_path)
    if descriptor.get("document_type") != ROOT_DESCRIPTOR_TYPE:
        raise IdentityError("invalid_root_descriptor_type")
    if descriptor.get("ceremony_version") != CEREMONY_VERSION:
        raise IdentityError("unsupported_ceremony_version")
    if descriptor.get("algorithm") != "Ed25519":
        raise IdentityError("unsupported_root_algorithm")
    try:
        raw_public = _unb64(descriptor["public_key"])
        Ed25519PublicKey.from_public_bytes(raw_public)
    except (KeyError, ValueError) as exc:
        raise IdentityError("invalid_root_public_key") from exc
    if descriptor.get("root_id") != _root_id(raw_public):
        raise IdentityError("root_descriptor_digest_mismatch")

    root = _identity_dir(workspace)
    if root.exists() and any(root.iterdir()):
        raise IdentityError("identity_already_initialized")
    root.mkdir(parents=True, mode=0o700, exist_ok=True)
    os.chmod(root, 0o700)
    trust = {
        "document_type": ROOT_DESCRIPTOR_TYPE,
        "ceremony_version": CEREMONY_VERSION,
        "root_id": descriptor["root_id"],
        "algorithm": "Ed25519",
        "public_key": descriptor["public_key"],
        "custody": "offline",
    }
    _atomic_json(root / "trust-root.json", trust)
    state = {
        "workspace_id": secrets.token_hex(16),
        "root_id": descriptor["root_id"],
        "policy_version": "0",
        "registry_version": "0",
        "custody": "offline",
    }
    _atomic_json(root / "ceremony-state.json", state, mode=0o600)
    return state


def generate_enrollment_request(
    producer_id: str,
    principal: str,
    private_key_path: Path,
    request_path: Path,
    *,
    capabilities: Iterable[str] = ("message",),
    topics: Iterable[str] = ("okf/handoff",),
) -> dict:
    """Generate one peer key and a public enrollment request."""
    if private_key_path.exists() or request_path.exists():
        raise IdentityError("enrollment_target_exists")
    if not producer_id or not principal:
        raise IdentityError("invalid_enrollment_identity")
    private_key_path.parent.mkdir(parents=True, exist_ok=True)
    request_path.parent.mkdir(parents=True, exist_ok=True)
    private = Ed25519PrivateKey.generate()
    fd = os.open(private_key_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as handle:
        handle.write(
            private.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
        )
        handle.flush()
        os.fsync(handle.fileno())
    public = _public_raw(private.public_key())
    request = {
        "document_type": ENROLLMENT_TYPE,
        "ceremony_version": CEREMONY_VERSION,
        "producer_id": producer_id,
        "principal": principal,
        "key_id": f"{producer_id}-{secrets.token_hex(6)}",
        "algorithm": "Ed25519",
        "public_key": _b64(public),
        "capabilities": sorted(set(capabilities)),
        "topics": sorted(set(topics)),
        "state": "active",
    }
    _atomic_json(request_path, request)
    return request


def _current_document_digest(path: Path) -> str | None:
    if not path.is_file():
        return None
    return "sha256:" + hashlib.sha256(canonical_bytes(_read_json(path))).hexdigest()


def export_policy_request(
    workspace: Path,
    output_path: Path,
    *,
    mode: str = "audit",
    reference_monitor: str = "isolated_broker",
    enrollment_paths: Iterable[Path] = (),
    revoke_key_ids: Iterable[str] = (),
) -> dict:
    """Export a canonical request for detached root signing."""
    if mode not in {"audit", "protected", "strict"}:
        raise IdentityError("invalid_identity_mode")
    if reference_monitor not in {"local_audit", "isolated_broker"}:
        raise IdentityError("invalid_reference_monitor")
    root = _identity_dir(workspace)
    trust = _read_json(root / "trust-root.json")
    state = _read_json(root / "ceremony-state.json")
    if trust.get("custody") != "offline" or trust.get("root_id") != state.get("root_id"):
        raise IdentityError("offline_root_state_mismatch")

    existing_keys: list[dict] = []
    existing_revoked: set[str] = set()
    existing_principals: dict[str, dict[str, str]] = {}
    registry_path = root / "registry.json"
    policy_path = root / "policy.json"
    if registry_path.is_file() and policy_path.is_file():
        from agentbus.identity import load_trust_state

        current = load_trust_state(workspace, update_high_water=False)
        existing_keys = [dict(item) for item in current.registry.get("keys", [])]
        existing_revoked = set(current.registry.get("revoked_key_ids") or [])
        existing_principals = dict(current.policy.get("principals") or {})
        state["policy_version"] = str(current.policy_version)
        state["registry_version"] = str(current.registry_version)

    keys_by_id = {str(item["key_id"]): item for item in existing_keys}
    principals = existing_principals
    for path in enrollment_paths:
        enrollment = _read_json(path)
        if enrollment.get("document_type") != ENROLLMENT_TYPE:
            raise IdentityError("invalid_enrollment_request")
        key_id = str(enrollment.get("key_id") or "")
        producer_id = str(enrollment.get("producer_id") or "")
        principal = str(enrollment.get("principal") or "")
        if not key_id or not producer_id or not principal or key_id in keys_by_id:
            raise IdentityError("duplicate_or_invalid_enrollment")
        entry = {
            key: enrollment[key]
            for key in (
                "key_id",
                "producer_id",
                "algorithm",
                "public_key",
                "capabilities",
                "topics",
                "state",
            )
        }
        keys_by_id[key_id] = entry
        previous = principals.get(producer_id)
        binding = {"username": principal}
        if previous is not None and previous != binding:
            raise IdentityError("producer_principal_rebind_requires_rotation")
        principals[producer_id] = binding

    revoked = set(existing_revoked)
    for key_id in revoke_key_ids:
        if key_id not in keys_by_id:
            raise IdentityError(f"identity_key_not_found: {key_id}")
        keys_by_id[key_id] = {**keys_by_id[key_id], "state": "revoked"}
        revoked.add(key_id)

    previous_policy = _current_document_digest(policy_path)
    previous_registry = _current_document_digest(registry_path)
    next_policy = int(state.get("policy_version", "0")) + 1
    next_registry = int(state.get("registry_version", "0")) + 1
    request = {
        "document_type": POLICY_REQUEST_TYPE,
        "ceremony_version": CEREMONY_VERSION,
        "root_id": state["root_id"],
        "workspace_id": state["workspace_id"],
        "previous_policy_digest": previous_policy,
        "previous_registry_digest": previous_registry,
        "proposed": {
            "policy_version": str(next_policy),
            "registry_version": str(next_registry),
            "mode": mode,
            "reference_monitor": reference_monitor,
            "principals": principals,
            "keys": sorted(keys_by_id.values(), key=lambda item: str(item["key_id"])),
            "revoked_key_ids": sorted(revoked),
        },
    }
    _atomic_json(output_path, request)
    return request


def sign_policy_request(
    request_path: Path, root_private_key_path: Path, output_path: Path
) -> dict:
    """Sign a policy request in an offline environment."""
    request = _read_json(request_path)
    if request.get("document_type") != POLICY_REQUEST_TYPE:
        raise IdentityError("invalid_policy_request")
    if request.get("ceremony_version") != CEREMONY_VERSION:
        raise IdentityError("unsupported_ceremony_version")
    proposed = request.get("proposed")
    if not isinstance(proposed, dict):
        raise IdentityError("invalid_policy_request")
    private = _load_private(root_private_key_path)
    raw_public = _public_raw(private.public_key())
    if request.get("root_id") != _root_id(raw_public):
        raise IdentityError("policy_request_wrong_root")
    workspace_id = str(request.get("workspace_id") or "")
    if not workspace_id:
        raise IdentityError("invalid_policy_request")
    registry = {
        "registry_version": str(proposed["registry_version"]),
        "workspace_id": workspace_id,
        "previous_registry_digest": request.get("previous_registry_digest"),
        "keys": proposed.get("keys") or [],
        "revoked_key_ids": proposed.get("revoked_key_ids") or [],
    }
    policy = {
        "policy_version": str(proposed["policy_version"]),
        "workspace_id": workspace_id,
        "previous_policy_digest": request.get("previous_policy_digest"),
        "mode": proposed["mode"],
        "registry_version": registry["registry_version"],
        "registry_digest": _sha256_jcs(registry),
        "restricted_topics": list(DEFAULT_RESTRICTED_TOPICS),
        "replay_window_seconds": "300",
        "reference_monitor": proposed["reference_monitor"],
        "principals": proposed.get("principals") or {},
        "action_producers": DEFAULT_ACTION_PRODUCERS,
    }
    bundle = {
        "document_type": SIGNED_BUNDLE_TYPE,
        "ceremony_version": CEREMONY_VERSION,
        "root_id": request["root_id"],
        "request_digest": "sha256:"
        + hashlib.sha256(canonical_bytes(request)).hexdigest(),
        "policy": _signed_document(policy, private),
        "registry": _signed_document(registry, private),
    }
    _atomic_json(output_path, bundle)
    return bundle


def import_signed_policy(workspace: Path, bundle_path: Path) -> dict:
    """Verify and install a detached bundle, refusing rollback or transplant."""
    root = _identity_dir(workspace)
    trust = _read_json(root / "trust-root.json")
    state = _read_json(root / "ceremony-state.json")
    bundle = _read_json(bundle_path)
    if bundle.get("document_type") != SIGNED_BUNDLE_TYPE:
        raise IdentityError("invalid_signed_policy_bundle")
    if bundle.get("root_id") != trust.get("root_id"):
        raise IdentityError("signed_bundle_wrong_root")
    public = Ed25519PublicKey.from_public_bytes(_unb64(trust["public_key"]))
    policy = _verify_document(bundle.get("policy"), public, "policy")
    registry = _verify_document(bundle.get("registry"), public, "registry")
    if policy.get("workspace_id") != state.get("workspace_id"):
        raise VerificationError("policy_workspace_mismatch")
    if registry.get("workspace_id") != state.get("workspace_id"):
        raise VerificationError("registry_workspace_mismatch")
    if policy.get("registry_digest") != _sha256_jcs(registry):
        raise VerificationError("registry_digest_mismatch")
    expected_policy_previous = _current_document_digest(root / "policy.json")
    expected_registry_previous = _current_document_digest(root / "registry.json")
    if policy.get("previous_policy_digest") != expected_policy_previous:
        raise VerificationError("policy_previous_digest_mismatch")
    if registry.get("previous_registry_digest") != expected_registry_previous:
        raise VerificationError("registry_previous_digest_mismatch")
    policy_version = int(policy["policy_version"])
    registry_version = int(registry["registry_version"])
    if policy_version <= int(state.get("policy_version", "0")):
        raise VerificationError("policy_rollback")
    if registry_version <= int(state.get("registry_version", "0")):
        raise VerificationError("registry_rollback")

    # Registry first and policy second intentionally fails closed if interrupted:
    # the old policy digest will not authorize a partially installed registry.
    _atomic_json(root / "registry.json", bundle["registry"])
    _atomic_json(root / "policy.json", bundle["policy"])
    state["policy_version"] = str(policy_version)
    state["registry_version"] = str(registry_version)
    _atomic_json(root / "ceremony-state.json", state, mode=0o600)
    from agentbus.identity import load_trust_state

    loaded = load_trust_state(workspace, update_high_water=True)
    return {
        "workspace_id": loaded.workspace_id,
        "policy_version": loaded.policy_version,
        "registry_version": loaded.registry_version,
        "mode": loaded.mode,
        "custody": "offline",
    }
