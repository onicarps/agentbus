"""Swarm RBAC — role definitions, token mapping, publish enforcement."""

from __future__ import annotations

import fnmatch
import json
import os
import secrets
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml

ROLES_FILENAME = "roles.yaml"
DROID_PROOFS_FILENAME = "droid_proofs.json"
DEFAULT_DROID_PROOF_TTL_MINUTES = 30


class ForbiddenError(Exception):
    """RBAC denial — maps to HTTP 403 in MCP/CLI."""

    def __init__(self, message: str, *, code: int = 403) -> None:
        super().__init__(message)
        self.code = code


@dataclass
class RoleDef:
    can_publish_topics: list[str] = field(default_factory=lambda: ["okf/handoff"])
    forbidden_payloads: list[str] = field(default_factory=list)
    requires_droid_proof: bool = False
    can_approve: bool = False

    @classmethod
    def from_dict(cls, data: dict) -> RoleDef:
        return cls(
            can_publish_topics=list(data.get("can_publish_topics", ["okf/handoff"])),
            forbidden_payloads=list(data.get("forbidden_payloads", [])),
            requires_droid_proof=bool(
                data.get("requires_droid_proof") or data.get("requires_crypto_proof")
            ),
            can_approve=bool(data.get("can_approve")),
        )


@dataclass
class RbacConfig:
    roles: dict[str, RoleDef] = field(default_factory=dict)
    producers: dict[str, str] = field(default_factory=dict)
    token_roles: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict) -> RbacConfig:
        roles = {
            name: RoleDef.from_dict(defn)
            for name, defn in (data.get("roles") or {}).items()
        }
        return cls(
            roles=roles,
            producers=dict(data.get("producers") or {}),
            token_roles=dict(data.get("token_roles") or {}),
        )


def rbac_disabled() -> bool:
    return os.environ.get("AGENTBUS_DISABLE_RBAC", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def roles_path(workspace: Path) -> Path:
    return workspace.resolve() / ".agentbus" / ROLES_FILENAME


def droid_proofs_path(workspace: Path) -> Path:
    return workspace.resolve() / ".agentbus" / DROID_PROOFS_FILENAME


def load_rbac_config(workspace: Path) -> RbacConfig | None:
    if rbac_disabled():
        return None
    path = roles_path(workspace)
    if not path.is_file():
        return None
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return RbacConfig.from_dict(data)


def save_rbac_config(workspace: Path, config: RbacConfig) -> Path:
    path = roles_path(workspace)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "roles": {
            name: {
                "can_publish_topics": role.can_publish_topics,
                **({"forbidden_payloads": role.forbidden_payloads} if role.forbidden_payloads else {}),
                **({"requires_droid_proof": True} if role.requires_droid_proof else {}),
                **({"can_approve": True} if role.can_approve else {}),
            }
            for name, role in config.roles.items()
        },
        "producers": config.producers,
        "token_roles": config.token_roles,
    }
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    os.chmod(path, 0o600)
    return path


def default_rbac_config() -> RbacConfig:
    return RbacConfig(
        roles={
            "architect": RoleDef(
                can_publish_topics=["okf/handoff", "okf/approval"],
                can_approve=True,
            ),
            "engineer": RoleDef(
                can_publish_topics=["okf/handoff"],
                forbidden_payloads=["*PASS*", "*FAIL*"],
            ),
            "qa_droid": RoleDef(
                can_publish_topics=["okf/handoff"],
                requires_droid_proof=True,
            ),
        },
        producers={
            "grok": "engineer",
            "agy": "architect",
            "hermes": "qa_droid",
        },
    )


def ensure_default_roles(workspace: Path) -> RbacConfig:
    existing = load_rbac_config(workspace)
    if existing and existing.roles:
        return existing
    config = default_rbac_config()
    save_rbac_config(workspace, config)
    return config


def resolve_role(
    workspace: Path,
    *,
    producer_id: str,
    auth_token: str | None = None,
) -> str | None:
    config = load_rbac_config(workspace)
    if not config:
        return None
    if auth_token and auth_token in config.token_roles:
        return config.token_roles[auth_token]
    return config.producers.get(producer_id)


def _payload_blob(payload: dict) -> str:
    return json.dumps(payload, ensure_ascii=False)


def _matches_forbidden(patterns: list[str], payload: dict) -> str | None:
    blob = _payload_blob(payload)
    for pattern in patterns:
        if fnmatch.fnmatchcase(blob.upper(), pattern.upper()) or fnmatch.fnmatchcase(
            blob, pattern
        ):
            return pattern
    return None


def _topic_allowed(role: RoleDef, topic: str) -> bool:
    for pattern in role.can_publish_topics:
        if fnmatch.fnmatchcase(topic, pattern):
            return True
    return False


def _load_droid_proofs(workspace: Path) -> dict[str, dict]:
    path = droid_proofs_path(workspace)
    if not path.is_file():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _save_droid_proofs(workspace: Path, proofs: dict[str, dict]) -> None:
    path = droid_proofs_path(workspace)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(proofs, indent=2) + "\n", encoding="utf-8")
    os.chmod(path, 0o600)


def mint_droid_proof(
    workspace: Path,
    *,
    mission_id: str | None = None,
    ttl_minutes: int = DEFAULT_DROID_PROOF_TTL_MINUTES,
) -> dict[str, str]:
    proof = secrets.token_urlsafe(24)
    expires = (
        datetime.now(timezone.utc) + timedelta(minutes=ttl_minutes)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")
    proofs = _load_droid_proofs(workspace)
    proofs[proof] = {
        "expires": expires,
        "mission_id": mission_id or "",
        "used": False,
    }
    _save_droid_proofs(workspace, proofs)
    return {"droid_proof": proof, "expires": expires, "mission_id": mission_id or ""}


def verify_droid_proof(workspace: Path, proof: str | None) -> bool:
    if not proof:
        return False
    proofs = _load_droid_proofs(workspace)
    entry = proofs.get(proof)
    if not entry:
        return False
    if entry.get("used"):
        return False
    expires = entry.get("expires", "")
    if expires and expires < datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"):
        return False
    entry["used"] = True
    proofs[proof] = entry
    _save_droid_proofs(workspace, proofs)
    return True


def check_publish_rbac(
    workspace: Path,
    *,
    producer_id: str,
    topic: str,
    payload: dict,
    auth_token: str | None = None,
) -> None:
    """Raise ForbiddenError (403) when role cannot publish."""
    config = load_rbac_config(workspace)
    if not config:
        return

    role_name = resolve_role(workspace, producer_id=producer_id, auth_token=auth_token)
    if not role_name:
        raise ForbiddenError(
            f"403 Forbidden: no RBAC role for producer '{producer_id}'"
        )

    role = config.roles.get(role_name)
    if not role:
        raise ForbiddenError(f"403 Forbidden: unknown role '{role_name}'")

    if not _topic_allowed(role, topic):
        raise ForbiddenError(
            f"403 Forbidden: role '{role_name}' cannot publish to topic '{topic}'"
        )

    forbidden = _matches_forbidden(role.forbidden_payloads, payload)
    if forbidden:
        raise ForbiddenError(
            f"403 Forbidden: role '{role_name}' blocked by pattern '{forbidden}'"
        )

    if role.requires_droid_proof:
        proof = payload.get("droid_proof")
        if not verify_droid_proof(workspace, proof if isinstance(proof, str) else None):
            raise ForbiddenError(
                f"403 Forbidden: role '{role_name}' requires valid droid_proof"
            )


def check_approve_rbac(
    workspace: Path,
    *,
    reviewer_id: str,
    auth_token: str | None = None,
) -> None:
    config = load_rbac_config(workspace)
    if not config:
        return

    role_name = resolve_role(workspace, producer_id=reviewer_id, auth_token=auth_token)
    if not role_name:
        raise ForbiddenError(
            f"403 Forbidden: no RBAC role for reviewer '{reviewer_id}'"
        )
    role = config.roles.get(role_name)
    if not role or not role.can_approve:
        raise ForbiddenError(
            f"403 Forbidden: role '{role_name}' cannot approve/reject HITL events"
        )


def assign_producer_role(workspace: Path, producer_id: str, role_name: str) -> RbacConfig:
    config = ensure_default_roles(workspace)
    if role_name not in config.roles:
        raise ValueError(f"unknown_role: {role_name}")
    config.producers[producer_id] = role_name
    save_rbac_config(workspace, config)
    return config


def assign_token_role(workspace: Path, token: str, role_name: str) -> RbacConfig:
    config = ensure_default_roles(workspace)
    if role_name not in config.roles:
        raise ValueError(f"unknown_role: {role_name}")
    config.token_roles[token] = role_name
    save_rbac_config(workspace, config)
    return config