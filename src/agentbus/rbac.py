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
MAX_DROID_PROOF_USES = 100
MAX_DROID_PROOF_TTL_MINUTES = 60


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


def _identity_protected(workspace: Path) -> bool:
    from agentbus.identity import configured, load_trust_state

    return configured(workspace) and load_trust_state(workspace).mode in {
        "protected",
        "strict",
    }


def rbac_disabled(workspace: Path | None = None) -> bool:
    requested = os.environ.get("AGENTBUS_DISABLE_RBAC", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    if requested and workspace is not None and _identity_protected(workspace):
        return False
    return requested


def roles_path(workspace: Path) -> Path:
    return workspace.resolve() / ".agentbus" / ROLES_FILENAME


def droid_proofs_path(workspace: Path) -> Path:
    return workspace.resolve() / ".agentbus" / DROID_PROOFS_FILENAME


def load_rbac_config(workspace: Path) -> RbacConfig | None:
    if rbac_disabled(workspace):
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
                can_publish_topics=["okf/handoff", "okf/approval", "system/*"],
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
            "observer": RoleDef(
                can_publish_topics=["system/*"],
            ),
            "bridge": RoleDef(
                can_publish_topics=["okf/handoff", "okf/dead-letter"],
            ),
            "qa": RoleDef(
                can_publish_topics=["okf/handoff"],
            ),
            "ops": RoleDef(
                can_publish_topics=["okf/handoff", "system/*"],
            ),
        },
        producers={
            "codex": "engineer",
            "grok": "engineer",
            "agy": "architect",
            "hermes": "bridge",
            "factory": "qa",
            "factory_droid": "qa_droid",
            "aider": "ops",
            "pi": "ops",
            "slack": "bridge",
            "wiretap": "observer",
            "os-watcher": "observer",
            "swarm-tail": "observer",
            "coderabbit": "architect",
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
    if (
        auth_token
        and not _identity_protected(workspace)
        and auth_token in config.token_roles
    ):
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


def _locked_proofs_update(
    workspace: Path, mutate: "callable[[dict[str, dict]], object]"
) -> object:
    """Read-modify-write the proofs ledger under an exclusive file lock (F4 2.3.6).

    Prevents TOCTOU use-counter races across parallel droid processes.
    """
    import fcntl

    path = droid_proofs_path(workspace)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            handle.seek(0)
            raw = handle.read().strip()
            proofs = json.loads(raw) if raw else {}
            result = mutate(proofs)
            handle.seek(0)
            handle.truncate()
            handle.write(json.dumps(proofs, indent=2) + "\n")
            handle.flush()
            os.chmod(path, 0o600)
            return result
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def mint_droid_proof(
    workspace: Path,
    *,
    mission_id: str | None = None,
    ttl_minutes: int = DEFAULT_DROID_PROOF_TTL_MINUTES,
    batch_id: str | None = None,
    max_uses: int = 1,
) -> dict[str, str | int]:
    """Mint a droid proof (F4/A8).

    Without ``max_uses`` this is a legacy single-use proof. With ``max_uses=N``
    (batch-scoped) the proof authorizes up to N publishes bound to
    ``batch_id``/``mission_id`` under the Agy ruling constraints:
    max_uses ceiling 100, TTL default 30 minutes, hard ceiling 60 minutes.
    """
    uses = max(1, min(int(max_uses), MAX_DROID_PROOF_USES))
    ttl = max(1, min(int(ttl_minutes), MAX_DROID_PROOF_TTL_MINUTES))
    now = datetime.now(timezone.utc)
    expires = (now + timedelta(minutes=ttl)).strftime("%Y-%m-%dT%H:%M:%SZ")
    created = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    proof = secrets.token_urlsafe(24)

    def _add(ledger: dict[str, dict]) -> None:
        ledger[proof] = {
            "expires": expires,
            "mission_id": mission_id or "",
            "used": False,
            "batch_id": batch_id or "",
            "max_uses": uses,
            "uses_count": 0,
            "created_at": created,
        }

    _locked_proofs_update(workspace, _add)
    return {
        "droid_proof": proof,
        "expires": expires,
        "mission_id": mission_id or "",
        "batch_id": batch_id or "",
        "max_uses": uses,
    }


def get_or_mint_proof(
    workspace: Path,
    *,
    mission_id: str | None = None,
    batch_id: str | None = None,
    max_uses: int = 1,
    ttl_minutes: int = DEFAULT_DROID_PROOF_TTL_MINUTES,
) -> dict[str, str | int]:
    """SDK helper (F4 2.5.3): reuse a live batch proof or mint a new one.

    Trusted-orchestrator convenience for local workspace context: returns an
    existing unexpired proof with matching scope and remaining uses when one
    exists, otherwise mints. Never bypasses verification at publish time.
    """
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    proofs = _load_droid_proofs(workspace)
    want_batch = batch_id or ""
    want_mission = mission_id or ""
    for proof, entry in proofs.items():
        if entry.get("used"):
            continue
        if entry.get("expires", "") <= now:
            continue
        if int(entry.get("max_uses", 1) or 1) - int(
            entry.get("uses_count", 0) or 0
        ) <= 0:
            continue
        if entry.get("batch_id", "") != want_batch or entry.get(
            "mission_id", ""
        ) != want_mission:
            continue
        return {
            "droid_proof": proof,
            "expires": entry.get("expires", ""),
            "mission_id": entry.get("mission_id", ""),
            "batch_id": entry.get("batch_id", ""),
            "max_uses": int(entry.get("max_uses", 1) or 1),
            "reused": True,
        }
    minted = mint_droid_proof(
        workspace,
        mission_id=mission_id,
        ttl_minutes=ttl_minutes,
        batch_id=batch_id,
        max_uses=max_uses,
    )
    minted["reused"] = False
    return minted


def verify_droid_proof(
    workspace: Path,
    proof: str | None,
    *,
    batch_id: str | None = None,
    mission_id: str | None = None,
) -> bool:
    if not proof:
        return False
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    result = {"ok": False}

    def _consume(ledger: dict[str, dict]) -> None:
        entry = ledger.get(proof)
        if not entry:
            return
        # Backwards compatibility (F4 2.4): legacy entries are single-use.
        max_uses = int(entry.get("max_uses", 1) or 1)
        uses = int(entry.get("uses_count", 0) or 0)
        if entry.get("used") or uses >= max_uses:
            return
        expires = entry.get("expires", "")
        if expires and expires <= now:
            return
        # Scope matching (F4 2.3.4): a scoped proof must match declared scope.
        scoped_batch = entry.get("batch_id", "")
        scoped_mission = entry.get("mission_id", "")
        if scoped_batch and batch_id is not None and batch_id != scoped_batch:
            return
        if scoped_mission and mission_id is not None and mission_id != scoped_mission:
            return
        entry["uses_count"] = uses + 1
        if entry["uses_count"] >= max_uses:
            entry["used"] = True
        ledger[proof] = entry
        result["ok"] = True

    _locked_proofs_update(workspace, _consume)
    return result["ok"]


def check_publish_rbac(
    workspace: Path,
    *,
    producer_id: str,
    topic: str,
    payload: dict,
    auth_token: str | None = None,
    identity_verified: bool = False,
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
        if _identity_protected(workspace):
            if not identity_verified:
                raise ForbiddenError(
                    f"403 Forbidden: role '{role_name}' requires verified AgentID"
                )
            return
        proof = payload.get("droid_proof")
        scope_batch = payload.get("batch_id")
        scope_mission = payload.get("mission_id")
        if not verify_droid_proof(
            workspace,
            proof if isinstance(proof, str) else None,
            batch_id=scope_batch if isinstance(scope_batch, str) else None,
            mission_id=scope_mission if isinstance(scope_mission, str) else None,
        ):
            raise ForbiddenError(
                f"403 Forbidden: role '{role_name}' requires valid droid_proof "
                "(single-use; mint with 'agentbus droid mint', or mint a "
                "batch-scoped proof with --max-uses for publish-batch)"
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
            f"403 Forbidden: no RBAC role for reviewer '{reviewer_id}'. "
            "Map the reviewer to a role that grants can_approve, e.g. add to "
            ".agentbus/roles.yaml:  roles: {approver: {can_approve: true}}  "
            f"producers: {{{reviewer_id}: approver}}   "
            "(or run: agentbus config init-rbac)"
        )
    role = config.roles.get(role_name)
    if not role or not role.can_approve:
        raise ForbiddenError(
            f"403 Forbidden: role '{role_name}' cannot approve/reject HITL events. "
            "Add 'can_approve: true' to that role in .agentbus/roles.yaml, or map "
            f"the reviewer to an approving role:  producers: {{{reviewer_id}: approver}}"
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
