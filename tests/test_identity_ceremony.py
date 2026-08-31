from __future__ import annotations

import json
import os
import stat
from pathlib import Path

import pytest
from click.testing import CliRunner

from agentbus.ceremony import (
    export_policy_request,
    generate_enrollment_request,
    generate_offline_root,
    import_signed_policy,
    initialize_offline_identity,
    sign_policy_request,
)
from agentbus.identity import (
    IdentityError,
    VerificationError,
    configured,
    load_trust_state,
    sign_event_envelope,
)
from agentbus.cli import main


def _ceremony(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    workspace = tmp_path / "workspace"
    offline = tmp_path / "offline"
    workspace.mkdir()
    root_key = offline / "root.pem"
    descriptor = offline / "root.json"
    generate_offline_root(root_key, descriptor)
    initialize_offline_identity(workspace, descriptor)
    peer_key = tmp_path / "peer" / "codex.pem"
    enrollment = tmp_path / "peer" / "codex.json"
    generate_enrollment_request(
        "codex",
        "agentbus-codex",
        peer_key,
        enrollment,
        capabilities=("message", "implementation"),
    )
    request = workspace / "request.json"
    bundle = offline / "bundle.json"
    export_policy_request(workspace, request, enrollment_paths=(enrollment,))
    sign_policy_request(request, root_key, bundle)
    return workspace, root_key, peer_key, bundle


def test_offline_ceremony_installs_public_only_trust_state(tmp_path: Path) -> None:
    workspace, root_key, _peer_key, bundle = _ceremony(tmp_path)
    identity = workspace / ".agentbus" / "identity"

    assert stat.S_IMODE(root_key.stat().st_mode) == 0o600
    assert not (identity / "private" / "workspace-root.pem").exists()
    assert configured(workspace) is False

    result = import_signed_policy(workspace, bundle)
    state = load_trust_state(workspace)

    assert configured(workspace) is True
    assert result["custody"] == "offline"
    assert state.mode == "audit"
    assert state.policy["reference_monitor"] == "isolated_broker"
    assert state.policy["principals"] == {
        "codex": {"username": "agentbus-codex"}
    }
    assert state.registry["keys"][0]["producer_id"] == "codex"
    assert not (identity / "private").exists()


def test_bundle_replay_and_wrong_root_are_rejected(tmp_path: Path) -> None:
    workspace, _root_key, _peer_key, bundle = _ceremony(tmp_path)
    import_signed_policy(workspace, bundle)
    with pytest.raises(VerificationError, match="previous_digest_mismatch"):
        import_signed_policy(workspace, bundle)

    other_key = tmp_path / "other" / "root.pem"
    other_descriptor = tmp_path / "other" / "root.json"
    generate_offline_root(other_key, other_descriptor)
    request = workspace / "second-request.json"
    export_policy_request(workspace, request)
    with pytest.raises(IdentityError, match="wrong_root"):
        sign_policy_request(request, other_key, tmp_path / "other-bundle.json")


def test_offline_targets_are_never_overwritten(tmp_path: Path) -> None:
    key = tmp_path / "root.pem"
    descriptor = tmp_path / "root.json"
    generate_offline_root(key, descriptor)
    original = key.read_bytes()
    with pytest.raises(IdentityError, match="target_exists"):
        generate_offline_root(key, descriptor)
    assert key.read_bytes() == original
    assert os.access(key, os.R_OK)


def test_offline_ceremony_cli_round_trip(tmp_path: Path) -> None:
    runner = CliRunner()
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    root_key = tmp_path / "offline" / "root.pem"
    descriptor = tmp_path / "offline" / "root.json"
    peer_key = tmp_path / "peer" / "codex.pem"
    enrollment = tmp_path / "peer" / "codex.json"
    request = tmp_path / "request.json"
    bundle = tmp_path / "offline" / "bundle.json"

    commands = [
        [
            "identity",
            "root",
            "generate",
            "--private-key",
            str(root_key),
            "--descriptor",
            str(descriptor),
        ],
        [
            "identity",
            "init",
            "--workspace",
            str(workspace),
            "--offline-root-pubkey",
            str(descriptor),
        ],
        [
            "identity",
            "key",
            "generate",
            "codex",
            "--principal",
            "agentbus-codex",
            "--private-key",
            str(peer_key),
            "--request",
            str(enrollment),
        ],
        [
            "identity",
            "export-policy-request",
            "--workspace",
            str(workspace),
            "--output",
            str(request),
            "--enrollment",
            str(enrollment),
        ],
        [
            "identity",
            "ceremony",
            "sign",
            "--root-key",
            str(root_key),
            "--request",
            str(request),
            "--output",
            str(bundle),
        ],
        [
            "identity",
            "import-signed-policy",
            str(bundle),
            "--workspace",
            str(workspace),
        ],
        ["identity", "list", "--workspace", str(workspace)],
    ]
    results = [runner.invoke(main, command) for command in commands]
    assert [result.exit_code for result in results] == [0] * len(commands), [
        result.output for result in results
    ]
    assert json.loads(results[-1].output)["keys"][0]["producer_id"] == "codex"


def test_offline_rotation_and_revocation_are_one_signed_transition(
    tmp_path: Path,
) -> None:
    workspace, root_key, old_key, first_bundle = _ceremony(tmp_path)
    import_signed_policy(workspace, first_bundle)
    old_key_id = load_trust_state(workspace).registry["keys"][0]["key_id"]

    new_key = tmp_path / "peer" / "codex-v2.pem"
    new_enrollment = tmp_path / "peer" / "codex-v2.json"
    generate_enrollment_request(
        "codex",
        "agentbus-codex",
        new_key,
        new_enrollment,
        capabilities=("message", "implementation"),
    )
    request = tmp_path / "rotate-request.json"
    bundle = tmp_path / "offline" / "rotate-bundle.json"
    export_policy_request(
        workspace,
        request,
        enrollment_paths=(new_enrollment,),
        revoke_key_ids=(old_key_id,),
    )
    sign_policy_request(request, root_key, bundle)
    import_signed_policy(workspace, bundle)

    state = load_trust_state(workspace)
    assert state.policy_version == 2
    assert state.registry_version == 2
    assert state.registry["revoked_key_ids"] == [old_key_id]
    assert [item["state"] for item in state.registry["keys"]].count("active") == 1
    payload = {"from": "codex", "to": "agy", "summary": "new key"}
    sign_event_envelope(
        workspace,
        topic="okf/handoff",
        producer_id="codex",
        schema_version="1.0",
        payload=payload,
        private_key_path=new_key,
    )
    with pytest.raises(IdentityError, match="signing_key_identity_mismatch"):
        sign_event_envelope(
            workspace,
            topic="okf/handoff",
            producer_id="codex",
            schema_version="1.0",
            payload=payload,
            private_key_path=old_key,
        )
