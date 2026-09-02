from __future__ import annotations

import base64
import json
import hashlib
import sqlite3
import threading
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)

from agentbus.identity import (
    IdentityError,
    VerificationError,
    bootstrap_workspace_identity,
    canonical_bytes,
    delegate_identity,
    enroll_identity,
    issue_wake_capability,
    load_trust_state,
    revoke_identity_key,
    rotate_identity,
    set_policy_mode,
    sign_event_envelope,
    strict_json_loads,
    verify_envelope,
)
from agentbus.cli import main
from agentbus.runner import load_runner_config, run_once
from agentbus.runner.adapters.prompt_common import runner_subprocess_env
from agentbus.runner.types import WakeEnvelope
from agentbus.store import EventStore
from agentbus.wake_ingress import WakeIngressServer


def _boot_and_enroll(workspace: Path, producer: str = "codex") -> None:
    bootstrap_workspace_identity(workspace)
    enroll_identity(
        workspace,
        producer,
        capabilities=("message", "implementation"),
        topics=("okf/handoff", "system/"),
    )


def test_strict_json_rejects_ambiguous_inputs() -> None:
    with pytest.raises(IdentityError, match="duplicate_json_key"):
        strict_json_loads('{"from":"codex","from":"agy"}')
    with pytest.raises(IdentityError, match="non_finite"):
        strict_json_loads('{"n": NaN}')
    with pytest.raises(IdentityError, match="non_finite"):
        strict_json_loads('{"n": Infinity}')
    with pytest.raises(IdentityError, match="invalid_unicode_scalar"):
        strict_json_loads('{"value": "\\ud800"}')
    with pytest.raises(IdentityError, match="non_nfc"):
        canonical_bytes({"value": "e\u0301"})
    with pytest.raises(IdentityError, match="unsafe_integer"):
        canonical_bytes({"value": 9007199254740993})


def test_bootstrap_enroll_sign_and_verify(tmp_path: Path) -> None:
    _boot_and_enroll(tmp_path)
    state = load_trust_state(tmp_path)
    assert state.mode == "audit"
    assert state.registry_version == 2

    payload = {"from": "codex", "to": "factory", "summary": "candidate"}
    artifacts = [{"type": "file_content", "name": "proof.txt", "content": "314 passed"}]
    envelope = sign_event_envelope(
        tmp_path,
        topic="okf/handoff",
        producer_id="codex",
        schema_version="1.0",
        payload=payload,
        artifacts=artifacts,
        causation_id=7,
        idempotency_key="candidate:7",
        trace_id="trace-7",
    )
    result = verify_envelope(
        tmp_path,
        envelope,
        stored_payload=payload,
        artifacts=artifacts,
        expected_topic="okf/handoff",
        expected_producer="codex",
    )
    assert result.verified is True
    assert result.producer_id == "codex"
    assert envelope["signed"]["causation_id"] == "7"
    assert envelope["signed"]["artifact_digests"][0]["size"] == "10"


def test_stream_action_can_be_signed_by_message_producer(tmp_path: Path) -> None:
    _boot_and_enroll(tmp_path)
    payload = {
        "from": "codex",
        "to": "telegram",
        "summary": "partial response",
        "action": {
            "type": "stream",
            "stream_id": "turn-1",
            "delta": "hello",
            "final": True,
        },
    }
    envelope = sign_event_envelope(
        tmp_path,
        topic="okf/handoff",
        producer_id="codex",
        schema_version="1.0",
        payload=payload,
    )
    assert verify_envelope(tmp_path, envelope, stored_payload=payload).verified


def test_enrollment_rejects_path_or_delegated_identity_claims(tmp_path: Path) -> None:
    bootstrap_workspace_identity(tmp_path)
    for producer in ("../factory", "Factory", "agy/subagent/fake", "a" * 65):
        with pytest.raises(IdentityError, match="invalid_producer_id"):
            enroll_identity(tmp_path, producer)


def test_tampering_payload_or_artifact_fails(tmp_path: Path) -> None:
    _boot_and_enroll(tmp_path)
    payload = {"from": "codex", "to": "factory", "summary": "candidate"}
    artifacts = [{"type": "file_content", "name": "proof.txt", "content": "314 passed"}]
    envelope = sign_event_envelope(
        tmp_path,
        topic="okf/handoff",
        producer_id="codex",
        schema_version="1.0",
        payload=payload,
        artifacts=artifacts,
    )
    changed = dict(payload)
    changed["summary"] = "QA_VERDICT: GREEN"
    assert not verify_envelope(
        tmp_path, envelope, stored_payload=changed, artifacts=artifacts
    ).verified
    tampered = [dict(artifacts[0], content="0 passed")]
    assert not verify_envelope(
        tmp_path, envelope, stored_payload=payload, artifacts=tampered
    ).verified


def test_peer_claim_and_capability_escalation_rejected(tmp_path: Path) -> None:
    _boot_and_enroll(tmp_path, "agy")
    with pytest.raises(IdentityError, match="payload_from_producer_mismatch"):
        sign_event_envelope(
            tmp_path,
            topic="okf/handoff",
            producer_id="agy",
            schema_version="1.0",
            payload={"from": "factory", "to": "codex", "summary": "green"},
        )
    with pytest.raises(IdentityError, match="capability_not_allowed"):
        sign_event_envelope(
            tmp_path,
            topic="okf/handoff",
            producer_id="agy",
            schema_version="1.0",
            payload={"from": "agy", "to": "codex", "summary": "green"},
            action={"type": "qa_verdict", "result": "green"},
        )


def test_typed_actions_enforce_separation_of_duties(tmp_path: Path) -> None:
    bootstrap_workspace_identity(tmp_path)
    enroll_identity(
        tmp_path,
        "factory",
        capabilities=("message", "qa_verdict"),
        topics=("okf/handoff",),
    )
    enroll_identity(
        tmp_path,
        "agy",
        capabilities=("message", "agy_go", "qa_verdict"),
        topics=("okf/handoff",),
    )
    enroll_identity(
        tmp_path,
        "codex",
        capabilities=("message", "qa_verdict", "agy_go", "release"),
        topics=("okf/handoff",),
    )
    factory_payload = {"from": "factory", "to": "codex", "summary": "GREEN"}
    qa = sign_event_envelope(
        tmp_path,
        topic="okf/handoff",
        producer_id="factory",
        schema_version="1.0",
        payload=factory_payload,
        action={"type": "qa_verdict", "result": "green", "mission_id": "qa-1"},
    )
    assert verify_envelope(tmp_path, qa, stored_payload=factory_payload).verified

    agy_payload = {"from": "agy", "to": "codex", "summary": "Phase 4 GO"}
    go = sign_event_envelope(
        tmp_path,
        topic="okf/handoff",
        producer_id="agy",
        schema_version="1.0",
        payload=agy_payload,
        action={"type": "agy_go", "phase": "4", "scope": "migration"},
    )
    assert verify_envelope(tmp_path, go, stored_payload=agy_payload).verified

    for producer, action in (
        ("codex", {"type": "qa_verdict", "result": "green"}),
        ("codex", {"type": "agy_go", "phase": "4"}),
        ("agy", {"type": "qa_verdict", "result": "green"}),
    ):
        with pytest.raises(IdentityError, match="action_producer_not_allowed"):
            sign_event_envelope(
                tmp_path,
                topic="okf/handoff",
                producer_id=producer,
                schema_version="1.0",
                payload={"from": producer, "to": "all", "summary": "forged"},
                action=action,
            )

    with pytest.raises(IdentityError, match="unknown_action_type"):
        sign_event_envelope(
            tmp_path,
            topic="okf/handoff",
            producer_id="factory",
            schema_version="1.0",
            payload=factory_payload,
            action={"type": "pretend_factory"},
        )
    with pytest.raises(IdentityError, match="invalid_qa_verdict_result"):
        sign_event_envelope(
            tmp_path,
            topic="okf/handoff",
            producer_id="factory",
            schema_version="1.0",
            payload=factory_payload,
            action={"type": "qa_verdict", "result": "maybe"},
        )


def test_store_publishes_and_binds_typed_action(tmp_path: Path) -> None:
    bootstrap_workspace_identity(tmp_path)
    enroll_identity(
        tmp_path,
        "factory",
        capabilities=("message", "qa_verdict"),
        topics=("okf/handoff",),
    )
    set_policy_mode(tmp_path, "protected")
    store = EventStore(tmp_path)
    event, _ = store.publish(
        topic="okf/handoff",
        producer_id="factory",
        schema_version="1.0",
        payload={"from": "factory", "to": "codex", "summary": "GREEN"},
        action={"type": "qa_verdict", "result": "green", "mission_id": "qa-2"},
        skip_rbac=True,
    )
    assert event.verification_status == "verified"
    assert event.identity_envelope["signed"]["action"]["type"] == "qa_verdict"
    with pytest.raises(IdentityError, match="payload_action_mismatch"):
        store.publish(
            topic="okf/handoff",
            producer_id="factory",
            schema_version="1.0",
            payload={
                "from": "factory",
                "to": "codex",
                "summary": "mismatch",
                "action": {"type": "message"},
            },
            action={"type": "qa_verdict", "result": "green"},
            skip_rbac=True,
        )
    store.close()


def test_delegated_child_is_bounded_and_cannot_impersonate_factory(
    tmp_path: Path,
) -> None:
    bootstrap_workspace_identity(tmp_path)
    enroll_identity(
        tmp_path,
        "agy",
        capabilities=("message", "implementation", "qa_verdict", "release"),
        topics=("okf/handoff", "system/"),
    )
    child, key_id = delegate_identity(
        tmp_path,
        "agy",
        "incident-fixture",
        capabilities=("message",),
        topics=("okf/handoff",),
        ttl_seconds=60,
    )
    assert child == "agy/subagent/incident-fixture"
    state = load_trust_state(tmp_path)
    entry = next(item for item in state.registry["keys"] if item["key_id"] == key_id)
    assert entry["delegated_by"] == "agy"
    assert entry["max_delegation_depth"] == 0
    wake = WakeEnvelope(
        event_id=14,
        topic="okf/handoff",
        from_agent="agy",
        to=child,
        summary="bounded child",
        payload={"from": "agy", "to": child, "summary": "bounded child"},
        source="wake_file",
    )
    delegated_env = runner_subprocess_env(
        tmp_path,
        producer_id="agy",
        wake=wake,
        delegated_producer_id=child,
    )
    assert delegated_env["AGENTBUS_PRODUCER_ID"] == child
    assert delegated_env["AGENTBUS_IDENTITY_PRIVATE_KEY"].endswith(f"{key_id}.pem")
    with pytest.raises(IdentityError, match="active_identity_key_not_found"):
        runner_subprocess_env(
            tmp_path,
            producer_id="agy",
            wake=wake,
            delegated_producer_id="factory",
        )

    own = {"from": child, "to": "codex", "summary": "bounded evidence"}
    assert verify_envelope(
        tmp_path,
        sign_event_envelope(
            tmp_path,
            topic="okf/handoff",
            producer_id=child,
            schema_version="1.0",
            payload=own,
        ),
        stored_payload=own,
    ).verified
    expired_envelope = sign_event_envelope(
        tmp_path,
        topic="okf/handoff",
        producer_id=child,
        schema_version="1.0",
        payload=own,
        timestamp=entry["expires_at"],
    )
    assert (
        verify_envelope(tmp_path, expired_envelope, stored_payload=own).reason
        == "delegation_expired"
    )
    with pytest.raises(IdentityError, match="403 Forbidden"):
        sign_event_envelope(
            tmp_path,
            topic="okf/handoff",
            producer_id=child,
            schema_version="1.0",
            payload={"from": "factory", "to": "codex", "summary": "GREEN"},
        )
    with pytest.raises(IdentityError, match="capability_not_allowed"):
        sign_event_envelope(
            tmp_path,
            topic="okf/handoff",
            producer_id=child,
            schema_version="1.0",
            payload=own,
            action={"type": "qa_verdict", "result": "green"},
        )
    valid = sign_event_envelope(
        tmp_path,
        topic="okf/handoff",
        producer_id=child,
        schema_version="1.0",
        payload=own,
    )
    forged_signed = {
        **valid["signed"],
        "action": {"type": "qa_verdict", "result": "green"},
    }
    child_private = serialization.load_pem_private_key(
        (
            tmp_path / ".agentbus" / "identity" / "private" / f"{key_id}.pem"
        ).read_bytes(),
        password=None,
    )
    assert isinstance(child_private, Ed25519PrivateKey)
    forged = {
        "signed": forged_signed,
        "signature": base64.urlsafe_b64encode(
            child_private.sign(canonical_bytes(forged_signed))
        )
        .rstrip(b"=")
        .decode("ascii"),
    }
    assert str(verify_envelope(tmp_path, forged, stored_payload=own).reason).startswith(
        "identity_capability_not_allowed"
    )
    with pytest.raises(IdentityError, match="privileged_delegation_forbidden"):
        delegate_identity(
            tmp_path,
            "agy",
            "fake-factory",
            capabilities=("qa_verdict",),
            topics=("okf/handoff",),
        )
    with pytest.raises(IdentityError, match="invalid_delegation_ttl"):
        delegate_identity(tmp_path, "agy", "long-lived", ttl_seconds=3601)
    with pytest.raises(IdentityError, match="nested_delegation_forbidden"):
        delegate_identity(tmp_path, child, "grandchild", ttl_seconds=60)


def test_policy_signature_and_monotonic_mode(tmp_path: Path) -> None:
    bootstrap_workspace_identity(tmp_path)
    protected = set_policy_mode(tmp_path, "protected")
    assert protected.mode == "protected"
    with pytest.raises(IdentityError, match="downgrade"):
        set_policy_mode(tmp_path, "audit")
    with pytest.raises(IdentityError, match="isolated_reference_monitor"):
        set_policy_mode(tmp_path, "strict")

    policy_path = tmp_path / ".agentbus" / "identity" / "policy.json"
    document = json.loads(policy_path.read_text(encoding="utf-8"))
    document["signed"]["mode"] = "audit"
    policy_path.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(VerificationError, match="invalid_policy_signature"):
        load_trust_state(tmp_path)


def test_rotation_and_immediate_revocation(tmp_path: Path) -> None:
    _boot_and_enroll(tmp_path)
    old = sign_event_envelope(
        tmp_path,
        topic="okf/handoff",
        producer_id="codex",
        schema_version="1.0",
        payload={"from": "codex", "to": "factory", "summary": "before rotate"},
    )
    old_key = old["signed"]["key_id"]
    new_key = rotate_identity(tmp_path, "codex", grace_seconds=300)
    assert new_key != old_key
    after = sign_event_envelope(
        tmp_path,
        topic="okf/handoff",
        producer_id="codex",
        schema_version="1.0",
        payload={"from": "codex", "to": "factory", "summary": "after rotate"},
    )
    assert after["signed"]["key_id"] == new_key
    revoke_identity_key(tmp_path, old_key)
    result = verify_envelope(
        tmp_path,
        old,
        stored_payload=old["signed"]["payload"],
        expected_topic="okf/handoff",
        expected_producer="codex",
    )
    assert result.verified is False
    assert result.reason == "key_revoked"


def test_environment_can_raise_but_not_lower_mode(tmp_path: Path, monkeypatch) -> None:
    bootstrap_workspace_identity(tmp_path)
    monkeypatch.setenv("AGENTBUS_IDENTITY_MODE", "protected")
    assert load_trust_state(tmp_path).mode == "protected"
    set_policy_mode(tmp_path, "protected")
    monkeypatch.setenv("AGENTBUS_IDENTITY_MODE", "audit")
    assert load_trust_state(tmp_path).mode == "protected"


def test_cross_language_fixture_canonical_bytes_and_signature() -> None:
    fixture_path = (
        Path(__file__).parent / "fixtures" / "agentid" / "cross_language_v1.json"
    )
    fixture = json.loads(fixture_path.read_text(encoding="utf-8"))
    raw = canonical_bytes(fixture["envelope"]["signed"])
    assert hashlib.sha256(raw).hexdigest() == fixture["canonical_sha256"]
    public = Ed25519PublicKey.from_public_bytes(
        base64.urlsafe_b64decode(fixture["public_key"] + "==")
    )
    public.verify(
        base64.urlsafe_b64decode(fixture["envelope"]["signature"] + "=="),
        raw,
    )


def test_store_signs_and_recomputes_artifact_verification(tmp_path: Path) -> None:
    _boot_and_enroll(tmp_path)
    store = EventStore(tmp_path)
    event, duplicate = store.publish(
        topic="okf/handoff",
        producer_id="codex",
        schema_version="1.0",
        payload={
            "from": "codex",
            "to": "factory",
            "summary": "candidate",
            "artifacts": [
                {
                    "type": "file_content",
                    "name": "proof.txt",
                    "content": "314 passed",
                }
            ],
        },
    )
    assert duplicate is False
    assert event.verification_status == "verified"
    assert store.verify_event(event.event_id).verified is True
    store._conn.execute(
        "UPDATE artifacts SET content_blob='0 passed' WHERE event_id=?",
        (event.event_id,),
    )
    store._conn.commit()
    result = store.verify_event(event.event_id)
    assert result.verified is False
    assert result.reason == "artifact_digest_mismatch"
    store.close()


def test_store_rejects_signed_nonce_replay(tmp_path: Path, monkeypatch) -> None:
    _boot_and_enroll(tmp_path)
    payload = {"from": "codex", "to": "factory", "summary": "nonce fixture"}
    envelope = sign_event_envelope(
        tmp_path,
        topic="okf/handoff",
        producer_id="codex",
        schema_version="1.0",
        payload=payload,
    )
    store = EventStore(tmp_path)
    store.publish(
        topic="okf/handoff",
        producer_id="codex",
        schema_version="1.0",
        payload=payload,
        identity_envelope=envelope,
    )
    monkeypatch.setattr(store, "_find_recent_content_duplicate", lambda *a, **k: None)
    with pytest.raises(IdentityError, match="identity_nonce_replay"):
        store.publish(
            topic="okf/handoff",
            producer_id="codex",
            schema_version="1.0",
            payload=payload,
            identity_envelope=envelope,
        )
    store.close()


@pytest.mark.parametrize("round_number", range(10))
def test_concurrent_identical_envelope_has_one_commit(
    tmp_path: Path, round_number: int
) -> None:
    _boot_and_enroll(tmp_path)
    payload = {
        "from": "codex",
        "to": "factory",
        "summary": f"one envelope round {round_number}",
    }
    envelope = sign_event_envelope(
        tmp_path,
        topic="okf/handoff",
        producer_id="codex",
        schema_version="1.0",
        payload=payload,
    )
    EventStore(tmp_path).close()  # finish migrations before concurrent opens

    def submit() -> str:
        store = EventStore(tmp_path, auto_prune=False)
        try:
            store.publish(
                topic="okf/handoff",
                producer_id="codex",
                schema_version="1.0",
                payload=payload,
                identity_envelope=envelope,
            )
            return "committed"
        except IdentityError as exc:
            return str(exc)
        finally:
            store.close()

    with ThreadPoolExecutor(max_workers=4) as pool:
        outcomes = list(pool.map(lambda _: submit(), range(4)))
    assert outcomes.count("committed") == 1
    assert outcomes.count("identity_nonce_replay") == 3


def test_protected_publish_authenticates_before_scoped_dedup(tmp_path: Path) -> None:
    _boot_and_enroll(tmp_path)
    enroll_identity(tmp_path, "agy", capabilities=("message",), topics=("okf/handoff",))
    set_policy_mode(tmp_path, "protected")
    store = EventStore(tmp_path)
    original, _ = store.publish(
        topic="okf/handoff",
        producer_id="codex",
        schema_version="1.0",
        payload={"from": "codex", "to": "factory", "summary": "candidate"},
        idempotency_key="shared-probe",
    )
    with pytest.raises(IdentityError, match="403 Forbidden"):
        store.publish(
            topic="okf/handoff",
            producer_id="agy",
            schema_version="1.0",
            payload={"from": "codex", "to": "factory", "summary": "candidate"},
            idempotency_key="shared-probe",
        )
    independent, duplicate = store.publish(
        topic="okf/handoff",
        producer_id="agy",
        schema_version="1.0",
        payload={"from": "agy", "to": "codex", "summary": "independent"},
        idempotency_key="shared-probe",
    )
    assert duplicate is False
    assert independent.event_id != original.event_id
    store.close()


def test_protected_cli_requires_explicit_matching_signing_handle(tmp_path: Path) -> None:
    bootstrap_workspace_identity(tmp_path)
    key_id = enroll_identity(
        tmp_path,
        "factory",
        capabilities=("message", "qa_verdict"),
        topics=("okf/handoff",),
    )
    codex_key_id = enroll_identity(
        tmp_path, "codex", capabilities=("message",), topics=("okf/handoff",)
    )
    set_policy_mode(tmp_path, "protected")
    args = [
        "publish",
        "--workspace",
        str(tmp_path),
        "--topic",
        "okf/handoff",
        "--producer-id",
        "factory",
        "--payload",
        json.dumps({"from": "factory", "to": "codex", "summary": "GREEN"}),
        "--action",
        json.dumps({"type": "qa_verdict", "result": "green"}),
    ]
    runner = CliRunner()
    denied = runner.invoke(main, args, env={"AGENTBUS_IDENTITY_PRIVATE_KEY": ""})
    assert denied.exit_code == 1
    assert "403 Forbidden" in denied.output
    store = EventStore(tmp_path, auto_prune=False)
    assert store.latest_event_id() == 0
    store.close()

    wrong_private = (
        tmp_path / ".agentbus" / "identity" / "private" / f"{codex_key_id}.pem"
    )
    wrong_identity = runner.invoke(
        main,
        args,
        env={"AGENTBUS_IDENTITY_PRIVATE_KEY": str(wrong_private)},
    )
    assert wrong_identity.exit_code == 1
    assert "signing_key_identity_mismatch" in wrong_identity.output

    private_path = tmp_path / ".agentbus" / "identity" / "private" / f"{key_id}.pem"
    allowed = runner.invoke(
        main,
        args,
        env={"AGENTBUS_IDENTITY_PRIVATE_KEY": str(private_path)},
    )
    assert allowed.exit_code == 0, allowed.output
    store = EventStore(tmp_path, auto_prune=False)
    published = store.get_event(1)
    assert published is not None
    assert published.identity_envelope["signed"]["action"] == {
        "type": "qa_verdict",
        "result": "green",
    }
    store.close()


def test_protected_reads_filter_database_and_artifact_tampering(tmp_path: Path) -> None:
    _boot_and_enroll(tmp_path)
    set_policy_mode(tmp_path, "protected")
    store = EventStore(tmp_path)
    event, _ = store.publish(
        topic="okf/handoff",
        producer_id="codex",
        schema_version="1.0",
        payload={
            "from": "codex",
            "to": "factory",
            "summary": "phase 2",
            "artifacts": [
                {"type": "file_content", "name": "proof.txt", "content": "green"}
            ],
        },
        trace_id="phase-2-trace",
    )
    store._conn.execute(
        "UPDATE artifacts SET content_blob='forged' WHERE event_id=?",
        (event.event_id,),
    )
    store._conn.commit()

    assert store.verify_event(event.event_id).reason == "artifact_digest_mismatch"
    assert store.get_event(event.event_id) is None
    polled = store.poll("okf/handoff")
    assert polled["events"] == []
    assert polled["latest_id"] == event.event_id
    assert store.fetch_trace_events("phase-2-trace") == []
    assert store.fetch_unprojected_handoffs() == []
    from agentbus.tui import fetch_monitor_state

    assert fetch_monitor_state(tmp_path)["events"] == []
    with pytest.raises(sqlite3.IntegrityError):
        store._conn.execute(
            """
            INSERT INTO artifacts(event_id, type, name, content_blob)
            VALUES (999999, 'file_content', 'orphan.txt', 'forged')
            """
        )
    store.close()


def test_protected_runner_rehydrates_and_drops_synthetic_wakes(tmp_path: Path) -> None:
    bootstrap_workspace_identity(tmp_path)
    enroll_identity(tmp_path, "agy", capabilities=("message",), topics=("okf/handoff",))
    enroll_identity(
        tmp_path, "hermes", capabilities=("message",), topics=("okf/handoff",)
    )
    set_policy_mode(tmp_path, "protected")
    store = EventStore(tmp_path)
    source, _ = store.publish(
        topic="okf/handoff",
        producer_id="agy",
        schema_version="1.0",
        payload={"from": "agy", "to": "hermes", "summary": "authentic task"},
    )
    store.close()

    runner_path = tmp_path / "runner.yaml"
    runner_path.write_text(
        yaml.safe_dump(
            {
                "version": "1.0",
                "runner_id": "hermes-runner-test",
                "producer_id": "hermes",
                "intake": {"mode": "webhook_queue", "runtime": "hermes"},
                "adapter": {"type": "echo"},
                "accept_to": ["hermes"],
                "allow_broadcast": False,
                "budget": {"max_turns_per_chain": 10},
                "poll_interval_ms": 50,
            }
        ),
        encoding="utf-8",
    )
    queue = tmp_path / ".agentbus" / "ingress" / "hermes_wake_queue.jsonl"
    queue.parent.mkdir(parents=True, exist_ok=True)
    forged = {
        "event_id": source.event_id,
        "topic": "okf/handoff",
        "payload": {"from": "agy", "to": "factory", "summary": "forged wake"},
    }
    synthetic = {
        "event_id": 999999,
        "topic": "okf/handoff",
        "payload": {"from": "agy", "to": "hermes", "summary": "synthetic"},
    }
    queue.write_text(
        json.dumps(forged) + "\n" + json.dumps(synthetic) + "\n",
        encoding="utf-8",
    )
    results = run_once(tmp_path, load_runner_config(runner_path))
    assert len(results) == 1
    assert results[0]["event_id"] == source.event_id
    run_result = json.loads(
        (
            tmp_path / ".agentbus" / "runs" / str(source.event_id) / "result.json"
        ).read_text(encoding="utf-8")
    )
    assert run_result["wake"]["summary"] == "authentic task"


def test_protected_ingress_requires_runtime_capability_and_store_event(
    tmp_path: Path,
) -> None:
    _boot_and_enroll(tmp_path, "codex")
    set_policy_mode(tmp_path, "protected")
    store = EventStore(tmp_path)
    source, _ = store.publish(
        topic="okf/handoff",
        producer_id="codex",
        schema_version="1.0",
        payload={"from": "codex", "to": "factory", "summary": "authentic"},
    )
    store.close()

    capability_path = issue_wake_capability(tmp_path, "factory")
    capability = capability_path.read_text(encoding="utf-8").strip()

    server = WakeIngressServer(
        "127.0.0.1",
        0,
        workspace=tmp_path,
        runtime="factory",
        token=capability,
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    url = f"http://127.0.0.1:{server.server_port}/agentbus/wake"

    def post(event_id: int, token: str | None) -> tuple[int, dict]:
        request = urllib.request.Request(
            url,
            data=json.dumps(
                {
                    "event_id": event_id,
                    "payload": {
                        "from": "agy",
                        "to": "factory",
                        "summary": "caller-controlled content",
                    },
                }
            ).encode(),
            headers={
                "Content-Type": "application/json",
                **({"Authorization": f"Bearer {token}"} if token else {}),
            },
        )
        try:
            with urllib.request.urlopen(request) as response:
                return response.status, json.loads(response.read())
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read())

    try:
        assert post(source.event_id, None)[0] == 401
        assert post(999999, capability)[0] == 404
        assert post(source.event_id, capability)[0] == 200
        queued = json.loads(server.store.queue_path.read_text(encoding="utf-8"))
        assert queued["summary"] == "authentic"
        assert queued["from"] == "codex"
    finally:
        server.shutdown()
        server.server_close()
        server.store.close()
        thread.join(timeout=1)
