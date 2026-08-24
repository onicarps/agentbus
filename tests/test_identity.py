from __future__ import annotations

import base64
import json
import hashlib
import threading
import urllib.error
import urllib.request
from pathlib import Path

import pytest
import yaml
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

from agentbus.identity import (
    IdentityError,
    VerificationError,
    bootstrap_workspace_identity,
    canonical_bytes,
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
from agentbus.runner import load_runner_config, run_once
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
