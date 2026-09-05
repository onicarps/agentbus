"""AgentID Phase 4 migration, recovery, and incident regressions."""

from __future__ import annotations

import base64
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from click.testing import CliRunner
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from agentbus.identity import (
    IdentityError,
    VerificationError,
    bootstrap_workspace_identity,
    canonical_bytes,
    configure_reference_monitor,
    delegate_identity,
    enroll_identity,
    load_recovery_ledger,
    load_trust_state,
    record_break_glass_recovery,
    rotate_identity,
    set_policy_mode,
    sign_event_envelope,
    verify_envelope,
)
from agentbus.cli import main
from agentbus.runner.wait_store import WaitPredicate, match_predicate
from agentbus.store import EventStore

FIXTURES = Path(__file__).parent / "fixtures" / "agentid"
REPO = Path(__file__).resolve().parents[1]


def test_all_32_adversarial_scenarios_have_executable_evidence() -> None:
    matrix = json.loads((FIXTURES / "negative_matrix.json").read_text(encoding="utf-8"))
    assert set(matrix) == {f"N{number}" for number in range(1, 36)}
    for scenario, evidence in matrix.items():
        assert evidence, scenario
        for node_id in evidence:
            relative, symbol = node_id.split("::", 1)
            source = REPO / relative
            assert source.is_file(), node_id
            assert symbol in source.read_text(encoding="utf-8"), node_id


def _create_released_database(workspace: Path, event: dict) -> sqlite3.Connection:
    bus = workspace / ".agentbus"
    bus.mkdir(parents=True)
    conn = sqlite3.connect(bus / "events.db")
    conn.executescript(
        """
        CREATE TABLE events (
          event_id INTEGER PRIMARY KEY AUTOINCREMENT,
          topic TEXT NOT NULL, producer_id TEXT NOT NULL, timestamp TEXT NOT NULL,
          schema_version TEXT NOT NULL, payload TEXT NOT NULL,
          causation_id INTEGER, idempotency_key TEXT UNIQUE,
          status TEXT NOT NULL DEFAULT 'PUBLISHED', pending_until TEXT,
          rejection_reason TEXT, projected_to_log INTEGER NOT NULL DEFAULT 0,
          sla_timeout_minutes INTEGER, sla_deadline TEXT,
          sla_cleared INTEGER NOT NULL DEFAULT 0,
          trace_id TEXT, span_id TEXT, parent_span_id TEXT
        );
        CREATE INDEX idx_events_topic_id ON events(topic, event_id);
        CREATE INDEX idx_events_trace_id ON events(trace_id);
        CREATE TABLE artifacts (
          id INTEGER PRIMARY KEY AUTOINCREMENT, event_id INTEGER NOT NULL,
          type TEXT NOT NULL, name TEXT NOT NULL, content_blob TEXT NOT NULL,
          FOREIGN KEY (event_id) REFERENCES events(event_id)
        );
        CREATE INDEX idx_artifacts_event_id ON artifacts(event_id);
        """
    )
    conn.execute(
        """
        INSERT INTO events(
          event_id, topic, producer_id, timestamp, schema_version, payload,
          causation_id, idempotency_key
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            event["event_id"],
            event["topic"],
            event["producer_id"],
            event["timestamp"],
            event["schema_version"],
            event["payload"],
            event["causation_id"],
            event["idempotency_key"],
        ),
    )
    conn.commit()
    return conn


def test_released_database_fixtures_migrate_additively(tmp_path: Path) -> None:
    fixture = json.loads(
        (FIXTURES / "released_database_fixtures.json").read_text(encoding="utf-8")
    )
    event = fixture["event"]
    stable_fields = (
        "event_id",
        "topic",
        "producer_id",
        "timestamp",
        "schema_version",
        "payload",
        "causation_id",
        "idempotency_key",
    )
    for release in fixture["releases"]:
        workspace = tmp_path / release["version"]
        conn = _create_released_database(workspace, event)
        before = conn.execute(
            f"SELECT {', '.join(stable_fields)} FROM events WHERE event_id = 158"
        ).fetchone()
        conn.close()

        store = EventStore(workspace, retention_days=0, auto_prune=False)
        after = store._conn.execute(
            f"SELECT {', '.join(stable_fields)} FROM events WHERE event_id = 158"
        ).fetchone()
        migrated = store._conn.execute(
            "SELECT verification_status, identity_envelope FROM events WHERE event_id=158"
        ).fetchone()
        version = store._conn.execute(
            "SELECT version FROM schema_version WHERE component='event_store'"
        ).fetchone()[0]
        assert tuple(after) == tuple(before)
        assert tuple(migrated) == ("legacy_unverified", None)
        assert version == 20
        assert store.verify_event(158).reason == "legacy_unverified"
        store.close()


def test_mode_promotion_downgrade_and_nonretroactive_recovery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bootstrap_workspace_identity(tmp_path)
    enroll_identity(tmp_path, "codex")
    store = EventStore(tmp_path, retention_days=0)
    legacy, _ = store.publish(
        topic="okf/handoff",
        producer_id="codex",
        schema_version="1.0",
        payload={"from": "codex", "to": "agy", "summary": "legacy audit row"},
        auto_sign=False,
        skip_rbac=True,
    )
    assert legacy.verification_status == "legacy_unverified"
    store.close()

    set_policy_mode(tmp_path, "protected")
    with pytest.raises(IdentityError, match="isolated_reference_monitor"):
        set_policy_mode(tmp_path, "strict")
    with pytest.raises(IdentityError, match="attestation_required"):
        configure_reference_monitor(tmp_path, "isolated_broker")
    configure_reference_monitor(
        tmp_path, "isolated_broker", isolation_attested=True
    )
    assert set_policy_mode(tmp_path, "strict").mode == "strict"
    monkeypatch.setenv("AGENTBUS_IDENTITY_MODE", "audit")
    assert load_trust_state(tmp_path).mode == "strict"
    with pytest.raises(IdentityError, match="downgrade"):
        set_policy_mode(tmp_path, "protected")
    with pytest.raises(IdentityError, match="monitor_downgrade"):
        configure_reference_monitor(tmp_path, "local_audit")

    recovery = record_break_glass_recovery(
        tmp_path,
        reason="rotate a lost online key",
        effective_after_event_id=legacy.event_id,
    )
    assert recovery["non_retroactive"] is True
    assert recovery["effective_after_event_id"] == str(legacy.event_id)
    assert load_recovery_ledger(tmp_path)["recovery_version"] == "1"
    recovery_path = tmp_path / ".agentbus" / "identity" / "recovery.json"
    recovery_v1 = recovery_path.read_bytes()
    record_break_glass_recovery(
        tmp_path,
        reason="second prospective recovery",
        effective_after_event_id=legacy.event_id,
    )
    recovery_path.write_bytes(recovery_v1)
    with pytest.raises(VerificationError, match="recovery_rollback"):
        load_recovery_ledger(tmp_path)
    store = EventStore(tmp_path, retention_days=0, auto_prune=False)
    assert store.verify_event(legacy.event_id).reason == "legacy_unverified"
    with pytest.raises(IdentityError, match="identity_verification_required"):
        store.publish(
            topic="okf/handoff",
            producer_id="codex",
            schema_version="1.0",
            payload={"from": "codex", "to": "agy", "summary": "unsigned strict"},
            auto_sign=False,
            skip_rbac=True,
        )
    store.close()
    tampered = json.loads(recovery_path.read_text(encoding="utf-8"))
    tampered["signed"]["events"][0]["non_retroactive"] = False
    recovery_path.write_text(json.dumps(tampered), encoding="utf-8")
    with pytest.raises(VerificationError, match="invalid_recovery_signature"):
        load_recovery_ledger(tmp_path)


def test_reference_monitor_and_recovery_cli(tmp_path: Path) -> None:
    bootstrap_workspace_identity(tmp_path)
    runner = CliRunner()
    denied = runner.invoke(
        main,
        [
            "identity",
            "reference-monitor",
            "isolated_broker",
            "--workspace",
            str(tmp_path),
        ],
    )
    assert denied.exit_code == 1
    assert "attestation_required" in denied.output
    configured = runner.invoke(
        main,
        [
            "identity",
            "reference-monitor",
            "isolated_broker",
            "--isolation-attested",
            "--workspace",
            str(tmp_path),
        ],
    )
    assert configured.exit_code == 0, configured.output
    assert json.loads(configured.output)["strict_ready"] is False
    recovered = runner.invoke(
        main,
        [
            "identity",
            "record-recovery",
            "--workspace",
            str(tmp_path),
            "--reason",
            "lost online signer",
        ],
    )
    assert recovered.exit_code == 0, recovered.output
    assert json.loads(recovered.output)["non_retroactive"] is True


def test_policy_clock_workspace_and_rotation_replays_fail_closed(tmp_path: Path) -> None:
    workspace = tmp_path / "one"
    other = tmp_path / "two"
    bootstrap_workspace_identity(workspace)
    old_policy = (workspace / ".agentbus" / "identity" / "policy.json").read_bytes()
    old_registry = (workspace / ".agentbus" / "identity" / "registry.json").read_bytes()
    key_id = enroll_identity(workspace, "codex")
    bootstrap_workspace_identity(other)
    enroll_identity(other, "codex")
    payload = {"from": "codex", "to": "agy", "summary": "replay fixture"}
    current_timestamp = (
        datetime.now(timezone.utc)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )
    first = sign_event_envelope(
        workspace,
        topic="okf/handoff",
        producer_id="codex",
        schema_version="1.0",
        payload=payload,
        timestamp=current_timestamp,
    )
    assert verify_envelope(other, first, stored_payload=payload).reason == (
        "workspace_id_mismatch"
    )
    store = EventStore(workspace)
    store.publish(
        topic="okf/handoff",
        producer_id="codex",
        schema_version="1.0",
        payload=payload,
        identity_envelope=first,
        skip_rbac=True,
    )
    older = sign_event_envelope(
        workspace,
        topic="okf/handoff",
        producer_id="codex",
        schema_version="1.0",
        payload={**payload, "summary": "older clock"},
        timestamp=(datetime.now(timezone.utc) - timedelta(seconds=1))
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z"),
    )
    with pytest.raises(IdentityError, match="event_timestamp_rollback"):
        store.publish(
            topic="okf/handoff",
            producer_id="codex",
            schema_version="1.0",
            payload=older["signed"]["payload"],
            identity_envelope=older,
            skip_rbac=True,
        )
    store.close()

    before_rotate = sign_event_envelope(
        workspace,
        topic="okf/handoff",
        producer_id="codex",
        schema_version="1.0",
        payload=payload,
    )
    rotate_identity(workspace, "codex", grace_seconds=0)
    state = load_trust_state(workspace)
    old_entry = next(item for item in state.registry["keys"] if item["key_id"] == key_id)
    forged_signed = {
        **before_rotate["signed"],
        "timestamp": "9999-12-31T23:59:59.000000Z",
        "nonce": "rotation-expired-fixture",
    }
    private = serialization.load_pem_private_key(
        (
            workspace
            / ".agentbus"
            / "identity"
            / "private"
            / f"{old_entry['key_id']}.pem"
        ).read_bytes(),
        password=None,
    )
    assert isinstance(private, Ed25519PrivateKey)
    expired = {
        "signed": forged_signed,
        "signature": base64.urlsafe_b64encode(private.sign(canonical_bytes(forged_signed)))
        .rstrip(b"=")
        .decode("ascii"),
    }
    assert verify_envelope(workspace, expired, stored_payload=payload).reason == (
        "rotation_grace_expired"
    )

    policy_path = workspace / ".agentbus" / "identity" / "policy.json"
    policy_path.write_bytes(old_policy)
    (workspace / ".agentbus" / "identity" / "registry.json").write_bytes(
        old_registry
    )
    with pytest.raises(VerificationError, match="policy_rollback"):
        load_trust_state(workspace)
    (workspace / ".agentbus" / "identity" / "high-water.json").write_text(
        json.dumps({"policy_version": 0, "registry_version": 2}),
        encoding="utf-8",
    )
    with pytest.raises(VerificationError, match="registry_rollback"):
        load_trust_state(workspace)


def test_raw_signed_looking_row_is_untrusted(tmp_path: Path) -> None:
    bootstrap_workspace_identity(tmp_path)
    enroll_identity(tmp_path, "codex")
    set_policy_mode(tmp_path, "protected")
    store = EventStore(tmp_path)
    store._conn.execute(
        """
        INSERT INTO events(
          topic, producer_id, timestamp, schema_version, payload, status,
          identity_envelope, verification_status
        ) VALUES (?, ?, ?, ?, ?, 'PUBLISHED', ?, 'verified')
        """,
        (
            "okf/handoff",
            "factory",
            "2026-08-25T00:00:00Z",
            "1.0",
            json.dumps({"from": "factory", "to": "codex", "summary": "forged"}),
            json.dumps({"signed": {"producer_id": "factory"}, "signature": "fake"}),
        ),
    )
    event_id = store._conn.execute("SELECT MAX(event_id) FROM events").fetchone()[0]
    store._conn.commit()
    assert store.get_event(event_id) is None
    assert not store.verify_event(event_id).verified
    store.close()


def test_exact_july_incident_procedures_cannot_substitute_peers_or_factory(
    tmp_path: Path,
) -> None:
    incident = json.loads(
        (FIXTURES / "july_2026_incident.json").read_text(encoding="utf-8")
    )
    assert incident["certification_substitution"]["child_type"] == "self"
    assert incident["certification_substitution"]["historical_event_id"] == 158
    bootstrap_workspace_identity(tmp_path)
    enroll_identity(
        tmp_path,
        "agy",
        capabilities=("message", "qa_verdict"),
        topics=("okf/handoff",),
    )
    child, _ = delegate_identity(tmp_path, "agy", "self", ttl_seconds=60)
    set_policy_mode(tmp_path, "protected")
    store = EventStore(tmp_path)
    before = store.latest_event_id()
    for attempt in incident["direct_peer_impersonation"]:
        assert f'"from":"{attempt["claimed_identity"]}"' in attempt["literal_command"]
        with pytest.raises(IdentityError, match="payload_from_producer_mismatch"):
            store.publish(
                topic="okf/handoff",
                producer_id=child,
                schema_version="1.0",
                payload={
                    "from": attempt["claimed_identity"],
                    "to": "all",
                    "summary": "historical literal attempt",
                },
                skip_rbac=True,
            )
    with pytest.raises(IdentityError, match="capability_not_allowed"):
        store.publish(
            topic="okf/handoff",
            producer_id=child,
            schema_version="1.0",
            payload={"from": child, "to": "codex", "summary": "86 green tests"},
            action={"type": "qa_verdict", "result": "green"},
            skip_rbac=True,
        )
    assert store.latest_event_id() == before
    forged_gate = {
        "producer_id": "agy",
        "causation_id": 1211,
        "verification_status": "verified",
        "identity_envelope": {"signed": {"action": {"type": "message"}}},
        "payload": {"from": "agy", "summary": "QA_VERDICT: GREEN"},
    }
    assert not match_predicate(
        WaitPredicate(
            from_any=["factory"],
            causation_id=1211,
            action_type="qa_verdict",
            action_result="green",
        ),
        forged_gate,
        waiter_producer_id="codex",
    )
    store.close()
