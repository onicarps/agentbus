"""v0.16 async suspend / await hard gates (Factory QA gates + design §4)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import yaml
from click.testing import CliRunner

from agentbus.cli import main
from agentbus.runner import load_runner_config, run_once
from agentbus.runner.budget import ChainBudget
from agentbus.runner.config import runner_state_path
from agentbus.runner.types import AWAIT_EXIT_CODE, TurnResult
from agentbus.runner.wait_store import (
    WaitPredicate,
    WaitStore,
    build_resume_payload,
    build_resume_wake,
    clamp_timeout_hours,
    match_predicate,
    resume_idempotency_key,
    suspend_ack_idempotency_key,
    write_await_drop,
)
from agentbus.runner.wait_tick import fulfill_wait, tick_waits
from agentbus.schemas import DEAD_LETTER_TOPIC
from agentbus.store import EventStore


def _write_runner_yaml(path: Path, **overrides) -> Path:
    data = {
        "version": "1.0",
        "runner_id": "test-runner-1",
        "producer_id": "hermes",
        "intake": {"mode": "webhook_queue", "runtime": "hermes"},
        "adapter": {"type": "echo"},
        "accept_to": ["hermes", "devops"],
        "allow_broadcast": False,
        "budget": {"max_turns_per_chain": 10},
        "poll_interval_ms": 50,
    }
    for k, v in overrides.items():
        if isinstance(v, dict) and isinstance(data.get(k), dict):
            data[k] = {**data[k], **v}
        else:
            data[k] = v
    path.write_text(yaml.safe_dump(data), encoding="utf-8")
    return path


def _enqueue(
    ws: Path,
    event_id: int,
    *,
    to: str = "hermes",
    frm: str = "agy",
    summary: str = "do work",
    causation_id: int | None = None,
):
    qdir = ws / ".agentbus" / "ingress"
    qdir.mkdir(parents=True, exist_ok=True)
    rec = {
        "received_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "event_id": event_id,
        "runtime": "hermes",
        "from": frm,
        "to": to,
        "summary": summary,
        "topic": "okf/handoff",
        "causation_id": causation_id,
        "raw": {
            "event_id": event_id,
            "topic": "okf/handoff",
            "causation_id": causation_id,
            "payload": {"from": frm, "to": to, "summary": summary},
        },
    }
    with (qdir / "hermes_wake_queue.jsonl").open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(rec) + "\n")


def test_turn_result_status_and_ok_property():
    assert TurnResult(status="ok", summary="x").ok is True
    assert TurnResult(status="suspended", summary="x").ok is True
    assert TurnResult(status="error", summary="x").ok is False
    assert TurnResult(ok=True, summary="legacy").status == "ok"
    assert TurnResult(ok=False, summary="legacy").status == "error"


def test_clamp_timeout_hours():
    assert clamp_timeout_hours(None) == 4.0
    assert clamp_timeout_hours(0) == 4.0
    assert clamp_timeout_hours(-1) == 4.0
    assert clamp_timeout_hours(2) == 2.0
    assert clamp_timeout_hours(100) == 24.0


def test_match_predicate_primary_and_self_guard():
    pred = WaitPredicate(from_any=["factory"], causation_id=100, summary_contains="QA_VERDICT")
    good = {
        "event_id": 200,
        "topic": "okf/handoff",
        "producer_id": "factory",
        "causation_id": 100,
        "payload": {"from": "factory", "to": "grok", "summary": "QA_VERDICT: GREEN"},
    }
    assert match_predicate(pred, good, waiter_producer_id="grok") is True

    # self-fulfillment guard
    self_ev = {
        **good,
        "producer_id": "grok",
        "payload": {"from": "grok", "to": "agy", "summary": "QA_VERDICT: GREEN"},
    }
    assert match_predicate(pred, self_ev, waiter_producer_id="grok") is False

    # wrong causation
    bad_c = {**good, "causation_id": 99}
    assert match_predicate(pred, bad_c, waiter_producer_id="grok") is False

    # free-text only not allowed
    free = WaitPredicate(summary_contains="hello")
    assert match_predicate(free, good, waiter_producer_id="grok") is False


def test_build_resume_schema_and_idempotency_keys(tmp_path: Path):
    waits = WaitStore(tmp_path)
    wait = waits.create(
        runner_id="r1",
        producer_id="hermes",
        chain_key="394",
        origin_event_id=412,
        predicate=WaitPredicate(from_any=["factory"], causation_id=412),
        reason="wait factory",
        timeout_hours=4,
        wait_id="w_test1",
    )
    payload = build_resume_payload(
        wait, fulfilled_by=450, status="ok", reason="matched"
    )
    assert set(payload.keys()) >= {"from", "to", "summary", "resume"}
    resume = payload["resume"]
    assert set(resume.keys()) == {
        "wait_id",
        "chain_key",
        "origin_event_id",
        "fulfilled_by",
        "status",
        "reason",
    }
    assert resume["chain_key"] == "394"
    assert resume["status"] == "ok"
    assert payload["summary"].startswith("RESUME:")
    assert resume_idempotency_key("w_test1", 450) == "resume:w_test1:450"
    assert suspend_ack_idempotency_key("r1", 412) == "suspend-ack:r1:412"

    env = build_resume_wake(
        wait, resume_event_id=999, fulfilled_by=450, status="ok", reason="matched"
    )
    assert env.causation_id == 394  # chain_key as int
    assert env.source == "resume"
    assert env.to == "hermes"


def test_await_cli_writes_drop_and_exits_75(tmp_path: Path):
    # Pin resolve_workspace to tmp_path (parent /tmp may have .agentbus)
    (tmp_path / ".agentbus").mkdir(parents=True, exist_ok=True)
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "await",
            "--workspace",
            str(tmp_path),
            "--event-id",
            "412",
            "--expect-from",
            "factory",
            "--causation-id",
            "412",
            "--match",
            "QA_VERDICT",
            "--timeout-hours",
            "4",
            "--producer-id",
            "grok",
        ],
    )
    assert result.exit_code == AWAIT_EXIT_CODE
    drop = tmp_path / ".agentbus" / "runs" / "412" / "await.json"
    assert drop.is_file()
    data = json.loads(drop.read_text(encoding="utf-8"))
    assert data["origin_event_id"] == 412
    assert data["predicate"]["from_any"] == ["factory"]
    assert data["predicate"]["causation_id"] == 412
    assert data["timeout_hours"] == 4.0


def test_await_cli_rejects_match_only(tmp_path: Path):
    (tmp_path / ".agentbus").mkdir(parents=True, exist_ok=True)
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "await",
            "--workspace",
            str(tmp_path),
            "--event-id",
            "1",
            "--match",
            "only-summary",
        ],
    )
    assert result.exit_code != 0
    assert result.exit_code != AWAIT_EXIT_CODE


def test_suspend_via_await_drop_echo(tmp_path: Path):
    """await drop → RUNNER_SUSPEND + WaitRegistration + suspend-ack key."""
    cfg_path = _write_runner_yaml(tmp_path / "runner.yaml")
    _enqueue(tmp_path, 50, to="hermes", frm="agy", summary="need QA")
    write_await_drop(
        tmp_path,
        50,
        {
            "wait_id": "w_s50",
            "origin_event_id": 50,
            "producer_id": "hermes",
            "runner_id": "test-runner-1",
            "timeout_hours": 4,
            "reason": "wait factory",
            "predicate": {
                "from_any": ["factory"],
                "causation_id": 50,
                "summary_contains": "QA_VERDICT",
                "topic": "okf/handoff",
            },
        },
    )
    cfg = load_runner_config(cfg_path)
    results = run_once(tmp_path, cfg)
    # may include wait_tick empty results
    suspended = [r for r in results if r.get("status") == "suspended"]
    assert len(suspended) == 1
    assert suspended[0]["ok"] is True
    assert suspended[0]["wait_id"] == "w_s50"
    assert suspended[0]["summary"].startswith("RUNNER_SUSPEND:")
    assert "blocked" not in suspended[0]["summary"].lower()
    assert "error" not in suspended[0]["summary"].lower()

    wait = WaitStore(tmp_path).load("w_s50")
    assert wait is not None
    assert wait.status == "pending"
    assert wait.chain_key == "50"  # no causation → event_id
    assert wait.producer_id == "hermes"

    store = EventStore(tmp_path)
    try:
        events = store.poll("okf/handoff", since_id=0)["events"]
        acks = [e for e in events if e["payload"].get("summary", "").startswith("RUNNER_SUSPEND:")]
        assert len(acks) == 1
        assert acks[0]["idempotency_key"] == "suspend-ack:test-runner-1:50"
        assert acks[0]["causation_id"] == 50
    finally:
        store.close()


def test_fulfillment_resume_and_budget_continuity(tmp_path: Path):
    """Predicate match → single resume with causation_id=chain_key; budget shared."""
    cfg_path = _write_runner_yaml(
        tmp_path / "runner.yaml",
        budget={"max_turns_per_chain": 3},
    )
    # Origin chain root 100
    _enqueue(tmp_path, 100, to="hermes", frm="agy", summary="start", causation_id=None)
    write_await_drop(
        tmp_path,
        100,
        {
            "wait_id": "w_chain",
            "origin_event_id": 100,
            "producer_id": "hermes",
            "timeout_hours": 4,
            "predicate": {
                "from_any": ["factory"],
                "causation_id": 100,
                "summary_contains": "QA_VERDICT",
                "topic": "okf/handoff",
            },
        },
    )
    cfg = load_runner_config(cfg_path)
    r1 = run_once(tmp_path, cfg)
    assert any(x.get("status") == "suspended" for x in r1)

    store = EventStore(tmp_path)
    try:
        # Fulfilling event from factory
        store.publish(
            topic="okf/handoff",
            producer_id="factory",
            schema_version="1.0",
            payload={
                "from": "factory",
                "to": "hermes",
                "summary": "QA_VERDICT: GREEN mission done",
            },
            causation_id=100,
            idempotency_key="factory-qa-100",
            skip_rbac=True,
            skip_intercept=True,
        )
        outcomes = tick_waits(tmp_path, store)
        assert len(outcomes) == 1
        assert outcomes[0]["status"] == "ok"
        assert outcomes[0]["wait_status"] == "fulfilled"

        polled = store.poll("okf/handoff", since_id=0)["events"]
        resumes = [
            e
            for e in polled
            if isinstance(e.get("payload"), dict) and "resume" in e["payload"]
        ]
        assert len(resumes) == 1
        res = resumes[0]
        # Budget continuity: causation_id must be chain root (100)
        assert res["causation_id"] == 100
        assert res["idempotency_key"] == resume_idempotency_key("w_chain", outcomes[0]["fulfilled_by"])
        assert res["payload"]["resume"]["chain_key"] == "100"
        assert res["payload"]["resume"]["status"] == "ok"

        # Second tick must not double-wake
        outcomes2 = tick_waits(tmp_path, store)
        assert outcomes2 == []
        resumes2 = [
            e
            for e in store.poll("okf/handoff", since_id=0)["events"]
            if isinstance(e.get("payload"), dict) and "resume" in e["payload"]
        ]
        assert len(resumes2) == 1

        # Delivered intake (queue)
        q = tmp_path / ".agentbus" / "ingress" / "hermes_wake_queue.jsonl"
        lines = [json.loads(x) for x in q.read_text().splitlines() if x.strip()]
        resume_lines = [x for x in lines if (x.get("raw") or {}).get("source") == "resume" or x.get("from") == "agentbus"]
        assert len(resume_lines) >= 1
    finally:
        store.close()

    # Resume turn counts toward same chain (max 3: suspend=1, resume=1, one more, then trip)
    # Manually process resume envelope if in queue with event_id of resume
    store = EventStore(tmp_path)
    try:
        resume_ev = [
            e
            for e in store.poll("okf/handoff", since_id=0)["events"]
            if isinstance(e.get("payload"), dict) and "resume" in e["payload"]
        ][0]
        _enqueue(
            tmp_path,
            resume_ev["event_id"],
            to="hermes",
            frm="agentbus",
            summary=resume_ev["payload"]["summary"],
            causation_id=resume_ev["causation_id"],
        )
        # Patch queue record to include resume payload
        qpath = tmp_path / ".agentbus" / "ingress" / "hermes_wake_queue.jsonl"
        with qpath.open("a", encoding="utf-8") as fh:
            fh.write(
                json.dumps(
                    {
                        "event_id": resume_ev["event_id"],
                        "from": "agentbus",
                        "to": "hermes",
                        "summary": resume_ev["payload"]["summary"],
                        "topic": "okf/handoff",
                        "causation_id": 100,
                        "raw": {
                            "event_id": resume_ev["event_id"],
                            "causation_id": 100,
                            "payload": resume_ev["payload"],
                            "source": "resume",
                        },
                    }
                )
                + "\n"
            )
        cfg = load_runner_config(cfg_path)
        r2 = run_once(tmp_path, cfg)
        processed = [x for x in r2 if x.get("status") == "processed" and x.get("event_id") == resume_ev["event_id"]]
        assert processed
        budget = ChainBudget(runner_state_path(tmp_path, "test-runner-1"), 3)
        # chain_key for causation 100
        assert budget.remaining("100") == 1  # 3 - suspend - resume
    finally:
        store.close()


def test_timeout_dead_letter_and_late_fulfill_no_double_wake(tmp_path: Path):
    waits = WaitStore(
        tmp_path,
        now_fn=lambda: datetime(2026, 7, 17, 12, 0, 0, tzinfo=timezone.utc),
    )
    past = datetime(2026, 7, 17, 7, 0, 0, tzinfo=timezone.utc)
    wait = waits.create(
        runner_id="r1",
        producer_id="hermes",
        chain_key="10",
        origin_event_id=10,
        predicate=WaitPredicate(from_any=["factory"], causation_id=10),
        timeout_hours=1,
        wait_id="w_to",
        now=past,
    )
    # timeout_at = past + 1h = 08:00; now=12:00 → expired
    assert waits.is_expired(
        wait, now=datetime(2026, 7, 17, 12, 0, 0, tzinfo=timezone.utc)
    )

    store = EventStore(tmp_path)
    try:
        outcomes = tick_waits(
            tmp_path,
            store,
            waits=waits,
            now=datetime(2026, 7, 17, 12, 0, 0, tzinfo=timezone.utc),
        )
        assert len(outcomes) == 1
        assert outcomes[0]["status"] == "timeout"
        reloaded = waits.load("w_to")
        assert reloaded is not None
        assert reloaded.status == "timeout"
        assert reloaded.is_terminal

        dl = store.poll(DEAD_LETTER_TOPIC, since_id=0)["events"]
        assert len(dl) == 1
        assert dl[0]["payload"]["reason"] == "WAIT_TIMEOUT"

        resumes = [
            e
            for e in store.poll("okf/handoff", since_id=0)["events"]
            if isinstance(e.get("payload"), dict) and "resume" in e["payload"]
        ]
        assert len(resumes) == 1
        assert resumes[0]["payload"]["resume"]["status"] == "timeout"
        assert resumes[0]["causation_id"] == 10

        # Late fulfill after timeout — no second live resume
        store.publish(
            topic="okf/handoff",
            producer_id="factory",
            schema_version="1.0",
            payload={
                "from": "factory",
                "to": "hermes",
                "summary": "QA_VERDICT: GREEN late",
            },
            causation_id=10,
            skip_rbac=True,
            skip_intercept=True,
        )
        late = tick_waits(
            tmp_path,
            store,
            waits=waits,
            now=datetime(2026, 7, 17, 13, 0, 0, tzinfo=timezone.utc),
        )
        assert late == []
        resumes2 = [
            e
            for e in store.poll("okf/handoff", since_id=0)["events"]
            if isinstance(e.get("payload"), dict) and "resume" in e["payload"]
        ]
        assert len(resumes2) == 1
    finally:
        store.close()


def test_lost_wakeup_fulfill_before_tick(tmp_path: Path):
    """Event lands before wait is registered; later tick still resolves."""
    store = EventStore(tmp_path)
    try:
        ev, _ = store.publish(
            topic="okf/handoff",
            producer_id="factory",
            schema_version="1.0",
            payload={
                "from": "factory",
                "to": "hermes",
                "summary": "QA_VERDICT: GREEN early",
            },
            causation_id=77,
            skip_rbac=True,
            skip_intercept=True,
        )
        # Wait registered AFTER fulfillment event exists
        waits = WaitStore(tmp_path)
        waits.create(
            runner_id="r1",
            producer_id="hermes",
            chain_key="77",
            origin_event_id=77,
            predicate=WaitPredicate(
                from_any=["factory"],
                causation_id=77,
                summary_contains="QA_VERDICT",
            ),
            wait_id="w_lost",
            timeout_hours=4,
        )
        # Cursor starts at 0; scan includes the early event
        outcomes = tick_waits(tmp_path, store, waits=waits)
        assert len(outcomes) == 1
        assert outcomes[0]["status"] == "ok"
        assert outcomes[0]["fulfilled_by"] == ev.event_id
    finally:
        store.close()


def test_duplicate_resume_idempotent(tmp_path: Path):
    waits = WaitStore(tmp_path)
    wait = waits.create(
        runner_id="r1",
        producer_id="hermes",
        chain_key="5",
        origin_event_id=5,
        predicate=WaitPredicate(from_any=["factory"], causation_id=5),
        wait_id="w_dup",
    )
    store = EventStore(tmp_path)
    try:
        o1 = fulfill_wait(
            tmp_path,
            store,
            waits,
            wait,
            fulfilled_by=99,
            status="ok",
            reason="first",
        )
        assert o1 is not None
        assert o1["duplicate"] is False
        # Second call on already-terminal wait
        reloaded = waits.load("w_dup")
        assert reloaded is not None
        o2 = fulfill_wait(
            tmp_path,
            store,
            waits,
            reloaded,
            fulfilled_by=99,
            status="ok",
            reason="second",
        )
        assert o2 is None
        resumes = [
            e
            for e in store.poll("okf/handoff", since_id=0)["events"]
            if isinstance(e.get("payload"), dict) and "resume" in e["payload"]
        ]
        assert len(resumes) == 1
    finally:
        store.close()


def test_corrupt_wait_file_skipped(tmp_path: Path):
    waits = WaitStore(tmp_path)
    waits.ensure_dir()
    bad = waits.waits_dir / "w_bad.json"
    bad.write_text("{not json", encoding="utf-8")
    waits.create(
        runner_id="r1",
        producer_id="hermes",
        chain_key="1",
        origin_event_id=1,
        predicate=WaitPredicate(from_any=["factory"], causation_id=1),
        wait_id="w_good",
    )
    listed = waits.list_waits(status="pending")
    assert [w.wait_id for w in listed] == ["w_good"]
