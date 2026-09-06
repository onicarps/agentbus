"""Reliability regression tests from the 2026-09-06 swarm review consolidation.

Covers: A2 (concurrent idempotent publish must be a benign duplicate, never a
runner crash), A3 (invalid publish input yields clean CLI errors, never
tracebacks), A5 (trace trees are lossless and cycle-safe), A9 (doctor surfaces
the AgentID cutover checklist), and the Local-offset timezone label format.
"""

from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone

import pytest
from click.testing import CliRunner

from agentbus.cli import main as cli_main
from agentbus.devex import format_timezone_label
from agentbus.doctor import check_identity
from agentbus.store import EventStore
from agentbus.tracing import build_trace_tree, format_trace_tree_plain


@pytest.fixture
def store(tmp_path):
    s = EventStore(tmp_path)
    yield s
    s.close()


def _publish(store: EventStore, key: str):
    return store.publish(
        topic="okf/handoff",
        producer_id="codex",
        schema_version="1.0",
        payload={"from": "codex", "to": "agy", "summary": f"race {key}"},
        idempotency_key=key,
        skip_rbac=True,
    )


# ---------------------------------------------------------------------------
# A2: concurrent idempotent publish — benign duplicate, never IntegrityError
# ---------------------------------------------------------------------------


def test_concurrent_idempotent_publish_returns_benign_duplicate(tmp_path):
    """Two stores racing the same idempotency key: one insert, one duplicate.

    Regression: the loser of the race used to escape _insert_commit with an
    unhandled sqlite3.IntegrityError that crashed runner daemons
    (hermes-runner.stderr.log, 2026-09-06).
    """
    stores = [EventStore(tmp_path) for _ in range(2)]
    barrier = threading.Barrier(2)
    results: dict[int, tuple] = {}

    def worker(idx: int) -> None:
        barrier.wait()
        results[idx] = _publish(stores[idx], "race-key-1")

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)

    assert len(results) == 2, "a racing publish crashed or hung"
    fresh = [r for r in results.values() if not r[1]]
    dups = [r for r in results.values() if r[1]]
    assert len(fresh) == 1, f"expected exactly one fresh insert, got {len(fresh)}"
    assert len(dups) == 1, f"expected exactly one benign duplicate, got {len(dups)}"
    assert fresh[0][0].event_id == dups[0][0].event_id
    store_probe = EventStore(tmp_path)
    try:
        rows = store_probe.poll("okf/handoff", since_id=0)["events"]
        assert len(rows) == 1, "duplicate race must not create two rows"
    finally:
        store_probe.close()
        for s in stores:
            s.close()


def test_sequential_idempotent_publish_still_deduplicates(store):
    first = _publish(store, "seq-key")
    second = _publish(store, "seq-key")
    assert second[1] is True
    assert second[0].event_id == first[0].event_id


# ---------------------------------------------------------------------------
# A3: invalid publish input — clean CLI errors, never tracebacks
# ---------------------------------------------------------------------------


@pytest.fixture
def cli_env(tmp_path, monkeypatch):
    monkeypatch.setenv("AGENTBUS_WORKSPACE", str(tmp_path))
    return CliRunner()


@pytest.mark.parametrize(
    "args",
    [
        ["--topic", "okf/handoff", "--payload", "{bad json", "--producer-id", "codex"],
        ["--topic", "okf/handoff", "--payload", '["not","an","object"]', "--producer-id", "codex"],
        ["--topic", "okf/handoff", "--payload", '{"x":1}', "--producer-id", "codex"],
        ["--topic", "okf/handoff", "--payload", '{"a":1}', "--action", "{bad", "--producer-id", "codex"],
    ],
)
def test_invalid_publish_emits_clean_error(cli_env, args):
    result = cli_env.invoke(cli_main, ["publish", *args])
    assert result.exit_code != 0
    assert "Traceback" not in result.output
    assert result.output.startswith("Error:")


# ---------------------------------------------------------------------------
# A5: trace trees are lossless and cycle-safe
# ---------------------------------------------------------------------------


def _ev(eid: int, span: str, parent: str | None = None) -> dict:
    return {
        "event_id": eid,
        "span_id": span,
        "parent_span_id": parent,
        "producer_id": "x",
        "payload": {"summary": f"e{eid}"},
    }


def _all_ids(roots: list[dict]) -> set[int]:
    ids: set[int] = set()

    def walk(node: dict) -> None:
        ids.add(node["event_id"])
        for child in node.get("children", []):
            walk(child)

    for r in roots:
        walk(r)
    return ids


def test_trace_duplicate_span_ids_preserve_both_events():
    roots = build_trace_tree([_ev(1, "s1"), _ev(2, "s1")])
    assert _all_ids(roots) == {1, 2}


def test_trace_parent_cycle_demotes_to_roots():
    roots = build_trace_tree([_ev(1, "s1", "s2"), _ev(2, "s2", "s1")])
    assert _all_ids(roots) == {1, 2}


def test_trace_self_loop_renders_as_root():
    roots = build_trace_tree([_ev(1, "s1", "s1")])
    assert _all_ids(roots) == {1}


def test_trace_spanless_event_renders_as_root():
    roots = build_trace_tree([{"event_id": 1, "payload": {"summary": "no span"}}])
    assert _all_ids(roots) == {1}


def test_trace_duplicate_event_ids_are_deduplicated():
    roots = build_trace_tree([_ev(1, "s1"), _ev(1, "s1")])
    assert _all_ids(roots) == {1}


def test_trace_merged_branches_render_each_event_once():
    events = [_ev(1, "s1"), _ev(2, "s2", "s1"), _ev(3, "s3", "s1"), _ev(4, "s4", "s2")]
    rendered = format_trace_tree_plain("t", build_trace_tree(events))
    for eid in ("e1", "e2", "e3", "e4"):
        assert rendered.count(eid) == 1, f"{eid} rendered {(rendered.count(eid))} times"


# ---------------------------------------------------------------------------
# A9: doctor surfaces the AgentID staged-cutover checklist when unconfigured
# ---------------------------------------------------------------------------


def test_doctor_identity_unconfigured_includes_cutover_steps(tmp_path):
    check = check_identity(tmp_path)
    assert check.status == "WARN"
    assert check.details is not None
    steps = check.details.get("strict_cutover_steps")
    assert isinstance(steps, list) and steps
    assert any("identity root" in step for step in steps)
    assert any("identity mode" in step for step in steps)


# ---------------------------------------------------------------------------
# A10: Local offset labels omit the redundant 'UTC' prefix
# ---------------------------------------------------------------------------


def test_local_fixed_offset_label_has_no_utc_prefix():
    label = format_timezone_label(timezone(timedelta(hours=8)))
    assert label == "+08:00"
    label_neg = format_timezone_label(timezone(timedelta(hours=-5)))
    assert label_neg == "-05:00"


def test_local_string_label_format():
    label = format_timezone_label("local")
    assert label.startswith("Local (")
    assert "UTC" not in label
    datetime.now().astimezone()  # sanity: local tz resolvable on host
