"""OS-specific SQLite PRAGMA configuration."""

from __future__ import annotations

import threading
import time
from unittest import mock

from agentbus.store import EventStore


def test_windows_pragma_memory_and_busy_timeout(tmp_path):
    with mock.patch("os.name", "nt"):
        store = EventStore(tmp_path)
        try:
            mode = store._conn.execute("PRAGMA journal_mode").fetchone()[0]
            assert mode.upper() == "MEMORY"
            timeout = store._conn.execute("PRAGMA busy_timeout").fetchone()[0]
            assert int(timeout) == 10000
        finally:
            store.close()


def test_posix_pragma_wal(tmp_path):
    with mock.patch("os.name", "posix"):
        store = EventStore(tmp_path)
        try:
            mode = store._conn.execute("PRAGMA journal_mode").fetchone()[0]
            assert mode.upper() == "WAL"
            timeout = store._conn.execute("PRAGMA busy_timeout").fetchone()[0]
            assert int(timeout) == 5000
        finally:
            store.close()


def test_windows_busy_timeout_allows_concurrent_writers(tmp_path):
    """Two connections should not raise unexpected lock errors under busy_timeout."""
    with mock.patch("os.name", "nt"):
        a = EventStore(tmp_path)
        b = EventStore(tmp_path)
        errors: list[BaseException] = []

        def writer(store: EventStore, n: int) -> None:
            try:
                for i in range(20):
                    store.publish(
                        topic="okf/handoff",
                        producer_id=f"p{n}",
                        schema_version="1.0",
                        payload={
                            "from": f"p{n}",
                            "to": "swarm",
                            "summary": f"w{n}-{i}",
                        },
                    )
            except BaseException as exc:  # noqa: BLE001 — collect for assertion
                errors.append(exc)

        try:
            t1 = threading.Thread(target=writer, args=(a, 1))
            t2 = threading.Thread(target=writer, args=(b, 2))
            t1.start()
            t2.start()
            t1.join(timeout=30)
            t2.join(timeout=30)
            assert not t1.is_alive() and not t2.is_alive()
            assert not errors, f"unexpected errors: {errors}"
            # Both stores share the same db file
            st = a.status()
            assert st["event_count"] >= 20
        finally:
            a.close()
            b.close()
