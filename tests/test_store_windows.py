"""OS-specific SQLite PRAGMA configuration."""

from __future__ import annotations

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
            # Host is Linux; WAL should apply under posix branch.
            assert mode.upper() == "WAL"
            timeout = store._conn.execute("PRAGMA busy_timeout").fetchone()[0]
            assert int(timeout) == 5000
        finally:
            store.close()
