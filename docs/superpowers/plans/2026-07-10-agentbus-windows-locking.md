# AgentBus Windows SQLite Locking Fix Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers-extended-cc:subagent-driven-development (recommended) or superpowers-extended-cc:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prevent "database is locked" crashes on Windows environments caused by missing POSIX locking mechanisms under high concurrent load.

**Architecture:** We will modify `src/agentbus/store.py`. During SQLite initialization, the code will check `os.name == 'nt'`. If on Windows, we gracefully degrade the connection from `WAL` mode to `MEMORY` (storing the rollback journal in RAM to bypass corporate Antivirus file locks) and set an aggressive `busy_timeout`. For Mac/Linux, we retain the high-performance `WAL` mode.

**Tech Stack:** Python 3.10+, `sqlite3`, `os`, `pytest`.

---

### Task 0: PRAGMA OS Detection Branching

**Goal:** Dynamically configure SQLite PRAGMAs based on the underlying operating system.

**Files:**
- Modify: `src/agentbus/store.py`
- Create: `tests/test_store_windows.py`

**Acceptance Criteria:**
- [x] Uses `WAL` mode on POSIX systems.
- [x] Uses `MEMORY` journal mode and `busy_timeout=10000` on Windows (`nt`).

**Verify:** `pytest tests/test_store_windows.py -v` → expected output: PASS

**Steps:**

- [ ] **Step 1: Write the failing test**

Create `tests/test_store_windows.py`:
```python
import pytest
import sqlite3
import os
from unittest import mock
from agentbus.store import EventStore

def test_windows_pragma_degradation(tmp_path):
    with mock.patch('os.name', 'nt'):
        store = EventStore(str(tmp_path / "win.db"))
        cursor = store.conn.execute("PRAGMA journal_mode;")
        mode = cursor.fetchone()[0].upper()
        assert mode == "MEMORY"

def test_posix_pragma_wal(tmp_path):
    with mock.patch('os.name', 'posix'):
        store = EventStore(str(tmp_path / "posix.db"))
        cursor = store.conn.execute("PRAGMA journal_mode;")
        mode = cursor.fetchone()[0].upper()
        assert mode == "WAL"
```

- [ ] **Step 2: Run test to verify it fails**
Expected: FAIL (Windows test will likely return WAL)

- [ ] **Step 3: Write minimal implementation**

Modify `src/agentbus/store.py` SQLite initialization:
```python
import sqlite3
import os

class EventStore:
    def __init__(self, db_path: str):
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self._configure_pragmas()
        
    def _configure_pragmas(self):
        self.conn.execute("PRAGMA synchronous = NORMAL;")
        if os.name == 'nt':
            # Windows lacks POSIX locks; WAL is highly unstable under concurrency.
            # Using DELETE triggers corporate AV file-scanning locks, so use MEMORY instead.
            self.conn.execute("PRAGMA journal_mode = MEMORY;")
            self.conn.execute("PRAGMA busy_timeout = 10000;")
        else:
            # Mac/Linux
            self.conn.execute("PRAGMA journal_mode = WAL;")
            self.conn.execute("PRAGMA busy_timeout = 5000;")
        self.conn.commit()
```

- [ ] **Step 4: Run test to verify it passes**
Expected: PASS

- [ ] **Step 5: Commit**
```bash
git add src/agentbus/store.py tests/test_store_windows.py
git commit -m "fix(core): use journal_mode=MEMORY on Windows to reduce lock storms"
```
