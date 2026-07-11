# AgentBus Jupyter Integration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers-extended-cc:subagent-driven-development (recommended) or superpowers-extended-cc:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create a native, non-blocking AgentBus integration for Jupyter Notebooks so Data Scientists can orchestrate swarms without the polling loop freezing their execution cells.

**Architecture:** Standard Python AgentBus relies on synchronous `time.sleep()` polling, which blocks the Python GIL and freezes Jupyter. Since Jupyter inherently runs an `asyncio` event loop, this module will implement an `AsyncAgentBus` client that hooks directly into Jupyter's loop via `asyncio.create_task()`. It will also expose an IPython magic command (`%agentbus`) for seamless cell-level orchestration.

**Tech Stack:** Python 3.10+, `asyncio`, `IPython.core.magic`, `pytest-asyncio`.

---

### Task 0: Project Setup and Async Client Skeleton

**Goal:** Initialize the `agentbus.jupyter` submodule and set up the async testing harness.

**Files:**
- Create: `src/agentbus/jupyter/__init__.py`
- Create: `src/agentbus/jupyter/client.py`
- Create: `tests/jupyter/test_client.py`

**Acceptance Criteria:**
- [x] Module is importable.
- [x] Async test suite runs and passes.

**Verify:** `pytest tests/jupyter/test_client.py -v` → expected output: PASS

**Steps:**

- [ ] **Step 1: Create directories and __init__.py**

```bash
mkdir -p src/agentbus/jupyter tests/jupyter
touch src/agentbus/jupyter/__init__.py
```

- [ ] **Step 2: Write the failing async test**

Create `tests/jupyter/test_client.py`:
```python
import pytest
import asyncio
from agentbus.jupyter.client import AsyncAgentBus

@pytest.mark.asyncio
async def test_async_bus_initialization():
    bus = AsyncAgentBus()
    assert bus is not None
    assert hasattr(bus, "poll_async")
```

- [ ] **Step 3: Run test to verify it fails**

Run: `pytest tests/jupyter/test_client.py -v`
Expected: FAIL (ModuleNotFoundError or AttributeError)

- [ ] **Step 4: Write minimal implementation**

Create `src/agentbus/jupyter/client.py`:
```python
import asyncio

class AsyncAgentBus:
    def __init__(self):
        self._running = False
        self._task = None

    async def poll_async(self):
        pass
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pytest tests/jupyter/test_client.py -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/agentbus/jupyter tests/jupyter
git commit -m "feat(jupyter): initialize AsyncAgentBus skeleton"
```

---

### Task 1: Non-Blocking Async Polling Loop

**Goal:** Implement the background `asyncio` loop that fetches events via MCP without blocking the main thread.

**Files:**
- Modify: `src/agentbus/jupyter/client.py`
- Modify: `tests/jupyter/test_client.py`

**Acceptance Criteria:**
- [x] `start_background()` spawns an asyncio task.
- [x] Loop yields control back to Jupyter via `asyncio.sleep()`.

**Verify:** `pytest tests/jupyter/test_client.py -v` → expected output: PASS

**Steps:**

- [ ] **Step 1: Write the failing test**

Append to `tests/jupyter/test_client.py`:
```python
@pytest.mark.asyncio
async def test_background_polling_yields():
    bus = AsyncAgentBus()
    await bus.start_background(interval=0.1)
    
    # Prove the event loop is not blocked
    await asyncio.sleep(0.2)
    assert bus._running is True
    
    await bus.stop()
    assert bus._running is False
```

- [ ] **Step 2: Run test to verify it fails**
Expected: FAIL (AttributeError for `start_background`)

- [ ] **Step 3: Write minimal implementation**

Modify `src/agentbus/jupyter/client.py` to add start/stop logic:
```python
import asyncio
import logging

logger = logging.getLogger(__name__)

class AsyncAgentBus:
    def __init__(self):
        self._running = False
        self._task = None
        self.callbacks = []

    async def poll_async(self):
        # Pseudo implementation: MCP fetch logic here
        pass

    async def _loop(self, interval: float):
        while self._running:
            try:
                await self.poll_async()
            except Exception as e:
                logger.error(f"AgentBus poll error: {e}")
            await asyncio.sleep(interval) # Crucial: Yields to Jupyter

    async def start_background(self, interval: float = 1.0):
        if self._running:
            return
        self._running = True
        loop = asyncio.get_event_loop()
        self._task = loop.create_task(self._loop(interval))

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
```

- [ ] **Step 4: Run test to verify it passes**
Expected: PASS

- [ ] **Step 5: Commit**
```bash
git add src/agentbus/jupyter/client.py tests/jupyter/test_client.py
git commit -m "feat(jupyter): implement non-blocking asyncio polling loop"
```

---

### Task 2: IPython %agentbus Magic Command

**Goal:** Create a Jupyter magic extension so users can easily start/stop the bus via `%agentbus start`.

**Files:**
- Create: `src/agentbus/jupyter/magic.py`
- Create: `tests/jupyter/test_magic.py`

**Acceptance Criteria:**
- [x] Registers with IPython `load_ipython_extension`.
- [x] Handles `%agentbus start` and `%agentbus stop` commands.

**Verify:** `pytest tests/jupyter/test_magic.py -v` → expected output: PASS

**Steps:**

- [ ] **Step 1: Write the failing test**

Create `tests/jupyter/test_magic.py`:
```python
import pytest
from IPython.testing.globalipapp import get_ipython
from agentbus.jupyter.magic import load_ipython_extension

def test_magic_registration():
    ip = get_ipython()
    load_ipython_extension(ip)
    assert 'agentbus' in ip.magics_manager.magics['line']
```

- [ ] **Step 2: Run test to verify it fails**
Expected: FAIL

- [ ] **Step 3: Write minimal implementation**

Create `src/agentbus/jupyter/magic.py`:
```python
from IPython.core.magic import Magics, magics_class, line_magic
from .client import AsyncAgentBus
import asyncio

@magics_class
class AgentBusMagics(Magics):
    def __init__(self, shell):
        super().__init__(shell)
        self.bus = AsyncAgentBus()

    @line_magic
    def agentbus(self, line):
        args = line.strip().split()
        if not args:
            print("Usage: %agentbus [start|stop]")
            return

        cmd = args[0].lower()
        if cmd == "start":
            asyncio.create_task(self.bus.start_background())
            print("AgentBus background polling started.")
        elif cmd == "stop":
            asyncio.create_task(self.bus.stop())
            print("AgentBus stopped.")
        else:
            print(f"Unknown command: {cmd}")

def load_ipython_extension(ipython):
    ipython.register_magics(AgentBusMagics)
```

- [ ] **Step 4: Run test to verify it passes**
Expected: PASS

- [ ] **Step 5: Commit**
```bash
git add src/agentbus/jupyter/magic.py tests/jupyter/test_magic.py
git commit -m "feat(jupyter): add IPython magic commands for agentbus"
```
