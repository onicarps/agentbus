# AgentBus Phase C: MCPSafe Middleware

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers-extended-cc:subagent-driven-development (recommended) or superpowers-extended-cc:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the decoupled `mcpsafe` runtime security middleware into the Python `AgentBus` router.

**Architecture:**
- Create `src/agentbus/mcpsafe.py` which exposes a `PolicyEnforcer` class.
- The class takes a path to a `.mcpsafe.lock` file.
- It parses the JSON lockfile into a dictionary.
- The `AgentBus` `publish` and `poll` methods, if `--enable-mcpsafe` is active, will call `PolicyEnforcer.evaluate(payload)`.
- If blocked, `AgentBus` drops the message and returns an `AccessDenied` error event.

**Tech Stack:** Python 3.10+, `pytest`, `json`.

---

### Task 0: Implement PolicyEnforcer

**Goal:** Build the nanosecond hash map lookup logic.

**Files:**
- Create: `src/agentbus/mcpsafe.py`
- Create: `tests/test_mcpsafe.py`

**Acceptance Criteria:**
- [ ] `PolicyEnforcer` loads a `.mcpsafe.lock` JSON file.
- [ ] Returns `True` if tool is in `allowed_tools` list.
- [ ] Returns `False` if tool is in `blocked_tools` list or missing.

**Steps:**
- [ ] Step 1: Write `tests/test_mcpsafe.py` to verify lockfile parsing and evaluation.
- [ ] Step 2: Implement `src/agentbus/mcpsafe.py`.

### Task 1: Integrate into AgentBus

**Goal:** Wire the enforcer into the main bus router.

**Files:**
- Modify: `src/agentbus/core.py` (or `store.py`)
- Modify: `src/agentbus/cli.py`

**Acceptance Criteria:**
- [ ] Add `--enable-mcpsafe` flag to CLI.
- [ ] If enabled, bus uses `PolicyEnforcer`.
- [ ] Unsafe messages are dropped.

**Steps:**
- [ ] Step 1: Update CLI arguments.
- [ ] Step 2: Update router logic to check payloads.
- [ ] Step 3: Write integration test.
