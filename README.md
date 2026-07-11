# AgentBus

[![Test](https://github.com/onicarps/agentbus/actions/workflows/test.yml/badge.svg)](https://github.com/onicarps/agentbus/actions/workflows/test.yml)
[![Python](https://img.shields.io/pypi/pyversions/okf-agentbus.svg)](https://pypi.org/project/okf-agentbus/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**The SQLite-Backed MCP Event Bus & Control Surface for Heterogeneous Agent Swarms.**

When Cursor, Claude, Antigravity, and Terminal Agents (like Hermes) share a workspace, they usually coordinate through fragile append-only files (`log.md`). That works until you need SLA timeouts, Human-in-the-Loop (HITL) intercepts, strict schema validation, or cryptographic RBAC.

AgentBus replaces the "Game of Telephone" with a **localhost sidecar**: a Python MCP server backed by SQLite. No orchestrator runtime lock-in. No heavy cloud dashboard. Just a hyper-fast local pub/sub built for top-tier AI orchestration.

> **v0.11.0 (July 2026):** **Phase 1 DX** — Jupyter `AsyncAgentBus` + `%agentbus` magics; TypeScript client under `packages/js/agentbus-client`. Orchestration DX (`up/down/ps/logs`) + God View remain from v0.10/v0.9.
>
> **Note:** Install as **`okf-agentbus`** (CLI command remains `agentbus`). Extras: `[obs,devex,jupyter,sdk]`.

## ⚡ The "Ah-Ha" Moment: Zero-Restart Integration
Already running Aider, OpenHands, or custom agents in tmux panes? **Do not kill your sessions.**
AgentBus features a dual-interface architecture (MCP + CLI). You don't need to wire up JSON configs to test it today. Just prompt your running agent:
> *"Use your terminal to run `agentbus publish --topic okf/handoff --payload '{\"from\":\"grok\",\"to\":\"hermes\",\"summary\":\"Write tests\"}'`"*

The SQLite bus instantly captures it, without requiring the MCP server. Once you're convinced by the Mission Control TUI, you can wire up the strongly-typed MCP server for your next boot.

## Why AgentBus?

| Alternative | Limitation | AgentBus |
|-------------|------------|----------|
| `log.md` blackboard | No schema, race conditions | Typed topics, monotonic IDs, advisory locks |
| LangGraph / CrewAI | Same-runtime lock-in | Heterogeneous out-of-process clients (IDE + CLI) |
| LangSmith | Cloud-only, backward-looking | Local SQLite, forward-looking Execution TUI |
| Redis pub/sub | Extra daemon, complex setup | Zero-config SQLite, native stdio MCP |

## Feature Arsenal (v0.3 - v0.11)

*   **Jupyter async client (v0.11):** `from agentbus.jupyter import AsyncAgentBus` + `%agentbus start` — non-blocking polls that yield to the notebook event loop.
*   **TypeScript client (v0.11):** `packages/js/agentbus-client` — Node EventEmitter + MCP stdio spawn (`@okf/agentbus-client`, path install for now).
*   **God View Mesh (v0.9):** Passive OS + MCP observability so silent agents still leave bus footprints (`system/mcp`, `system/fs`, `system/shell`, `system/monologue`).
*   **Mission Control TUI (v0.8+):** A rich, keyboard-driven `Textual` dashboard (`agentbus monitor`). Trace waterfall, HITL, Wiretap pane, Dark Agent warnings.
*   **Pluggable Pydantic Schemas (v0.7):** Code-first `@bus.topic` decorators to enforce strict JSON schemas at the insertion layer.
*   **Distributed Context (v0.6):** Pass massive context (like git diffs) via `--attach`. Hard 1MB payload limits prevent context window explosion.
*   **Agentic Observability (v0.5):** Native OpenTelemetry-style `trace_id` and `parent_span_id` lineage.
*   **SLA Timeouts & Dead-Letter (v0.4):** Prevent phantom deadlocks. If an agent ghosts the swarm, SLA timers route the payload to `okf/dead-letter`.
*   **Swarm RBAC & Droid Proofs (v0.3):** Cryptographic JWT/UUID tokens ensure only authorized agents can publish to restricted topics.
*   **HITL Intercepts (v0.3):** Catch dangerous payloads (e.g., `DROP TABLE`) and place them in `PENDING_APPROVAL` for human review via the TUI.

## Install

**The fastest way (Installs & Auto-wires your IDEs in one step):**
```bash
curl -sSL https://raw.githubusercontent.com/onicarps/agentbus/main/install.sh | bash
```

**Or manually via pip:**
```bash
pip install -U "okf-agentbus[devex,sdk]"
agentbus init --apply --producer-id my-agent
```

**Jupyter notebooks:**

```bash
pip install -U "okf-agentbus[jupyter]"
```

```python
%load_ext agentbus.jupyter
%agentbus start
# or
from agentbus.jupyter import AsyncAgentBus
bus = AsyncAgentBus()  # AGENTBUS_WORKSPACE or cwd
bus.on_event(print)
await bus.start_background(interval=1.0)
```

**TypeScript (from monorepo checkout):**

```bash
cd packages/js/agentbus-client && npm install && npm test
# set AGENTBUS_WORKSPACE + agentbus on PATH, then use createStdioMcpClient / AgentBus
```

## Quickstart & Examples

The best way to understand AgentBus is to read our copy-pasteable examples. 

See the **[`examples/`](examples/)** directory for 7 flawless, isolated Python scripts covering every feature from basic Pub/Sub to Pydantic Schemas and SLA Timeouts.

```bash
# Terminal A — Launch the Mission Control TUI
agentbus monitor

# Terminal B — Publish a handoff
agentbus publish \
  --topic okf/handoff \
  --payload '{"from":"cursor","to":"hermes","summary":"Write tests"}'
```

**God View Observability (v0.9.0):**

Track silent agents by wiretapping their operations:

```bash
# Intercept MCP tool calls
mcp-serve --wiretap

# Watch file edits and command executions
agentbus watch

# Tail internal agent reasoning logs
agentbus tail
```
Events will stream into the TUI's Wiretap pane as `system/mcp`, `system/fs`, `system/shell`, and `system/monologue` topics.

## 📦 Workspace Isolation (The `.agentbus` directory)

AgentBus operates on the concept of **Workspace Isolation** (similar to how `git` uses `.git` or Docker uses `docker-compose.yml`). 

The bus and its events are physically scoped to the directory you run it in. This prevents your Swarm from tracking your entire operating system or cross-contaminating different projects.

When you navigate to a specific project folder and run `agentbus init` or `agentbus up`, it creates a localized `.agentbus/events.db` SQLite database specifically for that directory.

### 1-Click Swarm Orchestration (v0.10.0)
Instead of manually opening 5 tmux panes to start your agents and observability daemons, use the new orchestrator:

```bash
cd /path/to/my-project

# 1. Generate a boilerplate .agentbus/swarm.yaml
agentbus up --init

# 2. Boot the swarm (Watchers, Agents, and TUI) in one click
agentbus up

# 3. View running background agents
agentbus ps

# 4. Safely kill the entire swarm
agentbus down
```


## Documentation

For full architectural documentation, see the `docs/` directory.

## License

MIT — see [LICENSE](LICENSE).