# AgentBus

[![Test](https://github.com/onicarps/agentbus/actions/workflows/test.yml/badge.svg)](https://github.com/onicarps/agentbus/actions/workflows/test.yml)
[![PyPI](https://img.shields.io/pypi/v/okf-agentbus.svg)](https://pypi.org/project/okf-agentbus/)
[![Python](https://img.shields.io/pypi/pyversions/okf-agentbus.svg)](https://pypi.org/project/okf-agentbus/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**The Zero-Daemon, SQLite-Backed Event Bus & Visual Cockpit for Multi-Agent Coding.**

When Cursor, Claude Desktop, Antigravity, terminal bots (Hermes, Aider), and custom Python scripts work together in the same workspace, coordinating them usually devolves into messy, append-only `log.md` files or heavy cloud dashboards.

AgentBus gives your AI agents a fast, typed, local pub/sub highway backed by a single SQLite file (`.agentbus/events.db`) and gives *you* an interactive **God View Mission Control TUI** to watch every handoff, trace execution lineage, and approve dangerous actions in real time.

---

## ⚡ 30-Second Quickstart (The "Ah-Ha" Moment)

You don't need complex configurations or Docker daemons to see AgentBus in action.

### 1. Install

```bash
# Fastest: Installs AgentBus and configures your path automatically
curl -sSL https://raw.githubusercontent.com/onicarps/agentbus/main/install.sh | bash
```

*(Or via package manager: `pip install okf-agentbus` or `pipx install okf-agentbus` — the interactive TUI monitor is bundled out-of-the-box!)*

### 2. Launch the Visual Monitor (Terminal A)

```bash
agentbus monitor
```
*(Or use the standalone command: `agentbus-monitor`)*

You are now in the **God View TUI** dashboard:
* **Live Event Stream**: Watch real-time multi-agent activity as it occurs.
* **Trace Waterfall**: Inspect `trace_id` lineage and parent-span relationships.
* **HITL Approvals**: Review pending approvals with one-keystroke hotkeys (`[a]` approve / `[r]` reject).

### 3. Send Your First Message (Terminal B)

Open another terminal in the same folder and publish a handoff event:

```bash
agentbus publish \
  --topic okf/handoff \
  --payload '{"from":"cursor","to":"claude","summary":"Added auth endpoints; please review and write tests."}'
```

👉 **Look back at Terminal A:** the event immediately pops up on your monitor!

---

## 🛠️ Why AgentBus?

| Approach | Limitations | How AgentBus Solves It |
|---|---|---|
| **Shared `log.md` file** | Race conditions, zero schemas, no delivery guarantees, unsearchable history | Typed topics, monotonic event IDs, SQLite transactional integrity, advisory file locks |
| **Cloud SaaS (e.g. LangSmith)** | Latency, cloud data egress, vendor lock-in, heavy setup for local development | 100% local SQLite storage, zero cloud egress by default, instant startup |
| **Heavy Brokers (Redis, Kafka)** | Background daemon requirements, port collisions, Docker overhead | Zero daemons: embedded SQLite sidecar per project directory (`.agentbus/events.db`) |
| **Framework Runtimes (LangGraph, CrewAI)** | Locked into a single Python runtime or process | Connects heterogeneous agents across IDEs (Cursor, Claude Desktop), CLI tools, and scripts via standard MCP and CLI |

---

## 📦 Fresh Boot & Installation Guide

AgentBus requires **Python 3.11+**. The God View interactive monitor (`rich` + `textual`) is **installed automatically** with the base package.

### Option A: 1-Line Universal Installer (Recommended for fresh systems)

Handles fresh boots, non-root environments, and Linux PEP 668 (`externally-managed-environment`) automatically:

```bash
curl -sSL https://raw.githubusercontent.com/onicarps/agentbus/main/install.sh | bash
```

### Option B: Standalone CLI Tool (pipx or uv)

If you prefer isolated CLI tools without creating project virtual environments:

```bash
# Using uv (fastest)
uv tool install okf-agentbus

# Using pipx
pipx install okf-agentbus
```

### Option C: In an existing virtual environment

If you have an active project virtual environment (`.venv`):

```bash
pip install -U okf-agentbus
```

> **💡 Troubleshooting "Python / Virtual Environment" Issues:**
> On modern Linux (Ubuntu 24.04+, Debian 12+) and macOS Homebrew, system Python restricts bare `pip install` to protect OS packages (PEP 668). If you see `error: externally-managed-environment`, either run the 1-line `curl` installer above (which creates an isolated tool runtime in `~/.local/share/agentbus/venv` and symlinks to `~/.local/bin/agentbus`) or use `pipx install okf-agentbus`.

---

## 🔌 Connecting Your AI Coding Agents

AgentBus speaks standard **Model Context Protocol (MCP)** over stdio as well as standard CLI commands.

### 1-Click Auto-Configuration

Navigate to your project workspace and run:

```bash
agentbus init --apply --producer-id my-agent
```

AgentBus scans for installed tools (Cursor, Claude Desktop, Antigravity) and automatically generates the necessary MCP configuration!

### Manual IDE Configurations

#### For Cursor (`~/.cursor/mcp.json` or `.cursor/mcp.json`)

```json
{
  "mcpServers": {
    "agentbus": {
      "command": "agentbus",
      "args": ["mcp-serve"],
      "env": {
        "AGENTBUS_WORKSPACE": "/path/to/your/project",
        "AGENTBUS_PRODUCER_ID": "cursor"
      }
    }
  }
}
```

#### For Claude Desktop (`claude_desktop_config.json`)

```json
{
  "mcpServers": {
    "agentbus": {
      "command": "agentbus",
      "args": ["mcp-serve"],
      "env": {
        "AGENTBUS_WORKSPACE": "/path/to/your/project",
        "AGENTBUS_PRODUCER_ID": "claude"
      }
    }
  }
}
```

Once connected, your AI agents will automatically see tools like:
* `agentbus_publish` — publish task updates, handoffs, and questions.
* `agentbus_poll` — check for incoming assignments.
* `agentbus_lock_acquire` / `agentbus_lock_release` — prevent file edit collisions.

---

## 💡 Practical Examples for Beginners & Intermediates

### Example 1: CLI Coordination (Zero Code)

Simulate two agents working together directly from the shell:

```bash
# Agent A publishes a completed task:
agentbus publish \
  --topic okf/handoff \
  --payload '{"from":"alice","to":"bob","summary":"Refactored database models"}'

# Agent B polls for new tasks:
agentbus poll --topic okf/handoff --since-id 0
```

### Example 2: Python Script (Publishing & Polling)

Integrate any custom Python agent or bot in 5 lines:

```python
from pathlib import Path
from agentbus.rbac import ensure_default_roles
from agentbus.store import EventStore

# Connect to current project workspace
workspace = Path.cwd()
ensure_default_roles(workspace)  # install the local role policy first
store = EventStore(workspace)

# Publish an event
event, _ = store.publish(
    topic="okf/handoff",
    producer_id="codex",
    schema_version="1.0",
    payload={"from": "codex", "to": "all", "summary": "Job completed successfully"},
)
print(f"Published event #{event.event_id}")

# Poll new events
messages = store.poll("okf/handoff", since_id=0)
for ev in messages["events"]:
    print(f"[{ev['event_id']}] {ev['payload']['summary']}")

store.close()
```

### Example 3: Human-in-the-Loop (HITL) Safety Intercepts

Catch dangerous AI operations (e.g. database migrations, shell scripts, deletions) before they execute:

```bash
# 1. Register an intercept rule: any handoff containing 'DROP' requires human approval
agentbus config set-intercept --topic okf/handoff --contains "DROP" --ttl-minutes 30

# 2. When an agent tries to publish:
agentbus publish \
  --topic okf/handoff \
  --payload '{"from":"agent","to":"db","summary":"DROP TABLE users"}'
# => Status becomes: PENDING_APPROVAL

# 3. View and approve/reject it in the TUI:
agentbus monitor
# Press [A] to approve or [R] to reject!
```

### Example 4: Preventing File Collisions with Advisory Locks

When multiple agents write code at the same time, they can accidentally overwrite each other. Use advisory leases:

```bash
# Agent 1 locks a file before editing:
agentbus lock acquire --resource src/models.py --owner-id agent-1 --ttl 300

# Agent 2 checks lock status:
agentbus lock status --resource src/models.py

# Agent 1 releases the lock when finished:
agentbus lock release --resource src/models.py --owner-id agent-1 --lease-id <LEASE_ID>
```

### Example 5: 1-Click Multi-Agent Swarm (`agentbus up`)

Instead of opening 5 terminal windows, orchestrate your agents, file watchers, and monitor together:

```bash
# 1. Generate default .agentbus/swarm.yaml
agentbus up --init

# 2. Start the swarm (daemons + God View monitor)
agentbus up

# 3. Check running background workers
agentbus ps

# 4. Tear down the swarm cleanly
agentbus down
```

---

## 📚 Complete Examples Catalog

Check out the **[`examples/`](examples/)** folder for fully isolated, executable Python scripts:

* **[`01_core_pub_sub.py`](examples/01_core_pub_sub.py)** — Fundamental publish and poll flow.
* **[`02_hitl_intercepts.py`](examples/02_hitl_intercepts.py)** — Catching sensitive payloads for human sign-off.
* **[`03_swarm_rbac.py`](examples/03_swarm_rbac.py)** — Swarm role-based topic permissions and identity tokens.
* **[`04_sla_timeouts.py`](examples/04_sla_timeouts.py)** — Automated dead-letter routing when an agent ghosts.
* **[`05_observability.py`](examples/05_observability.py)** — Distributed trace waterfall lineage (`trace_id`, `parent_span_id`).
* **[`06_distributed_context.py`](examples/06_distributed_context.py)** — Sharing large artifacts/diffs via content hashing.
* **[`07_pydantic_schemas.py`](examples/07_pydantic_schemas.py)** — Validating topic payloads with Pydantic models.
* **[`08_god_view.py`](examples/08_god_view.py)** — Passive OS filesystem, shell, and MCP call observation.
* **[`09_langgraph_bridge.py`](examples/09_langgraph_bridge.py)** — A LangGraph-node handoff skeleton.
* **[`10_crewai_bridge.py`](examples/10_crewai_bridge.py)** — A CrewAI-task handoff skeleton.

See the [Examples Guide](examples/README.md) for full documentation on each recipe.

---

## ⌨️ CLI Cheat Sheet

| Command | What it does |
|---|---|
| `agentbus monitor` | Launch the interactive God View TUI dashboard (`agentbus-monitor` also works) |
| `agentbus publish --topic <t> --payload '<json>'` | Publish an event to the local SQLite bus |
| `agentbus poll --topic <t> [--since-id <id>]` | Read published events from the bus |
| `agentbus init --apply --producer-id <id>` | Auto-discover MCP configs for Cursor/Claude/Antigravity |
| `agentbus doctor` | Run non-destructive diagnostic checks on SQLite, schema, and environment |
| `agentbus lock acquire --resource <path>` | Acquire an exclusive lease/lock on a file or resource |
| `agentbus lock release --resource <path>` | Release a previously held lease |
| `agentbus config set-intercept --topic <t>` | Create Human-in-the-Loop approval rules |
| `agentbus up` | Boot multi-agent swarm services declared in `swarm.yaml` |
| `agentbus ps` | View active swarm background services |
| `agentbus down` | Terminate all active swarm processes |

---

## 📖 Deep-Dive Documentation

* [End-to-End Walkthrough (Alice, Bob, & Charlie)](docs/WALKTHROUGH.md)
* [Model Context Protocol (MCP) Tool Schema](docs/MCP_SCHEMA.md)
* [AgentID Security Model & Identity System](docs/AGENTID.md)
* [Client Compatibility Matrix](docs/CLIENT_MATRIX.md)
* [PostHog Outbound Telemetry](docs/POSTHOG.md)
* [Release Runbook & Process](docs/RELEASE.md)
* [Roadmap](ROADMAP.md)

---

## 📄 License

MIT — see [LICENSE](LICENSE).
