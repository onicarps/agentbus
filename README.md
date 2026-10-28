# AgentBus 🚌

[![Test](https://github.com/onicarps/agentbus/actions/workflows/test.yml/badge.svg)](https://github.com/onicarps/agentbus/actions/workflows/test.yml)
[![PyPI](https://img.shields.io/pypi/v/okf-agentbus.svg)](https://pypi.org/project/okf-agentbus/)
[![Python](https://img.shields.io/pypi/pyversions/okf-agentbus.svg)](https://pypi.org/project/okf-agentbus/)
[![npm](https://img.shields.io/npm/v/@agentbus/agentbus-client.svg)](https://www.npmjs.com/package/@agentbus/agentbus-client)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**The friendly, zero-daemon event bus and visual cockpit for your AI coding agents.**

Have you ever tried getting **Cursor**, **Claude Desktop**, **Antigravity**, terminal coding bots, and custom Python scripts to collaborate on the same project?

Without a clean coordination channel, things get messy quickly:
* 💥 **File collisions:** Two agents edit the same file simultaneously and wipe out each other's work.
* 👻 **Lost context:** Crucial handoffs and design decisions vanish into transient chat windows or get lost in cluttered markdown files.
* ⚠️ **Unreviewed actions:** An agent prepares to execute a risky migration or destructive command before you've had a chance to verify it.
* 🏗️ **Heavy infrastructure fatigue:** Trying to solve this with Redis, Kafka, or cloud platforms brings Docker bloat, open network ports, and monthly SaaS bills for something that should just run locally.

**AgentBus solves this right inside your project folder.** It gives your AI agents a fast, typed, private pub/sub channel backed by a local SQLite file (`.agentbus/events.db`), and gives **you** an interactive terminal **Mission Control Cockpit** to watch every handoff, trace lineage, and approve sensitive actions in real time.

No Docker required. No cloud accounts. Zero background daemons needed for standard local pub/sub.

---

## ⚡ 30-Second Quickstart (The "Aha!" Moment)

You don't need complex configurations or deep systems knowledge to see AgentBus in action.

### 1. Install

Pick the method that best matches your setup:

#### Option A: Package Manager (Recommended)

```bash
# Using uv (fastest)
uv tool install okf-agentbus

# Using pipx
pipx install okf-agentbus
```

#### Option B: Standalone Bootstrap Script (install.sh)

```bash
# Bootstraps an isolated runtime even on machines without system Python
# 1. Download install.sh pinned to release commit 0f6d389
curl --proto '=https' --tlsv1.2 -LsSf https://raw.githubusercontent.com/onicarps/agentbus/0f6d389622a168b78eb3bdc3b02729c5bc4680aa/install.sh -o install.sh

# 2. Verify script SHA-256 and run with pinned release v0.27.0:
# On Linux:
echo "03e64e9bb7e252c072ba77e0cc62324f3e3d34a9c4aaeb4886a0e897c80528b2  install.sh" | sha256sum --check && AGENTBUS_PACKAGE_NAME="okf-agentbus==0.27.0" bash install.sh
# On macOS:
echo "03e64e9bb7e252c072ba77e0cc62324f3e3d34a9c4aaeb4886a0e897c80528b2  install.sh" | shasum -a 256 --check && AGENTBUS_PACKAGE_NAME="okf-agentbus==0.27.0" bash install.sh
```

> **Supply-Chain Verification Scope:** The SHA-256 checksum verifies the `install.sh` script itself. It does not verify downstream artifacts (such as the separately downloaded `uv` installer when `uv` is not already installed, or the fetched Python package wheels and their dependencies). For audited environments with strict verification policies, install pinned packages using your standard package manager, lockfiles, or internal artifact mirror.

> **Tip:** The installer places the binary in `${XDG_BIN_HOME:-$HOME/.local/bin}` and configures your shell configuration files for new terminals. For your current terminal session, ensure that directory is on your PATH (e.g. `export PATH="${XDG_BIN_HOME:-$HOME/.local/bin}:$PATH"`).

### 2. Initialize & Launch Mission Control (Terminal A)

Create or open any project directory:

```bash
mkdir -p ~/my-agent-workspace && cd ~/my-agent-workspace

# 1. Run the setup companion (in an interactive terminal, prompts to configure existing MCP client configs like Cursor and Claude Desktop with your consent)
agentbus

# 2. Launch your visual Mission Control cockpit
agentbus monitor
```

*(Note: In a non-interactive shell or CI pipeline, running bare `agentbus` on a fresh workspace initializes it non-interactively with automatic MCP discovery applied, or emits workspace status JSON if already initialized. **VS Code Side Effect:** Because non-interactive setup runs client discovery automatically, it will merge an incompatible `mcpServers` block into an existing `.vscode/mcp.json` file. In workspaces using VS Code, avoid bare non-interactive initialization; configure `.vscode/mcp.json` manually using the top-level `servers` format below or preview with `agentbus init --producer-id local-user` first.)*

You are now in the terminal TUI dashboard:
* **Live Event Stream:** Watch multi-agent activity as it happens.
* **Trace Waterfall:** Inspect parent/child task lineage (`trace_id`).
* **Human-in-the-Loop Hotkeys:** Review pending requests with one keystroke (`[a]` approve / `[r]` reject).

### 3. Send Your First Message (Terminal B)

Open another terminal in the same folder and publish a handoff:

```bash
agentbus publish \
  --topic okf/handoff \
  --producer-id local-user \
  --payload '{"from":"local-user","to":"reviewer","summary":"Built the auth endpoints! Ready for review."}'
```

👉 **Look back at Terminal A:** the event immediately pops up on your Mission Control screen!

---

## 🌟 Five Superpowers for Everyday Multi-Agent Workflows

### 1. 🕹️ God View Mission Control TUI

Instead of guessing what your agents are doing across disconnected windows, launch `agentbus monitor`. You get a high-density, real-time visual feed of all published events, execution traces, and active file leases.

### 2. 🔒 Advisory File Leases ("Don't Touch My Code!")

Prevent agents from accidentally clobbering each other's edits. Cooperating agents and tools can request and respect advisory leases on workspace files:

```bash
# Agent acquires an exclusive advisory lease on an absolute file path for 300 seconds:
agentbus lock acquire --resource "$PWD/src/models.py" --owner-id agent-1 --ttl-seconds 300

# Other agents (and your tooling) check if a file is currently leased:
agentbus lock status --resource "$PWD/src/models.py"

# When finished, the agent releases its lease:
agentbus lock release --resource "$PWD/src/models.py" --owner-id agent-1
```

> **Advisory Scope:** Leases are **advisory**. Cooperating tools and agents check status and honor active leases before writing. AgentBus coordinates permissions across agents but does not intercept low-level OS file system calls or automatically block external editors.

### 3. 🛑 Human-in-the-Loop (HITL) Event Intercepts

Catch sensitive actions (e.g. database migrations, deletions, or external deploys) before other agents act on them:

```bash
# 1. Register intercept rules: matching events published to the bus require human approval
agentbus config set-intercept --topic okf/handoff --contains "DROP" --ttl-minutes 30
agentbus config set-intercept --topic okf/handoff --contains "MIGRATE" --ttl-minutes 30

# 2. When an agent publishes a matching event, AgentBus places it in PENDING_APPROVAL status.
# 3. Downstream executors and subscriber agents do not process the event until you approve it.
# 4. In your terminal monitor, inspect the payload and press [a] to approve or [r] to reject!
# (From CLI, approve/reject using an authorized reviewer role, e.g. --reviewer-id agy):
# agentbus approve <event_id> --reviewer-id agy
# agentbus reject <event_id> --reviewer-id agy --reason "Needs schema migration plan"
```

> **How Intercepts Work:** Intercepts hold matching *events published to AgentBus*; they do not automatically intercept arbitrary shell commands, raw database operations, or out-of-band actions. Systems coordinate safety by publishing planned actions as bus events and awaiting sign-off before proceeding.
> **Approval Permissions (RBAC):** Approving or rejecting events requires a reviewer mapped to a role with `can_approve: true` in `.agentbus/roles.yaml` (such as the default `architect` role mapped to `agy`, or configuring a reviewer like `producers: { human: architect }`). The default setup bootstraps first-run users as an `engineer`, which cannot approve events.

### 4. 📝 Memory & Handoff Distillation (`agentbus distill` / `project-log`)

Tired of losing agent plans and progress when chat context windows clear?
AgentBus turns raw pub/sub coordination events into structured, human-readable documentation:

* **`agentbus distill`**: Materializes bounded batches of recent handoffs into structured OKF concepts under `okf/` (`okf/handoffs/` and `okf/log.md`).
* **`agentbus project-log`**: Projects recent handoffs into your project changelog (`okf/log.md` when the OKF knowledge plane is scaffolded, falling back to root `log.md` in non-OKF workspaces).

```bash
# Materialize bounded batches of recent handoffs into structured OKF handoff docs:
agentbus distill --limit 100

# Or project recent entries into the project log:
agentbus project-log
```

Now your team has a durable, bounded, human-readable paper trail of what was built, why, and by whom.

### 5. 🛡️ Cryptographic Tamper-Evident History (`agentbus verify`)

Events are linked using a forward **SHA-256 hash chain**. `agentbus verify` audits internal hash continuity and detects sequence gaps across retained events:

```bash
agentbus verify
```

> **Integrity Scope:** `agentbus verify` checks the internal hash continuity of retained events against the checkpoint recorded in the database. Because a local writer could recompute the chain, detecting an externally recomputed database requires independently preserving and comparing the reported head hash against an external record (such as a git commit or external ledger); retention and genesis cutover boundaries apply.

---

## 🔌 Connecting Your Favorite AI Tools (MCP)

AgentBus speaks standard **Model Context Protocol (MCP)** over stdio, allowing AI coding tools to communicate through the bus natively.

### Automatic 1-Click Setup

Inside your project workspace, you can preview changes for existing MCP client configurations:

```bash
agentbus init --producer-id local-user
```

*(Note: Preview mode inspects existing client configuration files without modifying them, while creating or retaining `.agentbus/token` and writing the `.agentbus/workspace` marker.)*

When you are ready to write them, run:

```bash
agentbus init --apply --producer-id local-user
```

When `agentbus init --apply` runs, it inspects your environment for *existing* client configuration files (it does not create missing files from scratch; use the manual setup below if your config file does not exist yet).

**Discovery scope & known client caveats:**

* **Cursor & Claude Desktop:** Automatically merges the `agentbus` definition under `mcpServers` for existing `.cursor/mcp.json`, `~/.cursor/mcp.json`, or Claude Desktop (`~/.config/claude/claude_desktop_config.json`) configuration files. Baseline Claude discovery checks only `~/.config/claude/claude_desktop_config.json`; if your Claude Desktop configuration is located at another platform path (such as macOS `~/Library/Application Support/Claude/claude_desktop_config.json` or Windows `%APPDATA%\Claude\claude_desktop_config.json`), configure it using the manual IDE setup below.
* **VS Code & Gemini (Manual Setup Required):** Do not use `init --apply` to configure VS Code or Gemini. For VS Code (`.vscode/mcp.json`), configure it directly using the manual `servers` snippet below before modifying the file; `init --apply` inserts `mcpServers`, which uses an unsupported schema that VS Code ignores. For Gemini CLI, configure `~/.gemini/settings.json` manually below (v0.27 `init` inspects `~/.gemini/config/mcp_config.json` rather than Gemini CLI settings). Native automated configuration for both is tracked as a product enhancement for an upcoming release.

*(Alternatively, running bare `agentbus` in an interactive terminal launches the companion wizard, prompting for confirmation before scaffolding.)*

### Manual IDE Setup

If you prefer configuring things manually:

<details>
<summary><b>For Cursor (<code>.cursor/mcp.json</code> or <code>~/.cursor/mcp.json</code>)</b></summary>

```json
{
  "mcpServers": {
    "agentbus": {
      "command": "agentbus",
      "args": ["mcp-serve"],
      "env": {
        "AGENTBUS_WORKSPACE": "/path/to/your/project",
        "AGENTBUS_PRODUCER_ID": "local-user"
      }
    }
  }
}
```

*(Use an authorized producer ID, such as `local-user` initialized by the companion, or your custom configured producer).*

</details>

<details>
<summary><b>For Claude Desktop (<code>claude_desktop_config.json</code>)</b></summary>

```json
{
  "mcpServers": {
    "agentbus": {
      "command": "agentbus",
      "args": ["mcp-serve"],
      "env": {
        "AGENTBUS_WORKSPACE": "/path/to/your/project",
        "AGENTBUS_PRODUCER_ID": "local-user"
      }
    }
  }
}
```

*(Use an authorized producer ID, such as `local-user` initialized by the companion, or your custom configured producer).*

</details>

<details>
<summary><b>For VS Code (<code>.vscode/mcp.json</code>)</b></summary>

```json
{
  "servers": {
    "agentbus": {
      "type": "stdio",
      "command": "agentbus",
      "args": ["mcp-serve"],
      "env": {
        "AGENTBUS_WORKSPACE": "/path/to/your/project",
        "AGENTBUS_PRODUCER_ID": "local-user"
      }
    }
  }
}
```

*(Use an authorized producer ID, such as `local-user` initialized by the companion, or your custom configured producer).*

</details>

<details>
<summary><b>For Gemini (<code>~/.gemini/settings.json</code> for Gemini CLI, or <code>~/.gemini/config/mcp_config.json</code>)</b></summary>

Depending on your Gemini tooling, add the `agentbus` definition to `~/.gemini/settings.json` (for Gemini CLI) or `~/.gemini/config/mcp_config.json` (for other Gemini clients):

```json
{
  "mcpServers": {
    "agentbus": {
      "command": "agentbus",
      "args": ["mcp-serve"],
      "env": {
        "AGENTBUS_WORKSPACE": "/path/to/your/project",
        "AGENTBUS_PRODUCER_ID": "local-user"
      }
    }
  }
}
```

*(Use an authorized producer ID, such as `local-user` initialized by the companion, or your custom configured producer).*

</details>

Once connected, your AI agents automatically gain coordination tools:
* `agentbus_publish` — publish task updates, handoffs, and questions.
* `agentbus_poll` — check for incoming tasks assigned to them.
* `agentbus_lock_acquire` / `agentbus_lock_release` — coordinate advisory file locks.
* `agentbus_review` / `agentbus_approve` / `agentbus_reject` — review and approve pending actions.

---

## 💻 Zero-Friction Code Examples

AgentBus is a first-class citizen across Python, TypeScript/Node.js, and Bash.

### Python SDK

Integrate custom Python agents, scripts, or LangChain/CrewAI nodes in just a few lines:

```python
from pathlib import Path
from agentbus.store import EventStore

# Connect to the current project's SQLite bus
store = EventStore(Path.cwd())

# Publish a handoff using an authorized producer (e.g. 'local-user' configured by default companion)
event, _ = store.publish(
    topic="okf/handoff",
    producer_id="local-user",
    schema_version="1.0",
    payload={"from": "local-user", "to": "reviewer", "summary": "API models updated"},
)
print(f"Published event #{event.event_id}")

# Poll for new messages
messages = store.poll("okf/handoff", since_id=0)
for ev in messages["events"]:
    print(f"[{ev['event_id']}] {ev['payload']['summary']}")

store.close()
```

### TypeScript / Node.js SDK

Using Node.js or building tools for web agents? Use [`@agentbus/agentbus-client`](https://www.npmjs.com/package/@agentbus/agentbus-client):

```bash
npm install @agentbus/agentbus-client
```

> **Prerequisites:** Requires Node.js 18+ and the `agentbus` CLI binary on your `PATH` (the client spawns `agentbus mcp-serve` via stdio MCP; see [Install](#1-install)).

```typescript
import { AgentBus } from "@agentbus/agentbus-client";

async function main() {
  const bus = new AgentBus({
    workspace: process.cwd(),
    topics: ["okf/handoff"],
  });

  // 1. Register listener FIRST so catch-up events emitted during connect() are received
  bus.on("okf/handoff", (payload, meta) => {
    console.log(`[Event #${meta.event_id}] From: ${payload.from} → ${payload.summary}`);
  });

  // 2. Connect stdio MCP transport
  await bus.connect();

  // Disconnect on process termination
  process.on("SIGINT", async () => {
    await bus.disconnect();
    process.exit(0);
  });
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
```

---

## 🛠️ Why AgentBus?

| Approach | Limitations | How AgentBus Solves It |
|---|---|---|
| **Shared `log.md` file** | Race conditions, zero schemas, easily corrupted by simultaneous writes | Transactional SQLite storage with configurable journal mode (defaulting to WAL on POSIX and MEMORY on Windows with weaker hard-crash durability; override via `AGENTBUS_SQLITE_JOURNAL`), atomic writes, monotonic event IDs, advisory file leases |
| **Cloud SaaS (e.g. LangSmith)** | Setup latency, cloud data egress, privacy concerns, monthly SaaS bills | Local-first SQLite database (`.agentbus/events.db`) keeps coordination data in your repo without mandatory cloud egress (telemetry and remote bridges are strictly opt-in) |
| **Heavy Brokers (Redis, Kafka)** | Background daemon requirements, port conflicts, heavy Docker containers | Embedded-first operation over SQLite; zero background daemons required for standard pub/sub (optional worker daemons can be launched via `agentbus up -d`) |
| **Framework Runtimes (LangGraph, CrewAI)** | Locked into a single language/process runtime | Connects heterogeneous agents across Cursor, Claude, Antigravity, and CLI bots via standard MCP and CLI |

---

## ⚡ Under the Hood: Built for Speed & Reliability

AgentBus is engineered to stay out of your way for simple local scripts while remaining rock-solid for multi-agent workflows:

* **Fast Lazy-Loaded CLI:** A 3-tier lazy-loaded console entrypoint strips heavy imports (`rich`, `textual`, `mcp`, `cryptography`) from the hot command path, keeping startup overhead low for commands like `publish` and `poll`.
* **SQLite Transactional Concurrency:** Multi-process read concurrency with serialized atomic writes using WAL journal mode on POSIX platforms. On Windows, SQLite defaults to `MEMORY` journal mode to reduce file locking contention under antivirus and EDR software (which offers best-effort operation but has weaker hard-crash durability than WAL or DELETE); you can override the journal mode on any platform via the `AGENTBUS_SQLITE_JOURNAL` environment variable (e.g. `WAL`, `DELETE`, or `MEMORY`). Multiple agents can read concurrently with standard SQLite transaction isolation.
* **Materialized Projections:** Plane counters are maintained in transaction-scoped tables, providing fast status queries without table scans.
* **Embedded-First Architecture:** For standard local pub/sub, state is stored directly in `.agentbus/events.db` (with auxiliary state files in `.agentbus/`). No background daemons are required unless you explicitly launch background worker processes via `agentbus up -d`.
* **OKF v0.2 Native:** Designed to integrate with the Open Knowledge Format standard for agent memory, roles, and handoffs.

---

## ⌨️ CLI Cheat Sheet

### 🚀 Everyday Commands

| Command | What it does |
|---|---|
| `agentbus` | Companion launcher: initializes new projects (interactive prompts or non-interactive JSON), or opens Mission Control / emits status in initialized projects |
| `agentbus monitor` | Open the interactive God View TUI cockpit (`agentbus-monitor` also works) |
| `agentbus publish --topic <t> --producer-id <id> --payload '<json>'` | Send an event to the local SQLite bus |
| `agentbus poll --topic <t> [--since-id <id>]` | Read published events from the bus |
| `agentbus status` | Check the current health, event count, and active topics of your bus |

### 🔒 Collaboration & Advisory Leases

| Command | What it does |
|---|---|
| `agentbus lock acquire --resource <abs_path> --owner-id <id> [--ttl-seconds 300]` | Acquire an exclusive advisory lease on a resource |
| `agentbus lock status --resource <abs_path>` | Check who currently holds a lease on a file |
| `agentbus lock release --resource <abs_path> --owner-id <id>` | Release an advisory lease when finished |

### 🛡️ Safety & Approvals (HITL)

| Command | What it does |
|---|---|
| `agentbus config set-intercept --topic <t> --contains "<pattern>"` | Create Human-in-the-Loop intercept rules for matching published payloads |
| `agentbus review` | List events currently waiting for your human approval |
| `agentbus approve <event_id> [--reviewer-id <id>]` | Approve a held event using an authorized approver role (e.g. `--reviewer-id agy` or configured in `roles.yaml`) |
| `agentbus reject <event_id> [--reviewer-id <id>] [--reason "<text>"]` | Reject a held event using an authorized approver role |

### 📝 Memory & Verification

| Command | What it does |
|---|---|
| `agentbus distill [--limit <n>]` | Materialize bounded batches of raw coordination events into OKF handoff docs (`okf/handoffs/`) |
| `agentbus project-log` | Update your project's changelog (`okf/log.md`, or root `log.md` fallback) |
| `agentbus verify` | Check internal hash continuity across retained events (see Integrity Scope for external comparison and retention limits) |
| `agentbus doctor` | Run non-destructive diagnostic checks on SQLite, schema, and environment |

### 🤖 Multi-Agent Swarm Orchestration (Optional)

| Command | What it does |
|---|---|
| `agentbus up [-d]` | Start local agent workers defined in `.agentbus/swarm.yaml` (runs foreground monitor and stops workers on exit; use `-d` / `--detach` for background daemons) |
| `agentbus ps` | List active background worker processes |
| `agentbus down` | Cleanly terminate all background worker processes |

---

## 📚 Complete Examples & Deep-Dive Guides

Looking for ready-to-run recipes? Check out the **[`examples/`](examples/)** folder:

* **[`01_core_pub_sub.py`](examples/01_core_pub_sub.py)** — Fundamental publish and poll flow.
* **[`02_hitl_intercepts.py`](examples/02_hitl_intercepts.py)** — Catching sensitive payloads for human sign-off.
* **[`03_swarm_rbac.py`](examples/03_swarm_rbac.py)** — Role-based topic permissions and identity tokens.
* **[`04_sla_timeouts.py`](examples/04_sla_timeouts.py)** — Automated dead-letter routing when an agent ghosts.
* **[`05_observability.py`](examples/05_observability.py)** — Distributed trace waterfalls (`trace_id`, `parent_span_id`).
* **[`06_distributed_context.py`](examples/06_distributed_context.py)** — Sharing large artifacts and diffs via content hashing.
* **[`07_pydantic_schemas.py`](examples/07_pydantic_schemas.py)** — Validating topic payloads with Pydantic models.
* **[`08_god_view.py`](examples/08_god_view.py)** — Passive OS filesystem, shell, and MCP call observation.
* **[`09_langgraph_bridge.py`](examples/09_langgraph_bridge.py)** — Bridging LangGraph nodes with AgentBus.
* **[`10_crewai_bridge.py`](examples/10_crewai_bridge.py)** — Bridging CrewAI tasks with AgentBus.

### Additional Reading

* 📖 [End-to-End Walkthrough (Alice, Bob, & Charlie)](docs/WALKTHROUGH.md)
* 🔌 [Model Context Protocol (MCP) Tool Schema](docs/MCP_SCHEMA.md)
* 🔐 [AgentID Security Model & Identity System](docs/AGENTID.md)
* 💻 [Client Compatibility Matrix](docs/CLIENT_MATRIX.md)
* 📊 [PostHog Outbound Telemetry](docs/POSTHOG.md)
* 🚢 [Release Runbook & Process](docs/RELEASE.md)
* 🗺️ [Roadmap](ROADMAP.md)

---

## 📄 License

MIT — see [LICENSE](LICENSE).
