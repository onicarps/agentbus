# AgentBus

[![Test](https://github.com/onicarps/agentbus/actions/workflows/test.yml/badge.svg)](https://github.com/onicarps/agentbus/actions/workflows/test.yml)
[![PyPI](https://img.shields.io/pypi/v/okf-agentbus.svg)](https://pypi.org/project/okf-agentbus/)
[![Python](https://img.shields.io/pypi/pyversions/okf-agentbus.svg)](https://pypi.org/project/okf-agentbus/)
[![npm](https://img.shields.io/npm/v/@agentbus/agentbus-client.svg)](https://www.npmjs.com/package/@agentbus/agentbus-client)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**A private, local walkie-talkie and dashboard for your AI coding assistants.**

If you use AI coding tools like Cursor, Claude Code, VS Code, or custom scripts on the same project, you have likely run into the coordination problem:
* **Overwritten code:** Two AI assistants edit the same file at the same time and erase each other's changes.
* **Lost context:** A plan or decision stays trapped in one chat window, forcing you to copy and paste text back and forth.
* **Risky actions:** An assistant prepares to run a database migration or delete files before you have had a chance to look it over.
* **Heavy infrastructure:** Setting up cloud message brokers or Docker containers just to let local tools talk to each other is complicated, slow, and expensive.

**AgentBus solves this right inside your project folder.** It gives your AI tools a fast, private messaging channel backed by a single local database file (`.agentbus/events.db`), and gives you an interactive visual dashboard in your terminal to watch every handoff, track progress, and approve sensitive actions in real time.

No cloud accounts. No subscriptions. No background servers required for local coordination.

---

## Quickstart in 60 Seconds

### 1. Install AgentBus

Choose the method that works best for your system:

#### Option A: Package Manager (Recommended)

```bash
# Using uv (fastest)
uv tool install okf-agentbus

# Using pipx
pipx install okf-agentbus
```

#### Option B: Standalone Installer Script

```bash
# 1. Download install.sh pinned to release commit 0f6d389
curl --proto '=https' --tlsv1.2 -LsSf https://raw.githubusercontent.com/onicarps/agentbus/0f6d389622a168b78eb3bdc3b02729c5bc4680aa/install.sh -o install.sh

# 2. Verify script SHA-256 and run with release v0.28.0:
# On Linux:
echo "03e64e9bb7e252c072ba77e0cc62324f3e3d34a9c4aaeb4886a0e897c80528b2  install.sh" | sha256sum --check && AGENTBUS_PACKAGE_NAME="okf-agentbus==0.28.0" bash install.sh
# On macOS:
echo "03e64e9bb7e252c072ba77e0cc62324f3e3d34a9c4aaeb4886a0e897c80528b2  install.sh" | shasum -a 256 --check && AGENTBUS_PACKAGE_NAME="okf-agentbus==0.28.0" bash install.sh
```

> **Supply-Chain Verification Note:** The SHA-256 checksum verifies the `install.sh` script itself. It does not verify downstream artifacts (such as the separately downloaded `uv` installer when `uv` is not already installed, or the fetched Python package wheels and their dependencies). For audited environments with strict verification policies, install pinned packages using your standard package manager, lockfiles, or internal artifact mirror.
>
> **Path Note:** The installer places the executable in `${XDG_BIN_HOME:-$HOME/.local/bin}` and updates your shell profile for new sessions. For your current terminal, make sure that directory is on your PATH (for example: `export PATH="${XDG_BIN_HOME:-$HOME/.local/bin}:$PATH"`).

### 2. Connect Your Project (Terminal A)

Go to your project directory and run the companion setup:

```bash
cd ~/my-project

# Run the setup assistant (finds installed tools and asks to connect them)
agentbus
```

Now open the visual dashboard:

```bash
agentbus monitor
```

You are now in the terminal dashboard:
* **Live Feed:** Watch messages and tasks as they are sent between tools.
* **Task History:** Inspect which agent started a task and who is continuing it.
* **One-Key Approvals:** Press `a` to approve or `r` to reject actions waiting for human review.

*(Note: In automated scripts or CI pipelines, running `agentbus` on a fresh project initializes it non-interactively with project-scoped tool discovery applied, or outputs JSON status if already set up. Automated setup only changes project files like `.cursor/mcp.json` or `.vscode/mcp.json`. Changes to global user files require the explicit flag `agentbus connect --apply --allow-user-scope`.)*

### 3. Send Your First Message (Terminal B)

Open a second terminal window in the same folder and publish a message:

```bash
agentbus publish \
  --topic okf/handoff \
  --producer-id local-user \
  --payload '{"from":"local-user","to":"reviewer","summary":"Built the auth endpoints. Ready for review."}'
```

Switch back to Terminal A: your message appears immediately on your dashboard screen.

---

## How It Works in Practice

Here is an everyday example of how AgentBus helps two different AI assistants work together smoothly:

```
[Claude Code] ---------------------------> [AgentBus] ---------------------------> [Codex CLI]
(Writes code in terminal)           (Local events.db)                 (Runs automated audit)
       |                                   |                                     |
       | 1. Takes advisory lease           |                                     |
       |    on src/auth.py                 |                                     |
       | 2. Updates endpoints              |                                     |
       | 3. Publishes handoff event -----> | Stores event #42                    |
       |                                   | Notifies subscribers -------------> | 4. Reads event #42
       |                                   |                                     | 5. Audits src/auth.py
       |                                   | <--- 6. Publishes review passed --- | 6. Sends completion report
       |                                   |
                               [Your Terminal Cockpit]
                         (Watch handoffs live, approve/reject)
```

1. **Claude Code** finishes writing an authentication feature and publishes a handoff event to AgentBus.
2. **AgentBus** logs the message to your project's local `.agentbus/events.db` file.
3. **Codex CLI** polls the bus, sees the handoff assigned to it, and runs verification checks.
4. **You** watch the entire conversation unfold on your terminal dashboard (`agentbus monitor`) without needing to switch windows or copy-paste prompts.

---

## Five Core Capabilities

### 1. Live Visual Dashboard

Instead of wondering what your AI assistants are doing across separate editor windows, run `agentbus monitor`. You get a clean, high-density live view of:
* Active messages and task handoffs.
* Task relationships (`trace_id`) showing which action triggered another.
* Pending requests waiting for human approval.
* Active file reservations.

### 2. File Reservations (Advisory Leases)

Prevent two AI assistants from editing the same file at the same time:

```bash
# Agent 1 checks out models.py for 5 minutes (300 seconds):
agentbus lock acquire --resource "$PWD/src/models.py" --owner-id agent-1 --ttl-seconds 300

# Other tools can check if someone else is currently editing the file:
agentbus lock status --resource "$PWD/src/models.py"

# When Agent 1 finishes, it releases the reservation:
agentbus lock release --resource "$PWD/src/models.py" --owner-id agent-1
```

> **Advisory Scope Note:** File leases are **advisory**. Cooperating tools and agents check status and honor active leases before writing. AgentBus coordinates permissions across agents but does not intercept low-level operating system file calls or block external text editors.

### 3. Human Approval Gates

Hold high-risk actions (such as database migrations, deletions, or external deployments) until a human gives permission:

```bash
# 1. Create a safety rule: any handoff containing 'DROP' or 'MIGRATE' requires approval
agentbus config set-intercept --topic okf/handoff --contains "DROP" --ttl-minutes 30
agentbus config set-intercept --topic okf/handoff --contains "MIGRATE" --ttl-minutes 30

# 2. When an agent publishes a matching action, AgentBus pauses it in PENDING_APPROVAL status.
# 3. Downstream agents will not execute the action until you approve it.
# 4. In your terminal dashboard, press 'a' to approve or 'r' to reject.
# (Or approve via the command line with an authorized role):
# agentbus approve <event_id> --reviewer-id agy
# agentbus reject <event_id> --reviewer-id agy --reason "Needs schema migration plan"
```

> **How Approvals Work:** Approval rules hold matching *events published to AgentBus*. They do not intercept arbitrary shell commands or out-of-band actions. Tools coordinate safety by publishing planned actions as bus events and waiting for human sign-off before proceeding.
> **Reviewer Roles:** Approving or rejecting events requires a reviewer mapped to a role with `can_approve: true` in `.agentbus/roles.yaml` (such as the default `architect` role mapped to `agy`, or configuring a reviewer in `producers`). The initial setup configures the default user as an `engineer`, which cannot approve events without an authorized reviewer role.

### 4. Automatic Project Documentation

Do not lose progress when an AI assistant's chat history clears. AgentBus can convert raw coordination messages into structured Markdown notes:

* **`agentbus distill`**: Materializes recent `okf/handoff` events into deterministic concept documents under `okf/handoffs/YYYY/MM/`, indexed by a bounded `okf/log.md`.
* **`agentbus project-log`**: In an OKF workspace with default settings, runs the bounded distiller to update `okf/log.md` and partitioned concepts; in non-OKF workspaces, appends recent handoffs to the root `log.md`.

```bash
# Save recent handoffs into organized documentation:
agentbus distill --limit 100

# Update the project changelog:
agentbus project-log
```

This leaves your team with a durable, human-readable paper trail of what was built, why, and by whom.

### 5. Verifiable Message History

Retained events are linked using a cryptographic SHA-256 hash chain (starting from the workspace genesis checkpoint or post-migration cutover boundary). You can check hash continuity across stored events at any time:

```bash
agentbus verify
```

> **Integrity Scope Note:** `agentbus verify` checks internal hash continuity across retained events against the checkpoint stored in the database. Because pre-hash legacy rows serve as the initial unhashed checkpoint and a local writer could recompute the entire chain, detecting an externally recomputed database requires comparing the reported head hash against an external record (such as a git commit hash or external log).

---

## Connecting Your AI Tools (Model Context Protocol)

AgentBus supports the open **Model Context Protocol (MCP)** over standard input/output (stdio). This allows tools like Cursor, Claude Code, and VS Code to talk to AgentBus natively.

### Automatic Setup: Discover, Preview, Connect

AgentBus includes built-in discovery for popular AI tools. Discovery is completely safe and read-only by default:

```bash
# 1. Discover: See what tools exist on your system and what is already connected
agentbus discover
agentbus discover --json

# 2. Preview: Plan configuration changes without writing any files
agentbus connect

# 3. Apply: Connect detected auto-connect tools for the current project
agentbus connect --apply

# 4. Verify: Test that the connection command successfully communicates
agentbus connect --apply --verify

# Optional: Connect opt-in tools that default to auto_connect: false
agentbus connect --apply --client claude-code-project
agentbus connect --apply --client codex-project

# Optional: Connect global user tools (in interactive terminals you can confirm via prompt; non-interactive runs require --allow-user-scope)
agentbus connect --scope user --apply --allow-user-scope
```

*(Note: Standard bulk runs with `agentbus connect --apply` configure auto-connect tools such as Cursor and VS Code. Opt-in tools like Claude Code (`--client claude-code-project` or `--client claude-code-user`) and Codex project (`--client codex-project`) declare `auto_connect: false` and are connected by specifying their client name. For user-scope files, interactive terminals prompt for confirmation, while non-interactive runs require `--allow-user-scope`.)*

When you inspect your tools, AgentBus clearly reports their status:
* **Configured:** The configuration file matches what AgentBus expects.
* **Differs:** A configuration already exists but has different settings. AgentBus will not silently overwrite it.
* **Awaiting Approval:** The configuration file was created or updated, but your AI tool needs to be restarted or needs you to click "Allow" in its settings.
* **Command Verified:** When run with `--verify`, AgentBus proved that the configured command starts and answers MCP requests properly.

### Safety Guarantees

* **Zero unexpected writes:** `agentbus discover` and `agentbus connect` (without `--apply`) never write, modify, or delete any files on your computer.
* **One bounded version probe:** `agentbus connect` may run a single time-boxed `opencode --version` (with a temporary, isolated HOME) to tell the OpenCode v1 and v2 layouts apart. `agentbus discover` does not launch client CLIs or start the MCP server, but command resolution may run bounded validation probes.
* **Conditional rollback:** Before each replacement AgentBus re-checks the file's SHA-256 digest and reports `file_created_externally`, `file_deleted_externally`, or `file_modified_externally` instead of overwriting a concurrent edit. If a later write fails, rollback restores only files that still match what AgentBus wrote and reports a conflict for anything changed or deleted since; outside work is never clobbered.
* **Automatic backups:** When replacing an existing configuration file, AgentBus creates a unique backup file in the same directory (such as `.cursor/mcp.json.agentbus.bak.<timestamp>.<pid>`). Newly created files are not backed up. If an error occurs during an existing file's write, it makes a best-effort rollback.
* **Secret protection:** Passwords, API keys, and sensitive environment variables are automatically masked as `[REDACTED]` in terminal displays, logs, and JSON outputs.
* **Safe file replacement:** Configuration files are written to a temporary file first and replaced atomically per file so a crash during write does not leave a half-written file. Multi-file installations proceed sequentially, so crashes or non-cooperating writers can still leave partial multi-file state.
* **Process locking:** Setup operations use an advisory lock file (`.agentbus/init.lock`) so two setup commands running at the same time cannot corrupt each other.

### Supported Tools

* **Cursor:** Project configuration (`.cursor/mcp.json`) and user configuration (`~/.cursor/mcp.json`).
* **Claude Desktop:** Linux, macOS, and Windows configuration files.
* **Claude Code:** Project configuration (`.mcp.json`) and user configuration (`~/.claude.json`).
* **Codex CLI:** Project configuration (`.codex/config.toml`) and global settings (`~/.codex/config.toml`).
* **VS Code:** Project configuration (`.vscode/mcp.json`, using the standard `servers` schema).
* **Gemini:** AgentBus writes the legacy user configuration (`~/.gemini/config/mcp_config.json`) for compatibility. Gemini CLI reads `~/.gemini/settings.json`; add AgentBus to that file manually.
* **Goose:** User configuration (`${XDG_CONFIG_HOME:-~/.config}/goose/config.yaml`, `%APPDATA%\Block\goose\config\config.yaml` on Windows) under the `extensions` map. AgentBus rewrites only `extensions.agentbus` with a round-trip YAML writer that preserves comments, anchors, and key order, and refuses multi-document, custom-tag, recursive-anchor, or duplicate-key files.
* **Cline CLI:** User configuration (`~/.cline/data/settings/cline_mcp_settings.json`) with `"disabled": false`.
* **Mistral Vibe:** Project configuration (`.vibe/config.toml`) and user configuration (`${VIBE_HOME:-~/.vibe}/config.toml`). AgentBus updates the matching `[[mcp_servers]]` element in place and preserves every other table and comment.
* **GitHub Copilot CLI:** User configuration (`~/.copilot/mcp-config.json`) with `"type": "local"` and `"tools": ["*"]`.
* **OpenCode:** Project (`opencode.json`) and user (`${XDG_CONFIG_HOME:-~/.config}/opencode/opencode.json`) configuration. OpenCode ships two incompatible layouts: v1 stores servers under `mcp` with `enabled`, v2 under `mcp.servers` with `disabled`. AgentBus selects one from `--client opencode-v1|opencode-v2`, then the config `$schema`, then a bounded `opencode --version` probe, then the file structure, and refuses to guess when the signals are missing or disagree.
* **Qwen Code:** Project (`.qwen/settings.json`) and user (`~/.qwen/settings.json`) configuration.

### Manual Tool Configuration

If you prefer editing configuration files by hand, you can add AgentBus directly.

> **RBAC Producer Authorization Note:** In a workspace with role-based access control enabled (`.agentbus/roles.yaml`), any `AGENTBUS_PRODUCER_ID` you configure (such as `local-user`) must have a corresponding role mapping under `producers:` (for example: `producers: { local-user: engineer }`). If `roles.yaml` exists and the configured producer is not listed, publish calls will return `403 Forbidden`. You can change `local-user` in the snippets below to any producer ID that already has an authorized role mapping. `agentbus connect` never creates or edits `.agentbus/roles.yaml`: connecting a client does not grant a producer role. Workspaces without an active RBAC configuration, or with `AGENTBUS_DISABLE_RBAC=1` outside protected identity mode, keep working as before.

<details>
<summary><b>Cursor (<code>.cursor/mcp.json</code> or <code>~/.cursor/mcp.json</code>)</b></summary>

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

</details>

<details>
<summary><b>Claude Desktop (<code>claude_desktop_config.json</code>)</b></summary>

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

</details>

<details>
<summary><b>Claude Code (<code>.mcp.json</code>)</b></summary>

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

</details>

<details>
<summary><b>VS Code (<code>.vscode/mcp.json</code>)</b></summary>

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

</details>

<details>
<summary><b>Codex CLI (<code>.codex/config.toml</code> or <code>~/.codex/config.toml</code>)</b></summary>

```toml
[mcp_servers.agentbus]
command = "agentbus"
args = ["mcp-serve"]

[mcp_servers.agentbus.env]
AGENTBUS_WORKSPACE = "/path/to/your/project"
AGENTBUS_PRODUCER_ID = "local-user"
```

*(Place this configuration in your workspace at `.codex/config.toml` for project scope, or in `~/.codex/config.toml` for user global scope. Note: Codex loads project-scope `.codex/config.toml` only for trusted projects; for untrusted projects, trust the workspace or configure the user-scope file `~/.codex/config.toml` so AgentBus is available).*

</details>

<details>
<summary><b>Gemini CLI (<code>~/.gemini/settings.json</code>) or legacy (<code>~/.gemini/config/mcp_config.json</code>)</b></summary>

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

</details>

Once connected, your AI tools gain these functions:
* `agentbus_publish`: Send task updates, questions, and handoffs.
* `agentbus_poll`: Check for incoming tasks assigned to them.
* `agentbus_lock_acquire` / `agentbus_lock_release`: Reserve files before editing.
* `agentbus_review` / `agentbus_approve` / `agentbus_reject`: Review and approve actions that need human permission.

---

## Code Examples

AgentBus works seamlessly with Python, TypeScript/Node.js, and shell scripts.

### Python SDK

Integrate custom Python agents, scripts, or LangChain/CrewAI tools in a few lines:

```python
from pathlib import Path
from agentbus.store import EventStore

# Connect to the SQLite bus in the current project directory
store = EventStore(Path.cwd())

# Send a message to other agents
event, _ = store.publish(
    topic="okf/handoff",
    producer_id="local-user",
    schema_version="1.0",
    payload={"from": "local-user", "to": "reviewer", "summary": "API models updated"},
)
print(f"Published event #{event.event_id}")

# Read incoming messages
messages = store.poll("okf/handoff", since_id=0)
for ev in messages["events"]:
    print(f"[{ev['event_id']}] {ev['payload']['summary']}")

store.close()
```

### TypeScript / Node.js SDK

For Node.js projects, install [`@agentbus/agentbus-client`](https://www.npmjs.com/package/@agentbus/agentbus-client):

```bash
npm install @agentbus/agentbus-client
```

> **Prerequisites:** Node.js 18+ and the `agentbus` command available on your PATH.

```typescript
import { AgentBus } from "@agentbus/agentbus-client";

async function main() {
  const bus = new AgentBus({
    workspace: process.cwd(),
    topics: ["okf/handoff"],
  });

  // Register listener before connecting so you do not miss initial events
  bus.on("okf/handoff", (payload, meta) => {
    console.log(`[Event #${meta.event_id}] From: ${payload.from} -> ${payload.summary}`);
  });

  await bus.connect();

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

## Comparison: Why AgentBus?

| Approach | Drawbacks | How AgentBus Solves It |
|---|---|---|
| **Shared text files (`log.md`)** | Collisions when two agents write at once, no structure, easy to corrupt | Uses a local SQLite database with transactional writes, monotonic event numbering, and advisory file reservations. |
| **Cloud platforms (e.g. LangSmith)** | Setup friction, network latency, data leaves your machine, monthly subscription costs | Local-first design. Event storage and auxiliary files in `.agentbus/` stay on your machine by default (outbound PostHog telemetry is strictly opt-in and sends only allowlisted metadata when configured). |
| **Heavy brokers (Redis, Kafka)** | Requires background servers, Docker containers, open ports, and system administration | Runs directly in your project. Standard messaging requires zero background daemons. |
| **Single-framework runtimes** | Locks you into a single library or programming language | Connects any assistant (Cursor, Claude Code, VS Code, Python scripts) using open Model Context Protocol standards. |

---

## Technical Architecture (Under the Hood)

<details>
<summary><b>Click to expand technical specifications</b></summary>

AgentBus is designed to stay out of the way for simple local scripts while remaining reliable under multi-process workloads:

* **Fast Lazy-Loaded CLI:** A 3-tier lazy-loaded console entrypoint strips heavy imports (`rich`, `textual`, `mcp`, `cryptography`) from the hot command path, keeping startup latency low for commands like `publish` and `poll`.
* **SQLite Transactional Concurrency:** Multi-process read concurrency with serialized atomic writes using WAL journal mode on POSIX platforms. On Windows, SQLite defaults to `MEMORY` journal mode to reduce file locking contention under antivirus and EDR software (which offers best-effort operation but has weaker hard-crash durability than WAL or DELETE); you can override the journal mode on any platform via the `AGENTBUS_SQLITE_JOURNAL` environment variable (e.g. `WAL`, `DELETE`, or `MEMORY`). Multiple agents can read concurrently with standard SQLite transaction isolation.
* **Materialized Projections:** Total, latest, pending-approval, SLA, and topic counters are maintained in transaction-scoped tables, providing fast status queries without full-table scans.
* **Embedded-First Architecture:** For standard local pub/sub, state is stored directly in `.agentbus/events.db` (with auxiliary state files in `.agentbus/`). No background daemons are required unless you explicitly launch background worker processes via `agentbus up -d`.
* **OKF v0.2 Native:** Designed to integrate with the Open Knowledge Format standard for agent memory, roles, and handoffs.

</details>

---

## CLI Reference

### Daily Essentials

| Command | What it does |
|---|---|
| `agentbus` | Companion assistant: sets up new projects with interactive prompts, or opens the dashboard |
| `agentbus monitor` | Open the interactive terminal dashboard |
| `agentbus publish --topic <topic> --producer-id <id> --payload '<json>'` | Send a message or handoff to the bus |
| `agentbus poll --topic <topic> [--since-id <id>]` | Read published messages from the bus |
| `agentbus status` | Check project bus health, event totals, and active topics |

### Tool Discovery & Connection

| Command | What it does |
|---|---|
| `agentbus discover` | Safely inspect installed AI tools and their connection status |
| `agentbus connect` | Preview planned configuration changes without writing anything |
| `agentbus connect [--scope project] --apply [--client <id>]` | Apply connections to detected tools (defaults to all scopes; non-interactive runs require `--allow-user-scope` before writing user-level files, and `--client` to connect opt-in tools) |
| `agentbus connect --apply --verify` | Apply connections and verify MCP handshake communication |
| `agentbus connect --scope user --apply --allow-user-scope` | Connect global user-level tools with explicit authorization |

### File Reservations (Advisory Leases)

| Command | What it does |
|---|---|
| `agentbus lock acquire --resource <path> --owner-id <id> [--ttl-seconds 300]` | Reserve a file before editing it |
| `agentbus lock status --resource <path>` | Check whether a file is currently reserved |
| `agentbus lock release --resource <path> --owner-id <id>` | Release a file reservation when finished |

### Human Safety & Approvals

| Command | What it does |
|---|---|
| `agentbus config set-intercept --topic <topic> --contains "<pattern>"` | Require human approval for messages matching a keyword |
| `agentbus review` | List actions currently waiting for approval |
| `agentbus approve <event_id> [--reviewer-id <id>]` | Approve a held action using an authorized reviewer role |
| `agentbus reject <event_id> [--reviewer-id <id>] [--reason "<text>"]` | Reject a held action with an explanation |

### Documentation & Maintenance

| Command | What it does |
|---|---|
| `agentbus distill [--limit <count>]` | Materialize recent handoff events into deterministic OKF concept documents |
| `agentbus project-log` | Update your project changelog (runs bounded distiller in OKF workspaces, or appends to `log.md`) |
| `agentbus verify` | Check cryptographic hash continuity across stored events |
| `agentbus doctor` | Run non-destructive health checks on your database and setup |

### Swarm Worker Daemons (Optional)

| Command | What it does |
|---|---|
| `agentbus up [-d]` | Start workers defined in `.agentbus/swarm.yaml`; use `-d` / `--detach` to run them in the background |
| `agentbus ps` | List running background worker processes |
| `agentbus down` | Safely stop all background worker processes |

---

## Examples and Guides

Explore complete, ready-to-run examples in the **[`examples/`](examples/)** folder:

* [`01_core_pub_sub.py`](examples/01_core_pub_sub.py) - Basic publish and poll workflow.
* [`02_hitl_intercepts.py`](examples/02_hitl_intercepts.py) - Pausing sensitive actions for human approval.
* [`03_swarm_rbac.py`](examples/03_swarm_rbac.py) - Topic permissions and security roles.
* [`04_sla_timeouts.py`](examples/04_sla_timeouts.py) - Automated routing when an assistant stops responding.
* [`05_observability.py`](examples/05_observability.py) - Distributed trace waterfalls (`trace_id`, `parent_span_id`).
* [`06_distributed_context.py`](examples/06_distributed_context.py) - Sharing large files and diffs via content hashing.
* [`07_pydantic_schemas.py`](examples/07_pydantic_schemas.py) - Validating message payloads with Pydantic.
* [`08_god_view.py`](examples/08_god_view.py) - File system, shell, and MCP observation.
* [`09_langgraph_bridge.py`](examples/09_langgraph_bridge.py) - Bridging LangGraph nodes with AgentBus.
* [`10_crewai_bridge.py`](examples/10_crewai_bridge.py) - Bridging CrewAI tasks with AgentBus.

### Additional Documentation

* [End-to-End Walkthrough (Alice, Bob, and Charlie)](docs/WALKTHROUGH.md)
* [Model Context Protocol (MCP) Tool Reference](docs/MCP_SCHEMA.md)
* [AgentID Security Model and Identity System](docs/AGENTID.md)
* [Client Compatibility Matrix](docs/CLIENT_MATRIX.md)
* [Telemetry and Diagnostics](docs/POSTHOG.md)
* [Release Runbook and Verification](docs/RELEASE.md)
* [Project Roadmap](ROADMAP.md)

---

## License

MIT - see [LICENSE](LICENSE).
