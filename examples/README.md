# AgentBus Examples & Recipes

This directory contains standalone, copy-pasteable examples and configurations for **AgentBus**. Every example is self-contained and tested to run directly.

---

## 🚀 Quick Run

Run any example with Python:

```bash
python examples/01_core_pub_sub.py
python examples/02_hitl_intercepts.py
python examples/09_langgraph_bridge.py
```

---

## 📚 Examples Directory

| Script | Level | Concept | Description |
|--------|-------|---------|-------------|
| **[01_core_pub_sub.py](01_core_pub_sub.py)** | 🟢 Beginner | **Core Pub/Sub** | Publish and poll events on local SQLite bus with zero extra daemons. |
| **[02_hitl_intercepts.py](02_hitl_intercepts.py)** | 🟡 Intermediate | **Human-in-the-Loop (HITL)** | Intercept dangerous actions (e.g., database drops) and require human approval. |
| **[03_swarm_rbac.py](03_swarm_rbac.py)** | 🔴 Advanced | **Swarm RBAC & Identity** | Enforce topic-level permissions so only authorized agents can publish to sensitive channels. |
| **[04_sla_timeouts.py](04_sla_timeouts.py)** | 🟡 Intermediate | **SLA Timeouts & Dead-Letter** | Catch deadlocks or ghosting agents by automatically expiring timed-out tasks into a dead-letter queue. |
| **[05_observability.py](05_observability.py)** | 🟡 Intermediate | **Distributed Lineage** | Propagate `trace_id` and `parent_span_id` across multi-agent handoffs for OpenTelemetry-style waterfalls. |
| **[06_distributed_context.py](06_distributed_context.py)** | 🟡 Intermediate | **Large Context Attachments** | Pass large files or git diffs via SHA256-verified attachments without blowing up payload limits. |
| **[07_pydantic_schemas.py](07_pydantic_schemas.py)** | 🟢 Beginner | **Pydantic Validation** | Enforce strict JSON schemas at the insertion layer using Python typing and Pydantic models. |
| **[08_god_view.py](08_god_view.py)** | 🟡 Intermediate | **Passive God View Mesh** | Observe shell commands, file edits, and MCP tool calls in real time. |
| **[09_langgraph_bridge.py](09_langgraph_bridge.py)** | 🟡 Intermediate | **LangGraph handoff skeleton** | A framework-neutral publish block to call from a LangGraph node after RBAC registration. |
| **[10_crewai_bridge.py](10_crewai_bridge.py)** | 🟡 Intermediate | **CrewAI handoff skeleton** | A framework-neutral Pi-QA handoff block to call from a CrewAI task after RBAC registration. |

---

## 🔌 IDE Configuration Templates

Use these ready-to-copy JSON templates to connect your favourite AI coding tools:

* **[mcp-cursor.json](mcp-cursor.json)**: Configuration for Cursor (`~/.cursor/mcp.json` or `.cursor/mcp.json`).
* **[mcp-claude-desktop.json](mcp-claude-desktop.json)**: Configuration for Claude Desktop (`claude_desktop_config.json`).
* **[mcp-hermes.json](mcp-hermes.json)**: Configuration for CLI terminal agents (e.g. Hermes).

### Easy 1-Step Setup
Instead of configuring manually, run:
```bash
agentbus init --apply --producer-id my-agent
```
AgentBus will automatically discover installed IDEs (Cursor, Claude Desktop, Antigravity) and wire the MCP configuration for you!

---

## 🐝 Swarm Configuration Templates

* **[swarm.yaml](swarm.yaml)**: Multi-agent service orchestration configuration for `agentbus up`.
* **[roles.yaml](roles.yaml)**: Role-Based Access Control (RBAC) rules.
* **`runner.*.yaml`**: Pre-configured headless runner configurations for named agents (`agy`, `codex`, `pi`, `aider`, `hermes`, `factory`).
