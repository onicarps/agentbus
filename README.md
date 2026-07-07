# AgentBus

[![Test](https://github.com/onicarps/agentbus/actions/workflows/test.yml/badge.svg)](https://github.com/onicarps/agentbus/actions/workflows/test.yml)
[![Python](https://img.shields.io/pypi/pyversions/agentbus-mcp.svg)](https://pypi.org/project/agentbus-mcp/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**Local MCP event log and advisory lease coordinator for multi-agent workspaces.**

When Cursor, Claude Code, Antigravity, Hermes, and other agents share a workspace, they usually coordinate through append-only files (`log.md`, sticky notes, Slack). That works until you need ordering, idempotency, cursors, schema validation, or file-edit mutexes.

AgentBus is a **localhost sidecar**: a Python MCP server backed by SQLite. Agents publish structured events; peers poll with `since_id` cursors. Advisory lease locks prevent concurrent edits. No orchestrator runtime lock-in.

> **v0.2 (July 2026):** Events + advisory lease locks — `publish`, `poll`, `status`, `lock_*`. SSE subscribe and custom topics on the [roadmap](ROADMAP.md).

> **Note:** The PyPI name `agentbus` is used by an unrelated NATS project. Install this package as **`agentbus-mcp`** (CLI command remains `agentbus`).

## Why AgentBus?

| Alternative | Limitation | AgentBus |
|-------------|------------|----------|
| `log.md` blackboard | No schema, races, manual cursors | Typed topics, monotonic IDs, poll API |
| `flock` / git mutex | No agent identity, no TTL | Advisory leases with heartbeat renewal |
| Redis pub/sub | Extra daemon, no MCP path | stdio MCP — works in existing IDEs |
| LangGraph / AutoGen | Same-runtime only | Heterogeneous out-of-process clients |

## Install

```bash
# Recommended — from GitHub (PyPI name: agentbus-mcp)
pip install "agentbus-mcp @ git+https://github.com/onicarps/agentbus@v0.2.2"

# Or clone for development
git clone https://github.com/onicarps/agentbus.git
cd agentbus
python3 -m venv .venv
.venv/bin/pip install -e ".[dev]"
```

## Quickstart

```bash
export AGENTBUS_WORKSPACE=/path/to/workspace
export AGENTBUS_PRODUCER_ID=my-agent

# Terminal A — MCP server (stdio)
agentbus serve --workspace "$AGENTBUS_WORKSPACE"

# Terminal B — publish a handoff
agentbus publish \
  --topic okf/handoff \
  --payload '{"from":"my-agent","to":"peer","summary":"Hello bus"}'

# Poll events
agentbus poll --topic okf/handoff --since-id 0

# Acquire an advisory lease before editing a shared file
agentbus lock acquire --resource "$AGENTBUS_WORKSPACE/log.md" --owner-id my-agent
agentbus lock status --resource "$AGENTBUS_WORKSPACE/log.md"
agentbus lock release --resource "$AGENTBUS_WORKSPACE/log.md" --lease-id <uuid> --owner-id my-agent
```

## MCP setup

Use `scripts/mcp-serve.sh` so the workspace token is injected automatically:

```json
{
  "mcpServers": {
    "agentbus": {
      "command": "/path/to/agentbus/scripts/mcp-serve.sh",
      "env": {
        "AGENTBUS_WORKSPACE": "/path/to/workspace",
        "AGENTBUS_PRODUCER_ID": "cursor"
      }
    }
  }
}
```

Client-specific templates:

| Client | Config |
|--------|--------|
| Cursor | [examples/mcp-cursor.json](examples/mcp-cursor.json) |
| Claude Desktop | [examples/mcp-claude-desktop.json](examples/mcp-claude-desktop.json) |
| Hermes | [examples/mcp-hermes.json](examples/mcp-hermes.json) |

### MCP tools

| Tool | Auth | Purpose |
|------|------|---------|
| `agentbus_publish` | Yes | Append one validated event |
| `agentbus_poll` | No | Fetch events after `since_id` (at-least-once) |
| `agentbus_status` | No | Event counts, topics, health |
| `agentbus_lock_acquire` | Yes | Exclusive advisory lease on a resource path |
| `agentbus_lock_release` | Yes | Release a held lease |
| `agentbus_lock_renew` | Yes | Extend TTL (heartbeat) |
| `agentbus_lock_status` | No | Check lock state without acquiring |

Full schema: [docs/MCP_SCHEMA.md](docs/MCP_SCHEMA.md)

## Authentication

On `serve`, AgentBus writes an ephemeral token to `{workspace}/.agentbus/token` (mode `0600`). Publish and lock mutations require a matching token via:

- `scripts/mcp-serve.sh` (recommended for MCP — reads workspace file, not stale env)
- `AGENTBUS_TOKEN` environment variable
- `--token` CLI flag
- `auth_token` MCP tool argument (optional; omit when using `mcp-serve.sh`)

Poll, status, and lock_status remain open. Set `AGENTBUS_AUTH=off` to disable during local development.

Details: [docs/AUTH.md](docs/AUTH.md)

## Topics (v0.2)

| Topic | Purpose |
|-------|---------|
| `okf/handoff` | Structured agent handoff (`from`, `to`, `summary`) |
| `okf/status/<name>` | Status ping (`idle`, `active`, `blocked`, `complete`) |

Custom topics are planned — [contributions welcome](CONTRIBUTING.md).

## CLI reference

```bash
agentbus serve [--workspace PATH] [--rotate-token]
agentbus publish --topic TOPIC --payload JSON [--producer-id ID]
agentbus poll --topic TOPIC [--since-id N] [--limit N]
agentbus status [--producer-id ID]
agentbus token show|ensure|rotate
agentbus lock acquire|release|renew|status
agentbus project-log [--log-file log.md]   # optional markdown projection
```

Set `AGENTBUS_WORKSPACE` to avoid repeating `--workspace`.

## Development

```bash
.venv/bin/pytest tests/ -q          # 40+ tests
.venv/bin/pytest tests/ --cov=agentbus --cov-report=term-missing
```

## Project layout

```
src/agentbus/     # Python package
tests/            # pytest suite (40 tests)
docs/             # MCP schema, auth guide
examples/         # MCP client configs (Cursor, Claude, Hermes)
scripts/          # mcp-serve.sh wrapper
```

## Roadmap

See [ROADMAP.md](ROADMAP.md). v0.2 ships events + advisory locks; v0.3 targets custom topics and HTTP transport.

## Contributing

We want real-world use cases from teams running heterogeneous agent stacks. See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

MIT — see [LICENSE](LICENSE).