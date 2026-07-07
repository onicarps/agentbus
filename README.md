# AgentBus

[![Test](https://github.com/onicarps/agentbus/actions/workflows/test.yml/badge.svg)](https://github.com/onicarps/agentbus/actions/workflows/test.yml)
[![Python](https://img.shields.io/pypi/pyversions/agentbus.svg)](https://pypi.org/project/agentbus/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**Local MCP event log for multi-agent workspaces.**

When Cursor, Claude Code, Antigravity, Hermes, and other agents share a workspace, they usually coordinate through append-only files (`log.md`, sticky notes, Slack). That works until you need ordering, idempotency, cursors, or schema validation.

AgentBus is a **localhost sidecar**: a Python MCP server backed by SQLite. Agents publish structured events; peers poll with `since_id` cursors. No orchestrator runtime lock-in.

> **v0.1 (July 2026):** Events-only MVP — `publish`, `poll`, `status`. Locks and SSE subscribe are on the [roadmap](ROADMAP.md).

## Why AgentBus?

| Alternative | Limitation | AgentBus |
|-------------|------------|----------|
| `log.md` blackboard | No schema, races, manual cursors | Typed topics, monotonic IDs, poll API |
| Redis pub/sub | Extra daemon, no MCP path | stdio MCP — works in existing IDEs |
| LangGraph / AutoGen | Same-runtime only | Heterogeneous out-of-process clients |

## Quickstart

```bash
git clone https://github.com/onicarps/agentbus.git
cd agentbus
python3 -m venv .venv
.venv/bin/pip install -e ".[dev]"

# Terminal A — MCP server
export AGENTBUS_PRODUCER_ID=my-agent
.venv/bin/agentbus serve --workspace /path/to/workspace

# Terminal B — CLI publish
.venv/bin/agentbus publish \
  --workspace /path/to/workspace \
  --topic okf/handoff \
  --producer-id my-agent \
  --payload '{"from":"my-agent","to":"peer","summary":"Hello bus"}'

# Poll events
.venv/bin/agentbus poll --workspace /path/to/workspace --topic okf/handoff --since-id 0
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

See [examples/](examples/) for Cursor and Claude Desktop templates.

### MCP tools

| Tool | Purpose |
|------|---------|
| `agentbus_publish` | Append one validated event |
| `agentbus_poll` | Fetch events after `since_id` (at-least-once) |
| `agentbus_status` | Event counts, topics, health |

Full schema: [docs/MCP_SCHEMA.md](docs/MCP_SCHEMA.md)

## Authentication

On `serve`, AgentBus writes an ephemeral token to `{workspace}/.agentbus/token` (mode `0600`). Publish calls require a matching token via:

- `AGENTBUS_TOKEN` environment variable (recommended for MCP)
- `--token` CLI flag
- `auth_token` MCP tool argument

Poll and status remain open. Set `AGENTBUS_AUTH=off` to disable during local development.

Details: [docs/AUTH.md](docs/AUTH.md)

## Topics (v0.1)

| Topic | Purpose |
|-------|---------|
| `okf/handoff` | Structured agent handoff (`from`, `to`, `summary`) |
| `okf/status/<name>` | Status ping (`idle`, `active`, `blocked`, `complete`) |

Custom topics are planned for v0.2 — [contributions welcome](CONTRIBUTING.md).

## CLI reference

```bash
agentbus serve [--workspace PATH] [--rotate-token]
agentbus publish --topic TOPIC --payload JSON [--producer-id ID]
agentbus poll --topic TOPIC [--since-id N] [--limit N]
agentbus status [--producer-id ID]
agentbus token show|ensure|rotate
agentbus project-log [--log-file log.md]   # optional markdown projection
```

Set `AGENTBUS_WORKSPACE` to avoid repeating `--workspace`.

## Development

```bash
.venv/bin/pytest tests/ -q
```

## Project layout

```
src/agentbus/     # Python package
tests/            # pytest suite (22 tests)
docs/             # MCP schema, auth guide
examples/         # MCP client configs
scripts/          # mcp-serve.sh wrapper
```

## Roadmap

See [ROADMAP.md](ROADMAP.md). Phase 5 targets advisory lease locks; v0.1 deliberately ships events-only.

## Contributing

We want real-world use cases from teams running heterogeneous agent stacks. See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

MIT — see [LICENSE](LICENSE).