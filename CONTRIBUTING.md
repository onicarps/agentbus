# Contributing to AgentBus

Thank you for helping make multi-agent local coordination better for everyone.

## Quick start

```bash
git clone https://github.com/onicarps/agentbus.git
cd agentbus
python3 -m venv .venv
.venv/bin/pip install -e ".[dev]"
.venv/bin/pytest tests/ -q
python tests/factory_validate.py   # MCP integration validation (Agy gate)
```

## What to work on

See [ROADMAP.md](ROADMAP.md) and [open issues](https://github.com/onicarps/agentbus/issues). High-value contribution areas:

1. **New topic schemas** — propose JSON Schema for real-world handoff patterns
2. **Client recipes** — tested MCP configs for Cursor, Claude Desktop, Windsurf, etc.
3. **Poll ergonomics** — helpers, SDKs, retry/backoff patterns
4. **Docs** — architecture notes, deployment guides, comparison with file-based blackboards

## Pull request checklist

- [ ] Tests pass: `pytest tests/ -q`
- [ ] New behavior has tests
- [ ] Public API changes documented in `docs/MCP_SCHEMA.md` or README
- [ ] No secrets, tokens, or machine-local paths committed

## Architecture notes

| Module | Role |
|--------|------|
| `store.py` | SQLite event log, retention, idempotency |
| `server.py` | FastMCP tool definitions |
| `auth.py` | Workspace ephemeral token |
| `schemas.py` | Topic registry + JSON Schema validation |
| `leases.py` | Advisory lease store (SQLite `leases` table) |
| `cli.py` | `serve`, `publish`, `poll`, `status`, `token`, `lock`, `project-log` |

## Versioning

AgentBus is **0.x** (pre-1.0). Minor releases may add topics and tools; patch releases are bugfixes. Breaking changes will be noted in [CHANGELOG.md](CHANGELOG.md).

## Code of conduct

See [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).