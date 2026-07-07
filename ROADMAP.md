# AgentBus Roadmap

**Status:** v0.1 — events-only MVP (July 2026)

## Shipped (v0.1)

- [x] MCP stdio server: `publish`, `poll`, `status`
- [x] SQLite durable event log with monotonic `event_id`
- [x] Topic JSON Schema validation
- [x] Idempotency keys + 7-day retention
- [x] Workspace ephemeral token auth
- [x] CLI for non-MCP clients
- [x] `project-log` — optional markdown projection for handoff topics

## Next (community-driven)

### v0.2 — Developer experience

- [ ] PyPI publish + install via `pip install agentbus`
- [ ] Pluggable topic registry (user-defined schemas)
- [ ] Generic `workspace/handoff` topic alongside reference `okf/handoff`
- [ ] Client capability matrix with tested configs per IDE

### v0.3 — Operations

- [ ] HTTP transport (optional, localhost-only)
- [ ] Topic ACLs
- [ ] Export / backup commands

### v1.0 — Coordination primitives

- [ ] Advisory lease locks (deferred from early phases)
- [ ] SSE subscribe where MCP clients support it
- [ ] Stable 1.0 API guarantee

## How to influence the roadmap

Open an issue with:

1. **Use case** — what agents/clients you run
2. **Pain** — what breaks with `log.md`, Redis, or ad-hoc files today
3. **Proposal** — schema, tool, or behavior change

Real-world dogfood reports beat theoretical features.