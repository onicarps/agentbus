# AgentBus Roadmap

**Status:** v0.2 — events + advisory lease locks (July 2026)

## Shipped (v0.1)

- [x] MCP stdio server: `publish`, `poll`, `status`
- [x] SQLite durable event log with monotonic `event_id`
- [x] Topic JSON Schema validation
- [x] Idempotency keys + 7-day retention
- [x] Workspace ephemeral token auth
- [x] CLI for non-MCP clients
- [x] `project-log` — optional markdown projection for handoff topics

## Shipped (v0.2)

- [x] Advisory lease locks in `events.db` (`leases` table)
- [x] MCP: `agentbus_lock_acquire`, `agentbus_lock_release`, `agentbus_lock_renew`, `agentbus_lock_status`
- [x] CLI: `agentbus lock acquire|release|renew|status`
- [x] 40 tests including lease store + MCP stdio round-trip
- [x] PyPI package name `agentbus-mcp` (avoids collision with unrelated `agentbus` on PyPI)

## Next (community-driven)

### v0.2.x — Distribution & onboarding

- [ ] PyPI publish as `agentbus-mcp`
- [ ] GitHub Release assets (wheel/sdist) on tag
- [ ] End-to-end multi-agent handoff walkthrough in docs
- [ ] Client capability matrix with tested configs per IDE

### v0.3 — Developer experience

- [ ] Pluggable topic registry (user-defined schemas)
- [ ] Generic `workspace/handoff` topic alongside reference `okf/handoff`
- [ ] Poll ergonomics helpers (backoff, cursor management)

### v0.4 — Operations

- [ ] HTTP transport (optional, localhost-only)
- [ ] Topic ACLs
- [ ] Export / backup commands

### v1.0 — Stable API

- [ ] SSE subscribe where MCP clients support it
- [ ] Stable 1.0 API guarantee

## How to influence the roadmap

Open an issue with:

1. **Use case** — what agents/clients you run
2. **Pain** — what breaks with `log.md`, Redis, or ad-hoc files today
3. **Proposal** — schema, tool, or behavior change

Real-world dogfood reports beat theoretical features.