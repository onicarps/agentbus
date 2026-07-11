# AgentBus Roadmap

**Status:** v0.11.0 on main (July 2026) — Phase 1 DX expansion

## Shipped (summary)

- [x] MCP stdio + SQLite event log, auth, leases, HITL, SLA, RBAC, schemas, TUI, God View, swarm `up/down`
- [x] PyPI `okf-agentbus` (CLI: `agentbus`)
- [x] **v0.11** TypeScript client package (`packages/js/agentbus-client`)
- [x] **v0.11** Jupyter `AsyncAgentBus` + `%agentbus` magics

## Next

### Phase 1–2 DX (active)

- [ ] Windows SQLite locking (single-writer + PRAGMA)
- [ ] CI / headless `--quiet` MCP stdio logging
- [ ] Tag + PyPI publish **0.11.0**
- [ ] Optional npm publish `@okf/agentbus-client`

### Later (gated)

- [ ] Strangler Bus Go `serve` sidecar spike (`--engine go`, pytest parity) — only after Phase 1–2 DX
- [ ] GitHub Release assets on tag
- [ ] Framework adapters / optional web UI (deferred)

### v1.0

- [ ] SSE subscribe where MCP clients support it
- [ ] Stable 1.0 API guarantee

## How to influence the roadmap

Open an issue with:

1. **Use case** — what agents/clients you run
2. **Pain** — what breaks with `log.md`, Redis, or ad-hoc files today
3. **Proposal** — schema, tool, or behavior change

Real-world dogfood reports beat theoretical features.