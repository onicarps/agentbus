# Changelog

All notable changes to this project are documented here.

## [0.2.0] - 2026-07-07

### Added

- Phase 5 advisory lease locks in `events.db` (`leases` table)
- MCP tools: `agentbus_lock_acquire`, `agentbus_lock_release`, `agentbus_lock_renew`, `agentbus_lock_status`
- CLI: `agentbus lock acquire|release|renew|status`
- `tests/test_leases.py` — acquire, conflict, renew, TTL expiry, auth, workspace boundary

## [0.1.0] - 2026-07-07

### Added

- MCP stdio server with `agentbus_publish`, `agentbus_poll`, `agentbus_status`
- SQLite event store with idempotency and retention
- Workspace ephemeral token authentication (`{workspace}/.agentbus/token`)
- CLI: `serve`, `publish`, `poll`, `status`, `token`, `project-log`
- `scripts/mcp-serve.sh` wrapper for MCP client token injection
- Reference topics: `okf/handoff`, `okf/status/<initiative>`
- 22 tests including MCP stdio round-trip

[0.1.0]: https://github.com/onicarps/agentbus/releases/tag/v0.1.0