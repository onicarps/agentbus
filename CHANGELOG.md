# Changelog

All notable changes to this project are documented here.

## [0.8.2] - 2026-07-08

### Added

- `agentbus publish-batch` — JSONL bulk publish in a single process (Power Ranking T06 harness fix)
- 60s content dedup for identical topic+producer+payload without `idempotency_key` (T03)
- Status response aliases: `total_events`, `pending_count` (T04 compat)
- `.agentbus/workspace` marker on `init --apply`; CLI resolves workspace via `resolve_workspace()` + `AGENTBUS_WORKSPACE` (T07)

## [0.8.1] - 2026-07-08

### Fixed

- `agentbus init --apply` no longer crashes on empty (0-byte) IDE MCP config files (`devex.py` `_load_json_config`)

## [0.8.0] - 2026-07-08

### Added

- Textual mission-control TUI for `agentbus monitor` (3-pane layout, HITL approve/reject hotkeys)
- `agentbus monitor --plain` for non-TUI tail mode
- Seven OSS onboarding scripts in `examples/` (v0.1–v0.7 feature tour)
- `format_trace_tree_plain()` for TUI trace waterfall

## [0.7.0] - 2026-07-07

### Added

- Pluggable topic schemas: `topic_schemas` SQLite registry
- `agentbus schema import/list/register` CLI
- `agentbus.sdk` with `@bus.topic` Pydantic decorator (`[sdk]` extra)
- Strict jsonschema validation for custom topics; 4 schema tests (77 total)

## [0.6.0] - 2026-07-07

### Added

- Distributed context: `artifacts` table, `publish --attach`, poll hydration
- 1MB per-artifact limit with `413 Payload Too Large`
- `artifacts` array in `okf/handoff` payload schema; 4 artifact tests (73 total)

## [0.5.0] - 2026-07-07

### Added

- Distributed tracing: `trace_id`, auto `span_id`, `parent_span_id` on events
- CLI/MCP publish: `--trace-id`, `--parent-span-id`
- `agentbus trace <trace_id>` rich waterfall visualization
- 4 trace tests (69 total)

## [0.4.0] - 2026-07-07

### Added

- SLA timeouts: `sla_timeout_minutes` on publish (CLI/MCP)
- Auto-escalation to `okf/dead-letter` on breach (`SLA_BREACH`, `TIMEOUT_FAILED`)
- `causation_id` reply clears SLA before deadline
- `sla_active_count` in `agentbus status`; 4 SLA tests (65 total)

## [0.3.2] - 2026-07-07

### Added

- Swarm RBAC: `.agentbus/roles.yaml`, producer→role map, forbidden payload patterns
- `droid_proof` mint/verify for `qa_droid` role (single-use, 30m TTL)
- CLI: `config init-rbac`, `droid mint`; `AGENTBUS_DISABLE_RBAC=1` bypass
- MCP/CLI 403 on unauthorized publish or approve/reject
- `examples/roles.yaml`; 8 RBAC tests (61 total)

## [0.2.3] - 2026-07-07

### Changed

- PyPI package renamed to **`okf-agentbus`** — `agentbus-mcp` rejected as too similar to existing `agentbus` project

## [0.2.2] - 2026-07-07

### Added

- `tests/factory_validate.py` — MCP integration validation (Agy event-56 criteria)
- `tests/test_factory_mcp_validation.py` — pytest `@integration` wrapper

### Fixed

- Integration validation: real TTL wait, 3-client concurrency hammer, MCP-only auth test

## [0.2.1] - 2026-07-07

### Fixed

- MCP auth: workspace token file now wins over stale `AGENTBUS_TOKEN` env in subprocesses
- `mcp-serve.sh` resolves `agentbus` from `$PATH` (pip install) before repo venv

### Changed

- PyPI package renamed to **`agentbus-mcp`** (avoids collision with unrelated `agentbus` NATS project)
- Docs updated to v0.2: lease tools, 40 tests, Hermes MCP example
- MCP integration tests for lock tools

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

[0.2.3]: https://github.com/onicarps/agentbus/releases/tag/v0.2.3
[0.2.2]: https://github.com/onicarps/agentbus/releases/tag/v0.2.2
[0.2.1]: https://github.com/onicarps/agentbus/releases/tag/v0.2.1
[0.2.0]: https://github.com/onicarps/agentbus/releases/tag/v0.2.0
[0.1.0]: https://github.com/onicarps/agentbus/releases/tag/v0.1.0