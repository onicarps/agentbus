# Changelog

## [Unreleased]

## [0.21.0] - 2026-08-31

### Added — isolated broker and offline-root security

- Add authenticated broker IPC transports for Python, Go, and TypeScript.
- Add offline-root ceremony, monotonic policy bundles, dynamic isolation probes,
  and multi-user deployment verification.
- Close CLI and MCP broker error boundaries with stable fail-closed responses.

### Added — v0.21 isolated broker foundations

- Add a Python Unix-domain reference-monitor broker with length-prefixed strict
  JSON requests, kernel `SO_PEERCRED` authentication, symbolic principal
  binding, broker-side AgentID verification, and no raw-SQL operation.
- Add offline root generation, public-only workspace initialization, peer-key
  enrollment requests, detached policy signing/import, digest-chained rotation,
  and revocation without an online workspace root key.
- Fix concurrent signed replay handling so an identical envelope cannot be
  returned as a successful content duplicate after racing the nonce precheck.

## [0.20.2] - 2026-08-30

### Fixed

- Align the Python runtime version with release metadata and make the packaging
  regression test compare those two authoritative values without a hard-coded
  release number.
- Prevent concurrent signed-envelope content deduplication from bypassing
  nonce-replay rejection, and stress the invariant across repeated concurrent
  submissions.

## [0.20.1] - 2026-08-30

### Fixed

- Contain RBAC rejection of generated runner acknowledgement text: retain the
  complete adapter output in the private durable run record, publish a
  schema-controlled fallback acknowledgement, and keep the long-running loop
  alive after an unexpected iteration error.
- Treat an explicit `resolve_workspace(path)` argument as authoritative instead
  of redirecting it to a Git or AgentBus ancestor (`ABUS-ECO-002`).

### Security

- Bound Go framed messages to 4 MiB, add fuzz targets for canonical JSON,
  Ed25519 verification, and framed-message parsing, and run Go tests with the
  race detector in CI.

## [0.20.0] - 2026-08-25

### Added — AgentID Phase 1

- Add root-signed workspace identity policy and public-key registry with Ed25519 enrollment, rotation, immediate revocation, monotonic policy/registry high-water checks, and runtime-specific wake capabilities.
- Add RFC 8785 signed event envelopes that bind the stored payload and ordered artifact digests, plus nonce and signer-clock replay state.
- Add byte-identical Python, TypeScript, and Go canonicalization/signature fixtures with explicit rejection of duplicate keys, non-NFC text, non-finite numbers, lone surrogates, and unsafe numeric integers.
- Rehydrate protected wake notifications from the event store and recompute AgentID verification before runner execution; reject synthetic or tampered event IDs and caller-controlled wake bodies.
- Add `agentbus identity` lifecycle and verification commands and an honest audit/shared-UID security guide.

### Added — AgentID Phase 2

- Add bounded `parent/subagent/<id>` delegation with one-hour maximum TTL,
  non-transitive scope, parent-signed proof, and hard denial of QA, GO,
  identity-admin, merge, push, and release authority.
- Scrub AgentBus signing/broker/token state, SSH/GPG agent sockets, and inherited
  file descriptors from headless adapter children.
- Authenticate publish envelopes before RBAC and deduplication, reject exact
  nonce replay, and scope idempotency to the verified producer.
- Recompute signatures and artifact digests across poll/await, trace, HITL, SLA,
  TUI/monitor, runner, and log-projection reads; protected consumers fail closed
  on tampered restricted-topic rows.
- Require CLI publishers to supply an explicit matching identity private-key
  handle instead of selecting credentials from caller-controlled `producer_id`.

### Added — AgentID Phase 3

- Add signed typed actions for QA verdict, Agy GO, merge, push, release, and
  identity administration, with capability checks plus root-signed
  producer/action separation-of-duties policy.
- Add typed CLI/MCP publish and durable-wait fields so privileged automation no
  longer derives authority from summary substrings; runner ACK remains
  operations-only.
- Make protected/strict mode ignore legacy auth/RBAC disable flags, token-role
  elevation, and droid-proof-only QA authority; bind MCP publishing to its
  supervised producer identity.
- Converge Go event-store migrations with AgentID columns, scoped idempotency,
  and identity replay tables, and require the verifying broker for restricted
  Go publish/poll when AgentID is configured.
- Extend TypeScript typed-action/JCS validation and `agentbus doctor` identity,
  rollback, credential-scrub, reference-monitor, and strict-readiness checks.

### Added — AgentID Phase 4

- Add released v0.16.4, v0.18.0, and v0.19.0 database-lineage fixtures proving
  additive schema migration, byte-stable history, and permanent
  `legacy_unverified` labeling for unsigned events.
- Add signed reference-monitor configuration, monotonic
  audit-to-protected-to-strict promotion, and root-signed prospective recovery
  records that cannot retroactively attest historical rows.
- Add an executable N1-N32 adversarial evidence matrix and exact July 2026
  certification-substitution plus Grok/Hermes peer-impersonation fixtures.
- Reject unpaired escaped UTF-16 surrogates at the Go canonicalization boundary
  before JSON decoding can replace and erase the invalid scalar.

## [0.19.0] - 2026-08-23

### Added

- Add honest `agentbus doctor` diagnostics for workspace resolution, SQLite
  integrity/schema/runtime settings, RBAC and registered schemas, packaged Go
  helper architecture, stale process state, disk capacity and redacted runtime
  versions. Publish/poll and MCP stdio probes use disposable workspaces and do
  not mutate the target bus.
- Add registered Codex and Pi headless adapters using their real CLI surfaces.
  Codex receives prompts on stdin via `codex exec`; Pi defaults to
  `--no-approve --no-extensions`. Runner config rejects unknown adapter types,
  and runtime construction failures enter the poison-pill completion path.
- Fence event summaries and payloads as untrusted serialized data so they cannot
  imitate the authoritative runner identity and policy sections.

### Changed

- Make PyPI tag reruns fail closed: existing filenames must match the candidate
  SHA-256 hashes, exact releases become no-ops, and matching partial uploads may
  resume with `skip-existing` only after validation.
- Apply the same fail-closed policy to npm using packed tarball SRI integrity,
  and remove the platform publication path that previously ignored all errors.
- Constrain Hatchling below 1.30 so release artifacts use Core Metadata 2.4,
  which remains compatible with the pinned PyPI publishing action.
- Refresh npm transitive locks within existing semver ranges to clear the
  release candidate's dependency audit findings.

### Documentation

- Add the proposed v0.19 product-hardening and durable-consumer roadmap with a
  cross-machine Swarm continuation handoff.
- Align README and roadmap entry points with the v0.19.0 release.

## [0.18.0] - 2026-08-10

### Changed — MCP Python SDK v2 migration

- Migrate the high-level server from the removed v1 `FastMCP` API to the v2
  `MCPServer` API.
- Require `mcp>=2,<3` and move the development version to `0.18.0`.
- Keep the existing stdio transport and low-level `ClientSession` integration;
  both remain supported by MCP SDK v2.

## [0.16.4] - 2026-08-09

### Fixed — MCP Python SDK 2.0 compatibility guard

- Constrain the Python MCP dependency to `mcp>=1.29,<2`. AgentBus still uses
  the MCP Python SDK v1 `FastMCP` API; fresh installs must not resolve the
  incompatible v2 SDK until the separate MCP `2026-07-28` migration lands.
- Add a packaging regression test for the dependency contract and bound the
  MCP stdio round-trip test with an explicit timeout.
- Align the packaged default RBAC producer map and proof tests with the active
  `factory_droid` identity; Hermes remains the legacy bridge role.
- This patch stays on the canonical post-rollback 0.16.x source lineage and
  does not restore the removed 0.17 ops package.

### Added — resilient messaging retry (Agy GO #1705 / #1698)

- **`agentbus.retry`:** exponential backoff + full jitter `RetryPolicy`, `call_with_retry`, SQLite lock classifier; env overrides `AGENTBUS_PUBLISH_*` / `AGENTBUS_DELIVERY_*`
- **`agentbus.resilience`:** `okf/dead-letter` reason **`RETRY_EXHAUSTED`**, file spillover DLQ at `.agentbus/dead-letter/spillover.jsonl`, `publish_or_spill` / `escalate_retry_exhausted`
- **`EventStore.publish`:** retries transient `database is locked` / busy with backoff+jitter (rollback-safe; no re-INSERT after commit)
- **Runner ACK path:** `publish_or_spill` so exhausted ACK publish marks done + spillover instead of crashing or hammering SQLite
- **Go webhook:** full-jitter backoff; on final failure publish `okf/dead-letter` + spillover JSONL
- **Structural ACK-storm breaker:** inbound `RUNNER_ACK`/`RUNNER_ERROR`/… prefixes skip LLM + re-ACK (`ops_noise`); Hermes bridge standing orders locked in prompt
- **Tests:** `tests/test_retry.py`, `tests/test_resilience.py`, busy-wait ops_noise gates

## [0.16.3] - 2026-07-18

### Fixed — monitor open-path prune write (follow-up Agy #601)

- **`EventStore(..., auto_prune=False)`:** monitor/TUI snapshots skip open-time `prune_expired()` DELETE so the 1s refresh never takes a write lock.
- Entire `refresh_data` body wrapped in try/except (including header/dark-bar updates) so unmount races cannot crash Textual.
- **Idempotent migrations:** concurrent `EventStore` opens tolerate `duplicate column` races during ADD COLUMN.
- Tests assert `prune_expired` is not called on monitor fetch.

## [0.16.2] - 2026-07-18

### Fixed — monitor TUI crash under event storm (Agy #601)

- **Root cause:** `fetch_monitor_state` called `expire_pending` / `expire_sla_breaches` / `review_pending` on every 1s Textual refresh. Under concurrent publish load (companion-ACK storm #543–#590) SQLite raised `OperationalError: database is locked`; unhandled exception in `set_interval` **crashed the God View TUI**.
- **Read-only snapshot:** monitor fetch uses pure SELECTs only; expiry remains on poll/publish/status paths.
- **Crash-proof refresh:** `refresh_data` catches errors, shows a red banner, retries next tick.
- **Skip no-op rebuilds:** fingerprint max/min event id + pending set so unchanged data does not `DataTable.clear()` thrash.
- **Markup safety:** escape dark-agent / payload / trace text so brackets in summaries cannot break Rich render.
- **Row key guards:** invalid/stale keys during clear no longer raise in highlight/select handlers.
- **Tests:** `tests/test_tui.py` — read-only fetch, storm volume, concurrent writers, refresh error swallow (CI-safe without textual).

## [0.16.1] - 2026-07-18

### Fixed — busy-wait / companion-ACK circuit breaker (Agy #597)

- **`TurnResult.suppress_ack`:** adapters may signal the outer runner to skip companion handoff publish
- **CLI markers:** `CHAIN_BREAK`, `TERMINAL_IDLE`, or `NO-OP` in CLI stdout/stderr set `suppress_ack=True` via `turn_result_from_cli_exit` (suspend/exit-75 path still publishes `RUNNER_SUSPEND`)
- **`process_envelope`:** when `suppress_ack`, still writes run log, records chain budget, marks wake done — **does not** publish `okf/handoff` RUNNER_ACK/ERROR (stops factory↔grok ping-pong after idle/spurious wakes)
- **Return dict:** `circuit_break: true|false` for runner logs
- **Tests:** `tests/test_busy_wait_breaker.py`

## [0.16.0] - 2026-07-17

### Added — async suspend / await MVP (Agy #467)

### Added

- **`agentbus await`:** cooperative wait CLI — writes `.agentbus/runs/<event_id>/await.json`, exits **75** (EX_TEMPFAIL); requires primary predicate (`--expect-from` and/or `--causation-id`); default timeout **4h** (max 24h)
- **WaitStore** (`.agentbus/waits/<wait_id>.json`): durable `WaitRegistration` + cursor; corrupt files skipped
- **Wait tick:** predicate match + mandatory timeout → synthetic **RESUME** wake (`payload.resume` locked keys); `causation_id` = stored `chain_key` (budget continuity); idempotency `resume:{wait_id}:{fulfilled_by}`
- **Runner:** `TurnResult.status` ∈ `{ok,error,suspended}`; `RUNNER_SUSPEND:` ACK with `suspend-ack:{runner_id}:{event_id}`; exit 75 / await drop → suspended (not ERROR)
- **Timeout path:** `okf/dead-letter` reason `WAIT_TIMEOUT` + resume `status=timeout`; late fulfill after terminal is no-op
- **Adapters:** `AGENTBUS_WAKE_EVENT_ID` / `AGENTBUS_CHAIN_KEY` env; exit 75 → suspended; prompts document await
- **Factory adapter:** `skip_permissions` (default True) → `droid exec --skip-permissions-unsafe`; omits `--auto` (CLI mutual exclusion). Fixes headless QA RUNNER_ERROR "insufficient permission… Re-run with --skip-permissions-unsafe"
- **Tests:** `tests/test_async_suspend.py` hard gates (budget chain, single-wake, timeout, lost-wakeup, self-fulfill guard)

### Schema locks

- Resume keys: `wait_id`, `chain_key`, `origin_event_id`, `fulfilled_by`, `status` (`ok`|`timeout`), `reason`
- Dead-letter reason enum includes `WAIT_TIMEOUT`

## [0.15.0] - 2026-07-17

### Added — headless reason-plane runner (Phases B–F)

- **`agentbus run --config runner.<id>.yaml [--once]`:** headless TurnAdapter loop with dual intake (bus poll + wake file), budget, and RUNNER_ACK/ERROR publish
- **Adapters:** Hermes, Factory (droid + auto high), Grok, Agy, Aider (SRE), Echo — isolated CLI oneshot processes (no interactive TUI mutation)
- **CI:** PATH binary preflight skipped when adapters receive injected `run_fn` (mocked unit tests green without vendor CLIs on PATH)
- **Runner package:** `agentbus.runner` (config, loop, intake, budget, types, adapters)
- **Example configs:** `examples/runner.{hermes,factory,grok,agy}.yaml`
- **Swarm composition (Phase F):** `enabled: false` on services; `agentbus up` skips disabled runners and reports `skipped[]`; example swarm documents opt-in headless services
- **Docs:** `docs/WAKE.md` headless runner plane

### Product decisions

- Headless runners are parallel processes; interactive sessions remain HITL
- Runners off by default in swarm; dogfood flips one bit per agent

## [0.14.0] - 2026-07-16

### Added — monitor from/to columns (Agy #210)

- TUI event stream: dedicated **from** and **to** columns extracted from payload JSON
- HITL pending table: **to** column added
- Plain CLI monitor lines: fixed-width from/to columns
- `format_event_row` robust fallbacks for system events

All notable changes to this project are documented here.

## [0.13.0] - 2026-07-16

### v0.13 webhook bridge (WEBHOOK_SPEC_GO #98 / Agy GO #188)

### Added

- **Workspace hard-ban (DrvFS):** reject WSL `/mnt/<drive>` at EventStore / CLI / Go store+worker (`AGENTBUS_ALLOW_DRVFS=1` break-glass)
- **`agentbus wake-ingress`:** Mode A localhost queue (Hermes :18787 / Factory :18788); dedupe SQLite; `GET /health`; token optional + loud warning
- **Webhook sender D1:** `Idempotency-Key`, `X-AgentBus-Token` / Bearer, 3× retry, dual-signal file+HTTP, `webhook_success_total` / `webhook_fail_total` in worker status
- **Session-bridge scripts:** `wake_notify.sh`, `drain_wake_queue.sh`, `grok_wake_notify.sh`
- **Docs / process:** `docs/WAKE.md` three-plane; `runbooks/swarm-session.md`; SDLC handoffs; PRD v0.13 dual-signal

### Changed

- Swarm: per-agent workers + `hermes-wake-ingress`; factory services opt-in
- Poll loop: paced `time.Sleep` channel (defensive)

### Product decisions reflected

- MVP mailbox + optional bridge; webhook for Hermes/Factory first; `causation_id` acks; Factory opt-in

## [0.12.1] - 2026-07-13

### Fixed — fat wheels actually embed Go binaries

- Hatch wheel build sets `ignore-vcs = true` so CI-injected `agentbus/bin/<plat>/*` are not stripped by `.gitignore`
- 0.12.0 wheels on PyPI were ~70KB pure-Python; 0.12.1 rebuilds platform wheels with full binaries (~5–6MB)

### Notes

- npm `@agentbus/go-worker-*` first publish still needs npm Trusted Publisher / package create for each new name (E404 on 0.12.0)

## [0.12.0] - 2026-07-13

### Added — Wake plane + fat Go binaries

- **agentbus-go-worker** wake plane (non-LLM): filter, fsnotify, WAKE.json, sleep/wake, role leases
- **agentbus worker** CLI wraps Go binary
- **Platform wheels** (Ruff-style): cross-compile matrix embeds `agentbus-go-worker` + `agentbus-go-serve` per platform
- **npm optionalDependencies** (esbuild-style): `@agentbus/go-worker-<plat>` packages — no runtime downloads
- Docs: `docs/WAKE.md`, release packaging notes

### Fixed

- Factory-droid CR: poison-pill cursor advance, role-scoped leases, success lease TTL tombstone

## [0.11.3] - 2026-07-12

### Added — Release hygiene + mcpsafe

- **mcpsafe middleware:** `PolicyEnforcer` from `.mcpsafe.lock`; `--enable-mcpsafe` / `AGENTBUS_ENABLE_MCPSAFE` on `serve`, `mcp-serve`, and CLI `publish`; tool + payload gates (`tool` / `tool_name` / `mcp_tool`)
- **npm package:** `@agentbus/agentbus-client` publish-ready (dist-only, provenance); OIDC `publish-npm` job on `v*` tags
- **Release docs:** `docs/RELEASE.md` Trusted Publisher field tables for PyPI + npm
- **Swarm listen:** `scripts/bus_listen_agy.py` cursor-based Agy→Grok handoff poller

### Fixed

- Auth before mcpsafe on MCP publish; RBAC before mcpsafe in store
- Release workflow: SHA-pinned npm job actions; pinned `npm@11.18.0`

## [0.11.2] - 2026-07-11

### Fixed / Improved — CR hardening pass

- SQLite: `AGENTBUS_SQLITE_JOURNAL` / `AGENTBUS_SQLITE_BUSY_TIMEOUT` overrides; `status` exposes live `sqlite_journal_mode` + `sqlite_busy_timeout_ms`
- TUI: init `_cached_events` for stream highlight → trace sync (ownership of hover path)
- Release workflow: optional OIDC PyPI publish job (Trusted Publisher)

## [0.11.1] - 2026-07-11

### Fixed / Added — Phase 2 ops DX

- **Windows SQLite locking:** on `os.name == "nt"`, use `PRAGMA journal_mode=MEMORY` + `busy_timeout=10000` (POSIX keeps WAL + 5000).
- **CLI `--quiet` / `-q`:** global flag forces root logger to `CRITICAL` on stderr so MCP/CI stdout stays clean.

## [0.11.0] - 2026-07-11

### Added — Phase 1 DX expansion

- **Jupyter / IPython** (`agentbus.jupyter`):
  - `AsyncAgentBus` — non-blocking poll loop for notebook event loops (`asyncio.sleep` yield, optional inject `poll_fn`, EventStore default)
  - `%load_ext agentbus.jupyter` + `%agentbus start|stop|status` line magics
  - Optional extra: `pip install 'okf-agentbus[jupyter]'` (IPython); `ipython` also in `[dev]` for CI
- **TypeScript client** (repo package, not npm-published yet):
  - `packages/js/agentbus-client` (`@agentbus/agentbus-client`) — EventEmitter + `fs.watch` + fallback poll
  - `createStdioMcpClient` auto-spawns `agentbus mcp-serve`; multi-topic cursors; per-topic emit safety

### Docs / process

- Implementation plans under `docs/superpowers/plans/` (TS SDK, Jupyter)
- Swarm gate: CodeRabbit + CI (Hermes QA path retired; Hermes → DevOps)

## [0.10.2] - 2026-07-10

### Changed

- Mission Control TUI (`agentbus monitor`): event stream, HITL, and Wiretap panes render **newest-first** (descending `event_id`).

### Fixed

- `agentbus up`: prepend active venv `bin` to service `PATH` so swarm children resolve the same `agentbus` install.
- `scripts/mcp-serve.sh`: prefer repo `.venv/bin/agentbus` over a stale PATH install.

## [0.10.1] - 2026-07-09

### Fixed

- **Critical:** `agentbus up` no longer steals SIGINT from Textual (Ctrl+C hang).
  Custom signal handlers removed; `finally: stop_all()` still tears down children.

## [0.10.0] - 2026-07-09

### Added — Orchestration DX

- `agentbus up` / `down` / `ps` / `logs` powered by `.agentbus/swarm.yaml`
- Cross-OS process groups: POSIX `start_new_session` + `killpg`; Windows `CREATE_NEW_PROCESS_GROUP` + CTRL_BREAK / taskkill
- Foreground `up` runs `agentbus monitor` then tears down children on exit
- `--detach` (`-d`) for background-only swarms; `up --init` writes example yaml
- Per-service logs under `.agentbus/logs/<name>.stdout.log`
- Example `examples/swarm.yaml`

## [0.9.1] - 2026-07-09

### Fixed

- **Critical:** `agentbus watch` infinite `system/fs` feedback loop (esp. Windows)
  - Case-insensitive ignore for `.agentbus` / `.AGENTBUS`
  - Windows path separators normalized before segment matching
  - Ignore `log.md`, `*.log`, SQLite db/journal/wal/shm, and other bus artifacts
  - Prevents publish→events.db / project-log→log.md re-entry storms

## [0.9.0] - 2026-07-09

### Added — God View Observability Mesh

- Built-in `system/mcp`, `system/fs`, `system/shell`, `system/monologue` topic schemas
- RBAC `observer` role + system producers (`wiretap`, `os-watcher`, `swarm-tail`)
- `agentbus mcp-serve --wiretap` / `--wiretap-log` — in-process tools/call instrumentation with secret redaction
- `agentbus watch` — OS filesystem + process daemon (`pip install 'okf-agentbus[obs]'`)
- `agentbus tail` — multiplex agent monologue logs; optional `--publish` to `system/monologue`
- TUI Wiretap pane + Dark Agent warnings in `agentbus monitor`
- Example `examples/08_god_view.py`

### Notes

- God View is **opt-in** (wiretap flag / `[obs]` extra / monologue `--publish`)
- 100% local — no cloud telemetry

## [0.8.4] - 2026-07-09

### Fixed

- Power Ranking follow-ups: RBAC roles on `init --apply`, `agentbus sla list|clear`, persisted `retention_days`

## [0.8.3] - 2026-07-08

### Added

- Built-in `okf/approval` topic schema (`event_id`, `approver`, `decision`) — Power Ranking AB-B06c

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
