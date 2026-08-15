# AgentBus Roadmap

**Current release:** v0.18.0 (August 2026)

**Next proposed milestone:** v0.19 — Product Hardening and Durable Consumers

The detailed cross-machine implementation handoff is:

- [AgentBus v0.19 Product Plan and Swarm Handoff](docs/plans/2026-08-15-agentbus-v0.19-handoff.md)

## Shipped

- [x] SQLite-backed MCP event log, CLI, schemas, attachments, tracing and leases
- [x] RBAC, HITL intercepts, SLA expiry and dead-letter handling
- [x] Mission Control TUI and God View observability
- [x] Workspace-scoped `up`, `down`, `ps` and `logs` process orchestration
- [x] Jupyter async client and TypeScript client
- [x] Go serve/worker spike, wake plane and platform wheel packaging
- [x] Headless runner adapters, async suspend/resume and retry/spillover resilience
- [x] MCP Python SDK v2 migration in v0.18

## v0.19 proposed scope

### P0 — release and product foundation

- [ ] Align README, changelog and release documentation with v0.18+
- [ ] Make same-tag release reruns safe and verifiably idempotent
- [ ] Add Linux, macOS and Windows test coverage plus Python, Go and TypeScript jobs
- [ ] Define the v0.19 compatibility and deprecation policy
- [ ] Add `agentbus doctor` diagnostics
- [ ] Remove or classify repository debris and generated artifacts
- [ ] Decide and document Go serve parity versus explicit feature deferral

### P1 — durable consumers and contracts

- [ ] Publish the durable-consumer semantics and state-machine design
- [ ] Add named consumer groups, claim leases, ack/nack, retries and per-group DLQ
- [ ] Add replay, contiguous acknowledgement watermark and lag reporting
- [ ] Define a canonical versioned event envelope and schema evolution rules
- [ ] Generate or validate Python, TypeScript and Go contract types
- [ ] Add cross-language and failure-mode conformance tests
- [ ] Replace Swarm's duplicate minimal store with the official package
- [ ] Prove the Swarm to AgentBus to Telegram round trip with correlation

### P2 — operational depth

- [ ] OpenTelemetry export and operational metrics
- [ ] Backup, integrity check, export/import and replay tooling
- [ ] Per-topic retention, compaction and redaction hooks
- [ ] Better schema/RBAC/HITL rejection explanations in CLI and TUI

## Explicitly deferred

- Encrypted multi-machine bridge or hosted service
- Web dashboard
- Framework-specific adapters that do not come from a demonstrated use case
- Performance tuning before correctness baselines and conformance tests exist
- Stable v1 API declaration until consumer and envelope contracts are dogfooded

## Roadmap policy

Every roadmap item should have:

1. a real use case and owner;
2. a written contract or acceptance criterion;
3. failure-mode and migration coverage;
4. independent review;
5. durable outcomes in Git and release notes.
