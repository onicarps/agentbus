# AgentBus Roadmap

**Current release:** v0.20.0 (August 2026)

**Next milestone:** v0.21 — isolated broker IPC and offline root custody

The v0.20 security contract is:

- [AgentID security model](docs/AGENTID.md)
- [AgentID trust-boundary ADR](docs/adr/2026-08-25-agentid-trust-boundaries.md)

## v0.20 AgentID

- [x] `ABUS-020-001`: independent threat model and trust-boundary ADR (Factory GREEN)
- [x] `ABUS-020-002`: canonical Ed25519/RFC 8785 envelope and cross-language fixtures
- [x] `ABUS-020-003`: signed policy/registry lifecycle, rotation, revocation, nonce and clock state
- [x] `ABUS-020-010`: protected store-backed wake rehydration and policy-bound ingress capability
- [x] `ABUS-020-004`: signer isolation and bounded child delegation
- [x] `ABUS-020-005`: mandatory verify-at-read and producer-scoped deduplication
- [x] `ABUS-020-006`: typed privileged actions and separation of duties
- [x] `ABUS-020-007`: complete SDK/doctor integration and honest strict readiness diagnostics
- [x] `ABUS-020-008`: released-fixture migration and staged rollout
- [x] `ABUS-020-009`: full 32-case adversarial suite plus exact July incident fixtures

## v0.21 Isolated Broker IPC

- [ ] `ABUS-021-001`: Python reference-monitor daemon, framed Unix socket, kernel peer credentials, and broker-only writes
- [ ] `ABUS-021-002`: Python, Go, and TypeScript broker client transports with strict closed behavior
- [ ] `ABUS-021-003`: offline root generation, detached policy ceremony, import, rotation, and revocation
- [ ] `ABUS-021-004`: active strict-readiness probes and expiring isolation receipts
- [ ] `ABUS-021-005`: distinct service users, systemd deployment, rollback drill, and live strict promotion

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
- [ ] Make same-tag release reruns safe and verifiably idempotent (`ABUS-019-002`; implementation awaiting independent QA)
- [ ] Add Linux, macOS and Windows test coverage plus Python, Go and TypeScript jobs
- [ ] Define the v0.19 compatibility and deprecation policy
- [ ] Add honest, documented `agentbus doctor` diagnostics (`ABUS-019-004`; implementation awaiting independent QA)
- [ ] Remove or classify repository debris and generated artifacts
- [x] Decide and document Go serve parity versus explicit feature deferral ([support boundary](docs/SUPPORT.md), `ABUS-019-010`)

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
