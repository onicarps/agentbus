# AgentBus support boundary

## v0.19 runtime responsibilities

The Python EventStore is authoritative for SQLite migrations, durable-consumer
state, RBAC, HITL, SLA and mcpsafe policy evaluation.

The Go binaries are operational helpers: worker daemon, filesystem wake router,
webhook delivery and an experimental stdio fast path. `agentbus-go-serve` does
not claim feature parity with the Python MCP server. TypeScript and Go clients
use MCP or CLI JSON; they do not write consumer-state tables directly.

## Platform tiers

| Tier | Platforms | Contract |
|------|-----------|----------|
| Tier 1 | Linux x86_64/arm64; macOS x86_64/arm64 | Release artifacts and CI matrix required |
| Tier 1 (conditional) | WSL2 with the workspace under native Linux storage such as `/home` | Same Linux contract; DrvFS excluded |
| Tier 3 / deprecated | Native Windows | Transitional wheel may be published, but no v0.19 correctness commitment |
| Unsupported | WSL DrvFS paths such as `/mnt/c` | Fails fast because SQLite WAL and wake notification guarantees do not hold |

Feature claims are scoped to the Python server unless a Go conformance test
explicitly proves parity. A packaged binary being discoverable is not parity.
