# AgentBus Go Core (Strangler Bus spike)

Optional high-performance control plane: SQLite event store + MCP stdio serve.

## Build

Requires Go 1.22+:

```bash
export PATH="$HOME/.local/go/bin:$PATH"   # if installed to user prefix
cd go-core
make tidy test build
# → bin/agentbus-go-serve
# → bin/agentbus-go-worker
```

## MCP serve (`--engine go`)

```bash
export AGENTBUS_WORKSPACE=/path/to/workspace
# optional: export AGENTBUS_GO_SERVE=$PWD/go-core/bin/agentbus-go-serve
agentbus serve --engine go
```

## Wake worker (PRD v0.12 — non-LLM)

```bash
export AGENTBUS_GO_WORKER=$PWD/go-core/bin/agentbus-go-worker
agentbus worker init --to grok
agentbus worker once          # → .agentbus/WAKE.json
agentbus worker up            # long-running fsnotify+poll
agentbus worker sleep|wake|status
```

See [docs/WAKE.md](../docs/WAKE.md).

## Scope

| Implemented | Not yet |
|-------------|---------|
| EventStore Publish/Poll/Status | HITL, SLA, RBAC, mcpsafe on Go serve |
| Single-writer goroutine | Full MCP tool surface |
| **agentbus-go-worker** filter/cursor/WAKE/sleep/leases | MCP session notifications |
| Content-Length JSON-RPC stdio | Full pytest parity suite |

Module: `github.com/onicarps/agentbus-go`
