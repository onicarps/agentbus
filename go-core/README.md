# AgentBus Go Core (Strangler Bus spike)

Optional high-performance control plane: SQLite event store + MCP stdio serve.

## Build

Requires Go 1.22+:

```bash
export PATH="$HOME/.local/go/bin:$PATH"   # if installed to user prefix
cd go-core
make tidy test build
# → bin/agentbus-go-serve
```

## Use from Python CLI

```bash
export AGENTBUS_WORKSPACE=/path/to/workspace
# optional: export AGENTBUS_GO_SERVE=$PWD/go-core/bin/agentbus-go-serve
agentbus serve --engine go
# or
agentbus mcp-serve --engine go
# or
AGENTBUS_ENGINE=go agentbus serve
```

## Scope (spike)

| Implemented | Not yet |
|-------------|---------|
| `Publish` / `Poll` / `Status` | HITL, SLA, RBAC, mcpsafe |
| Single-writer goroutine | Full MCP tool surface |
| Content-Length JSON-RPC stdio | Wiretap / God View |
| Idempotency key | Parity pytest suite |

Module: `github.com/onicarps/agentbus-go`
