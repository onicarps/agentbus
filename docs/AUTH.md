# Authentication

AgentBus v0.2 uses **workspace-scoped ephemeral tokens** to prevent unauthorized local processes from publishing events or mutating leases.

## How it works

1. `agentbus serve` calls `ensure_ephemeral_token(workspace)`
2. Token is written to `{workspace}/.agentbus/token` with file mode **0600**
3. `agentbus_publish` and `agentbus_lock_*` (except `lock_status`) must present the same token
4. `agentbus_poll`, `agentbus_status`, and `agentbus_lock_status` do **not** require a token

## Token resolution order

When validating publish/lock operations:

1. `auth_token` tool argument (if provided)
2. Workspace token file (`{workspace}/.agentbus/token`)
3. `AGENTBUS_TOKEN` environment variable

The workspace file **wins over** a stale `AGENTBUS_TOKEN` in MCP subprocesses. This is why `scripts/mcp-serve.sh` reads the file and exports a fresh value on each start.

## Providing the token

| Channel | Mechanism |
|---------|-----------|
| MCP (recommended) | `scripts/mcp-serve.sh` exports `AGENTBUS_TOKEN` from workspace file |
| MCP tool arg | `auth_token` parameter (optional when wrapper injects env) |
| CLI | Auto-reads workspace token file, or `--token`, or `AGENTBUS_TOKEN` env |
| Legacy | `AGENTBUS_EXPECTED_TOKEN` env (fallback when no token file) |

## Disabling auth

For local development only:

```bash
export AGENTBUS_AUTH=off
```

## Rotating tokens

```bash
agentbus serve --rotate-token
# or
agentbus token rotate --workspace /path/to/workspace
```

All MCP clients must restart to pick up the new token.

## Threat model

v0.2 assumes a **single-user localhost workspace**. The token file gates who can publish and acquire locks; it is not a multi-tenant identity system. Do not expose AgentBus over a network without additional hardening.