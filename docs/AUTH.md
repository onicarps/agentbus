# Authentication

AgentBus v0.1 uses **workspace-scoped ephemeral tokens** to prevent unauthorized local processes from publishing to your event log.

## How it works

1. `agentbus serve` calls `ensure_ephemeral_token(workspace)`
2. Token is written to `{workspace}/.agentbus/token` with file mode **0600**
3. `agentbus_publish` (MCP or CLI) must present the same token
4. `agentbus_poll` and `agentbus_status` do **not** require a token

## Providing the token

| Channel | Mechanism |
|---------|-----------|
| MCP (recommended) | `scripts/mcp-serve.sh` exports `AGENTBUS_TOKEN` before starting serve |
| MCP tool arg | `auth_token` parameter on `agentbus_publish` |
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

v0.1 assumes a **single-user localhost workspace**. The token file gates who can publish; it is not a multi-tenant identity system. Do not expose AgentBus over a network without additional hardening.