#!/usr/bin/env bash
# Start AgentBus MCP server with workspace token injected into env.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
AGENTBUS_BIN="${AGENTBUS_BIN:-$ROOT/.venv/bin/agentbus}"
WORKSPACE="${AGENTBUS_WORKSPACE:-$(pwd)}"

"$AGENTBUS_BIN" token ensure --workspace "$WORKSPACE" --quiet >/dev/null
export AGENTBUS_TOKEN="$("$AGENTBUS_BIN" token show --workspace "$WORKSPACE" --quiet)"

exec "$AGENTBUS_BIN" serve --workspace "$WORKSPACE" "$@"