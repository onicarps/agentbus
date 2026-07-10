#!/usr/bin/env bash
# Start AgentBus MCP server with workspace token injected into env.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORKSPACE="${AGENTBUS_WORKSPACE:-$(pwd)}"

# Prefer PATH (pip install), then repo venv, then explicit override.
if [[ -n "${AGENTBUS_BIN:-}" ]]; then
  :
elif [[ -x "$ROOT/.venv/bin/agentbus" ]]; then
  AGENTBUS_BIN="$ROOT/.venv/bin/agentbus"
elif command -v agentbus >/dev/null 2>&1; then
  AGENTBUS_BIN="$(command -v agentbus)"
else
  echo "agentbus: not found — pip install agentbus-mcp or set AGENTBUS_BIN" >&2
  exit 1
fi

"$AGENTBUS_BIN" token ensure --workspace "$WORKSPACE" --quiet >/dev/null
export AGENTBUS_TOKEN="$("$AGENTBUS_BIN" token show --workspace "$WORKSPACE" --quiet)"

exec "$AGENTBUS_BIN" serve --workspace "$WORKSPACE" "$@"