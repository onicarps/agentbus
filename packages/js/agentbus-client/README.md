# @agentbus/agentbus-client

TypeScript / Node.js EventEmitter client for [AgentBus](https://github.com/onicarps/agentbus).

- Resolves `events.db` from `AGENTBUS_WORKSPACE`
- Debounced `fs.watch` + slow fallback timer to trigger polls
- Spawns `agentbus mcp-serve` over stdio (or accepts an injected MCP client)
- `publish` / `poll` map to `agentbus_publish` / `agentbus_poll`
- Emits `event` and per-topic events (`bus.on('okf/handoff', ...)`)

## Prerequisites

1. Python AgentBus installed and on `PATH` (or set `AGENTBUS_BIN`):

   ```bash
   pip install 'okf-agentbus[devex]'
   # or use a repo venv:
   export AGENTBUS_BIN=/path/to/agentbus/.venv/bin/agentbus
   ```

2. Workspace initialized:

   ```bash
   export AGENTBUS_WORKSPACE=/path/to/project
   agentbus token ensure --workspace "$AGENTBUS_WORKSPACE"
   ```

## Install

```bash
cd packages/js/agentbus-client
npm install
npm test
```

## Usage

### Auto stdio (recommended)

```ts
import { AgentBus } from "@agentbus/agentbus-client";

const bus = new AgentBus({
  workspace: process.env.AGENTBUS_WORKSPACE,
  topics: ["okf/handoff"], // or ["*"] to expand via agentbus_status
});

bus.on("okf/handoff", (payload, meta) => {
  console.log("handoff", payload, meta.event_id);
});

await bus.connect(); // spawns `agentbus mcp-serve`
await bus.publish("okf/handoff", {
  from: "node-agent",
  to: "swarm",
  summary: "hello from TS",
});

// later
await bus.disconnect();
```

### Injected MCP client (tests / custom transports)

```ts
import { AgentBus, createStdioMcpClient } from "@agentbus/agentbus-client";

const mcp = await createStdioMcpClient({
  workspace: process.env.AGENTBUS_WORKSPACE!,
  command: process.env.AGENTBUS_BIN, // optional
});

const bus = new AgentBus({ mcp, workspace: process.env.AGENTBUS_WORKSPACE });
await bus.connect();
```

Set `AGENTBUS_WORKSPACE` to the project root that contains `.agentbus/events.db`.
