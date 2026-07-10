/** Minimal MCP tool surface used by AgentBus (stdio or mock). */
export type McpToolClient = {
  callTool(
    name: string,
    args: Record<string, unknown>,
  ): Promise<unknown>;
  close?(): Promise<void> | void;
};

export type BusEvent = {
  event_id: number;
  topic: string;
  producer_id?: string;
  timestamp?: string;
  status?: string;
  payload: unknown;
};

export type AgentBusOptions = {
  /** Override workspace (defaults to AGENTBUS_WORKSPACE). */
  workspace?: string;
  /** Fallback poll interval when fs.watch is quiet (ms). */
  fallbackMs?: number;
  /**
   * Injected MCP tool caller for tests / custom transports.
   * If omitted, `connect()` spawns stdio via `createStdioMcpClient`.
   */
  mcp?: McpToolClient;
  /** Initial poll cursor (exclusive). */
  sinceId?: number;
  /**
   * Default producer id for publish (also set as AGENTBUS_PRODUCER_ID on stdio).
   * Falls back to process.env.AGENTBUS_PRODUCER_ID.
   */
  producerId?: string;
  /**
   * Topics to poll each cycle.
   * Default: `["okf/handoff"]`.
   * Pass `["*"]` to expand via `agentbus_status` topics list each poll.
   */
  topics?: string[];
  /** Options for auto stdio spawn when `mcp` is not injected. */
  stdio?: {
    command?: string;
    args?: string[];
    cwd?: string;
    env?: Record<string, string>;
  };
};
