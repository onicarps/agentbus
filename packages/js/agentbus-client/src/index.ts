import { EventEmitter } from "events";
import { DatabaseWatcher } from "./watcher";
import { getDatabasePath } from "./locator";
import { createStdioMcpClient } from "./stdio";
import type { AgentBusOptions, BusEvent, McpToolClient } from "./types";

export { getDatabasePath } from "./locator";
export { DatabaseWatcher } from "./watcher";
export { createStdioMcpClient } from "./stdio";
export type {
  AgentBusOptions,
  BusEvent,
  McpToolClient,
} from "./types";
export type { StdioMcpOptions } from "./stdio";

/**
 * Idiomatic Node client: EventEmitter + publish/poll over AgentBus MCP tools,
 * with fs.watch-triggered polls and a slow timer fallback.
 *
 * When `options.mcp` is omitted, `connect()` spawns `agentbus mcp-serve` over
 * stdio (requires `agentbus` on PATH or `AGENTBUS_BIN`).
 */
export class AgentBus extends EventEmitter {
  private watcher: DatabaseWatcher | null = null;
  private options: AgentBusOptions;
  private lastId: number;
  private connected = false;
  private polling = false;
  private ownsMcp = false;
  private mcp: McpToolClient | undefined;

  constructor(options: AgentBusOptions = {}) {
    super();
    this.options = options;
    this.lastId = options.sinceId ?? 0;
    this.mcp = options.mcp;
  }

  async connect(): Promise<void> {
    if (this.connected) return;

    if (!this.mcp) {
      const producerId =
        this.options.producerId ?? process.env.AGENTBUS_PRODUCER_ID;
      const env: Record<string, string> = {
        ...(this.options.stdio?.env ?? {}),
      };
      if (producerId) {
        env.AGENTBUS_PRODUCER_ID = producerId;
      }
      this.mcp = await createStdioMcpClient({
        workspace: this.options.workspace,
        command: this.options.stdio?.command,
        args: this.options.stdio?.args,
        cwd: this.options.stdio?.cwd,
        env,
      });
      this.ownsMcp = true;
    }

    const dbPath = getDatabasePath(this.options.workspace);
    this.watcher = new DatabaseWatcher(
      dbPath,
      () => {
        void this.poll().catch((err: unknown) => {
          this.emit("error", err);
        });
      },
      this.options.fallbackMs ?? 5000,
    );
    this.watcher.start();
    this.connected = true;
    // Initial catch-up poll
    await this.poll();
  }

  async publish(
    topic: string,
    payload: unknown,
    opts?: { producerId?: string },
  ): Promise<unknown> {
    const mcp = this.requireMcp();
    // Server expects payload as object (dict), not a JSON string.
    const body =
      typeof payload === "string"
        ? safeParseJson(payload) ?? { text: payload }
        : payload;
    const producerId =
      opts?.producerId ??
      this.options.producerId ??
      process.env.AGENTBUS_PRODUCER_ID;
    const args: Record<string, unknown> = {
      topic,
      payload: body,
    };
    if (producerId) {
      args.producer_id = producerId;
    }
    const result = await mcp.callTool("agentbus_publish", args);
    return unwrapToolResult(result);
  }

  /** Force a poll (also used by watcher). */
  async poll(): Promise<BusEvent[]> {
    if (this.polling) return [];
    this.polling = true;
    try {
      const mcp = this.mcp;
      if (!mcp) {
        // Without MCP, watcher still runs; no events to emit.
        return [];
      }

      const topics = await this.resolveTopics(mcp);
      const since = this.lastId;
      const merged: BusEvent[] = [];
      const seen = new Set<number>();

      for (const topic of topics) {
        const raw = await mcp.callTool("agentbus_poll", {
          topic,
          since_id: since,
          limit: 50,
        });
        for (const ev of normalizePollResult(raw)) {
          if (seen.has(ev.event_id)) continue;
          seen.add(ev.event_id);
          merged.push(ev);
        }
      }

      merged.sort((a, b) => a.event_id - b.event_id);

      for (const ev of merged) {
        if (ev.event_id > this.lastId) this.lastId = ev.event_id;
        this.emit("event", ev);
        this.emit(ev.topic, ev.payload, ev);
      }
      return merged;
    } finally {
      this.polling = false;
    }
  }

  get cursor(): number {
    return this.lastId;
  }

  async disconnect(): Promise<void> {
    if (this.watcher) {
      this.watcher.stop();
      this.watcher = null;
    }
    this.connected = false;
    // Only close MCP clients we spawned; injected clients are caller-owned.
    if (this.ownsMcp && this.mcp?.close) {
      await this.mcp.close();
      this.mcp = undefined;
      this.ownsMcp = false;
    }
  }

  private requireMcp(): McpToolClient {
    if (!this.mcp) {
      throw new Error(
        "AgentBus.publish requires connect() first (or pass options.mcp)",
      );
    }
    return this.mcp;
  }

  private async resolveTopics(mcp: McpToolClient): Promise<string[]> {
    const configured = this.options.topics ?? ["okf/handoff"];
    if (!configured.includes("*")) {
      return configured;
    }
    const raw = await mcp.callTool("agentbus_status", {});
    const status = unwrapToolResult(raw) as { topics?: string[] } | null;
    const listed = Array.isArray(status?.topics) ? status!.topics! : [];
    // Keep any explicit topics alongside *, drop the star marker.
    const explicit = configured.filter((t) => t !== "*");
    const set = new Set([...explicit, ...listed]);
    return set.size > 0 ? [...set] : ["okf/handoff"];
  }
}

function safeParseJson(text: string): unknown {
  try {
    return JSON.parse(text);
  } catch {
    return null;
  }
}

/** Pull JSON out of MCP CallToolResult envelopes or raw values. */
export function unwrapToolResult(raw: unknown): unknown {
  if (raw == null) return null;
  if (typeof raw === "string") {
    return safeParseJson(raw) ?? raw;
  }
  if (typeof raw === "object") {
    const obj = raw as Record<string, unknown>;
    if (Array.isArray(obj.content)) {
      for (const part of obj.content as Array<Record<string, unknown>>) {
        if (part.type === "text" && typeof part.text === "string") {
          return safeParseJson(part.text) ?? part.text;
        }
      }
    }
  }
  return raw;
}

export function normalizePollResult(raw: unknown): BusEvent[] {
  const data = unwrapToolResult(raw);
  if (data == null) return [];
  if (Array.isArray(data)) return data as BusEvent[];
  if (typeof data === "object") {
    const obj = data as Record<string, unknown>;
    if (Array.isArray(obj.events)) {
      return (obj.events as BusEvent[]).map(normalizeEvent);
    }
  }
  return [];
}

function normalizeEvent(ev: BusEvent): BusEvent {
  // Payload may still be a JSON string from older projections.
  if (typeof ev.payload === "string") {
    const parsed = safeParseJson(ev.payload);
    if (parsed != null) return { ...ev, payload: parsed };
  }
  return ev;
}
