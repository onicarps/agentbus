import { describe, it, expect, vi, afterEach } from "vitest";
import {
  AgentBus,
  normalizePollResult,
  unwrapToolResult,
  type McpToolClient,
} from "../src/index";
import fs from "fs";
import os from "os";
import path from "path";

describe("AgentBus", () => {
  const tmpDirs: string[] = [];

  afterEach(async () => {
    vi.restoreAllMocks();
    delete process.env.AGENTBUS_WORKSPACE;
    for (const d of tmpDirs.splice(0)) {
      fs.rmSync(d, { recursive: true, force: true });
    }
  });

  it("should instantiate and inherit EventEmitter", () => {
    const bus = new AgentBus({ mcp: { callTool: async () => [] } });
    expect(bus.on).toBeDefined();
    expect(bus.publish).toBeDefined();
    expect(bus.poll).toBeDefined();
  });

  it("publish maps to agentbus_publish with object payload", async () => {
    const callTool = vi.fn(async () => ({
      content: [{ type: "text", text: JSON.stringify({ event_id: 1 }) }],
    }));
    const mcp: McpToolClient = { callTool };
    const bus = new AgentBus({ mcp, producerId: "grok" });
    const out = await bus.publish("okf/handoff", {
      from: "grok",
      to: "swarm",
      summary: "hi",
    });
    expect(callTool).toHaveBeenCalledWith("agentbus_publish", {
      topic: "okf/handoff",
      payload: { from: "grok", to: "swarm", summary: "hi" },
      producer_id: "grok",
    });
    expect(out).toEqual({ event_id: 1 });
  });

  it("poll emits topic events and advances cursor", async () => {
    const callTool = vi.fn(async (name: string, args: Record<string, unknown>) => {
      if (name === "agentbus_poll") {
        expect(args.topic).toBe("okf/handoff");
        return {
          content: [
            {
              type: "text",
              text: JSON.stringify({
                events: [
                  {
                    event_id: 10,
                    topic: "okf/handoff",
                    payload: { from: "agy", summary: "ping" },
                  },
                ],
                latest_id: 10,
                has_more: false,
              }),
            },
          ],
        };
      }
      return {};
    });
    const bus = new AgentBus({ mcp: { callTool }, sinceId: 0 });
    const seen: unknown[] = [];
    bus.on("okf/handoff", (payload) => seen.push(payload));
    const events = await bus.poll();
    expect(events).toHaveLength(1);
    expect(seen).toEqual([{ from: "agy", summary: "ping" }]);
    expect(bus.cursor).toBe(10);
  });

  it("polls multiple topics with per-topic cursors", async () => {
    const callTool = vi.fn(async (name: string, args: Record<string, unknown>) => {
      if (name !== "agentbus_poll") return {};
      const topic = args.topic as string;
      if (topic === "okf/handoff") {
        return {
          events: [
            { event_id: 2, topic: "okf/handoff", payload: { a: 1 } },
          ],
          has_more: false,
        };
      }
      if (topic === "okf/status") {
        return {
          events: [
            { event_id: 1, topic: "okf/status", payload: { b: 2 } },
            { event_id: 3, topic: "okf/status", payload: { c: 3 } },
          ],
          has_more: false,
        };
      }
      return { events: [], has_more: false };
    });
    const bus = new AgentBus({
      mcp: { callTool },
      topics: ["okf/handoff", "okf/status"],
      sinceId: 0,
    });
    const order: number[] = [];
    bus.on("event", (ev: { event_id: number }) => order.push(ev.event_id));
    const events = await bus.poll();
    expect(events.map((e) => e.event_id)).toEqual([1, 2, 3]);
    expect(order).toEqual([1, 2, 3]);
    // Global cursor is min of per-topic (handoff=2, status=3 → 2)
    expect(bus.cursor).toBe(2);
    expect(bus.getTopicCursor("okf/handoff")).toBe(2);
    expect(bus.getTopicCursor("okf/status")).toBe(3);
  });

  it("drains multi-page topics without skipping ids", async () => {
    // Topic A has 60 events (ids 1..60); topic B has ids 100..102.
    // A global cursor must not jump to 102 after one page of A + B.
    const callTool = vi.fn(
      async (name: string, args: Record<string, unknown>) => {
        if (name !== "agentbus_poll") return {};
        const topic = args.topic as string;
        const since = Number(args.since_id ?? 0);
        const limit = Number(args.limit ?? 50);

        if (topic === "A") {
          const all = Array.from({ length: 60 }, (_, i) => ({
            event_id: i + 1,
            topic: "A",
            payload: { n: i + 1 },
          }));
          const page = all.filter((e) => e.event_id > since).slice(0, limit);
          return {
            events: page,
            has_more: page.length > 0 && page[page.length - 1].event_id < 60,
          };
        }
        if (topic === "B") {
          const all = [100, 101, 102].map((id) => ({
            event_id: id,
            topic: "B",
            payload: { id },
          }));
          return {
            events: all.filter((e) => e.event_id > since),
            has_more: false,
          };
        }
        return { events: [], has_more: false };
      },
    );

    const bus = new AgentBus({
      mcp: { callTool },
      topics: ["A", "B"],
      sinceId: 0,
    });
    const ids: number[] = [];
    bus.on("event", (ev: { event_id: number }) => ids.push(ev.event_id));
    await bus.poll();

    // All 60 A + 3 B events must be emitted
    expect(ids.filter((id) => id <= 60)).toHaveLength(60);
    expect(ids.filter((id) => id >= 100)).toEqual([100, 101, 102]);
    expect(bus.getTopicCursor("A")).toBe(60);
    expect(bus.getTopicCursor("B")).toBe(102);
    // Global cursor is min of topics
    expect(bus.cursor).toBe(60);

    // A was polled at least twice (page drain)
    const aCalls = callTool.mock.calls.filter(
      (c) => c[0] === "agentbus_poll" && c[1].topic === "A",
    );
    expect(aCalls.length).toBeGreaterThanOrEqual(2);
  });

  it("connect starts watcher and initial poll", async () => {
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), "agentbus-js-"));
    tmpDirs.push(workspace);
    fs.mkdirSync(path.join(workspace, ".agentbus"), { recursive: true });
    const callTool = vi.fn(async () => ({ events: [], has_more: false }));
    const bus = new AgentBus({
      workspace,
      mcp: { callTool },
      fallbackMs: 60_000,
    });
    await bus.connect();
    expect(callTool).toHaveBeenCalledWith(
      "agentbus_poll",
      expect.objectContaining({ topic: "okf/handoff", since_id: 0 }),
    );
    await bus.disconnect();
  });

  it("disconnect does not close injected MCP clients", async () => {
    const close = vi.fn(async () => {});
    const callTool = vi.fn(async () => ({ events: [] }));
    const bus = new AgentBus({ mcp: { callTool, close } });
    await bus.disconnect();
    expect(close).not.toHaveBeenCalled();
  });

  it("rolls back connect when initial poll fails", async () => {
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), "agentbus-js-"));
    tmpDirs.push(workspace);
    fs.mkdirSync(path.join(workspace, ".agentbus"), { recursive: true });
    const close = vi.fn(async () => {});
    const callTool = vi.fn(async () => {
      throw new Error("initial poll boom");
    });
    // Simulate owned MCP by not injecting — but we inject for test control.
    // Use injected mcp so we only assert watcher cleanup + retryable connect.
    const bus = new AgentBus({
      workspace,
      mcp: { callTool, close },
      fallbackMs: 60_000,
    });
    await expect(bus.connect()).rejects.toThrow(/initial poll boom/);
    // Injected client must not be closed on rollback.
    expect(close).not.toHaveBeenCalled();
    // Second connect can retry (not stuck connected).
    callTool.mockResolvedValue({ events: [], has_more: false });
    await bus.connect();
    await bus.disconnect();
  });

  it("emits error when watcher-triggered poll rejects", async () => {
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), "agentbus-js-"));
    tmpDirs.push(workspace);
    fs.mkdirSync(path.join(workspace, ".agentbus"), { recursive: true });
    let pollCount = 0;
    const callTool = vi.fn(async () => {
      pollCount += 1;
      if (pollCount === 1) return { events: [], has_more: false }; // connect
      throw new Error("mcp down");
    });
    const bus = new AgentBus({
      workspace,
      mcp: { callTool },
      fallbackMs: 30,
    });
    const errPromise = new Promise<unknown>((resolve) => {
      bus.on("error", resolve);
    });
    await bus.connect();
    const err = await Promise.race([
      errPromise,
      new Promise((_, rej) =>
        setTimeout(() => rej(new Error("timeout waiting for error")), 2000),
      ),
    ]);
    expect(err).toBeInstanceOf(Error);
    expect((err as Error).message).toMatch(/mcp down/);
    await bus.disconnect();
  });

  it("does not throw when poll fails and no error listener is registered", async () => {
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), "agentbus-js-"));
    tmpDirs.push(workspace);
    fs.mkdirSync(path.join(workspace, ".agentbus"), { recursive: true });
    let pollCount = 0;
    const callTool = vi.fn(async () => {
      pollCount += 1;
      if (pollCount === 1) return { events: [], has_more: false };
      throw new Error("silent fail");
    });
    const bus = new AgentBus({
      workspace,
      mcp: { callTool },
      fallbackMs: 30,
    });
    const pollErr = new Promise<unknown>((resolve) => {
      bus.on("pollError", resolve);
    });
    await bus.connect();
    const err = await Promise.race([
      pollErr,
      new Promise((_, rej) =>
        setTimeout(() => rej(new Error("timeout pollError")), 2000),
      ),
    ]);
    expect((err as Error).message).toMatch(/silent fail/);
    await bus.disconnect();
  });
});

describe("normalizePollResult / unwrapToolResult", () => {
  it("unwraps MCP text content JSON", () => {
    const raw = {
      content: [
        {
          type: "text",
          text: JSON.stringify({
            events: [{ event_id: 1, topic: "t", payload: {} }],
          }),
        },
      ],
    };
    expect(normalizePollResult(raw)).toHaveLength(1);
    expect(unwrapToolResult(raw)).toEqual({
      events: [{ event_id: 1, topic: "t", payload: {} }],
    });
  });
});
