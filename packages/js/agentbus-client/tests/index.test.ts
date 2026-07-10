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

  it("polls multiple topics with a shared cursor", async () => {
    const callTool = vi.fn(async (name: string, args: Record<string, unknown>) => {
      if (name !== "agentbus_poll") return {};
      const topic = args.topic as string;
      if (topic === "okf/handoff") {
        return {
          events: [
            { event_id: 2, topic: "okf/handoff", payload: { a: 1 } },
          ],
        };
      }
      if (topic === "okf/status") {
        return {
          events: [
            { event_id: 1, topic: "okf/status", payload: { b: 2 } },
            { event_id: 3, topic: "okf/status", payload: { c: 3 } },
          ],
        };
      }
      return { events: [] };
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
    expect(bus.cursor).toBe(3);
    // Both polls used the same since_id
    const pollCalls = callTool.mock.calls.filter((c) => c[0] === "agentbus_poll");
    expect(pollCalls.every((c) => c[1].since_id === 0)).toBe(true);
  });

  it("connect starts watcher and initial poll", async () => {
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), "agentbus-js-"));
    tmpDirs.push(workspace);
    fs.mkdirSync(path.join(workspace, ".agentbus"), { recursive: true });
    const callTool = vi.fn(async () => ({ events: [] }));
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
