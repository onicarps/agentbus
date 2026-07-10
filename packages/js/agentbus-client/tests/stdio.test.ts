import { describe, it, expect, afterEach } from "vitest";
import { spawnSync } from "child_process";
import fs from "fs";
import os from "os";
import path from "path";
import { createStdioMcpClient } from "../src/stdio";
import { AgentBus } from "../src/index";

function agentbusAvailable(): string | null {
  const bin = process.env.AGENTBUS_BIN;
  if (bin && fs.existsSync(bin)) return bin;
  const which = spawnSync("bash", ["-lc", "command -v agentbus"], {
    encoding: "utf8",
  });
  if (which.status === 0 && which.stdout.trim()) {
    return which.stdout.trim();
  }
  return null;
}

const agentbusBin = agentbusAvailable();
const describeLive = agentbusBin ? describe : describe.skip;

describe("createStdioMcpClient (unit)", () => {
  it("requires workspace", async () => {
    const prev = process.env.AGENTBUS_WORKSPACE;
    delete process.env.AGENTBUS_WORKSPACE;
    await expect(createStdioMcpClient()).rejects.toThrow(/workspace/i);
    if (prev !== undefined) process.env.AGENTBUS_WORKSPACE = prev;
  });
});

describeLive("stdio MCP live integration", () => {
  const tmpDirs: string[] = [];

  afterEach(async () => {
    for (const d of tmpDirs.splice(0)) {
      fs.rmSync(d, { recursive: true, force: true });
    }
  });

  it("publish + poll round-trip via agentbus mcp-serve", async () => {
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), "agentbus-stdio-"));
    tmpDirs.push(workspace);

    // Ensure token + schema files exist for the temp workspace.
    const ensure = spawnSync(
      agentbusBin!,
      ["token", "ensure", "--workspace", workspace, "--quiet"],
      { encoding: "utf8" },
    );
    expect(ensure.status, ensure.stderr || ensure.stdout).toBe(0);

    const mcp = await createStdioMcpClient({
      workspace,
      command: agentbusBin!,
    });

    try {
      const pub = await mcp.callTool("agentbus_publish", {
        topic: "okf/handoff",
        payload: {
          from: "ts-sdk",
          to: "swarm",
          summary: "stdio integration test",
        },
        producer_id: "ts-sdk-test",
      });
      // Result is MCP envelope; text should parse to event_id
      const content = (pub as { content?: Array<{ text?: string }> }).content;
      expect(content?.[0]?.text).toBeTruthy();
      const body = JSON.parse(content![0].text!);
      expect(body.event_id).toBeGreaterThan(0);

      const poll = await mcp.callTool("agentbus_poll", {
        topic: "okf/handoff",
        since_id: 0,
        limit: 10,
      });
      const pollText = (poll as { content?: Array<{ text?: string }> }).content?.[0]
        ?.text;
      const pollBody = JSON.parse(pollText!);
      expect(pollBody.events.length).toBeGreaterThanOrEqual(1);
      expect(pollBody.events.some((e: { event_id: number }) => e.event_id === body.event_id)).toBe(
        true,
      );
    } finally {
      await mcp.close?.();
    }
  }, 30_000);

  it("AgentBus auto-spawns stdio and emits handoff", async () => {
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), "agentbus-auto-"));
    tmpDirs.push(workspace);
    const ensure = spawnSync(
      agentbusBin!,
      ["token", "ensure", "--workspace", workspace, "--quiet"],
      { encoding: "utf8" },
    );
    expect(ensure.status, ensure.stderr || ensure.stdout).toBe(0);

    // Store creates events.db on first publish; watcher falls back to timer until then.
    fs.mkdirSync(path.join(workspace, ".agentbus"), { recursive: true });

    const bus = new AgentBus({
      workspace,
      topics: ["okf/handoff"],
      fallbackMs: 60_000,
      producerId: "ts-sdk-test",
      stdio: { command: agentbusBin! },
    });

    const seen: unknown[] = [];
    bus.on("okf/handoff", (payload) => seen.push(payload));

    await bus.connect();
    await bus.publish("okf/handoff", {
      from: "ts-sdk",
      to: "swarm",
      summary: "auto-stdio",
    });
    // Force poll in case watch misses empty→write transition on fresh db.
    await bus.poll();
    expect(seen.length).toBeGreaterThanOrEqual(1);
    await bus.disconnect();
  }, 30_000);
});
