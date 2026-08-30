import net from "node:net";
import { randomUUID } from "node:crypto";
import type { McpToolClient } from "./types";

const VERSION = "1";
const MAX_FRAME = 4 * 1024 * 1024;

/** MCP-shaped adapter for the authenticated AgentBus Unix broker. */
export function createBrokerMcpClient(socketPath: string, timeoutMs = 30000): McpToolClient {
  async function callTool(name: string, args: Record<string, unknown>): Promise<unknown> {
    const operation = name === "agentbus_publish" ? "publish" :
      name === "agentbus_poll" ? "poll" : name === "agentbus_status" ? "status" :
      name === "agentbus_get_event" ? "get_event" : name === "agentbus_verify_event" ? "verify_event" : null;
    if (!operation) throw new Error(`broker_unsupported_tool:${name}`);
    const body = operation === "status" ? {} : args;
    const result = await request(socketPath, operation, body, timeoutMs);
    return { content: [{ type: "text", text: JSON.stringify(result) }] };
  }
  return { callTool };
}

function request(path: string, operation: string, body: Record<string, unknown>, timeoutMs: number): Promise<unknown> {
  return new Promise((resolve, reject) => {
    const id = randomUUID();
    const socket = net.createConnection({ path });
    let buffer = Buffer.alloc(0);
    const timer = setTimeout(() => { socket.destroy(); reject(new Error("broker_timeout")); }, timeoutMs);
    const fail = (err: unknown) => { clearTimeout(timer); reject(err); };
    socket.once("error", (err) => fail(new Error(`broker_unavailable:${String(err)}`)));
    socket.on("data", (chunk: Buffer) => {
      buffer = Buffer.concat([buffer, chunk]);
      if (buffer.length < 4) return;
      const size = buffer.readUInt32BE(0);
      if (size < 2 || size > MAX_FRAME) { socket.destroy(); return fail(new Error("invalid_broker_frame_size")); }
      if (buffer.length < size + 4) return;
      const response = JSON.parse(buffer.subarray(4, size + 4).toString("utf8")) as Record<string, unknown>;
      clearTimeout(timer); socket.end();
      if (response.protocol_version !== VERSION || response.request_id !== id) return reject(new Error("invalid_broker_response"));
      if (response.ok !== true) return reject(new Error(String((response.error as Record<string, unknown> | undefined)?.code ?? "broker_request_failed")));
      resolve(response.result);
    });
    socket.once("connect", () => {
      const raw = Buffer.from(JSON.stringify({ protocol_version: VERSION, request_id: id, operation, body }));
      if (raw.length > MAX_FRAME) return fail(new Error("invalid_broker_frame_size"));
      const header = Buffer.alloc(4); header.writeUInt32BE(raw.length, 0);
      socket.write(Buffer.concat([header, raw]));
    });
  });
}
