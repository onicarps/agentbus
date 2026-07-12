import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import {
  StdioClientTransport,
  getDefaultEnvironment,
} from "@modelcontextprotocol/sdk/client/stdio.js";
import type { McpToolClient } from "./types";

export type StdioMcpOptions = {
  /** Workspace root containing `.agentbus/` (defaults to AGENTBUS_WORKSPACE). */
  workspace?: string;
  /** Binary to spawn (default: AGENTBUS_BIN or `agentbus`). */
  command?: string;
  /** Args after command (default: `mcp-serve --workspace <ws>`). */
  args?: string[];
  cwd?: string;
  /** Extra env merged on top of MCP-safe defaults. */
  env?: Record<string, string>;
  /** Client name reported to the MCP server. */
  clientName?: string;
  /** Client version reported to the MCP server. */
  clientVersion?: string;
};

/**
 * Spawn `agentbus mcp-serve` over stdio and return a minimal tool client.
 *
 * Requires `agentbus` on PATH (or `AGENTBUS_BIN` / `options.command`) and a
 * workspace with a valid token (`agentbus init` / `token ensure`).
 */
export async function createStdioMcpClient(
  options: StdioMcpOptions = {},
): Promise<McpToolClient> {
  const workspace = options.workspace ?? process.env.AGENTBUS_WORKSPACE;
  if (!workspace) {
    throw new Error(
      "createStdioMcpClient: workspace or AGENTBUS_WORKSPACE is required",
    );
  }

  const command =
    options.command ?? process.env.AGENTBUS_BIN ?? "agentbus";
  const args =
    options.args ?? ["mcp-serve", "--workspace", workspace];

  const env: Record<string, string> = {
    ...getDefaultEnvironment(),
    ...options.env,
    AGENTBUS_WORKSPACE: workspace,
  };
  // Preserve PATH so the child can resolve `agentbus` / python when needed.
  if (process.env.PATH && !env.PATH) {
    env.PATH = process.env.PATH;
  }

  const transport = new StdioClientTransport({
    command,
    args,
    env,
    cwd: options.cwd,
    stderr: "pipe",
  });

  const client = new Client({
    name: options.clientName ?? "@agentbus/agentbus-client",
    version: options.clientVersion ?? "0.1.0",
  });

  await client.connect(transport);

  return {
    async callTool(name, toolArgs) {
      const result = await client.callTool({
        name,
        arguments: toolArgs,
      });
      if (result.isError) {
        const text = extractText(result as unknown);
        throw new Error(
          text || `MCP tool ${name} returned isError without detail`,
        );
      }
      return result;
    },
    async close() {
      await client.close();
    },
  };
}

function extractText(result: unknown): string {
  if (result == null || typeof result !== "object") return "";
  const content = (result as { content?: unknown }).content;
  if (!Array.isArray(content)) return "";
  return content
    .filter(
      (p): p is { type: string; text: string } =>
        !!p &&
        typeof p === "object" &&
        (p as { type?: string }).type === "text" &&
        typeof (p as { text?: unknown }).text === "string",
    )
    .map((p) => p.text)
    .join("\n");
}
