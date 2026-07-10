import path from "path";
import fs from "fs";

/**
 * Resolve the AgentBus SQLite events.db path from AGENTBUS_WORKSPACE
 * (or an explicit workspace override).
 */
export function getDatabasePath(workspace?: string): string {
  const ws = workspace ?? process.env.AGENTBUS_WORKSPACE;
  if (!ws) {
    throw new Error("AGENTBUS_WORKSPACE environment variable must be set");
  }

  if (!fs.existsSync(ws)) {
    throw new Error("Workspace directory not found: " + ws);
  }

  return path.join(ws, ".agentbus", "events.db");
}
