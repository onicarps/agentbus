import { describe, it, expect, vi, beforeEach } from "vitest";
import { getDatabasePath } from "../src/locator";
import fs from "fs";
import path from "path";

vi.mock("fs");

describe("getDatabasePath", () => {
  beforeEach(() => {
    delete process.env.AGENTBUS_WORKSPACE;
    vi.resetAllMocks();
  });

  it("should resolve the correct db path when AGENTBUS_WORKSPACE is set", () => {
    process.env.AGENTBUS_WORKSPACE = "/fake/workspace";
    vi.spyOn(fs, "existsSync").mockReturnValue(true);
    const dbPath = getDatabasePath();
    expect(dbPath).toBe(path.join("/fake/workspace", ".agentbus", "events.db"));
  });

  it("should throw if workspace directory does not exist", () => {
    process.env.AGENTBUS_WORKSPACE = "/invalid/workspace";
    vi.spyOn(fs, "existsSync").mockReturnValue(false);
    expect(() => getDatabasePath()).toThrow("Workspace directory not found");
  });

  it("should throw if AGENTBUS_WORKSPACE is unset", () => {
    expect(() => getDatabasePath()).toThrow(
      "AGENTBUS_WORKSPACE environment variable must be set",
    );
  });
});
