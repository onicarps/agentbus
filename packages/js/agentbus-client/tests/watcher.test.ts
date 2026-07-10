import { describe, it, expect, vi, afterEach } from "vitest";
import { DatabaseWatcher } from "../src/watcher";

describe("DatabaseWatcher", () => {
  afterEach(() => {
    vi.restoreAllMocks();
    vi.useRealTimers();
  });

  it("should call the callback on fallback interval", async () => {
    vi.useFakeTimers();
    const callback = vi.fn();
    const watcher = new DatabaseWatcher("/fake/path.db", callback, 1000);

    watcher.start();
    vi.advanceTimersByTime(1100);
    expect(callback).toHaveBeenCalledTimes(1);

    watcher.stop();
  });

  it("should stop the fallback interval", () => {
    vi.useFakeTimers();
    const callback = vi.fn();
    const watcher = new DatabaseWatcher("/fake/path.db", callback, 1000);

    watcher.start();
    watcher.stop();
    vi.advanceTimersByTime(3000);
    expect(callback).not.toHaveBeenCalled();
  });
});
