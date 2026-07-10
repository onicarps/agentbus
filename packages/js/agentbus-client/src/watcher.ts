import fs from "fs";
import path from "path";

/**
 * Debounced fs.watch on the events.db (or parent dir) plus a slow fallback poll.
 * WAL renames can make watching the file alone flaky — we prefer the .agentbus dir
 * when the db path does not exist yet.
 */
export class DatabaseWatcher {
  private dbPath: string;
  private onTrigger: () => void;
  private fallbackMs: number;
  private debounceMs: number;
  private timer: NodeJS.Timeout | null = null;
  private watcher: fs.FSWatcher | null = null;
  private debounceTimer: NodeJS.Timeout | null = null;

  constructor(
    dbPath: string,
    onTrigger: () => void,
    fallbackMs = 5000,
    debounceMs = 50,
  ) {
    this.dbPath = dbPath;
    this.onTrigger = onTrigger;
    this.fallbackMs = fallbackMs;
    this.debounceMs = debounceMs;
  }

  start(): void {
    this.timer = setInterval(() => this.trigger(), this.fallbackMs);
    const watchTarget = fs.existsSync(this.dbPath)
      ? this.dbPath
      : path.dirname(this.dbPath);
    try {
      this.watcher = fs.watch(watchTarget, () => {
        if (this.debounceTimer) clearTimeout(this.debounceTimer);
        this.debounceTimer = setTimeout(() => this.trigger(), this.debounceMs);
      });
    } catch {
      // Ignore if path is not watchable yet; fallback timer still runs.
    }
  }

  private trigger(): void {
    this.onTrigger();
  }

  stop(): void {
    if (this.timer) clearInterval(this.timer);
    if (this.debounceTimer) clearTimeout(this.debounceTimer);
    if (this.watcher) this.watcher.close();
    this.timer = null;
    this.debounceTimer = null;
    this.watcher = null;
  }
}
