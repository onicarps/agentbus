# AgentBus TypeScript SDK Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers-extended-cc:subagent-driven-development (recommended) or superpowers-extended-cc:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Create a Node.js/TypeScript wrapper for AgentBus that abstracts MCP tool calls and exposes an idiomatic `EventEmitter` interface for JS developers.

**Architecture:** The SDK will wrap the standard MCP stdio client. To prevent CPU-heavy continuous polling, it will use `fs.watch` on the AgentBus SQLite database file/directory to detect writes. When a change is detected, it triggers an MCP read via the `agentbus_poll` tool. Events are then emitted natively via Node.js `EventEmitter`. A slow periodic fallback poll is included to mitigate `fs.watch` flakiness on Windows/WAL.

**Tech Stack:** TypeScript, Node.js (`fs.watch`, `events`), `@modelcontextprotocol/sdk`, Vitest (for TDD).

---

### Task 0: Project Initialization and TDD Setup

**Goal:** Initialize the TypeScript package under `packages/js/agentbus-client` with strict typing and Vitest.

**Files:**
- Create: `packages/js/agentbus-client/package.json`
- Create: `packages/js/agentbus-client/tsconfig.json`
- Create: `packages/js/agentbus-client/vitest.config.ts`
- Create: `packages/js/agentbus-client/tests/setup.test.ts`

**Acceptance Criteria:**
- [x] Package has required dependencies (`@modelcontextprotocol/sdk`, `typescript`, `vitest`).
- [x] TypeScript compiles cleanly with strict mode.
- [x] A dummy test runs and passes via Vitest.

**Verify:** `cd packages/js/agentbus-client && npm run test` → expected output: 1 passed

**Steps:**

- [x] **Step 1: Create package.json and install deps**

```bash
mkdir -p packages/js/agentbus-client/tests
cd packages/js/agentbus-client
npm init -y
npm install @modelcontextprotocol/sdk events
npm install -D typescript vitest @types/node
```

- [x] **Step 2: Configure tsconfig.json and vitest.config.ts**

Create `packages/js/agentbus-client/tsconfig.json`:
```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "CommonJS",
    "strict": true,
    "esModuleInterop": true,
    "skipLibCheck": true,
    "forceConsistentCasingInFileNames": true,
    "outDir": "./dist"
  },
  "include": ["src/**/*"]
}
```

- [x] **Step 3: Write the failing dummy test**

Create `packages/js/agentbus-client/tests/setup.test.ts`:
```typescript
import { describe, it, expect } from 'vitest';

describe('Project Setup', () => {
    it('should run tests successfully', () => {
        expect(1 + 1).toBe(2);
    });
});
```

- [x] **Step 4: Add test script and run it**

In `packages/js/agentbus-client/package.json`, set `"test": "vitest run"`.
Run: `npm run test`
Expected: PASS

- [x] **Step 5: Commit**

```bash
git add packages/js/agentbus-client
git commit -m "chore(js): init agentbus-client TS package and vitest"
```

---

### Task 1: Database File Locator Utility

**Goal:** Create a utility to resolve the SQLite `.db` path based on the `AGENTBUS_WORKSPACE` environment variable.

**Files:**
- Create: `packages/js/agentbus-client/src/locator.ts`
- Create: `packages/js/agentbus-client/tests/locator.test.ts`

**Acceptance Criteria:**
- [x] Resolves `events.db` correctly when `AGENTBUS_WORKSPACE` is set.
- [x] Throws a clear error if the workspace directory does not exist.

**Verify:** `npm run test tests/locator.test.ts` → expected output: PASS

**Steps:**

- [x] **Step 1: Write the failing test**

Create `packages/js/agentbus-client/tests/locator.test.ts`:
```typescript
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { getDatabasePath } from '../src/locator';
import fs from 'fs';
import path from 'path';

vi.mock('fs');

describe('getDatabasePath', () => {
    beforeEach(() => {
        delete process.env.AGENTBUS_WORKSPACE;
        vi.resetAllMocks();
    });

    it('should resolve the correct db path when AGENTBUS_WORKSPACE is set', () => {
        process.env.AGENTBUS_WORKSPACE = '/fake/workspace';
        vi.spyOn(fs, 'existsSync').mockReturnValue(true);
        const dbPath = getDatabasePath();
        expect(dbPath).toBe(path.join('/fake/workspace', '.agentbus', 'events.db'));
    });

    it('should throw if workspace directory does not exist', () => {
        process.env.AGENTBUS_WORKSPACE = '/invalid/workspace';
        vi.spyOn(fs, 'existsSync').mockReturnValue(false);
        expect(() => getDatabasePath()).toThrow('Workspace directory not found');
    });
});
```

- [x] **Step 2: Run test to verify it fails**
Run: `npx vitest run tests/locator.test.ts`
Expected: FAIL with "Cannot find module '../src/locator'"

- [x] **Step 3: Write minimal implementation**

Create `packages/js/agentbus-client/src/locator.ts`:
```typescript
import path from 'path';
import fs from 'fs';

export function getDatabasePath(): string {
    const workspace = process.env.AGENTBUS_WORKSPACE;
    if (!workspace) {
        throw new Error('AGENTBUS_WORKSPACE environment variable must be set');
    }
    
    if (!fs.existsSync(workspace)) {
        throw new Error('Workspace directory not found: ' + workspace);
    }

    return path.join(workspace, '.agentbus', 'events.db');
}
```

- [x] **Step 4: Run test to verify it passes**
Run: `npx vitest run tests/locator.test.ts`
Expected: PASS

- [x] **Step 5: Commit**
```bash
git add packages/js/agentbus-client
git commit -m "feat(js): add database locator utility"
```

---

### Task 2: FileWatcher and Poller Abstraction

**Goal:** Implement an `fs.watch` monitor that debounces file changes and triggers an internal callback, with a fallback timer.

**Files:**
- Create: `packages/js/agentbus-client/src/watcher.ts`
- Create: `packages/js/agentbus-client/tests/watcher.test.ts`

**Acceptance Criteria:**
- [x] Triggers callback when the file changes (debounced).
- [x] Triggers callback on a fallback timer interval.

**Verify:** `npm run test tests/watcher.test.ts` → expected output: PASS

**Steps:**

- [x] **Step 1: Write the failing test**

Create `packages/js/agentbus-client/tests/watcher.test.ts`:
```typescript
import { describe, it, expect, vi, afterEach } from 'vitest';
import { DatabaseWatcher } from '../src/watcher';

describe('DatabaseWatcher', () => {
    afterEach(() => {
        vi.restoreAllMocks();
    });

    it('should call the callback on fallback interval', async () => {
        vi.useFakeTimers();
        const callback = vi.fn();
        const watcher = new DatabaseWatcher('/fake/path.db', callback, 1000);
        
        watcher.start();
        vi.advanceTimersByTime(1100);
        expect(callback).toHaveBeenCalledTimes(1);
        
        watcher.stop();
        vi.useRealTimers();
    });
});
```

- [x] **Step 2: Run test to verify it fails**
Expected: FAIL

- [x] **Step 3: Write minimal implementation**

Create `packages/js/agentbus-client/src/watcher.ts`:
```typescript
import fs from 'fs';

export class DatabaseWatcher {
    private dbPath: string;
    private onTrigger: () => void;
    private fallbackMs: number;
    private timer: NodeJS.Timeout | null = null;
    private watcher: fs.FSWatcher | null = null;
    private debounceTimer: NodeJS.Timeout | null = null;

    constructor(dbPath: string, onTrigger: () => void, fallbackMs = 5000) {
        this.dbPath = dbPath;
        this.onTrigger = onTrigger;
        this.fallbackMs = fallbackMs;
    }

    start() {
        this.timer = setInterval(() => this.trigger(), this.fallbackMs);
        try {
            this.watcher = fs.watch(this.dbPath, (eventType) => {
                if (eventType === 'change') {
                    if (this.debounceTimer) clearTimeout(this.debounceTimer);
                    this.debounceTimer = setTimeout(() => this.trigger(), 50);
                }
            });
        } catch (e) {
            // Ignore if file doesn't exist yet, fallback timer handles it
        }
    }

    private trigger() {
        this.onTrigger();
    }

    stop() {
        if (this.timer) clearInterval(this.timer);
        if (this.debounceTimer) clearTimeout(this.debounceTimer);
        if (this.watcher) this.watcher.close();
    }
}
```

- [x] **Step 4: Run test to verify it passes**
Expected: PASS

- [x] **Step 5: Commit**
```bash
git add packages/js/agentbus-client
git commit -m "feat(js): add debounced fs.watch database monitor"
```

---

### Task 3: AgentBus EventEmitter Client

**Goal:** Combine the watcher, MCP stdio client, and Node.js `EventEmitter` into the final `AgentBus` class.

**Files:**
- Create: `packages/js/agentbus-client/src/index.ts`
- Create: `packages/js/agentbus-client/tests/index.test.ts`

**Acceptance Criteria:**
- [x] Connects to MCP via stdio (`okf-agentbus`).
- [x] Exposes a `publish(topic, payload)` method mapping to `agentbus_publish`.
- [x] Automatically polls `agentbus_poll` and emits standard `bus.on(topic, data)` events.

**Verify:** `npm run test tests/index.test.ts` → expected output: PASS

**Steps:**

- [x] **Step 1: Write the failing test**

Create `packages/js/agentbus-client/tests/index.test.ts`:
```typescript
import { describe, it, expect } from 'vitest';
import { AgentBus } from '../src/index';

describe('AgentBus', () => {
    it('should instantiate and inherit EventEmitter', () => {
        const bus = new AgentBus();
        expect(bus.on).toBeDefined();
        expect(bus.publish).toBeDefined();
    });
});
```

- [x] **Step 2: Run test to verify it fails**
Expected: FAIL

- [x] **Step 3: Write minimal implementation**

Create `packages/js/agentbus-client/src/index.ts`:
```typescript
import { EventEmitter } from 'events';
import { DatabaseWatcher } from './watcher';
import { getDatabasePath } from './locator';

export class AgentBus extends EventEmitter {
    private watcher: DatabaseWatcher | null = null;

    constructor() {
        super();
    }

    async connect() {
        // Pseudo implementation for plan: initialize MCP StdioClientTransport
        const dbPath = getDatabasePath();
        this.watcher = new DatabaseWatcher(dbPath, () => this.poll());
        this.watcher.start();
    }

    async publish(topic: string, payload: any) {
        // Pseudo implementation: call MCP agentbus_publish
    }

    private async poll() {
        // Pseudo implementation: call MCP agentbus_poll and emit events
        // const events = await mcp.callTool('agentbus_poll', { since_id: this.lastId });
        // events.forEach(e => this.emit(e.topic, e.payload));
    }

    disconnect() {
        if (this.watcher) this.watcher.stop();
    }
}
```

- [x] **Step 4: Run test to verify it passes**
Expected: PASS

- [x] **Step 5: Commit**
```bash
git add packages/js/agentbus-client
git commit -m "feat(js): implement core AgentBus EventEmitter class"
```

---

### Task 4: Real MCP stdio transport

**Goal:** Ship a production `createStdioMcpClient` that spawns `agentbus mcp-serve`, auto-wire it from `AgentBus.connect()` when `mcp` is not injected, and align publish/poll with the Python MCP tool schemas.

**Files:**
- Create: `packages/js/agentbus-client/src/stdio.ts`
- Create: `packages/js/agentbus-client/src/types.ts`
- Create: `packages/js/agentbus-client/tests/stdio.test.ts`
- Modify: `packages/js/agentbus-client/src/index.ts`
- Modify: `packages/js/agentbus-client/tests/index.test.ts`
- Modify: `packages/js/agentbus-client/README.md`

**Acceptance Criteria:**
- [x] `createStdioMcpClient` spawns `agentbus mcp-serve --workspace …` via `@modelcontextprotocol/sdk` stdio transport.
- [x] `AgentBus.connect()` auto-creates stdio MCP when `options.mcp` is omitted.
- [x] `publish` sends object payloads + `producer_id` (or `AGENTBUS_PRODUCER_ID`).
- [x] `poll` requires topics (default `okf/handoff`); multi-topic uses a shared cursor; `["*"]` expands via `agentbus_status`.
- [x] Live integration tests pass when `agentbus` is on PATH.

**Verify:** `cd packages/js/agentbus-client && npm test && npx tsc --noEmit` → 15 passed, tsc clean

**Steps:**

- [x] **Step 1: Implement stdio transport + types**
- [x] **Step 2: Wire AgentBus auto-spawn, multi-topic poll, producer_id**
- [x] **Step 3: Unit + live integration tests**
- [x] **Step 4: README + plan/task tracking**
