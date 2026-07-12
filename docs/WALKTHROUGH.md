# Multi-Agent Coordination Walkthrough

This guide demonstrates a real-world scenario where **three independent AI agents**—living in completely different environments—use `okf-agentbus` to build a feature together without a centralized orchestrator like LangGraph.

## The Cast
1. **Alice (The PM - Claude Desktop):** Needs to define a product requirement.
2. **Bob (The Engineer - Cursor IDE):** Needs to write the code based on the requirement.
3. **Charlie (The QA - Terminal Bot):** Needs to run the tests and report back.

---

## 1. Alice Creates the Spec
Alice (Claude Desktop) wants to write a requirement document. She first checks if anyone else is editing it.

### Step 1: Acquire a Lease
Alice calls the `agentbus_lock_acquire` MCP tool:
```json
{
  "resource": "file:///workspace/docs/spec.md",
  "owner_id": "alice-claude",
  "ttl_seconds": 300
}
```
*Result:* Success. Alice now has an exclusive 5-minute lock.

### Step 2: Write the File & Release
Alice writes the specification to disk. Once done, she releases the lock:
```json
{
  "resource": "file:///workspace/docs/spec.md",
  "lease_id": "<uuid>",
  "owner_id": "alice-claude"
}
```

### Step 3: Handoff to Bob
Alice uses `agentbus_publish` to signal Bob:
```json
{
  "topic": "okf/handoff",
  "payload": {
    "from": "alice-claude",
    "to": "bob-cursor",
    "summary": "Spec is ready at docs/spec.md. Please implement."
  }
}
```

---

## 2. Bob Implements the Code
Bob (Cursor IDE) periodically polls the bus using `agentbus_poll`.

### Step 1: Receive the Handoff
Bob polls `okf/handoff` with `since_id: 14` and receives Alice's event. He reads `docs/spec.md`.

### Step 2: Write the Code
Bob creates `src/app.py`. Since he is the only one working on code, he doesn't need a lock, but it's best practice. He implements the feature.

### Step 3: Handoff to Charlie
Bob publishes an event to Charlie to run the tests:
```json
{
  "topic": "okf/handoff",
  "payload": {
    "from": "bob-cursor",
    "to": "charlie-qa",
    "summary": "Code pushed to src/app.py. Please run pytest."
  }
}
```

---

## 3. Charlie Runs the Tests
Charlie is a background terminal bot (perhaps Hermes or a GitHub Action) that constantly polls the bus.

### Step 1: Run Tests
Charlie receives Bob's event, runs `pytest`, and it passes.

### Step 2: Broadcast Success
Charlie broadcasts the success to everyone:
```json
{
  "topic": "okf/handoff",
  "payload": {
    "from": "charlie-qa",
    "to": "all",
    "summary": "Pytest passed. Feature is ready for production."
  }
}
```

## Summary
Without `okf-agentbus`, Alice, Bob, and Charlie would have collided trying to read/write a shared `log.md` file, leading to race conditions. By using the bus, they achieved asynchronous, deterministic, and safe coordination using only standard MCP tools.

---

## 4. Observing the Swarm with God View (v0.9.0)
Even if Alice, Bob, or Charlie don't explicitly publish to the bus, you can track opted-in activity using God View:
- Run `mcp-serve --wiretap` to intercept their MCP tool calls as `system/mcp` events (params may be redacted when secrets are present).
- Run `agentbus watch` to capture file edits as `system/fs` and shell activity as `system/shell` (excludes `.agentbus/` to avoid feedback loops; shell argv may be redacted).
- Run `agentbus tail` only when an agent opts in — monologue streams can expose sensitive reasoning.

These events appear in the Mission Control TUI (`agentbus monitor`) Wiretap pane for local observability — not unredacted universal capture.
