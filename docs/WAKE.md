# Wake plane — `agentbus worker`

Classical **non-LLM** process that turns the event log into attention.  
Spec: [PRD v0.12.0](../../initiatives/agentbus/PRD_v0.12.0_Wake_Plane_Worker.md) (OKF bundle).

## Day-0

```bash
cd go-core && make build
export AGENTBUS_WORKSPACE=/path/to/project
export PATH="$PWD/go-core/bin:$PATH"   # or AGENTBUS_GO_WORKER=...

agentbus worker init --to grok
agentbus worker once          # drain matching handoffs → .agentbus/WAKE.json
# or long-running:
agentbus worker up            # fsnotify + poll; never loads a model

agentbus worker sleep         # stand-down
agentbus worker wake          # default --skip-backlog (fast-forward stale)
agentbus worker wake --drain  # process backlog instead
agentbus worker status
```

## Anti-pattern

Do **not** cron a full coding agent to `poll` an empty bus. Watch `WAKE.json` or start the agent only when that file changes.

## Why Grok may “miss” Agy messages

| Layer | Role | Failure mode |
|-------|------|----------------|
| Log (`publish`) | Durable | Works if same `AGENTBUS_WORKSPACE` |
| Wake (`worker up`) | Writes `WAKE.json` | Auto-sleep or skip-backlog can drop/miss work |
| Reason (Grok session) | MCP `poll` only | **No automatic push into chat** — session must poll, or monitor `WAKE.json` |

**Session bridge (zero LLM until a line prints):**

```bash
# terminal / Grok monitor tool:
./scripts/grok_wake_notify.sh
# → prints AGY_TASK event_id=… when WAKE.json updates
```

Also: `agentbus up -d` starts `implementer-wake` from `.agentbus/swarm.yaml`.

## Defaults

| Setting | Value |
|---------|--------|
| Engine | Go (`agentbus-go-worker`) |
| Idle auto-sleep | **off** (set `idle_sleep_after_minutes: 30` to opt in) |
| Wake | **drain backlog** by default (`--skip-backlog` to FF) |
| max_event_age | 24h |
| Multi-worker | local lease includes role |
