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

## Defaults (Agy §14 + Grok §14.1)

| Setting | Value |
|---------|--------|
| Engine | Go (`agentbus-go-worker`) |
| Idle auto-sleep | 30 minutes (fast-forwards cursor) |
| Wake skip-backlog | default true |
| max_event_age | 24h (worker-side; optional payload `expires_at`) |
| Multi-worker | local lease `wake-locks/{topic}/{event_id}` |
