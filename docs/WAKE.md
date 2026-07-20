# Wake plane — three layers

Classical **non-LLM** attention for multi-agent workspaces.  
Specs: PRD v0.12 (worker) · PRD v0.13 (webhook) ·  
[wake-session-bridge tech discussion](../../initiatives/agentbus/decisions/wake-session-bridge-tech-discussion-2026-07-16.md)

## Repo vs bus (critical)

| | Path |
|--|------|
| **This git repo** (implementation) | `.../projects/agentbus` — source, tests, this file |
| **Coordination workspace** (bus) | `.../okf_agent_workspace` — set `AGENTBUS_WORKSPACE` **here** for swarm |

Wake files and `events.db` for Agy/Grok/Hermes/Factory live under the **OKF root** `.agentbus/`, not under this repo’s `.agentbus/` (legacy local DB may exist; do not use it for multi-agent).

## Three planes (do not conflate)

| Plane | Mechanism | Guarantees | Non-goals |
|-------|-----------|------------|-----------|
| **Log** | SQLite `events.db`, MCP `publish` / `poll` | Durability, cursored delivery | Push into IDE chat |
| **Wake** | `agentbus-go-worker`: filter → lease → file and/or webhook | Attention artifact without loading a model | Starting an LLM turn |
| **Reason** | Agent session (Grok, Agy, Hermes, Factory) | Planning + tools | Auto-observe the bus |

**MVP product stance:** durable mailbox + optional bridge. IDE hosts are not forced into autonomous turns via MCP stdio.

```
publish(e)  →  durable log
match(w,e)  →  WAKE.<agent>.json  and/or  HTTP webhook
observe(WAKE|webhook|poll)  →  agent turn   ← session-bridge / human / runtime
ack        →  okf/handoff with causation_id = wake event_id
```

## Workspace hard constraint (DrvFS ban)

Canonical `AGENTBUS_WORKSPACE` **must** be on a native Linux (or native host) filesystem:

| Allowed | Forbidden |
|---------|-----------|
| `/home/...`, `/tmp/...`, project trees on ext4/btrfs/xfs | `/mnt/c`, `/mnt/d`, other WSL DrvFS |
| Native macOS/Windows paths for non-WSL installs | `/cygdrive/...`, bare `C:\...` via WSL |

Enforced at **EventStore open**, **CLI workspace resolve**, and **go-worker / go store open**.  
Break-glass only: `AGENTBUS_ALLOW_DRVFS=1` (unsupported; wake/SQLite not guaranteed).

## Day-0 (file wake)

```bash
export AGENTBUS_WORKSPACE=/home/you/okf_agent_workspace   # not /mnt/c
agentbus worker init --to grok
agentbus worker once          # drain → .agentbus/WAKE.json or WAKE.grok.json
agentbus worker up --config .agentbus/worker.grok.yaml
```

Swarm multi-agent:

```bash
agentbus up -d   # watch + grok-wake + agy-wake + hermes-wake per swarm.yaml
```

## Webhook wake (v0.13 — Hermes / Factory first)

Programmatic runtimes get push via HTTP. File wake remains the local durable signal when `on_task.write` is set.

```yaml
# .agentbus/worker.factory.yaml (excerpt)
wake_mode: webhook          # file | webhook
webhook_url: http://127.0.0.1:8787/agentbus/wake
on_task:
  - write:
      path: .agentbus/WAKE.factory.json
```

**Delivery policy**

- POST JSON body = same shape as `WAKE.json` (`event_id`, `payload`, `hint`, …)
- Timeout 5s · up to **3 tries** · exponential backoff (200ms, 400ms)
- Headers: `Content-Type: application/json`, `X-AgentBus-Event-Id`, `X-AgentBus-Worker-Id`
- If file write succeeded and webhook still fails → **log only**, cursor advances (poison-pill)
- If webhook-only (no write) and all tries fail → dispatch error

## Session bridge (zero LLM until a line prints)

MCP does **not** push into chat. Bridges:

```bash
# Grok (default OKF workspace, WAKE.grok.json)
./scripts/grok_wake_notify.sh

# Generic: AGENT + optional WAKE file
./scripts/wake_notify.sh grok
./scripts/wake_notify.sh agy
./scripts/wake_notify.sh hermes
./scripts/wake_notify.sh factory
```

Prints one line per new wake event for tmux / Grok `monitor` / human paste.

### Anti-pattern (v0.15 Phase A): tmux send-keys for autonomy

**Unsupported for autonomous agent-to-agent work.** Do **not** use `tmux send-keys`
(or similar stdin injection) to drive interactive CLIs as background workers.

| Why | Effect |
|-----|--------|
| Concurrency collisions | Human typing / model generation races with injected keys |
| Structure loss | Drops `causation_id` / `trace_id` into raw terminal text |
| UX mismatch | Interactive TUI ≠ headless daemon |

**Supported path:** durable log + wake (file/webhook) + **headless runner**  
(`agentbus run` — Phase B skeleton; Hermes/Factory adapters Phase C/D).  
tmux notify scripts remain **human attention** only.

## Ack convention

When replying to a handoff that woke you:

| Field | Value |
|-------|--------|
| Topic | `okf/handoff` |
| `causation_id` | **bus field** = wake `event_id` (not payload) |
| payload `from` / `to` | your producer / requester |
| payload `summary` | what you did |

Do **not** invent a separate `okf/wake-receipt` topic (product decision 2026-07-16).

## Defaults

| Setting | Value |
|---------|--------|
| Engine | Go (`agentbus-go-worker`) |
| Idle auto-sleep | **off** (`idle_sleep_after_minutes: 0` or null) |
| Poll fallback | 1500ms (`time.Sleep` paced, not only fsnotify) |
| Wake backlog | drain by default |
| max_event_age | 24h |
| Multi-worker | per-agent config + role-scoped lease |

## Headless runner (v0.15 Phase B/C)

```bash
# Echo adapter (CI-safe)
# adapter.type: echo

# Hermes adapter (Phase C) — isolated oneshot LLM turn
export AGENTBUS_WORKSPACE=/home/oni/okf_agent_workspace
agentbus run --config examples/runner.hermes.yaml --once
# or: agentbus run --config .agentbus/runner.hermes.yaml --once
```

| Adapter | Mechanism |
|---------|-----------|
| `echo` | No LLM — run log + ACK (CI) |
| `hermes` | `hermes chat -q … -Q --max-turns N` subprocess |
| `factory` | `droid exec -f prompt.md --skip-permissions-unsafe -o text` (default; or `--auto high` when `skip_permissions: false`) |
| `grok` | `grok --prompt-file … --always-approve --max-turns N` (Phase E) |
| `agy` | `agy --print … --print-timeout …` (Phase E) |
| `aider` | `aider --message …` ops (devops + SRE/health; optional) |

```bash
# Factory / Grok / Agy
agentbus run --config examples/runner.factory.yaml --once
agentbus run --config examples/runner.grok.yaml --once   # wake_file default
agentbus run --config examples/runner.agy.yaml --once
```

Tech design: Phase B–F under `initiatives/agentbus/decisions/v0.15-*.md`  
Dual intake: `webhook_queue` | `wake_file`.

### Swarm composition (Phase F)

`.agentbus/swarm.yaml` may declare runners with **`enabled: false`** (default off):

```yaml
hermes-runner:
  enabled: false   # set true to dogfood
  command: "agentbus run --config .agentbus/runner.hermes.yaml"
```

`agentbus up` skips disabled services (listed in `skipped`). Wake plane stays on.

### Ingress ↔ runner pairing (required — #682)

`wake-ingress` only **enqueues** (Mode A JSONL under `.agentbus/ingress/<runtime>_wake_queue.jsonl`). A headless runner must **drain** that queue. Pair worker wake mode, ingress service, and runner intake together:

| Worker | Ingress service | Runner intake |
|--------|-----------------|---------------|
| `wake_mode: webhook` + `webhook_url` | `<runtime>-wake-ingress` **enabled** | `intake.mode: webhook_queue` + `runtime: <runtime>` |
| `wake_mode: file` (default) | ingress **disabled** / absent | `intake.mode: wake_file` |

**Anti-patterns (queue stagnation):**
- **`wake-ingress` on + matching runner `enabled: false`** — JSONL grows with no consumer (e.g. hermes-runner off while hermes-wake-ingress was left on).
- **Worker `wake_mode: webhook` + runner `intake.mode: wake_file`** — dual-signal still updates `WAKE.*.json`, but the ingress queue never drains (factory audit #682).

**Pre-flight (ops):** before `agentbus down` / `up`, run:

```bash
agentbus validate-config --workspace "$AGENTBUS_WORKSPACE" --text
# exit 0 = no hard errors (warnings OK); --strict fails on warnings too
```

Catches #682-class mismatches (ingress without consumer, webhook triad incomplete, runner config/roles issues).

**Telemetry (ops):** unified status + SLA + ingress queue depths:

```bash
agentbus metrics --workspace "$AGENTBUS_WORKSPACE" --text
# JSON default; --no-health skips HTTP /health; --no-waits omits wait_store
```

| Field | Meaning |
|-------|---------|
| `status.*` | Same core counts as `agentbus status` (events, pending, sla_active) |
| `sla.active` / `sla.dead_letter` | Active deadlines + `okf/dead-letter` by reason (`SLA_BREACH`, `WAIT_TIMEOUT`) |
| `ingress[].queue.line_count` | Total JSONL lines (matches HTTP `queue_depth`; historical) |
| `ingress[].queue.undrained` | True backlog: queue event_ids not in the done set |
| `ingress[].health` | Live GET `/agentbus/wake/health` when service is enabled |
| `ingress[].note` | e.g. `disabled_by_config` when ingress is off (Mode A) |

Read-only — safe to poll. Prefer edge-triggered SRE publish (state transitions only); do not bus-publish `SRE_STATUS: healthy` every tick.

## Async suspend / await (v0.16)

Cooperative **continuation-passing** waits (no LLM session freeze).

```bash
# Inside a headless turn (AGENTBUS_WAKE_EVENT_ID set by adapter):
agentbus await \
  --expect-from factory \
  --causation-id 412 \
  --match QA_VERDICT \
  --timeout-hours 4
# exit 75 → runner publishes RUNNER_SUSPEND + durable WaitRegistration
```

| Piece | Behavior |
|-------|----------|
| Drop file | `.agentbus/runs/<event_id>/await.json` |
| Wait store | `.agentbus/waits/<wait_id>.json` |
| Fulfill | Predicate match (`from_any` + `causation_id`; optional summary) |
| Timeout | Default 4h (max 24h) → `okf/dead-letter` `WAIT_TIMEOUT` + resume `status=timeout` |
| Resume wake | `payload.resume` locked keys; bus `causation_id` = stored `chain_key` (budget continuity) |
| Idempotency | `suspend-ack:{runner}:{event_id}` · `resume:{wait_id}:{fulfilled_by}` |

Design: `initiatives/agentbus/decisions/v0.16-async-suspend-design.md`

## Anti-patterns

- Cron a full coding agent to `poll` an empty bus every minute  
- Put the bus on `/mnt/c` and expect reliable wake  
- Assume `WAKE.json` injects an IDE turn without a bridge  
- Dual workspaces (`projects/agentbus` vs OKF root) for the same swarm  
- **`tmux send-keys` / stdin injection for autonomous multi-agent turns**  
- **Webhook worker + `wake_file` runner** or **ingress without consumer** — see Ingress ↔ runner pairing (#682)
