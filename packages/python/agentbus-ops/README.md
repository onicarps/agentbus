# okf-agentbus-ops

Opinionated **SRE ops** for [AgentBus](https://github.com/onicarps/agentbus): edge-triggered health watchdog as a deterministic Python process (`SREWatchdogAgent`).

| Item | Value |
|------|--------|
| PyPI name | `okf-agentbus-ops` |
| Import | `agentbus_ops` |
| CLI | `agentbus-ops` |
| Core dependency | `okf-agentbus>=0.16.3` |
| Version train | **Independent** of core `0.16.x` |

## Why a separate package

Core (`okf-agentbus`) stays pure: routing, metrics, validate-config.  
Opinionated policy (level transitions, cooldown, idempotency, hooks) lives here.

## Install (dogfood)

Until a public v1 release, path-install from the monorepo:

```bash
export AGENTBUS_WORKSPACE=/home/oni/okf_agent_workspace
pip install -e "$AGENTBUS_WORKSPACE/projects/agentbus/packages/python/agentbus-ops"
```

## CLI (parity with bash P2)

```bash
# One-shot edge decision (no publish)
agentbus-ops watchdog --dry-run --json

# Live: publish SRE_STATUS only on healthy↔degraded↔critical edges
agentbus-ops watchdog --json

# Bootstrap seed silent (default); force first publish:
agentbus-ops watchdog --force-bootstrap-publish

# Attach compact metrics line
agentbus-ops watchdog --include-metrics
```

Exit codes match bash:

| Code | Meaning |
|------|---------|
| 0 | silence / successful publish / dry-run decision |
| 1 | usage / config error |
| 2 | publish failed |

Health level is **not** mirrored as process exit (cron stays green on steady degraded).

## Library

```python
from agentbus_ops import SREWatchdogAgent

class MyWatchdog(SREWatchdogAgent):
    def on_critical_alert(self, cur, decision):
        # optional LLM / page — default is no-op
        pass

agent = SREWatchdogAgent(workspace="/path/to/okf")
decision = agent.run_once(dry_run=True)
print(decision.action, decision.level)
```

## Probe (v0.1 MVP)

v0.1 **wraps** the coordination-root bash probe:

`scripts/swarm_health_check.sh --json`

Pure-Python probe port is deferred. Edge policy, state file, and publish path are native Python and share `.agentbus/sre_last_state.json` with the bash strangler so cron can switch without dual state.

## State file

Same contract as [sre-edge-triggered](https://github.com/onicarps/agentbus) runbook:

`.agentbus/sre_last_state.json` under `AGENTBUS_WORKSPACE`.

## Not in scope (v0.1)

- Autonomous restart daemon
- Default-on LLM every tick
- TypeScript package
- Public PyPI release (dogfood first)

## License

MIT
