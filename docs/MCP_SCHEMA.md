# AgentBus MCP Schema — v0.2

Canonical tool and event contract.

## Server

| Property | Value |
|----------|-------|
| Name | `agentbus` |
| Transport | stdio |
| Runtime | Python 3.11+ |
| Persistence | `{workspace}/.agentbus/events.db` (events + leases tables) |

## Event envelope

```json
{
  "event_id": 42,
  "topic": "okf/handoff",
  "producer_id": "cursor",
  "timestamp": "2026-07-07T00:00:00Z",
  "schema_version": "1.0",
  "payload": {},
  "causation_id": null,
  "idempotency_key": null
}
```

| Field | Rules |
|-------|-------|
| `event_id` | Monotonic integer per workspace, server-assigned |
| `topic` | `^[a-z][a-z0-9._/-]*$`, max 128 chars |
| `producer_id` | `[a-z][a-z0-9-]*`, max 64; from env or tool arg |
| `timestamp` | ISO8601 UTC, server-assigned |
| `schema_version` | Semver string for payload schema |
| `idempotency_key` | Optional; duplicate returns existing event |

**Retention:** 7 days default (`--retention-days` / store config).

## Topics

### `okf/handoff` (v1.0)

```json
{
  "from": "cursor",
  "to": "hermes",
  "summary": "Implemented token auth.",
  "links": ["/docs/AUTH.md"],
  "initiative": "agentbus"
}
```

Required: `from`, `to`, `summary`.

### `okf/status/<initiative>` (v1.0)

```json
{
  "state": "active",
  "message": "Running spike tests"
}
```

`state` enum: `idle`, `active`, `blocked`, `complete`.

### `system/mcp` (v1.0)

```json
{
  "method": "tools/call",
  "params": {},
  "wiretap_latency_ms": 42
}
```

God View MCP wiretap events from `mcp-serve --wiretap`.

### `system/fs` (v1.0)

```json
{
  "path": "/workspace/src/app.py",
  "event_type": "modified"
}
```

God View filesystem events from `agentbus watch`.

### `system/shell` (v1.0)

```json
{
  "command": "pytest",
  "pid": 12345
}
```

God View shell execution events from `agentbus watch`.

### `system/monologue` (v1.0)

```json
{
  "agent_id": "grok",
  "thought": "I need to write tests for this module."
}
```

God View agent thought/reasoning logs from `agentbus tail`.


## Event tools

### `agentbus_publish`

**Auth:** Required when workspace token exists.

**Input:** `topic`, `payload`, optional `schema_version`, `producer_id`, `causation_id`, `idempotency_key`, `auth_token`

**Success:**

```json
{"event_id": 42, "topic": "okf/handoff", "timestamp": "...", "duplicate": false}
```

**Errors:** `unknown_topic`, `invalid_payload`, `unauthorized`

### `agentbus_poll`

**Auth:** Not required.

**Input:** `topic`, `since_id` (default 0), `limit` (max 100)

**Success:**

```json
{
  "events": [],
  "latest_id": 42,
  "has_more": false
}
```

Semantics: at-least-once delivery; client advances cursor to `latest_id`.

### `agentbus_status`

**Auth:** Not required.

**Success:**

```json
{
  "workspace": "/path",
  "event_count": 42,
  "latest_event_id": 42,
  "topics": ["okf/handoff"],
  "retention_days": 7
}
```

## Lease tools (v0.2)

Advisory locks only — clients must cooperate. Resources are **absolute paths** within the workspace.

| Parameter | Rules |
|-----------|-------|
| `resource` | Absolute file or directory path |
| `owner_id` | Agent identity (`[a-z][a-z0-9-]*`) |
| `ttl_seconds` | Default 300, max 3600 |
| `lease_id` | UUID returned by acquire |

### `agentbus_lock_acquire`

**Auth:** Required.

**Input:** `resource`, `owner_id`, optional `ttl_seconds`, `auth_token`

**Success (acquired):**

```json
{"acquired": true, "lease_id": "<uuid>", "expires_at": "2026-07-07T12:00:00Z", "resource": "/path"}
```

**Held by another owner:**

```json
{"acquired": false, "current_owner": "hermes", "expires_at": "...", "resource": "/path"}
```

### `agentbus_lock_release`

**Auth:** Required.

**Input:** `resource`, `lease_id`, `owner_id`, `auth_token`

**Success:** `{"released": true}` (idempotent if already expired)

### `agentbus_lock_renew`

**Auth:** Required.

**Input:** `resource`, `lease_id`, `owner_id`, optional `ttl_seconds`, `auth_token`

**Success:** `{"renewed": true, "expires_at": "..."}` or `{"renewed": false}` if expired

### `agentbus_lock_status`

**Auth:** Not required.

**Input:** `resource`

**Unlocked:**

```json
{"locked": false, "resource": "/path"}
```

**Locked:**

```json
{
  "locked": true,
  "resource": "/path",
  "lease_id": "<uuid>",
  "current_owner": "hermes",
  "acquired_at": "...",
  "expires_at": "..."
}
```

## Error model

MCP tools return structured JSON errors (`isError: true`) with message strings — not HTTP status codes.