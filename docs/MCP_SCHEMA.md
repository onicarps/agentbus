# AgentBus MCP Schema — v0.1

Canonical tool and event contract for the events-only MVP.

## Server

| Property | Value |
|----------|-------|
| Name | `agentbus` |
| Transport | stdio |
| Runtime | Python 3.11+ |
| Persistence | `{workspace}/.agentbus/events.db` |

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

## Tools

### `agentbus_publish`

**Input:** `topic`, `payload`, optional `schema_version`, `producer_id`, `causation_id`, `idempotency_key`, `auth_token`

**Success:**

```json
{"event_id": 42, "topic": "okf/handoff", "timestamp": "...", "duplicate": false}
```

**Errors:** `unknown_topic`, `invalid_payload`, `unauthorized`

### `agentbus_poll`

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

## Error model

MCP tools return structured JSON errors (`isError: true`) with message strings — not HTTP status codes.