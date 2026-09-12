# PostHog outbound telemetry

AgentBus v1 exports allowlisted operational metadata from its durable event log.
It does not expose an inbound webhook or grant authority from PostHog alerts.

## Configure

```bash
export AGENTBUS_WORKSPACE=/path/to/workspace
export POSTHOG_PROJECT_API_KEY=phc_project_key
export POSTHOG_HOST=https://us.i.posthog.com  # default

agentbus posthog test
agentbus posthog status
agentbus posthog stream
```

`POSTHOG_TELEMETRY_ENABLED=false` disables delivery. Optional tuning variables
are `POSTHOG_BATCH_SIZE` (default 50) and `POSTHOG_POLL_INTERVAL_SECONDS`
(default 2). Plain HTTP is rejected. A local development receiver may be used
only with `POSTHOG_ALLOW_INSECURE_DEV=true` and a loopback hostname.

The cursor is atomically persisted at `.agentbus/posthog_cursor.json` after a
successful HTTP 200 response. Non-retryable 400/401/403/422 batches are recorded
without event content in `.agentbus/posthog_quarantine.jsonl`. A singleton lock
is held at `.agentbus/posthog_streamer.pid`.

## Privacy contract

The exporter uses a separate allowlist for each event taxonomy. Handoff summary
text, prompts, completions, stack traces, raw URLs, credentials, artifacts, and
person profiles are never serialized by v1. `POSTHOG_CAPTURE_PROMPTS` is parsed
for forward-compatible configuration but v1 remains metadata-only even when it
is set; content export requires a later, explicit privacy-reviewed schema.

Supported output events are `swarm_handoff_dispatched`, `swarm_qa_verdict`,
`$ai_generation`, and `swarm_lock_event`. UUIDv5 identifiers make restart replay
idempotent in PostHog.
