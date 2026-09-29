# AgentBus Deployment Blueprints

Experimental stdio-container scaffold for AgentBus development and integration.

AgentBus currently exposes MCP over stdio, not an HTTP/SSE service. These files
do not provide a production web deployment, an HTTP port, or a `/healthz`
endpoint. Do not use the Railway or Fly.io manifests as a production service
until AgentBus ships a supported network daemon.

## 1. Railway (not a supported deployment target)

`railway.json` only records the valid stdio command for future work. Railway
cannot attach an MCP stdio client to a deployed web service, so this is not a
functional production deployment recipe.

### Configuration Variables
- `AGENTBUS_WORKSPACE`: Path to coordination root (default: `/data/workspace`).

---

## 2. Docker Compose (Self-Hosted)

Run the experimental interactive stdio container explicitly:

```bash
cd deploy
docker compose --profile experimental run --rm agentbus-stdio
```

This attaches the terminal to the MCP stdio process. It is not a background
service and has no HTTP health check.

The PostHog streamer is intentionally opt-in and requires a configured API
key. It is not part of the default Compose startup:

```bash
POSTHOG_API_KEY=... docker compose --profile telemetry up posthog-streamer
```

---

## 3. Fly.io (not supported)

Do not deploy this image to Fly.io until an HTTP/SSE daemon exists.
