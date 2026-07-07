# Security Policy

## Supported versions

| Version | Supported |
|---------|-----------|
| 0.2.x   | Yes       |
| 0.1.x   | Best effort |

## Reporting a vulnerability

**Do not** open a public GitHub issue for security vulnerabilities.

Open a [GitHub Security Advisory](https://github.com/onicarps/agentbus/security/advisories/new) or email the maintainers via GitHub with:

- Description of the issue
- Steps to reproduce
- Impact assessment (if known)

We aim to acknowledge reports within **72 hours**.

## Scope

In scope:

- The `agentbus-mcp` Python package (`src/agentbus/`)
- MCP stdio server and CLI
- Workspace token authentication (`{workspace}/.agentbus/token`)
- Advisory lease store (`leases` table in `events.db`)

Out of scope:

- Third-party MCP clients (Cursor, Claude Desktop, Hermes, etc.)
- User workspace content published to topics

## Threat model (v0.2)

AgentBus is designed for **local single-user workspaces**:

- Persistence is SQLite under `{workspace}/.agentbus/`
- Auth uses a workspace-scoped ephemeral token file (mode `0600`)
- Poll, status, and lock_status are unauthenticated
- Publish and lock mutations require a valid token when auth is enabled
- Advisory locks do not enforce OS-level file mutexes — malicious clients can ignore them
- There is no network listener in v0.2 (stdio MCP only)

Do not expose AgentBus beyond localhost without additional hardening.