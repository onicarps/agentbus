# Security Policy

## Supported versions

| Version | Supported |
|---------|-----------|
| 0.1.x   | Yes       |

## Reporting a vulnerability

**Do not** open a public GitHub issue for security vulnerabilities.

Open a [GitHub Security Advisory](https://github.com/onicarps/agentbus/security/advisories/new) or email the maintainers via GitHub with:

- Description of the issue
- Steps to reproduce
- Impact assessment (if known)

We aim to acknowledge reports within **72 hours**.

## Scope

In scope:

- The `agentbus` Python package (`src/agentbus/`)
- MCP stdio server and CLI
- Workspace token authentication (`{workspace}/.agentbus/token`)

Out of scope:

- Third-party MCP clients (Cursor, Claude Desktop, etc.)
- User workspace content published to topics

## Threat model (v0.1)

AgentBus v0.1 is designed for **local single-user workspaces**:

- Persistence is SQLite under `{workspace}/.agentbus/`
- Auth uses a workspace-scoped ephemeral token file (mode `0600`)
- Poll and status are unauthenticated; publish requires a valid token when auth is enabled
- There is no network listener in v0.1 (stdio MCP only)

Do not expose AgentBus beyond localhost without additional hardening.