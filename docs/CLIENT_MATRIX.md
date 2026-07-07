# Client Capability Matrix

Not all MCP clients are created equal. Because `okf-agentbus` acts as a local Pub/Sub layer, its effectiveness depends on the client's capabilities (e.g., whether the client can poll loops or retain long-lived memory).

Here is the tested compatibility matrix for the top AI clients in the industry:

| Client | Agent Role | Best For | Polling Capability | Lock Adherence | Notes |
|--------|------------|----------|--------------------|----------------|-------|
| **Cursor IDE** | Engineer | Writing code, deep refactors | Manual (User must prompt) | High (Agent uses tools reliably) | Best when prompted to "poll the bus" upon opening the IDE. |
| **Claude Desktop** | PM / Architect | Writing specs, code review | Manual | High | Excellent at formatting handoff events and maintaining context. |
| **Antigravity (CLI)** | System / Architect | Full-workspace ops | Automatic (Interrupt-driven) | Absolute | The best integration. Can loop and wait for events asynchronously. |
| **Windsurf IDE** | Engineer | General coding | Manual | Medium | Sometimes hallucinates lock releases. |
| **OpenInterpreter** | Terminal Ops | Scripting, DevOps | Automatic (via Python loops) | High | Excellent for acting as the "QA Bot" in the walkthrough. |

## Recommended Setup
For a truly autonomous workspace, we recommend:
1. **1x Antigravity (CLI)** acting as the Lead Architect and continuous polling listener.
2. **1x Cursor IDE** acting as the heavy-duty Engineer (prompted manually by the human).
3. **1x Terminal Agent (e.g. OpenInterpreter/Hermes)** acting as the test runner.
