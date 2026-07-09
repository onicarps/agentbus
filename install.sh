#!/bin/bash
set -e

echo "🚀 Installing AgentBus (latest)..."
pip install -U "okf-agentbus[devex,sdk]"

echo "🔌 Auto-discovering and wiring MCP configurations..."
# Generate a random 4-character ID or use hostname for a unique producer ID
PRODUCER_ID="local-$(hostname -s 2>/dev/null || echo $RANDOM)"
agentbus init --apply --producer-id "$PRODUCER_ID"

if [ ! -f "AGENTS.md" ]; then
    echo "📜 Generating default AGENTS.md rule file..."
    cat << 'EOF' > AGENTS.md
# Swarm Protocol (AgentBus)
You are part of a multi-agent swarm. All cross-agent communication MUST happen via the AgentBus.
- Use `agentbus_publish` to report task completion, ask for help, or hand off tasks to other agents.
- Check `agentbus_poll` frequently to see if tasks have been assigned to you.
- If you encounter a critical action (like deleting a database), publish an event to `okf/handoff` and wait for Human-in-the-Loop (HITL) approval.
EOF
fi

echo "✅ AgentBus successfully installed and wired!"
echo ""
echo "To view your live multi-agent events, run:"
echo "    agentbus monitor"
