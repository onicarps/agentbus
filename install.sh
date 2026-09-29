#!/bin/bash
set -e

echo "🚀 Installing AgentBus & God View Monitor..."

# 1. Verify Python 3
if ! command -v python3 >/dev/null 2>&1; then
    echo "❌ Error: Python 3 (3.11+) is required to install AgentBus."
    echo "Please install Python 3 and retry:"
    echo "  Debian/Ubuntu: sudo apt update && sudo apt install -y python3 python3-venv python3-pip"
    echo "  macOS:         brew install python"
    echo "  Fedora:        sudo dnf install -y python3 python3-pip"
    exit 1
fi

PYTHON_OK=$(python3 -c 'import sys; print(1 if sys.version_info >= (3, 11) else 0)' 2>/dev/null || echo 0)
if [ "$PYTHON_OK" != "1" ]; then
    echo "❌ Error: Python 3.11 or higher is required. Found: $(python3 --version 2>&1)"
    exit 1
fi

# 2. Install okf-agentbus (with monitor dependencies automatically included)
BIN_DIR="${XDG_BIN_HOME:-$HOME/.local/bin}"
INSTALLED_VIA=""

if command -v uv >/dev/null 2>&1; then
    echo "📦 Installing via uv tool..."
    uv tool install --upgrade okf-agentbus
    INSTALLED_VIA="uv"
elif command -v pipx >/dev/null 2>&1; then
    echo "📦 Installing via pipx..."
    pipx install --force okf-agentbus
    INSTALLED_VIA="pipx"
elif [ -n "$VIRTUAL_ENV" ]; then
    echo "📦 Active virtual environment detected ($VIRTUAL_ENV). Installing via pip..."
    python3 -m pip install -U okf-agentbus
    INSTALLED_VIA="venv"
else
    # Fresh boot / system python without venv: avoid PEP 668 "externally-managed-environment" error
    AGENTBUS_HOME="${XDG_DATA_HOME:-$HOME/.local/share}/agentbus"
    VENV_DIR="$AGENTBUS_HOME/venv"
    mkdir -p "$AGENTBUS_HOME" "$BIN_DIR"

    echo "📦 Fresh environment detected. Setting up dedicated isolated runtime in:"
    echo "   $VENV_DIR"

    if [ ! -d "$VENV_DIR" ]; then
        if ! python3 -m venv "$VENV_DIR" 2>/dev/null; then
            echo "⚠️  Python venv module not found. Attempting pip user install..."
            python3 -m pip install --user -U okf-agentbus || {
                echo "❌ Could not create virtualenv or install via pip."
                echo "Please install python3-venv: sudo apt install python3-venv"
                exit 1
            }
        fi
    fi

    if [ -d "$VENV_DIR" ]; then
        "$VENV_DIR/bin/pip" install --upgrade pip -q
        "$VENV_DIR/bin/pip" install -U okf-agentbus -q
        ln -sf "$VENV_DIR/bin/agentbus" "$BIN_DIR/agentbus"
        ln -sf "$VENV_DIR/bin/agentbus-monitor" "$BIN_DIR/agentbus-monitor"
        export PATH="$BIN_DIR:$PATH"
        INSTALLED_VIA="isolated-venv"
    fi
fi

# Ensure agentbus is available in PATH
if ! command -v agentbus >/dev/null 2>&1; then
    if [ -x "$BIN_DIR/agentbus" ]; then
        export PATH="$BIN_DIR:$PATH"
    fi
fi

AGENTBUS_CMD="agentbus"
if ! command -v agentbus >/dev/null 2>&1; then
    if [ -x "$BIN_DIR/agentbus" ]; then
        AGENTBUS_CMD="$BIN_DIR/agentbus"
    else
        echo "❌ agentbus executable not found in PATH or $BIN_DIR."
        exit 1
    fi
fi

echo "🔌 Auto-discovering and wiring MCP configurations..."
PRODUCER_ID="local-$(hostname -s 2>/dev/null || echo $RANDOM)"
if "$AGENTBUS_CMD" init --apply --producer-id "$PRODUCER_ID"; then
    echo "✅ MCP configuration discovery completed."
else
    echo "⚠️  AgentBus is installed, but automatic MCP configuration did not complete."
    echo "   Review the diagnostic above, then rerun: $AGENTBUS_CMD init --apply --producer-id $PRODUCER_ID"
fi

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

echo ""
echo "====================================================================="
echo "🎉 AgentBus & God View Monitor successfully installed!"
echo "====================================================================="
echo ""
echo "1. Launch the interactive God View TUI dashboard:"
echo "   agentbus monitor    (or: agentbus-monitor)"
echo ""
echo "2. Publish your first agent event in another terminal:"
echo "   agentbus publish \\"
echo "     --topic okf/handoff \\"
echo "     --payload '{\"from\":\"alice\",\"to\":\"bob\",\"summary\":\"Hello from AgentBus!\"}'"
echo ""

case ":$PATH:" in
    *":$BIN_DIR:"*) ;;
    *)
        echo "💡 Tip: Add $BIN_DIR to your PATH if not already present:"
        echo "   echo 'export PATH=\"\$HOME/.local/bin:\$PATH\"' >> ~/.bashrc && source ~/.bashrc"
        echo ""
        ;;
esac
