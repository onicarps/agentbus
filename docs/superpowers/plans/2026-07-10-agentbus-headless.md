# AgentBus CI/Headless Logging Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers-extended-cc:subagent-driven-development (recommended) or superpowers-extended-cc:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `--quiet` flag to prevent Python `logging` output from bleeding into standard output (stdout), which corrupts MCP JSON-RPC protocol messages and mangles GitHub Actions step logs.

**Architecture:** Modify `src/agentbus/cli.py` to accept a `-q` / `--quiet` flag. When active, the root logger is restricted to `CRITICAL` only, or redirected entirely to standard error (stderr), ensuring stdout remains absolutely clean for strict JSON MCP communication.

**Tech Stack:** Python 3.10+, `argparse`, `logging`.

---

### Task 0: Implement `--quiet` CLI Flag

**Goal:** Parse `--quiet` in the CLI and configure logging accordingly.

**Files:**
- Modify: `src/agentbus/cli.py`
- Create: `tests/test_cli.py`

**Acceptance Criteria:**
- [x] CLI accepts `-q` or `--quiet`.
- [x] When active, logging level is set to `CRITICAL` or forced to stderr.

**Verify:** `pytest tests/test_cli.py -v` → expected output: PASS

**Steps:**

- [ ] **Step 1: Write the failing test**

Use Click's CliRunner against the real `main` group (not argparse/sys.argv):

```python
from click.testing import CliRunner
from agentbus.cli import main

def test_quiet_flag_suppresses_logs():
    runner = CliRunner()
    result = runner.invoke(main, ["--quiet", "status", "--help"])
    assert result.exit_code == 0
```

- [ ] **Step 2: Run test to verify it fails**
Expected: FAIL

- [ ] **Step 3: Write minimal implementation**

Modify `src/agentbus/cli.py`:
```python
import argparse
import logging
from .server import run

def main():
    parser = argparse.ArgumentParser(prog="agentbus")
    parser.add_argument("-q", "--quiet", action="store_true", help="Suppress non-critical logs")
    subparsers = parser.add_subparsers(dest="command")
    
    serve_parser = subparsers.add_parser("serve", help="Run the MCP server")
    
    args = parser.parse_args()
    
    if args.quiet:
        logging.basicConfig(level=logging.CRITICAL, stream=sys.stderr)
    else:
        logging.basicConfig(level=logging.INFO, stream=sys.stderr)
        
    if args.command == "serve":
        run()
```

- [ ] **Step 4: Run test to verify it passes**
Expected: PASS

- [ ] **Step 5: Commit**
```bash
git add src/agentbus/cli.py tests/test_cli.py
git commit -m "feat(cli): add --quiet flag to protect MCP stdout"
```
