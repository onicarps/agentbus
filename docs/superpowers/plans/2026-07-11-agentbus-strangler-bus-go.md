# AgentBus Phase B: Strangler Bus (Go Sidecar)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers-extended-cc:subagent-driven-development (recommended) or superpowers-extended-cc:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the "Strangler Bus" architecture by extracting the high-frequency SQLite routing and MCP server logic into a standalone Go binary, invoked via the Python CLI.

**Architecture:** 
1. The Go codebase will live in `projects/agentbus/go-core/`.
2. It will implement a single-writer Goroutine proxy to absolutely prevent SQLite database locking on all platforms.
3. The Python CLI (`agentbus serve`) will gain an `--engine go` flag. If activated, Python defers to executing the Go binary (`agentbus-go-serve`).
4. The Go binary must pass the exact same Python `pytest` integration test suite to prove 1:1 parity before merging.

**Tech Stack:** Go 1.22+, `modernc.org/sqlite` (CGO-free SQLite), `github.com/mark3labs/mcp-go` (or similar Go MCP library).

---

### Task 0: Go Project Initialization & SQLite Store
**Goal:** Setup the Go module and the single-writer SQLite store.

**Files:**
- Create: `go-core/go.mod`
- Create: `go-core/internal/store/store.go`
- Create: `go-core/internal/store/store_test.go`

**Acceptance Criteria:**
- [ ] Initialize `github.com/okf/agentbus-go` module.
- [ ] Implement `EventStore` struct with `Publish` and `Poll` methods.
- [ ] Implement a Goroutine channel-based single-writer to guarantee no DB locks.

**Steps:**
- [ ] Step 1: `cd go-core && go mod init github.com/okf/agentbus-go`
- [ ] Step 2: Write tests in `store_test.go`.
- [ ] Step 3: Implement `store.go` with `modernc.org/sqlite` and a worker goroutine for all writes.

### Task 1: Go MCP Server Layer
**Goal:** Expose the Go `EventStore` via MCP Stdio.

**Files:**
- Create: `go-core/cmd/agentbus-go-serve/main.go`
- Create: `go-core/internal/mcp/server.go`

**Acceptance Criteria:**
- [ ] Implements the `tools/call` for `publish` and `poll`.
- [ ] Communicates strictly over Stdio.
- [ ] Builds to a static binary.

**Steps:**
- [ ] Step 1: Implement MCP server using a Go MCP library.
- [ ] Step 2: Wire the MCP server to the `EventStore`.
- [ ] Step 3: Add `Makefile` or build script to output binary to `bin/agentbus-go-serve`.

### Task 2: Python Integration (The `--engine go` Gated Spike)
**Goal:** Allow Python CLI to launch the Go binary and run integration tests.

**Files:**
- Modify: `src/agentbus/cli.py`
- Create: `tests/test_go_parity.py`

**Acceptance Criteria:**
- [ ] Python CLI accepts `--engine go`.
- [ ] Python `subprocess.Popen` launches `bin/agentbus-go-serve`.
- [ ] `pytest tests/test_go_parity.py` passes by interacting with the Go engine.

**Steps:**
- [ ] Step 1: Update `argparse` in `cli.py` for `--engine`.
- [ ] Step 2: Implement the subprocess launcher.
- [ ] Step 3: Copy the existing MCP integration tests into `test_go_parity.py`, but initialize the server with `--engine go`.
- [ ] Step 4: Ensure CI builds the Go binary before running Pytest.
