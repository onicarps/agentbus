// agentbus-go-serve — minimal stdio MCP server for publish/poll/status (Strangler spike).
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/onicarps/agentbus-go/internal/store"
)

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func main() {
	ws := os.Getenv("AGENTBUS_WORKSPACE")
	if ws == "" {
		cwd, _ := os.Getwd()
		ws = cwd
	}
	ws, _ = filepath.Abs(ws)

	es, err := store.Open(ws)
	if err != nil {
		fmt.Fprintf(os.Stderr, "store open: %v\n", err)
		os.Exit(1)
	}
	defer es.Close()

	// Content-Length framed JSON-RPC over stdio (MCP transport subset).
	in := bufio.NewReader(os.Stdin)
	var nextID atomic.Int64
	_ = nextID

	for {
		req, err := readMessage(in)
		if err == io.EOF {
			return
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "read: %v\n", err)
			return
		}
		resp := handle(es, req)
		if resp == nil {
			continue // notification
		}
		if err := writeMessage(os.Stdout, resp); err != nil {
			fmt.Fprintf(os.Stderr, "write: %v\n", err)
			return
		}
	}
}

func handle(es *store.EventStore, req *rpcRequest) *rpcResponse {
	switch req.Method {
	case "initialize":
		return &rpcResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Result: map[string]any{
				"protocolVersion": "2024-11-05",
				"capabilities": map[string]any{
					"tools": map[string]any{},
				},
				"serverInfo": map[string]any{
					"name":    "agentbus-go",
					"version": "0.0.1-spike",
				},
			},
		}
	case "notifications/initialized", "notifications/cancelled":
		return nil
	case "tools/list":
		return &rpcResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Result: map[string]any{
				"tools": []map[string]any{
					{
						"name":        "agentbus_publish",
						"description": "Append one event to the workspace event log (Go engine).",
						"inputSchema": map[string]any{
							"type": "object",
							"properties": map[string]any{
								"topic":           map[string]any{"type": "string"},
								"payload":         map[string]any{"type": "object"},
								"schema_version":  map[string]any{"type": "string"},
								"producer_id":     map[string]any{"type": "string"},
								"causation_id":    map[string]any{"type": "integer"},
								"idempotency_key": map[string]any{"type": "string"},
							},
							"required": []string{"topic", "payload"},
						},
					},
					{
						"name":        "agentbus_poll",
						"description": "Fetch events after cursor (Go engine).",
						"inputSchema": map[string]any{
							"type": "object",
							"properties": map[string]any{
								"topic":    map[string]any{"type": "string"},
								"since_id": map[string]any{"type": "integer"},
								"limit":    map[string]any{"type": "integer"},
							},
							"required": []string{"topic"},
						},
					},
					{
						"name":        "agentbus_status",
						"description": "Workspace bus health (Go engine).",
						"inputSchema": map[string]any{"type": "object", "properties": map[string]any{}},
					},
				},
			},
		}
	case "tools/call":
		return handleToolCall(es, req)
	case "ping":
		return &rpcResponse{JSONRPC: "2.0", ID: req.ID, Result: map[string]any{}}
	default:
		return &rpcResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Error:   &rpcError{Code: -32601, Message: "method not found: " + req.Method},
		}
	}
}

func handleToolCall(es *store.EventStore, req *rpcRequest) *rpcResponse {
	var params struct {
		Name      string         `json:"name"`
		Arguments map[string]any `json:"arguments"`
	}
	if err := json.Unmarshal(req.Params, &params); err != nil {
		return errResp(req.ID, -32602, err.Error())
	}
	args := params.Arguments
	if args == nil {
		args = map[string]any{}
	}

	var text string
	var isErr bool
	switch params.Name {
	case "agentbus_publish":
		topic, _ := args["topic"].(string)
		payload, _ := args["payload"].(map[string]any)
		if payload == nil {
			payload = map[string]any{}
		}
		pid, _ := args["producer_id"].(string)
		if pid == "" {
			pid = os.Getenv("AGENTBUS_PRODUCER_ID")
		}
		if pid == "" {
			pid = "go-serve"
		}
		sv, _ := args["schema_version"].(string)
		var causation *int64
		if v, ok := asInt64(args["causation_id"]); ok {
			causation = &v
		}
		var idem *string
		if v, ok := args["idempotency_key"].(string); ok && v != "" {
			idem = &v
		}
		ev, dup, err := es.Publish(context.Background(), store.PublishRequest{
			Topic:          topic,
			ProducerID:     pid,
			SchemaVersion:  sv,
			Payload:        payload,
			CausationID:    causation,
			IdempotencyKey: idem,
		})
		if err != nil {
			isErr = true
			text = mustJSON(map[string]any{"error": err.Error(), "code": 500})
		} else {
			text = mustJSON(map[string]any{
				"event_id":  ev.EventID,
				"topic":     ev.Topic,
				"timestamp": ev.Timestamp,
				"duplicate": dup,
				"span_id":   ev.SpanID,
			})
		}
	case "agentbus_poll":
		topic, _ := args["topic"].(string)
		since, _ := asInt64(args["since_id"])
		limit, _ := asInt64(args["limit"])
		if limit == 0 {
			limit = 50
		}
		res, err := es.Poll(topic, since, int(limit))
		if err != nil {
			isErr = true
			text = mustJSON(map[string]any{"error": err.Error()})
		} else {
			text = mustJSON(res)
		}
	case "agentbus_status":
		st, err := es.Status()
		if err != nil {
			isErr = true
			text = mustJSON(map[string]any{"error": err.Error()})
		} else {
			text = mustJSON(st)
		}
	default:
		isErr = true
		text = mustJSON(map[string]any{"error": "unknown tool " + params.Name})
	}

	return &rpcResponse{
		JSONRPC: "2.0",
		ID:      req.ID,
		Result: map[string]any{
			"content": []map[string]any{
				{"type": "text", "text": text},
			},
			"isError": isErr,
		},
	}
}

func asInt64(v any) (int64, bool) {
	switch t := v.(type) {
	case float64:
		return int64(t), true
	case int64:
		return t, true
	case int:
		return int64(t), true
	case json.Number:
		i, err := t.Int64()
		return i, err == nil
	default:
		return 0, false
	}
}

func mustJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func errResp(id json.RawMessage, code int, msg string) *rpcResponse {
	return &rpcResponse{JSONRPC: "2.0", ID: id, Error: &rpcError{Code: code, Message: msg}}
}

func readMessage(r *bufio.Reader) (*rpcRequest, error) {
	// Support Content-Length framing OR newline-delimited JSON for tests.
	peek, err := r.Peek(1)
	if err != nil {
		return nil, err
	}
	if peek[0] == '{' {
		line, err := r.ReadBytes('\n')
		if err != nil && len(line) == 0 {
			return nil, err
		}
		var req rpcRequest
		if err := json.Unmarshal(line, &req); err != nil {
			return nil, err
		}
		return &req, nil
	}
	// headers
	var contentLength int
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		if line == "\r\n" || line == "\n" {
			break
		}
		var n int
		if _, err := fmt.Sscanf(line, "Content-Length: %d", &n); err == nil {
			contentLength = n
		}
	}
	if contentLength <= 0 {
		return nil, fmt.Errorf("missing Content-Length")
	}
	body := make([]byte, contentLength)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, err
	}
	var req rpcRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	return &req, nil
}

func writeMessage(w io.Writer, resp *rpcResponse) error {
	body, err := json.Marshal(resp)
	if err != nil {
		return err
	}
	header := fmt.Sprintf("Content-Length: %d\r\n\r\n", len(body))
	if _, err := io.WriteString(w, header); err != nil {
		return err
	}
	_, err = w.Write(body)
	return err
}
