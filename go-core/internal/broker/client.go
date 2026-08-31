// Package broker implements the versioned framed Unix broker client.
package broker

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"time"
	"github.com/google/uuid"
)

const ProtocolVersion = "1"
const maxFrame = 4 * 1024 * 1024

type Client struct { SocketPath string; Timeout time.Duration }

func (c Client) Call(operation string, body any, result any) error {
	if c.SocketPath == "" { return fmt.Errorf("broker_socket_required") }
	timeout := c.Timeout; if timeout <= 0 { timeout = 30 * time.Second }
	conn, err := net.DialTimeout("unix", c.SocketPath, timeout); if err != nil { return fmt.Errorf("broker_unavailable: %w", err) }
	defer conn.Close(); _ = conn.SetDeadline(time.Now().Add(timeout))
	id := uuid.NewString()
	req := map[string]any{"protocol_version": ProtocolVersion, "request_id": id, "operation": operation, "body": body}
	raw, err := json.Marshal(req); if err != nil { return err }; if len(raw) > maxFrame { return fmt.Errorf("invalid_broker_frame_size") }
	var header [4]byte; binary.BigEndian.PutUint32(header[:], uint32(len(raw)))
	if _, err = conn.Write(append(header[:], raw...)); err != nil { return fmt.Errorf("broker_unavailable: %w", err) }
	if _, err = io.ReadFull(conn, header[:]); err != nil { return fmt.Errorf("truncated_broker_frame: %w", err) }
	size := binary.BigEndian.Uint32(header[:]); if size < 2 || size > maxFrame { return fmt.Errorf("invalid_broker_frame_size") }
	buf := make([]byte, size); if _, err = io.ReadFull(conn, buf); err != nil { return fmt.Errorf("truncated_broker_frame: %w", err) }
	var resp struct { ProtocolVersion string `json:"protocol_version"`; RequestID string `json:"request_id"`; OK bool `json:"ok"`; Result json.RawMessage `json:"result"`; Error struct { Code string `json:"code"` } `json:"error"` }
	if err = json.Unmarshal(buf, &resp); err != nil { return fmt.Errorf("invalid_broker_response") }
	if resp.ProtocolVersion != ProtocolVersion || resp.RequestID != id { return fmt.Errorf("invalid_broker_response") }
	if !resp.OK { if resp.Error.Code == "" { return fmt.Errorf("broker_request_failed") }; return fmt.Errorf(resp.Error.Code) }
	if result == nil { return nil }; return json.Unmarshal(resp.Result, result)
}
