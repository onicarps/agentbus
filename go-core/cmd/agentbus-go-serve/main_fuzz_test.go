package main

import (
	"bufio"
	"bytes"
	"testing"
)

// FuzzReadMessage covers both accepted stdio wire formats. Partial headers,
// inconsistent lengths, malformed JSON, and response-shaped input must be
// rejected without panics or unbounded allocation.
func FuzzReadMessage(f *testing.F) {
	f.Add([]byte("{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ping\"}\n"))
	f.Add([]byte("Content-Length: 41\r\n\r\n{\"jsonrpc\":\"2.0\",\"method\":\"ping\"}"))
	f.Add([]byte("Content-Length: 999999999\r\n\r\n{}"))
	f.Add([]byte("Content-Length: 2\r\n\r\n{"))
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 1<<20 {
			t.Skip()
		}
		request, err := readMessage(bufio.NewReader(bytes.NewReader(raw)))
		if err == nil && request == nil {
			t.Fatal("successful parse returned nil request")
		}
	})
}
