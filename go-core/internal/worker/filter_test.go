package worker

import (
	"testing"
	"time"

	"github.com/onicarps/agentbus-go/internal/store"
)

func TestMatchFromTo(t *testing.T) {
	cfg := DefaultConfig()
	now := time.Now().UTC()
	ev := store.Event{
		EventID:   1,
		Topic:     "okf/handoff",
		Timestamp: now.Format("2006-01-02T15:04:05Z"),
		Payload: map[string]any{
			"from":    "agy",
			"to":      "grok",
			"summary": "hi",
		},
	}
	if !Match(cfg, ev, now) {
		t.Fatal("expected match agy→grok")
	}
	ev.Payload["from"] = "hermes"
	if Match(cfg, ev, now) {
		t.Fatal("hermes should not match default from=[agy]")
	}
	ev.Payload["from"] = "agy"
	ev.Payload["to"] = "hermes"
	if Match(cfg, ev, now) {
		t.Fatal("to=hermes should not match")
	}
	ev.Payload["to"] = "grok,hermes"
	if !Match(cfg, ev, now) {
		t.Fatal("multi-target should match grok substring")
	}
	ev.Payload["to"] = "swarm"
	if !Match(cfg, ev, now) {
		t.Fatal("swarm should match")
	}
}

func TestMatchMaxAge(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Dispatch.MaxEventAge = "1h"
	now := time.Now().UTC()
	old := now.Add(-2 * time.Hour)
	ev := store.Event{
		EventID:   2,
		Topic:     "okf/handoff",
		Timestamp: old.Format("2006-01-02T15:04:05Z"),
		Payload:   map[string]any{"from": "agy", "to": "grok", "summary": "stale"},
	}
	if Match(cfg, ev, now) {
		t.Fatal("stale event should not match")
	}
}
