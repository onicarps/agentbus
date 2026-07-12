package store

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestPublishPollRoundTrip(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	ctx := context.Background()
	ev, dup, err := s.Publish(ctx, PublishRequest{
		Topic:         "okf/handoff",
		ProducerID:    "grok",
		SchemaVersion: "1.0",
		Payload: map[string]any{
			"from":    "grok",
			"to":      "agy",
			"summary": "hello go",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if dup {
		t.Fatal("unexpected duplicate")
	}
	if ev.EventID != 1 {
		t.Fatalf("event_id=%d want 1", ev.EventID)
	}

	poll, err := s.Poll("okf/handoff", 0, 50)
	if err != nil {
		t.Fatal(err)
	}
	if len(poll.Events) != 1 {
		t.Fatalf("events=%d want 1", len(poll.Events))
	}
	if poll.Events[0].Payload["summary"] != "hello go" {
		t.Fatalf("payload=%v", poll.Events[0].Payload)
	}
	if poll.LatestID != 1 {
		t.Fatalf("latest=%d", poll.LatestID)
	}
}

func TestIdempotencyKey(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	key := "idem-1"
	ctx := context.Background()
	_, _, err = s.Publish(ctx, PublishRequest{
		Topic:          "okf/handoff",
		ProducerID:     "grok",
		Payload:        map[string]any{"from": "a", "to": "b", "summary": "x"},
		IdempotencyKey: &key,
	})
	if err != nil {
		t.Fatal(err)
	}
	ev2, dup, err := s.Publish(ctx, PublishRequest{
		Topic:          "okf/handoff",
		ProducerID:     "grok",
		Payload:        map[string]any{"from": "a", "to": "b", "summary": "x"},
		IdempotencyKey: &key,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !dup || ev2.EventID != 1 {
		t.Fatalf("dup=%v id=%d", dup, ev2.EventID)
	}
}

func TestSingleWriterConcurrent(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	const n = 50
	var wg sync.WaitGroup
	errs := make(chan error, n)
	ctx := context.Background()
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _, err := s.Publish(ctx, PublishRequest{
				Topic:      "okf/handoff",
				ProducerID: "swarm",
				Payload: map[string]any{
					"from":    "swarm",
					"to":      "all",
					"summary": fmt.Sprintf("msg-%d", i),
				},
			})
			if err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	poll, err := s.Poll("okf/handoff", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(poll.Events) != n {
		t.Fatalf("got %d events want %d", len(poll.Events), n)
	}
	// sequential ids
	for i, e := range poll.Events {
		if e.EventID != int64(i+1) {
			t.Fatalf("event[%d].id=%d", i, e.EventID)
		}
	}
}

func TestDBPathUnderAgentbus(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	want := filepath.Join(dir, ".agentbus", "events.db")
	if s.dbPath != want {
		t.Fatalf("dbPath=%s want %s", s.dbPath, want)
	}
}

func TestPublishTimeout(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, _, err = s.Publish(ctx, PublishRequest{
		Topic:      "okf/handoff",
		ProducerID: "grok",
		Payload:    map[string]any{"from": "g", "to": "a", "summary": "t"},
	})
	if err != nil {
		t.Fatal(err)
	}
}
