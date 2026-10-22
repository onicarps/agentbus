package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
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
	other, otherDup, err := s.Publish(ctx, PublishRequest{
		Topic:          "okf/handoff",
		ProducerID:     "hermes",
		Payload:        map[string]any{"from": "hermes", "to": "agy", "summary": "y"},
		IdempotencyKey: &key,
	})
	if err != nil || otherDup || other.EventID == ev2.EventID {
		t.Fatalf("scoped idempotency err=%v dup=%v id=%d", err, otherDup, other.EventID)
	}
}

func TestIdentityConfiguredWorkspaceRequiresBrokerForRestrictedTopics(t *testing.T) {
	dir := t.TempDir()
	identityDir := filepath.Join(dir, ".agentbus", "identity")
	if err := os.MkdirAll(identityDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(identityDir, "trust-root.json"), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	_, _, err = s.Publish(context.Background(), PublishRequest{
		Topic:      "okf/handoff",
		ProducerID: "factory",
		Payload:    map[string]any{"from": "factory", "to": "codex", "summary": "forged"},
	})
	if err == nil {
		t.Fatal("restricted direct publish unexpectedly succeeded")
	}
	if _, err := s.Poll("okf/handoff", 0, 50); err == nil {
		t.Fatal("restricted direct poll unexpectedly succeeded")
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

func TestChainDigestMatchesPythonCanonicalFixture(t *testing.T) {
	body := hashBody(
		1,
		genesisHash,
		"okf/handoff",
		"codex",
		"2026-10-22T00:00:00Z",
		"1.0",
		`{"from":"codex","summary":"héllo\u2028","to":"agy"}`,
		nil,
		nil,
		"idem-1",
		nil,
		nil,
		"span-123",
		nil,
		nil,
		"legacy_unverified",
		nil,
	)
	digest, err := chainDigest(genesisHash, body)
	if err != nil {
		t.Fatal(err)
	}
	const want = "43ea0f3b257f0211c87954ccff0d1ab7cb9470beeed7f839f1db2c9392dda6b0"
	if digest != want {
		encoded, _ := canonicalJSON(body)
		t.Fatalf("digest=%s want=%s canonical=%s", digest, want, encoded)
	}
}

func TestLegacyCutoverAndTransactionalCounters(t *testing.T) {
	dir := t.TempDir()
	dbDir := filepath.Join(dir, ".agentbus")
	if err := os.MkdirAll(dbDir, 0o755); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", filepath.Join(dbDir, "events.db"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`
CREATE TABLE events (
  event_id INTEGER PRIMARY KEY AUTOINCREMENT,
  topic TEXT NOT NULL, producer_id TEXT NOT NULL, timestamp TEXT NOT NULL,
  schema_version TEXT NOT NULL, payload TEXT NOT NULL, causation_id INTEGER,
  idempotency_key TEXT UNIQUE, scoped_idempotency_key TEXT,
  status TEXT NOT NULL DEFAULT 'PUBLISHED', pending_until TEXT,
  rejection_reason TEXT, projected_to_log INTEGER NOT NULL DEFAULT 0,
  sla_timeout_minutes INTEGER, sla_deadline TEXT,
  sla_cleared INTEGER NOT NULL DEFAULT 0, trace_id TEXT, span_id TEXT,
  parent_span_id TEXT, identity_envelope TEXT,
  verification_status TEXT NOT NULL DEFAULT 'legacy_unverified',
  verification_reason TEXT
);
INSERT INTO events(topic, producer_id, timestamp, schema_version, payload)
VALUES('okf/handoff', 'legacy', '2026-01-01T00:00:00Z', '1.0',
       '{"from":"legacy","to":"agy","summary":"old"}');
`)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var checkpointID int
	var checkpointHash string
	if err := store.db.QueryRow(`
SELECT CAST(i.value AS INTEGER), h.value FROM meta i JOIN meta h
ON i.key = 'checkpoint_event_id' AND h.key = 'checkpoint_hash'
`).Scan(&checkpointID, &checkpointHash); err != nil {
		t.Fatal(err)
	}
	if checkpointID != 1 || len(checkpointHash) != 64 {
		t.Fatalf("checkpoint id=%d hash=%q", checkpointID, checkpointHash)
	}
	var legacyHash sql.NullString
	if err := store.db.QueryRow(`SELECT hash FROM events WHERE event_id = 1`).Scan(&legacyHash); err != nil {
		t.Fatal(err)
	}
	if legacyHash.Valid {
		t.Fatal("legacy row was rewritten")
	}

	event, _, err := store.Publish(context.Background(), PublishRequest{
		Topic:      "okf/handoff",
		ProducerID: "codex",
		Payload:    map[string]any{"from": "codex", "to": "agy", "summary": "new"},
	})
	if err != nil {
		t.Fatal(err)
	}
	var prevHash, eventHash string
	if err := store.db.QueryRow(
		`SELECT prev_hash, hash FROM events WHERE event_id = ?`, event.EventID,
	).Scan(&prevHash, &eventHash); err != nil {
		t.Fatal(err)
	}
	if prevHash != checkpointHash || len(eventHash) != 64 {
		t.Fatalf("prev=%q event_hash=%q checkpoint=%q", prevHash, eventHash, checkpointHash)
	}
	status, err := store.Status()
	if err != nil {
		t.Fatal(err)
	}
	if status["event_count"] != int64(2) {
		t.Fatalf("status=%v", status)
	}
}
