// Package store implements a single-writer SQLite event log for AgentBus Go core.
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

const (
	StatusPublished = "PUBLISHED"
)

// Event is one bus event row (parity subset with Python EventStore).
type Event struct {
	EventID        int64          `json:"event_id"`
	Topic          string         `json:"topic"`
	ProducerID     string         `json:"producer_id"`
	Timestamp      string         `json:"timestamp"`
	SchemaVersion  string         `json:"schema_version"`
	Payload        map[string]any `json:"payload"`
	CausationID    *int64         `json:"causation_id,omitempty"`
	IdempotencyKey *string        `json:"idempotency_key,omitempty"`
	Status         string         `json:"status"`
	TraceID        *string        `json:"trace_id,omitempty"`
	SpanID         *string        `json:"span_id,omitempty"`
	ParentSpanID   *string        `json:"parent_span_id,omitempty"`
}

// PublishRequest is an async write request handled by the single-writer loop.
type PublishRequest struct {
	Topic          string
	ProducerID     string
	SchemaVersion  string
	Payload        map[string]any
	CausationID    *int64
	IdempotencyKey *string
	TraceID        *string
	ParentSpanID   *string
	Result         chan PublishResult
}

// PublishResult is returned on PublishRequest.Result.
type PublishResult struct {
	Event     Event
	Duplicate bool
	Err       error
}

// PollResult matches Python poll envelope.
type PollResult struct {
	Events   []Event `json:"events"`
	LatestID int64   `json:"latest_id"`
	HasMore  bool    `json:"has_more"`
}

// EventStore is a SQLite-backed event log with a single writer goroutine.
type EventStore struct {
	db      *sql.DB
	dbPath  string
	writeCh chan PublishRequest
	wg      sync.WaitGroup
	closed  chan struct{}
	closeMu sync.Mutex
	closedB bool
}

// Open creates or opens events.db under workspace/.agentbus and starts the writer.
func Open(workspace string) (*EventStore, error) {
	ws, err := filepath.Abs(workspace)
	if err != nil {
		return nil, err
	}
	dir := filepath.Join(ws, ".agentbus")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	dbPath := filepath.Join(dir, "events.db")
	// modernc sqlite DSN
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, err
	}
	// Single connection — all writes serialize through writer goroutine anyway.
	db.SetMaxOpenConns(1)

	s := &EventStore{
		db:      db,
		dbPath:  dbPath,
		writeCh: make(chan PublishRequest, 256),
		closed:  make(chan struct{}),
	}
	if err := s.configurePRAGMAs(); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := s.initSchema(); err != nil {
		_ = db.Close()
		return nil, err
	}
	s.wg.Add(1)
	go s.writerLoop()
	return s, nil
}

func (s *EventStore) configurePRAGMAs() error {
	if _, err := s.db.Exec(`PRAGMA synchronous = NORMAL`); err != nil {
		return err
	}
	journal := "WAL"
	busy := 5000
	if runtime.GOOS == "windows" {
		journal = "MEMORY"
		busy = 10000
	}
	if v := os.Getenv("AGENTBUS_SQLITE_JOURNAL"); v != "" {
		journal = v
	}
	if v := os.Getenv("AGENTBUS_SQLITE_BUSY_TIMEOUT"); v != "" {
		fmt.Sscanf(v, "%d", &busy)
	}
	if _, err := s.db.Exec(fmt.Sprintf(`PRAGMA journal_mode = %s`, journal)); err != nil {
		return err
	}
	if _, err := s.db.Exec(fmt.Sprintf(`PRAGMA busy_timeout = %d`, busy)); err != nil {
		return err
	}
	return nil
}

func (s *EventStore) initSchema() error {
	_, err := s.db.Exec(`
CREATE TABLE IF NOT EXISTS events (
  event_id INTEGER PRIMARY KEY AUTOINCREMENT,
  topic TEXT NOT NULL,
  producer_id TEXT NOT NULL,
  timestamp TEXT NOT NULL,
  schema_version TEXT NOT NULL,
  payload TEXT NOT NULL,
  causation_id INTEGER,
  idempotency_key TEXT UNIQUE,
  status TEXT NOT NULL DEFAULT 'PUBLISHED',
  pending_until TEXT,
  rejection_reason TEXT,
  projected_to_log INTEGER NOT NULL DEFAULT 0,
  sla_timeout_minutes INTEGER,
  sla_deadline TEXT,
  sla_cleared INTEGER NOT NULL DEFAULT 0,
  trace_id TEXT,
  span_id TEXT,
  parent_span_id TEXT
);
CREATE INDEX IF NOT EXISTS idx_events_topic_id ON events(topic, event_id);
CREATE INDEX IF NOT EXISTS idx_events_trace_id ON events(trace_id);
`)
	return err
}

func (s *EventStore) writerLoop() {
	defer s.wg.Done()
	for {
		select {
		case <-s.closed:
			// drain remaining
			for {
				select {
				case req := <-s.writeCh:
					s.handlePublish(req)
				default:
					return
				}
			}
		case req := <-s.writeCh:
			s.handlePublish(req)
		}
	}
}

func (s *EventStore) handlePublish(req PublishRequest) {
	res := PublishResult{}
	defer func() { req.Result <- res }()

	if req.IdempotencyKey != nil && *req.IdempotencyKey != "" {
		existing, err := s.loadByIdempotency(*req.IdempotencyKey)
		if err != nil {
			res.Err = err
			return
		}
		if existing != nil {
			res.Event = *existing
			res.Duplicate = true
			return
		}
	}

	payloadJSON, err := json.Marshal(req.Payload)
	if err != nil {
		res.Err = err
		return
	}
	ts := time.Now().UTC().Format("2006-01-02T15:04:05Z")
	sv := req.SchemaVersion
	if sv == "" {
		sv = "1.0"
	}
	span := fmt.Sprintf("span-%x", time.Now().UnixNano())

	var causation any
	if req.CausationID != nil {
		causation = *req.CausationID
	}
	var idem any
	if req.IdempotencyKey != nil {
		idem = *req.IdempotencyKey
	}
	var trace, parent any
	if req.TraceID != nil {
		trace = *req.TraceID
	}
	if req.ParentSpanID != nil {
		parent = *req.ParentSpanID
	}

	r, err := s.db.Exec(`
INSERT INTO events (
  topic, producer_id, timestamp, schema_version, payload,
  causation_id, idempotency_key, status, span_id, trace_id, parent_span_id
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		req.Topic, req.ProducerID, ts, sv, string(payloadJSON),
		causation, idem, StatusPublished, span, trace, parent,
	)
	if err != nil {
		res.Err = err
		return
	}
	id, err := r.LastInsertId()
	if err != nil {
		res.Err = err
		return
	}
	ev := Event{
		EventID:        id,
		Topic:          req.Topic,
		ProducerID:     req.ProducerID,
		Timestamp:      ts,
		SchemaVersion:  sv,
		Payload:        req.Payload,
		CausationID:    req.CausationID,
		IdempotencyKey: req.IdempotencyKey,
		Status:         StatusPublished,
		SpanID:         &span,
		TraceID:        req.TraceID,
		ParentSpanID:   req.ParentSpanID,
	}
	res.Event = ev
}

func (s *EventStore) loadByIdempotency(key string) (*Event, error) {
	row := s.db.QueryRow(`
SELECT event_id, topic, producer_id, timestamp, schema_version, payload,
       causation_id, idempotency_key, status, trace_id, span_id, parent_span_id
FROM events WHERE idempotency_key = ?`, key)
	return scanEvent(row)
}

// Publish enqueues a write on the single-writer loop and waits for the result.
func (s *EventStore) Publish(ctx context.Context, req PublishRequest) (Event, bool, error) {
	if req.Result == nil {
		req.Result = make(chan PublishResult, 1)
	}
	select {
	case <-s.closed:
		return Event{}, false, fmt.Errorf("store closed")
	case <-ctx.Done():
		return Event{}, false, ctx.Err()
	case s.writeCh <- req:
	}
	select {
	case <-ctx.Done():
		return Event{}, false, ctx.Err()
	case res := <-req.Result:
		return res.Event, res.Duplicate, res.Err
	}
}

// Poll returns events after sinceID for topic (PUBLISHED only for MVP parity).
func (s *EventStore) Poll(topic string, sinceID int64, limit int) (PollResult, error) {
	if limit <= 0 {
		limit = 50
	}
	if limit > 100 {
		limit = 100
	}
	rows, err := s.db.Query(`
SELECT event_id, topic, producer_id, timestamp, schema_version, payload,
       causation_id, idempotency_key, status, trace_id, span_id, parent_span_id
FROM events
WHERE topic = ? AND event_id > ? AND status = ?
ORDER BY event_id ASC
LIMIT ?`, topic, sinceID, StatusPublished, limit+1)
	if err != nil {
		return PollResult{}, err
	}
	defer rows.Close()

	var events []Event
	for rows.Next() {
		ev, err := scanEventRows(rows)
		if err != nil {
			return PollResult{}, err
		}
		events = append(events, *ev)
	}
	hasMore := false
	if len(events) > limit {
		hasMore = true
		events = events[:limit]
	}
	var latest int64 = sinceID
	if len(events) > 0 {
		latest = events[len(events)-1].EventID
	}
	return PollResult{Events: events, LatestID: latest, HasMore: hasMore}, nil
}

// Status returns a minimal health payload.
func (s *EventStore) Status() (map[string]any, error) {
	var count, maxID int64
	_ = s.db.QueryRow(`SELECT COUNT(*), COALESCE(MAX(event_id), 0) FROM events`).Scan(&count, &maxID)
	return map[string]any{
		"event_count":      count,
		"latest_event_id":  maxID,
		"db_path":          s.dbPath,
		"engine":           "go",
	}, nil
}

// Close stops the writer and closes the DB.
func (s *EventStore) Close() error {
	s.closeMu.Lock()
	if s.closedB {
		s.closeMu.Unlock()
		return nil
	}
	s.closedB = true
	close(s.closed)
	s.closeMu.Unlock()
	s.wg.Wait()
	return s.db.Close()
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanEvent(row rowScanner) (*Event, error) {
	var (
		ev             Event
		payloadStr     string
		causation      sql.NullInt64
		idem           sql.NullString
		trace, span, p sql.NullString
	)
	err := row.Scan(
		&ev.EventID, &ev.Topic, &ev.ProducerID, &ev.Timestamp, &ev.SchemaVersion, &payloadStr,
		&causation, &idem, &ev.Status, &trace, &span, &p,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	_ = json.Unmarshal([]byte(payloadStr), &ev.Payload)
	if ev.Payload == nil {
		ev.Payload = map[string]any{}
	}
	if causation.Valid {
		v := causation.Int64
		ev.CausationID = &v
	}
	if idem.Valid {
		v := idem.String
		ev.IdempotencyKey = &v
	}
	if trace.Valid {
		v := trace.String
		ev.TraceID = &v
	}
	if span.Valid {
		v := span.String
		ev.SpanID = &v
	}
	if p.Valid {
		v := p.String
		ev.ParentSpanID = &v
	}
	return &ev, nil
}

func scanEventRows(rows *sql.Rows) (*Event, error) {
	return scanEvent(rows)
}
