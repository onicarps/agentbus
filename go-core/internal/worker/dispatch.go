package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/onicarps/agentbus-go/internal/store"
)

// WakeFile is the default on_task write payload (PRD §5.3 / v0.13 webhook).
type WakeFile struct {
	SchemaVersion string            `json:"schema_version"`
	WokenAt       string            `json:"woken_at"`
	WorkerID      string            `json:"worker_id"`
	EventID       int64             `json:"event_id"`
	Topic         string            `json:"topic"`
	TraceID       *string           `json:"trace_id"`
	CausationID   *int64            `json:"causation_id"`
	Payload       map[string]any    `json:"payload"`
	Hint          map[string]string `json:"hint"`
}

// DispatchResult reports side effects for worker metrics.
type DispatchResult struct {
	WebhookAttempted bool
	WebhookOK        bool
	DeadLetterOK     bool
}

// Webhook delivery policy (PRD v0.13 + resilient messaging GO 2026-07-22).
const (
	webhookTimeout     = 5 * time.Second
	webhookMaxTries    = 3
	webhookBackoffBase = 200 * time.Millisecond
	webhookBackoffMax  = 2 * time.Second
)

func Dispatch(cfg *Config, workspace string, ev store.Event) (DispatchResult, error) {
	return DispatchWithStore(cfg, workspace, ev, nil)
}

// DispatchWithStore is Dispatch plus optional EventStore for dead-letter escalate.
func DispatchWithStore(cfg *Config, workspace string, ev store.Event, es *store.EventStore) (DispatchResult, error) {
	var res DispatchResult
	wroteFile := false
	for _, action := range cfg.OnTask {
		if action.Write != nil {
			if err := writeWake(cfg, workspace, action.Write.Path, ev); err != nil {
				return res, err
			}
			wroteFile = true
		}
		if action.Exec != nil && len(action.Exec.Command) > 0 {
			if err := runExec(workspace, action.Exec, ev); err != nil {
				return res, err
			}
		}
	}

	// Dual-signal: webhook is additive push for Hermes/Factory runtimes.
	if cfg.WakeMode == "webhook" {
		res.WebhookAttempted = true
		if err := postWebhookWithRetry(cfg, ev); err != nil {
			log.Printf("webhook failed after retries event_id=%d: %v", ev.EventID, err)
			// Escalate to okf/dead-letter (RETRY_EXHAUSTED) + spillover file.
			if escalateWebhookDeadLetter(es, workspace, cfg, ev, err) {
				res.DeadLetterOK = true
			}
			if !wroteFile {
				return res, fmt.Errorf("webhook: %w", err)
			}
			// File wake landed — cursor may advance; count fail for metrics.
			return res, nil
		}
		res.WebhookOK = true
	}
	return res, nil
}

func makeWakeFile(cfg *Config, ev store.Event) WakeFile {
	return WakeFile{
		SchemaVersion: "1.0",
		WokenAt:       time.Now().UTC().Format("2006-01-02T15:04:05Z"),
		WorkerID:      cfg.WorkerID,
		EventID:       ev.EventID,
		Topic:         ev.Topic,
		TraceID:       ev.TraceID,
		CausationID:   ev.CausationID,
		Payload:       ev.Payload,
		Hint: map[string]string{
			"suggested_prompt_tokens_budget": "task-only",
			"do_not":                         "full-system-prompt-replay-for-idle-check",
			"ack_with_causation_id":           fmt.Sprintf("%d", ev.EventID),
		},
	}
}

func postWebhookWithRetry(cfg *Config, ev store.Event) error {
	var last error
	for attempt := 1; attempt <= webhookMaxTries; attempt++ {
		last = postWebhookOnce(cfg, ev)
		if last == nil {
			return nil
		}
		if attempt < webhookMaxTries {
			time.Sleep(webhookBackoff(attempt - 1))
		}
	}
	return last
}

// webhookBackoff returns full-jitter delay for zero-based failure index.
// delay = uniform(0, min(max, base * 2^attempt))
func webhookBackoff(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	raw := webhookBackoffBase * time.Duration(1<<attempt)
	if raw > webhookBackoffMax {
		raw = webhookBackoffMax
	}
	if raw <= 0 {
		return 0
	}
	// Full jitter: [0, raw]
	return time.Duration(rand.Int63n(int64(raw) + 1))
}

// escalateWebhookDeadLetter publishes okf/dead-letter RETRY_EXHAUSTED and
// always appends a spillover JSONL line for ops recovery.
func escalateWebhookDeadLetter(es *store.EventStore, workspace string, cfg *Config, ev store.Event, last error) bool {
	summary := fmt.Sprintf(
		"Webhook delivery retry exhausted worker=%s event_id=%d url=%s err=%v",
		cfg.WorkerID, ev.EventID, cfg.WebhookUrl, last,
	)
	if len(summary) > 2000 {
		summary = summary[:2000]
	}
	original := map[string]any{
		"event_id":    ev.EventID,
		"topic":       ev.Topic,
		"producer_id": ev.ProducerID,
		"payload":     ev.Payload,
		"worker_id":   cfg.WorkerID,
		"webhook_url": cfg.WebhookUrl,
		"error":       fmt.Sprintf("%v", last),
	}
	// File spillover always (even when bus publish works) for audit trail under load.
	_ = appendSpillover(workspace, map[string]any{
		"reason":             "RETRY_EXHAUSTED",
		"original_event_id":  ev.EventID,
		"original_event":     original,
		"summary":            summary,
		"spilled_at":         time.Now().UTC().Format("2006-01-02T15:04:05Z"),
		"kind":               "webhook_delivery",
	})

	if es == nil {
		return false
	}
	// original_event_id schema requires >= 1
	oid := ev.EventID
	if oid < 1 {
		oid = 1
	}
	idem := fmt.Sprintf("webhook-retry-exhausted:%s:%d", cfg.WorkerID, ev.EventID)
	causation := ev.EventID
	payload := map[string]any{
		"reason":             "RETRY_EXHAUSTED",
		"original_event_id":  oid,
		"original_event":     original,
		"summary":            summary,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _, err := es.Publish(ctx, store.PublishRequest{
		Topic:          "okf/dead-letter",
		ProducerID:     "agentbus",
		SchemaVersion:  "1.0",
		Payload:        payload,
		CausationID:    &causation,
		IdempotencyKey: &idem,
	})
	if err != nil {
		log.Printf("dead-letter publish failed event_id=%d: %v", ev.EventID, err)
		return false
	}
	return true
}

func appendSpillover(workspace string, record map[string]any) error {
	dir := filepath.Join(workspace, ".agentbus", "dead-letter")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	path := filepath.Join(dir, "spillover.jsonl")
	b, err := json.Marshal(record)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write(append(b, '\n')); err != nil {
		return err
	}
	return nil
}

func postWebhookOnce(cfg *Config, ev store.Event) error {
	wf := makeWakeFile(cfg, ev)
	b, err := json.Marshal(wf)
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: webhookTimeout}
	req, err := http.NewRequest("POST", cfg.WebhookUrl, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "agentbus-go-worker/0.13")
	req.Header.Set("X-AgentBus-Event-Id", fmt.Sprintf("%d", ev.EventID))
	req.Header.Set("X-AgentBus-Worker-Id", cfg.WorkerID)
	idem := fmt.Sprintf("%s:%d", cfg.WorkerID, ev.EventID)
	req.Header.Set("Idempotency-Key", idem)
	if tok := resolveWebhookToken(cfg); tok != "" {
		req.Header.Set("X-AgentBus-Token", tok)
		req.Header.Set("Authorization", "Bearer "+tok)
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	return nil
}

func resolveWebhookToken(cfg *Config) string {
	if cfg.WebhookToken != "" {
		return cfg.WebhookToken
	}
	// Env overrides for secrets not stored in yaml
	if v := os.Getenv("AGENTBUS_WEBHOOK_TOKEN"); v != "" {
		return v
	}
	return ""
}

func writeWake(cfg *Config, workspace, rel string, ev store.Event) error {
	path := ResolvePath(workspace, rel)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	wf := makeWakeFile(cfg, ev)
	b, err := json.MarshalIndent(wf, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".*.tmp")
	if err != nil {
		return err
	}
	tmp := f.Name()
	if _, err := f.Write(append(b, '\n')); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

func runExec(workspace string, ex *ExecAction, ev store.Event) error {
	timeout := time.Duration(ex.TimeoutSec) * time.Second
	if timeout <= 0 {
		timeout = time.Hour
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, ex.Command[0], ex.Command[1:]...)
	cmd.Dir = workspace
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("AGENTBUS_WAKE_EVENT_ID=%d", ev.EventID),
		"AGENTBUS_WAKE_TOPIC="+ev.Topic,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("exec: %w: %s", err, string(out))
	}
	return nil
}
