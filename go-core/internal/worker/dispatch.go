package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
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
}

// Webhook delivery policy (PRD v0.13 + WEBHOOK_SPEC_GO).
const (
	webhookTimeout     = 5 * time.Second
	webhookMaxTries    = 3
	webhookBackoffBase = 200 * time.Millisecond
)

func Dispatch(cfg *Config, workspace string, ev store.Event) (DispatchResult, error) {
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
			time.Sleep(webhookBackoffBase * time.Duration(1<<(attempt-1)))
		}
	}
	return last
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
