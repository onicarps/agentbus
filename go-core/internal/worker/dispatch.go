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

// WakeFile is the default on_task write payload (PRD §5.3).
type WakeFile struct {
	SchemaVersion string         `json:"schema_version"`
	WokenAt       string         `json:"woken_at"`
	WorkerID      string         `json:"worker_id"`
	EventID       int64          `json:"event_id"`
	Topic         string         `json:"topic"`
	TraceID       *string        `json:"trace_id"`
	CausationID   *int64         `json:"causation_id"`
	Payload       map[string]any `json:"payload"`
	Hint          map[string]string `json:"hint"`
}

func Dispatch(cfg *Config, workspace string, ev store.Event) error {
	for _, action := range cfg.OnTask {
		if action.Write != nil {
			if cfg.WakeMode == "webhook" {
				if err := postWebhook(cfg, ev); err != nil {
					log.Printf("webhook failed: %v", err)
				}
			} else {
				if err := writeWake(cfg, workspace, action.Write.Path, ev); err != nil {
					return err
				}
			}
		}
		if action.Exec != nil && len(action.Exec.Command) > 0 {
			if err := runExec(workspace, action.Exec, ev); err != nil {
				return err
			}
		}
	}
	return nil
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
		},
	}
}

func postWebhook(cfg *Config, ev store.Event) error {
	wf := makeWakeFile(cfg, ev)
	b, err := json.Marshal(wf)
	if err != nil {
		return err
	}
	
	client := &http.Client{Timeout: 5 * time.Second}
	req, err := http.NewRequest("POST", cfg.WebhookUrl, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	
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
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(b, '\n'), 0o644); err != nil {
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
	// argv only — no shell
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
