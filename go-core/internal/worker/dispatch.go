package worker

import (
	"context"
	"encoding/json"
	"fmt"
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
			if err := writeWake(cfg, workspace, action.Write.Path, ev); err != nil {
				return err
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

func writeWake(cfg *Config, workspace, rel string, ev store.Event) error {
	path := ResolvePath(workspace, rel)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	wf := WakeFile{
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
