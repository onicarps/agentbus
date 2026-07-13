package worker

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is .agentbus/worker.yaml (version 1.0).
type Config struct {
	Version     string         `yaml:"version"`
	WorkerID    string         `yaml:"worker_id"`
	Role        string         `yaml:"role"`
	ProducerID  string         `yaml:"producer_id"`
	CursorPath  string         `yaml:"cursor_path"`
	StatePath   string         `yaml:"state_path"`
	Subscribe   []SubscribeRule `yaml:"subscribe"`
	Watch       WatchConfig    `yaml:"watch"`
	OnTask      []OnTaskAction `yaml:"on_task"`
	Budget      BudgetConfig   `yaml:"budget"`
	Dispatch    DispatchConfig `yaml:"dispatch"`
	Dedupe      DedupeConfig   `yaml:"dedupe"`
	LeaseTTLSec int            `yaml:"lease_ttl_seconds"`
}

type SubscribeRule struct {
	Topic      string   `yaml:"topic"`
	From       []string `yaml:"from"`
	To         []string `yaml:"to"`
	Initiative []string `yaml:"initiative"`
}

type WatchConfig struct {
	Mode           string   `yaml:"mode"` // auto|fsnotify|poll
	PollFallbackMS int      `yaml:"poll_fallback_ms"`
	Paths          []string `yaml:"paths"`
}

type OnTaskAction struct {
	Write *WriteAction `yaml:"write"`
	Exec  *ExecAction  `yaml:"exec"`
}

type WriteAction struct {
	Path string `yaml:"path"`
}

type ExecAction struct {
	Command    []string `yaml:"command"`
	TimeoutSec int      `yaml:"timeout_sec"`
}

type BudgetConfig struct {
	MaxDispatchesPerHour  int  `yaml:"max_dispatches_per_hour"`
	MaxConcurrentExec     int  `yaml:"max_concurrent_exec"`
	IdleSleepAfterMinutes *int `yaml:"idle_sleep_after_minutes"`
	RequireWakeAfterSleep bool `yaml:"require_wake_after_sleep"`
}

type DispatchConfig struct {
	MaxEventAge string `yaml:"max_event_age"` // e.g. 24h
}

type DedupeConfig struct {
	ByIdempotencyKey bool `yaml:"by_idempotency_key"`
	ByEventID        bool `yaml:"by_event_id"`
}

// State is persisted sleep/run state.
type State struct {
	Sleeping     bool      `json:"sleeping"`
	SleptAt      *time.Time `json:"slept_at,omitempty"`
	LastMatchAt  *time.Time `json:"last_match_at,omitempty"`
	LastEventID  int64     `json:"last_event_id"`
	MatchesTotal int64     `json:"matches_total"`
	Dispatches   int64     `json:"dispatches_total"`
}

// DefaultConfig returns implementer-oriented defaults.
//
// idle_sleep_after_minutes is null by default: auto-sleep after 30m caused
// dogfood failure (Agy handoffs #802/#803 while worker slept; Grok never
// saw push). Stale work is gated by max_event_age, not auto-sleep.
// Set idle_sleep_after_minutes: 30 in worker.yaml if you want stand-down hygiene.
func DefaultConfig() *Config {
	return &Config{
		Version:    "1.0",
		WorkerID:   "implementer-1",
		Role:       "implementer",
		ProducerID: "grok",
		CursorPath: ".agentbus/worker.cursor",
		StatePath:  ".agentbus/worker.state.json",
		Subscribe: []SubscribeRule{{
			Topic: "okf/handoff",
			From:  []string{"agy"},
			// Worker identities; event.to swarm/*/all still matches via filter broadcast rules.
			To: []string{"grok", "implementer"},
		}},
		Watch: WatchConfig{
			Mode:           "auto",
			PollFallbackMS: 1500,
			Paths: []string{
				".agentbus/events.db",
				".agentbus/events.db-wal",
			},
		},
		OnTask: []OnTaskAction{{
			Write: &WriteAction{Path: ".agentbus/WAKE.json"},
		}},
		Budget: BudgetConfig{
			MaxDispatchesPerHour:  60,
			MaxConcurrentExec:     1,
			IdleSleepAfterMinutes: nil, // stay awake for handoffs; use max_event_age for stale
			RequireWakeAfterSleep: true,
		},
		Dispatch: DispatchConfig{
			MaxEventAge: "24h",
		},
		Dedupe: DedupeConfig{
			ByIdempotencyKey: true,
			ByEventID:        true,
		},
		LeaseTTLSec: 300,
	}
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	cfg := DefaultConfig()
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, err
	}
	if cfg.WorkerID == "" {
		return nil, fmt.Errorf("worker_id required")
	}
	if len(cfg.Subscribe) == 0 {
		return nil, fmt.Errorf("subscribe rules required")
	}
	if cfg.Watch.PollFallbackMS <= 0 {
		cfg.Watch.PollFallbackMS = 1500
	}
	if cfg.Dispatch.MaxEventAge == "" {
		cfg.Dispatch.MaxEventAge = "24h"
	}
	if cfg.LeaseTTLSec <= 0 {
		cfg.LeaseTTLSec = 300
	}
	return cfg, nil
}

func (c *Config) MaxEventAge() time.Duration {
	d, err := time.ParseDuration(c.Dispatch.MaxEventAge)
	if err != nil {
		return 24 * time.Hour
	}
	return d
}

func ResolvePath(workspace, rel string) string {
	if filepath.IsAbs(rel) {
		return rel
	}
	return filepath.Join(workspace, rel)
}

// WritePresetImplementer writes a default worker.yaml for AEs.
func WritePresetImplementer(workspace, to string) (string, error) {
	cfg := DefaultConfig()
	if to != "" {
		cfg.Subscribe[0].To = []string{to, "implementer", "swarm", "*", "all"}
		cfg.WorkerID = to + "-1"
		cfg.ProducerID = to
	}
	dir := filepath.Join(workspace, ".agentbus")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	path := filepath.Join(dir, "worker.yaml")
	b, err := yaml.Marshal(cfg)
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(path, b, 0o644); err != nil {
		return "", err
	}
	return path, nil
}
