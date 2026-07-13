package worker

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/onicarps/agentbus-go/internal/store"
)

// Service is the non-LLM wake loop.
type Service struct {
	cfg       *Config
	workspace string
	store     *store.EventStore
	leases    *LeaseStore
	mu        sync.Mutex
	state     State
	stopCh    chan struct{}
	wakeCh    chan struct{}
}

func NewService(workspace string, cfg *Config) (*Service, error) {
	es, err := store.Open(workspace)
	if err != nil {
		return nil, err
	}
	ls, err := NewLeaseStore(es.DB(), workspace)
	if err != nil {
		_ = es.Close()
		return nil, err
	}
	s := &Service{
		cfg:       cfg,
		workspace: workspace,
		store:     es,
		leases:    ls,
		stopCh:    make(chan struct{}),
		wakeCh:    make(chan struct{}, 1),
	}
	_ = s.loadState()
	return s, nil
}

func (s *Service) Close() error {
	select {
	case <-s.stopCh:
	default:
		close(s.stopCh)
	}
	return s.store.Close()
}

func (s *Service) cursorPath() string {
	return ResolvePath(s.workspace, s.cfg.CursorPath)
}

func (s *Service) statePath() string {
	return ResolvePath(s.workspace, s.cfg.StatePath)
}

func (s *Service) loadState() error {
	b, err := os.ReadFile(s.statePath())
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	return json.Unmarshal(b, &s.state)
}

func (s *Service) saveState() error {
	path := s.statePath()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(s.state, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(b, '\n'), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// Sleep pauses dispatch; cursor held (not advanced).
func (s *Service) Sleep() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state.Sleeping = true
	now := time.Now().UTC()
	s.state.SleptAt = &now
	return s.saveState()
}

// Wake resumes. skipBacklog=true fast-forwards cursor (PRD §14.1.1 default).
func (s *Service) Wake(skipBacklog bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if skipBacklog {
		maxID, err := s.store.MaxEventID()
		if err != nil {
			return err
		}
		if err := SaveCursor(s.cursorPath(), maxID); err != nil {
			return err
		}
		s.state.LastEventID = maxID
	}
	s.state.Sleeping = false
	s.state.SleptAt = nil
	if err := s.saveState(); err != nil {
		return err
	}
	s.kick()
	return nil
}

func (s *Service) kick() {
	select {
	case s.wakeCh <- struct{}{}:
	default:
	}
}

// StatusJSON returns operator-facing status.
func (s *Service) StatusJSON() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, _ := LoadCursor(s.cursorPath())
	maxID, _ := s.store.MaxEventID()
	out := map[string]any{
		"worker_id":            s.cfg.WorkerID,
		"sleeping":             s.state.Sleeping,
		"cursor":               cur,
		"max_event_id":         maxID,
		"last_event_id":        s.state.LastEventID,
		"matches_total":        s.state.MatchesTotal,
		"dispatches_total":     s.state.Dispatches,
		"llm_invocations_hour": 0,
		"engine":               "go",
	}
	return json.MarshalIndent(out, "", "  ")
}

// Once drains new events once (CI/debug).
func (s *Service) Once() (int, error) {
	return s.drain()
}

func (s *Service) drain() (int, error) {
	s.mu.Lock()
	sleeping := s.state.Sleeping
	s.mu.Unlock()
	if sleeping {
		return 0, nil
	}

	cur, err := LoadCursor(s.cursorPath())
	if err != nil {
		return 0, err
	}
	now := time.Now().UTC()
	matched := 0

	topics := map[string]struct{}{}
	for _, r := range s.cfg.Subscribe {
		topics[r.Topic] = struct{}{}
	}
	// track highest processed for non-match advance
	high := cur
	for topic := range topics {
		// page until empty
		since := cur
		for {
			res, err := s.store.Poll(topic, since, 100)
			if err != nil {
				return matched, err
			}
			if len(res.Events) == 0 {
				break
			}
			for _, ev := range res.Events {
				if ev.EventID > high {
					high = ev.EventID
				}
				if !Match(s.cfg, ev, now) {
					if err := SaveCursor(s.cursorPath(), ev.EventID); err != nil {
						return matched, err
					}
					continue
				}
				matched++
				s.mu.Lock()
				s.state.MatchesTotal++
				t := now
				s.state.LastMatchAt = &t
				s.mu.Unlock()

				role := s.cfg.Role
				if role == "" {
					role = s.cfg.WorkerID
				}
				ok, leaseID, err := s.leases.TryAcquire(ev.Topic, ev.EventID, s.cfg.WorkerID, role, s.cfg.LeaseTTLSec)
				if err != nil {
					return matched, err
				}
				if !ok {
					// Same-role peer owns it — advance so we do not stall.
					if err := SaveCursor(s.cursorPath(), ev.EventID); err != nil {
						return matched, err
					}
					continue
				}

				// Dispatch failures are non-fatal by default (PRD on_task):
				// log, advance cursor, keep lease as short tombstone (no early release
				// on success — factory-droid CR #1 poison-pill + #3 crash window).
				if err := Dispatch(s.cfg, s.workspace, ev); err != nil {
					log.Printf("dispatch event_id=%d: %v (advancing cursor)", ev.EventID, err)
					if err := SaveCursor(s.cursorPath(), ev.EventID); err != nil {
						return matched, err
					}
					// Do not Release on failure either if we advanced — optional release
					// only wastes a tombstone; leave TTL to expire.
					_ = leaseID
					continue
				}
				// Success: do NOT Release — lease TTL is dedupe tombstone until expiry.
				if err := SaveCursor(s.cursorPath(), ev.EventID); err != nil {
					return matched, err
				}
				s.mu.Lock()
				s.state.Dispatches++
				s.state.LastEventID = ev.EventID
				_ = s.saveState()
				s.mu.Unlock()
			}
			since = res.LatestID
			if !res.HasMore {
				break
			}
		}
	}
	return matched, nil
}

// Run blocks until Close (fsnotify + poll fallback).
func (s *Service) Run() error {
	mode := s.cfg.Watch.Mode
	if mode == "" || mode == "auto" {
		mode = "fsnotify"
	}

	var (
		watcher  *fsnotify.Watcher
		fsEvents <-chan fsnotify.Event
		fsErrors <-chan error
	)
	if mode == "fsnotify" {
		w, err := fsnotify.NewWatcher()
		if err != nil {
			log.Printf("fsnotify unavailable, falling back to poll: %v", err)
			mode = "poll"
		} else {
			watcher = w
			defer watcher.Close()
			fsEvents = watcher.Events
			fsErrors = watcher.Errors
			seen := map[string]struct{}{}
			for _, p := range s.cfg.Watch.Paths {
				full := ResolvePath(s.workspace, p)
				_ = os.MkdirAll(filepath.Dir(full), 0o755)
				dir := filepath.Dir(full)
				if _, ok := seen[dir]; ok {
					continue
				}
				seen[dir] = struct{}{}
				if err := watcher.Add(dir); err != nil {
					log.Printf("watch add %s: %v", dir, err)
				}
			}
		}
	}

	pollEvery := time.Duration(s.cfg.Watch.PollFallbackMS) * time.Millisecond
	ticker := time.NewTicker(pollEvery)
	defer ticker.Stop()

	var idleTimer *time.Timer
	var idleC <-chan time.Time
	if s.cfg.Budget.IdleSleepAfterMinutes != nil && *s.cfg.Budget.IdleSleepAfterMinutes > 0 {
		d := time.Duration(*s.cfg.Budget.IdleSleepAfterMinutes) * time.Minute
		idleTimer = time.NewTimer(d)
		idleC = idleTimer.C
		defer idleTimer.Stop()
	}

	resetIdle := func() {
		if idleTimer == nil || s.cfg.Budget.IdleSleepAfterMinutes == nil {
			return
		}
		if !idleTimer.Stop() {
			select {
			case <-idleTimer.C:
			default:
			}
		}
		idleTimer.Reset(time.Duration(*s.cfg.Budget.IdleSleepAfterMinutes) * time.Minute)
	}

	if n, err := s.drain(); err != nil {
		log.Printf("drain: %v", err)
	} else if n > 0 {
		resetIdle()
	}

	debounce := time.NewTimer(0)
	if !debounce.Stop() {
		<-debounce.C
	}
	pendingFS := false

	for {
		select {
		case <-s.stopCh:
			return nil
		case <-s.wakeCh:
			if n, err := s.drain(); err != nil {
				log.Printf("drain: %v", err)
			} else if n > 0 {
				resetIdle()
			}
		case <-ticker.C:
			if n, err := s.drain(); err != nil {
				log.Printf("drain: %v", err)
			} else if n > 0 {
				resetIdle()
			}
		case _, ok := <-fsEvents:
			if !ok {
				fsEvents = nil
				continue
			}
			pendingFS = true
			debounce.Reset(80 * time.Millisecond)
		case <-debounce.C:
			if pendingFS {
				pendingFS = false
				if n, err := s.drain(); err != nil {
					log.Printf("drain: %v", err)
				} else if n > 0 {
					resetIdle()
				}
			}
		case err, ok := <-fsErrors:
			if !ok {
				fsErrors = nil
				continue
			}
			log.Printf("fsnotify: %v", err)
		case <-idleC:
			s.mu.Lock()
			already := s.state.Sleeping
			s.mu.Unlock()
			if !already {
				log.Printf("idle sleep after %dm", *s.cfg.Budget.IdleSleepAfterMinutes)
				maxID, _ := s.store.MaxEventID()
				_ = SaveCursor(s.cursorPath(), maxID)
				_ = s.Sleep()
			}
			resetIdle()
		}
	}
}

// EnsurePID writes a pid file for the worker.
func EnsurePID(workspace, workerID string) (string, error) {
	path := filepath.Join(workspace, ".agentbus", "worker-"+workerID+".pid")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return "", err
	}
	return path, os.WriteFile(path, []byte(fmt.Sprintf("%d\n", os.Getpid())), 0o644)
}
