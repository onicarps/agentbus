// agentbus-go-worker — classical non-LLM wake plane (PRD v0.12.0).
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/onicarps/agentbus-go/internal/worker"
)

func main() {
	ws := flag.String("workspace", envOr("AGENTBUS_WORKSPACE", ""), "workspace root")
	config := flag.String("config", "", "path to worker.yaml (default: <ws>/.agentbus/worker.yaml)")
	cmd := flag.String("cmd", "up", "up|once|sleep|wake|status|init")
	skipBacklog := flag.Bool("skip-backlog", true, "on wake: fast-forward cursor (default true)")
	drain := flag.Bool("drain", false, "on wake: process backlog (sets skip-backlog=false)")
	to := flag.String("to", "grok", "init preset --to target")
	flag.Parse()

	if *ws == "" {
		cwd, _ := os.Getwd()
		*ws = cwd
	}
	abs, err := filepath.Abs(*ws)
	if err != nil {
		fail(err)
	}
	*ws = abs

	if *cmd == "init" {
		path, err := worker.WritePresetImplementer(*ws, *to)
		if err != nil {
			fail(err)
		}
		fmt.Println(path)
		return
	}

	cfgPath := *config
	if cfgPath == "" {
		cfgPath = filepath.Join(*ws, ".agentbus", "worker.yaml")
	}
	if _, err := os.Stat(cfgPath); os.IsNotExist(err) && *cmd != "status" {
		// auto-init implementer preset
		if _, err := worker.WritePresetImplementer(*ws, *to); err != nil {
			fail(err)
		}
	}
	cfg, err := worker.LoadConfig(cfgPath)
	if err != nil {
		// last resort defaults in memory + write
		cfg = worker.DefaultConfig()
		if *cmd != "status" {
			_, _ = worker.WritePresetImplementer(*ws, cfg.ProducerID)
			cfg, err = worker.LoadConfig(cfgPath)
			if err != nil {
				fail(err)
			}
		}
	}

	svc, err := worker.NewService(*ws, cfg)
	if err != nil {
		fail(err)
	}
	defer svc.Close()

	switch *cmd {
	case "up":
		_, _ = worker.EnsurePID(*ws, cfg.WorkerID)
		fmt.Fprintf(os.Stderr, "agentbus-go-worker up workspace=%s worker_id=%s\n", *ws, cfg.WorkerID)
		if err := svc.Run(); err != nil {
			fail(err)
		}
	case "once":
		n, err := svc.Once()
		if err != nil {
			fail(err)
		}
		fmt.Printf("{\"matched\":%d}\n", n)
	case "sleep":
		if err := svc.Sleep(); err != nil {
			fail(err)
		}
		fmt.Println(`{"sleeping":true}`)
	case "wake":
		skip := *skipBacklog
		if *drain {
			skip = false
		}
		if err := svc.Wake(skip); err != nil {
			fail(err)
		}
		fmt.Printf("{\"sleeping\":false,\"skip_backlog\":%v}\n", skip)
	case "status":
		b, err := svc.StatusJSON()
		if err != nil {
			fail(err)
		}
		fmt.Println(string(b))
	default:
		fail(fmt.Errorf("unknown cmd %q", *cmd))
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func fail(err error) {
	fmt.Fprintf(os.Stderr, "error: %v\n", err)
	os.Exit(1)
}
