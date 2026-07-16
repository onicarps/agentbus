// Package workspace enforces hard path constraints for AgentBus.
//
// Canonical bus must not live on WSL DrvFS (/mnt/c, …). See OKF decisions:
// wake-session-bridge-tech-discussion-2026-07-16.md
package workspace

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

var mntDrive = regexp.MustCompile(`(?i)^/mnt/[a-z](/|$)`)

// AssertSupported returns absolute path or an error if the workspace is on
// an unsupported filesystem (WSL DrvFS). Break-glass: AGENTBUS_ALLOW_DRVFS=1.
func AssertSupported(workspace string) (string, error) {
	abs, err := filepath.Abs(workspace)
	if err != nil {
		return "", err
	}
	if allowDrvFS() {
		return abs, nil
	}
	if looksLikeDrvFS(abs) {
		return "", fmt.Errorf(
			"unsupported AgentBus workspace %s: path is on WSL DrvFS (/mnt/<drive>) or Windows path; "+
				"use a native Linux path under /home (set AGENTBUS_ALLOW_DRVFS=1 only for emergency break-glass)",
			abs,
		)
	}
	return abs, nil
}

func allowDrvFS() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("AGENTBUS_ALLOW_DRVFS")))
	return v == "1" || v == "true" || v == "yes" || v == "on"
}

func looksLikeDrvFS(path string) bool {
	// Normalize to slash for matching
	p := filepath.ToSlash(path)
	if mntDrive.MatchString(p) {
		return true
	}
	if strings.HasPrefix(p, "/cygdrive/") {
		return true
	}
	// Windows absolute e.g. C:/Users
	if len(p) >= 2 && p[1] == ':' {
		return true
	}
	return false
}
