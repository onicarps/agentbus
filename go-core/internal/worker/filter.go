package worker

import (
	"strings"
	"time"

	"github.com/onicarps/agentbus-go/internal/store"
)

// Match reports whether event matches any subscribe rule and age policy.
func Match(cfg *Config, ev store.Event, now time.Time) bool {
	if cfg == nil {
		return false
	}
	// max age (worker-side anti-stale; PRD §14.1.1)
	if ts, err := time.Parse(time.RFC3339, strings.Replace(ev.Timestamp, "Z", "+00:00", 1)); err == nil {
		if now.Sub(ts) > cfg.MaxEventAge() {
			return false
		}
	} else if ts2, err2 := time.Parse("2006-01-02T15:04:05Z", ev.Timestamp); err2 == nil {
		if now.Sub(ts2) > cfg.MaxEventAge() {
			return false
		}
	}
	// optional payload expires_at
	if exp, ok := ev.Payload["expires_at"].(string); ok && exp != "" {
		if t, err := time.Parse(time.RFC3339, strings.Replace(exp, "Z", "+00:00", 1)); err == nil && now.After(t) {
			return false
		}
	}

	for _, rule := range cfg.Subscribe {
		if rule.Topic != "" && rule.Topic != ev.Topic {
			continue
		}
		if !matchFrom(rule.From, payloadString(ev.Payload, "from")) {
			continue
		}
		if !matchTo(rule.To, payloadString(ev.Payload, "to")) {
			continue
		}
		if len(rule.Initiative) > 0 {
			ini := payloadString(ev.Payload, "initiative")
			if !containsFold(rule.Initiative, ini) {
				continue
			}
		}
		return true
	}
	return false
}

func payloadString(p map[string]any, key string) string {
	if p == nil {
		return ""
	}
	v, ok := p[key]
	if !ok || v == nil {
		return ""
	}
	s, ok := v.(string)
	if !ok {
		return ""
	}
	return s
}

func matchFrom(allowed []string, from string) bool {
	if len(allowed) == 0 {
		return true
	}
	return containsFold(allowed, from)
}

// matchTo: event.to is for this worker if it is a broadcast, exact role match,
// or a multi-target list containing one of our allowed names.
// Note: "*", "all", "swarm" on the *event* are broadcasts (match any worker).
// Putting those tokens in allowed does not mean "match every event".
func matchTo(allowed []string, to string) bool {
	if len(allowed) == 0 {
		return true
	}
	toLower := strings.ToLower(strings.TrimSpace(to))
	// broadcast destinations — any worker may wake
	if toLower == "*" || toLower == "all" || toLower == "swarm" {
		return true
	}
	parts := strings.Split(toLower, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	for _, a := range allowed {
		a = strings.ToLower(strings.TrimSpace(a))
		if a == "" || a == "*" || a == "all" {
			// ignore magic tokens in allow-list; broadcasts handled above
			continue
		}
		if a == toLower {
			return true
		}
		for _, p := range parts {
			if p == a {
				return true
			}
		}
	}
	return false
}

func containsFold(list []string, v string) bool {
	v = strings.ToLower(strings.TrimSpace(v))
	for _, x := range list {
		if strings.ToLower(strings.TrimSpace(x)) == v {
			return true
		}
	}
	return false
}
