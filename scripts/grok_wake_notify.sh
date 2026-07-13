#!/usr/bin/env bash
# Bridge WAKE.json → stdout lines for Grok Build monitor / session hooks.
# Usage: monitor this script, or: inotifywait -m WAKE.json | ...
set -euo pipefail
WS="${AGENTBUS_WORKSPACE:-/home/oni/okf_agent_workspace/projects/agentbus}"
WAKE="${WS}/.agentbus/WAKE.json"
CURSOR="${WS}/.agentbus/grok_wake_notify.cursor"

last=0
if [[ -f "$CURSOR" ]]; then
  last=$(cat "$CURSOR" 2>/dev/null || echo 0)
fi

emit() {
  if [[ ! -f "$WAKE" ]]; then
    return
  fi
  eid=$(python3 -c "import json;print(json.load(open('$WAKE')).get('event_id',0))" 2>/dev/null || echo 0)
  if [[ -z "$eid" || "$eid" -le "$last" ]]; then
    return
  fi
  summary=$(python3 -c "import json;p=json.load(open('$WAKE'));print((p.get('payload')or{}).get('summary',''))" 2>/dev/null | tr '\n' ' ')
  fr=$(python3 -c "import json;p=json.load(open('$WAKE'));print((p.get('payload')or{}).get('from',''))" 2>/dev/null)
  to=$(python3 -c "import json;p=json.load(open('$WAKE'));print((p.get('payload')or{}).get('to',''))" 2>/dev/null)
  echo "AGY_TASK event_id=${eid} from=${fr} to=${to} summary=${summary}"
  echo "$eid" > "$CURSOR"
  last=$eid
}

# one-shot
if [[ "${1:-}" == "--once" ]]; then
  emit
  exit 0
fi

# poll loop (fsnotify not required)
while true; do
  emit
  sleep "${GROK_WAKE_POLL_SEC:-2}"
done
