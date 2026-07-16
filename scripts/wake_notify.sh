#!/usr/bin/env bash
# Zero-LLM session bridge: print one line when WAKE.<agent>.json advances.
# Usage:
#   ./scripts/wake_notify.sh grok
#   ./scripts/wake_notify.sh agy --once
#   AGENTBUS_WORKSPACE=/home/oni/okf_agent_workspace ./scripts/wake_notify.sh hermes
set -euo pipefail

AGENT="${1:-grok}"
shift || true
WS="${AGENTBUS_WORKSPACE:-/home/oni/okf_agent_workspace}"
WAKE="${AGENTBUS_WAKE_FILE:-${WS}/.agentbus/WAKE.${AGENT}.json}"
# Fallback legacy single file for implementer-only setups
if [[ ! -f "$WAKE" && -f "${WS}/.agentbus/WAKE.json" && "$AGENT" == "grok" ]]; then
  WAKE="${WS}/.agentbus/WAKE.json"
fi
CURSOR="${WS}/.agentbus/wake_notify.${AGENT}.cursor"

last=0
if [[ -f "$CURSOR" ]]; then
  last=$(cat "$CURSOR" 2>/dev/null || echo 0)
fi

emit() {
  if [[ ! -f "$WAKE" ]]; then
    return 0
  fi
  eid=$(python3 -c "import json;print(json.load(open('$WAKE')).get('event_id',0))" 2>/dev/null || echo 0)
  if [[ -z "$eid" || "$eid" -le "$last" ]]; then
    return 0
  fi
  summary=$(python3 -c "import json;p=json.load(open('$WAKE'));print((p.get('payload')or{}).get('summary',''))" 2>/dev/null | tr '\n' ' ')
  fr=$(python3 -c "import json;p=json.load(open('$WAKE'));print((p.get('payload')or{}).get('from',''))" 2>/dev/null)
  to=$(python3 -c "import json;p=json.load(open('$WAKE'));print((p.get('payload')or{}).get('to',''))" 2>/dev/null)
  echo "WAKE_TASK agent=${AGENT} event_id=${eid} from=${fr} to=${to} summary=${summary}"
  echo "$eid" > "$CURSOR"
  last=$eid
}

if [[ "${1:-}" == "--once" ]]; then
  emit
  exit 0
fi

while true; do
  emit
  sleep "${AGENTBUS_WAKE_POLL_SEC:-2}"
done
