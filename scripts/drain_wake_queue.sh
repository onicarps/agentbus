#!/usr/bin/env bash
# Peek / mark Mode A wake queues (Hermes / Factory).
# Usage:
#   ./scripts/drain_wake_queue.sh hermes --peek
#   ./scripts/drain_wake_queue.sh hermes --next   # print next undone line as JSON
set -euo pipefail
RUNTIME="${1:-hermes}"
shift || true
WS="${AGENTBUS_WORKSPACE:-/home/oni/okf_agent_workspace}"
Q="${WS}/.agentbus/ingress/${RUNTIME}_wake_queue.jsonl"
DONE="${WS}/.agentbus/ingress/${RUNTIME}_wake_done.ids"

mkdir -p "$(dirname "$Q")"
touch "$DONE" "$Q"

case "${1:---peek}" in
  --peek)
    echo "queue=$Q"
    wc -l < "$Q" | xargs echo "lines="
    tail -n 5 "$Q" || true
    ;;
  --next)
    while IFS= read -r line; do
      [[ -z "$line" ]] && continue
      eid=$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('event_id',0))" "$line")
      if grep -qx "$eid" "$DONE" 2>/dev/null; then
        continue
      fi
      echo "$line"
      exit 0
    done < "$Q"
    echo "{}" 
    ;;
  --ack)
    eid="${2:?event_id required}"
    echo "$eid" >> "$DONE"
    echo "acked $eid"
    ;;
  *)
    echo "usage: $0 <hermes|factory> --peek|--next|--ack <event_id>" >&2
    exit 2
    ;;
esac
