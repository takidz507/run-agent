#!/usr/bin/env bash
set -euo pipefail
source "$HATCH_HOOK_RUNTIME"

HB="$HOME/workspace/run-agent/heartbeat.txt"
NOW=$(date +%s)
AGE=999999
[ -f "$HB" ] && AGE=$((NOW - $(cat "$HB" | tr -d ' ')))

ACTIVE="unknown"
systemctl is-active run-agent-bridge.service >/dev/null 2>&1 && ACTIVE="active" || ACTIVE="down"

if [ "$ACTIVE" = "active" ] && [ "$AGE" -lt 180 ]; then
  silent "bridge healthy" '{}'
  exit 0
fi

PAYLOAD=$(python3 -c "
import json, sys
print(json.dumps({'service': '$ACTIVE', 'heartbeat_age_s': $AGE}))")
wake "Run Agent bridge is DOWN" "$PAYLOAD"
