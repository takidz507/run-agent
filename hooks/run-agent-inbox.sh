#!/usr/bin/env bash
set -euo pipefail
source "$HATCH_HOOK_RUNTIME"

INBOX="$HOME/workspace/run-agent/inbox.jsonl"
STATE="$HOME/workspace/run-agent/state.json"
MUTE="$HOME/workspace/run-agent/mute_until.txt"
WM_FILE="$HOME/hooks/state/run-agent-inbox.count"
NET_FILE="$HOME/hooks/state/run-agent-inbox.lastnet"
mkdir -p "$HOME/hooks/state"

[ -f "$INBOX" ] || { silent "no inbox yet" '{}'; exit 0; }

NOW=$(date +%s)
if [ -f "$MUTE" ]; then
  UNTIL=$(cat "$MUTE" 2>/dev/null || echo 0)
  if [ "$NOW" -lt "$UNTIL" ]; then
    silent "muted by owner command" '{}'
    exit 0
  fi
fi

extract() { # $1 = start line (1-based) or "STALE"
  python3 - "$INBOX" "$STATE" "$1" <<'PY'
import sys, json, time
inbox_path, state_path, mode = sys.argv[1], sys.argv[2], sys.argv[3]
lines = open(inbox_path, encoding="utf-8").read().splitlines()
read_acked = set()
try:
    read_acked = set(json.load(open(state_path, encoding="utf-8")).get("read_acked", []))
except Exception:
    pass
msgs = []
if mode == "STALE":
    now_ms = int(time.time() * 1000)
    for line in lines:
        try: r = json.loads(line)
        except Exception: continue
        if r.get("cmd"): continue
        if r.get("id") in read_acked: continue
        if now_ms - int(r.get("arrivedTs", now_ms)) > 60_000:
            msgs.append(r)
else:
    for line in lines[int(mode):]:
        try: r = json.loads(line)
        except Exception: continue
        if r.get("cmd"): continue
        msgs.append(r)
out = [{"author": r.get("author",""), "userId": r.get("userId",""), "type": r.get("type","text"),
        "text": (r.get("text","") or "")[:1500], "createdAt": r.get("createdAt","")} for r in msgs]
print(json.dumps({"count": len(out), "messages": out}, ensure_ascii=False))
PY
}

TOTAL=$(wc -l < "$INBOX" | tr -d ' ')
SEEN=0
[ -f "$WM_FILE" ] && SEEN=$(cat "$WM_FILE" | tr -d ' ')
[ "$TOTAL" -lt "$SEEN" ] && SEEN=0

if [ "$TOTAL" -gt "$SEEN" ]; then
  NEW_JSON=$(extract "$SEEN")
  COUNT=$(echo "$NEW_JSON" | python3 -c "import sys, json; print(json.load(sys.stdin)['count'])")
  if [ "${HATCH_HOOK_DRY_RUN:-0}" != "1" ]; then echo "$TOTAL" > "$WM_FILE"; fi
  if [ "$COUNT" -gt 0 ]; then
    wake "New message(s) in the Run Agent room" "$NEW_JSON"
    exit 0
  fi
  silent "only local bridge commands arrived" '{}'
  exit 0
fi

# Safety net: messages delivered but still unanswered after 1 minute -> re-wake (throttled to once / 4 min)
STALE_JSON=$(extract "STALE")
SCOUNT=$(echo "$STALE_JSON" | python3 -c "import sys, json; print(json.load(sys.stdin)['count'])")
if [ "$SCOUNT" -gt 0 ]; then
  LASTNET=0
  [ -f "$NET_FILE" ] && LASTNET=$(cat "$NET_FILE" | tr -d ' ')
  if [ $((NOW - LASTNET)) -ge 240 ]; then
    if [ "${HATCH_HOOK_DRY_RUN:-0}" != "1" ]; then echo "$NOW" > "$NET_FILE"; fi
    wake "UNANSWERED room message(s) older than 1 minute" "$STALE_JSON"
    exit 0
  fi
  silent "stale unanswered exist but net throttled" '{}'
  exit 0
fi

silent "no new room messages" '{}'
