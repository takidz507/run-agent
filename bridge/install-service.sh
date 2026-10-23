#!/usr/bin/env bash
# Reinstall + start the Run Agent bridge systemd service (unit lives in /etc, which
# does not survive VM replacement; this script is the recovery path).
set -euo pipefail
BASE="$HOME/workspace/run-agent"
cp "$BASE/run-agent-bridge.service" /etc/systemd/system/run-agent-bridge.service
python3 - <<'PY'
import os, pathlib
unit = pathlib.Path("/etc/systemd/system/run-agent-bridge.service")
t = unit.read_text()
adds = []
for var in ("https_proxy", "HTTPS_PROXY", "http_proxy", "HTTP_PROXY", "all_proxy", "ALL_PROXY", "no_proxy", "NO_PROXY"):
    v = os.environ.get(var)
    if v and f"Environment={var}=" not in t:
        adds.append(f"Environment={var}={v}")
if adds:
    t = t.replace("[Service]", "[Service]\n" + "\n".join(adds), 1)
    unit.write_text(t)
os.chmod(unit, 0o600)
print("unit installed (proxy env injected, values hidden)")
PY
systemctl daemon-reload
systemctl enable -q run-agent-bridge.service
systemctl restart run-agent-bridge.service
sleep 8
systemctl is-active run-agent-bridge.service
