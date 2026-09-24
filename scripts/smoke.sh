#!/usr/bin/env bash
# Smoke test for a running `docker compose up` stack (used by CI):
# the dashboard and API answer, and a wave of jobs runs locally and bursts to the simulated cloud.
set -euo pipefail
URL=${BURST_URL:-http://localhost:8000}

for _ in $(seq 60); do curl -fs "$URL/healthz" >/dev/null && break; sleep 1; done
curl -fs "$URL/" | grep -q "<title>Burst scheduler</title>"
echo "dashboard and API are up"

for _ in $(seq 60); do
  workers=$(curl -fs "$URL/stats" | python3 -c 'import sys, json; print(json.load(sys.stdin)["workers"])')
  [ "$workers" -ge 2 ] && break
  sleep 1
done
echo "$workers local workers"

python3 "$(dirname "$0")/load.py" --url "$URL" --waves 1 --jobs 12 --min-s 5 --max-s 5 --deadline-share 0 --seed 1

for _ in $(seq 90); do
  if python3 - "$URL" <<'PY'
import json, sys, urllib.request
jobs = json.load(urllib.request.urlopen(sys.argv[1] + "/jobs?limit=12"))
done = [j for j in jobs if j["state"] == "succeeded"]
backends = sorted({j["backend"] for j in done})
print(f"{len(done)}/12 succeeded on {backends}")
sys.exit(0 if len(done) == 12 and {"local", "simulated"} <= set(backends) else 1)
PY
  then exit 0; fi
  sleep 2
done
echo "jobs did not all succeed on both local workers and the simulated cloud" >&2
exit 1
