#!/usr/bin/env bash
# Run the upstream Java TCK against our synthetic SUT, one test at a
# time, bouncing HiveMQ between every test to avoid the persistence
# wedge we hit on long-running tests (SendComplexDataTest et al).
#
# Outputs a merged java.json in the current directory. The CI parity
# workflow calls this same script — running it locally before pushing
# is the fastest way to know whether a fix actually works.
#
# Prereqs:
#   - .upstream-tck-cache/sparkplug-master/tck/build/hivemq-home staged
#     (gradle :tck:prepareHivemqHome)
#   - sparkplug-tck-correctness binary built or available on PATH
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

# Allow caller to override the binary path; default to ./sparkplug-tck-correctness
CORRECTNESS_BIN="${CORRECTNESS_BIN:-./sparkplug-tck-correctness}"
if [[ ! -x "$CORRECTNESS_BIN" ]]; then
  echo "Building $CORRECTNESS_BIN" >&2
  go build -o sparkplug-tck-correctness ./cmd/sparkplug-tck-correctness
  CORRECTNESS_BIN="./sparkplug-tck-correctness"
fi

TESTS=(
  "edge SessionEstablishmentTest"
  "edge SendDataTest"
  "edge SendComplexDataTest"
  "edge SessionTerminationTest"
  "edge ReceiveCommandTest"
  "edge PrimaryHostTest"
  "host SessionEstablishmentTest"
  "host SessionTerminationTest"
  "host EdgeSessionTerminationTest"
  "host MessageOrderingTest"
  "host SendCommandTest"
)

OUT_DIR=".parity-per-test"
rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"

HIVEMQ_HOME="$ROOT/.upstream-tck-cache/sparkplug-master/tck/build/hivemq-home"
HIVEMQ_DATA="$HIVEMQ_HOME/data"
HIVEMQ_LOGS="$HIVEMQ_HOME/log"

for t in "${TESTS[@]}"; do
  slug="$(echo "$t" | tr ' /' '__')"
  echo "=== $t ===" >&2
  # The extension appends to these across boots; start each test clean.
  rm -f "$HIVEMQ_LOGS/sparkplug.log" "$HIVEMQ_LOGS/event.log"
  bash scripts/upstream-tck/start-hivemq.sh
  "$CORRECTNESS_BIN" -tests "$t" > "$OUT_DIR/${slug}.json" || true
  # NEW_TEST never acked: the broker is still up and still wedged, so
  # capture its threads before stopping it.
  if grep -q '"INFRA_FAILED"' "$OUT_DIR/${slug}.json" 2>/dev/null; then
    pid="$(pgrep -f 'hivemq.jar' | head -1 || true)"
    if [[ -n "$pid" ]]; then
      jcmd "$pid" Thread.print > "$OUT_DIR/${slug}.threads.txt" 2>&1 || true
    fi
  fi
  bash scripts/upstream-tck/stop-hivemq.sh || true
  # Keep each boot's logs; start-hivemq.sh overwrites hivemq.out.
  cp scripts/upstream-tck/hivemq.out "$OUT_DIR/${slug}.hivemq.log" 2>/dev/null || true
  cp "$HIVEMQ_LOGS/sparkplug.log" "$OUT_DIR/${slug}.sparkplug.log" 2>/dev/null || true
  cp "$HIVEMQ_LOGS/event.log" "$OUT_DIR/${slug}.event.log" 2>/dev/null || true
  rm -rf "$HIVEMQ_DATA" || true
done

# Merge per-test reports into one java.json (same shape as a full sweep).
python3 - <<'PY'
import json, glob
merged = []
for path in sorted(glob.glob('.parity-per-test/*.json')):
    try:
        data = json.load(open(path))
    except Exception:
        continue
    merged.extend(data.get('tests') or [])
json.dump({'tests': merged}, open('java.json', 'w'), indent=2)
print(f"Wrote java.json with {len(merged)} test reports")
PY
