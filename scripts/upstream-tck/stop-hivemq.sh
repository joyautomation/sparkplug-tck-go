#!/usr/bin/env bash
# Reap the HiveMQ instance started by start-hivemq.sh.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
PID_FILE="$ROOT/scripts/upstream-tck/hivemq.pid"

if [[ ! -f "$PID_FILE" ]]; then
  echo "No HiveMQ pid file at $PID_FILE — nothing to stop." >&2
  exit 0
fi

PID="$(cat "$PID_FILE")"
if ! kill -0 "$PID" 2>/dev/null; then
  rm -f "$PID_FILE"
  echo "HiveMQ pid $PID not running. Cleared stale pid file." >&2
  exit 0
fi

kill "$PID"
for i in $(seq 1 20); do
  if ! kill -0 "$PID" 2>/dev/null; then
    rm -f "$PID_FILE"
    echo "HiveMQ pid $PID stopped." >&2
    exit 0
  fi
  sleep 1
done

kill -9 "$PID" 2>/dev/null || true
rm -f "$PID_FILE"
echo "Force-killed HiveMQ pid $PID." >&2
