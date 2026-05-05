#!/usr/bin/env bash
# Boot HiveMQ + Sparkplug TCK extension headlessly. Used by the
# correctness benchmark to drive the same synthetic SUT through
# the upstream Java TCK that we drive through our Go harness, so
# we can diff per-ID verdicts.
#
# The HiveMQ home is staged by gradle :tck:prepareHivemqHome at
# .upstream-tck-cache/sparkplug-master/tck/build/hivemq-home — we
# just spawn it in the background and wait for port 1883.
#
# Writes the HiveMQ pid to scripts/upstream-tck/hivemq.pid so
# stop-hivemq.sh can reap it. Outputs the working directory the
# extension writes SparkplugTCKresults.log into on stdout.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
CACHE="$ROOT/.upstream-tck-cache/sparkplug-master"
HOME_DIR="$CACHE/tck/build/hivemq-home"
RUN_SH="$HOME_DIR/bin/run.sh"
PID_FILE="$ROOT/scripts/upstream-tck/hivemq.pid"
LOG_FILE="$ROOT/scripts/upstream-tck/hivemq.out"

if [[ ! -x "$RUN_SH" ]]; then
  echo "HiveMQ home not prepared. Run from $CACHE:" >&2
  echo "  ./gradlew :tck:prepareHivemqHome" >&2
  exit 1
fi

if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  echo "HiveMQ already running (pid $(cat "$PID_FILE"))." >&2
  exit 0
fi

cd "$HOME_DIR"
HIVEMQ_HOME="$HOME_DIR" nohup "$RUN_SH" >"$LOG_FILE" 2>&1 &
echo $! >"$PID_FILE"

# Wait up to 60s for HiveMQ to be fully ready. "Port 1883 open" is
# necessary but not sufficient on a freshly-booted JVM: we've seen
# QoS1 PUBACKs time out for several seconds after the listener starts
# accepting connections. Wait for the explicit "Started HiveMQ in"
# log line, which fires after the extension and listeners are wired
# AND the broker is ready to serve.
for i in $(seq 1 60); do
  if grep -q "Started HiveMQ in" "$LOG_FILE" 2>/dev/null \
     && (echo > /dev/tcp/127.0.0.1/1883) >/dev/null 2>&1; then
    # Tiny extra settle so the JVM's accept queue is actually serving.
    sleep 1
    echo "HiveMQ ready on 1883 (pid $(cat "$PID_FILE"))." >&2
    echo "$HOME_DIR"
    exit 0
  fi
  sleep 1
done

echo "Timed out waiting for HiveMQ. Tail of $LOG_FILE:" >&2
tail -40 "$LOG_FILE" >&2 || true
exit 1
