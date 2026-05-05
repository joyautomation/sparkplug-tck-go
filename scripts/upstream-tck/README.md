# Upstream Java TCK correctness benchmark

This directory hosts the harness for the **correctness** dimension of the
parity bench: drive a synthetic Sparkplug SUT through both the upstream
Eclipse Sparkplug TCK (running as a HiveMQ extension) and this repo's
Go harness, then diff per-ID verdicts.

## Prereqs

* Java 21 (the upstream TCK build needs `gradlew` + JDK 21)
* `.upstream-tck-cache/sparkplug-master/` populated by
  `scripts/update-upstream-inventory.sh`

## Boot the TCK

The TCK is built and the HiveMQ home is staged via gradle:

```sh
cd .upstream-tck-cache/sparkplug-master
./gradlew :tck:prepareHivemqHome
```

That writes a self-contained HiveMQ install with the TCK extension at
`tck/build/hivemq-home/`. Boot/teardown is wrapped:

```sh
scripts/upstream-tck/start-hivemq.sh   # ~10s, blocks until 1883 listens
scripts/upstream-tck/stop-hivemq.sh    # graceful, falls back to SIGKILL
```

## Run a single test

```sh
go build ./cmd/sparkplug-tck-correctness
./sparkplug-tck-correctness -test "edge SessionEstablishmentTest" >java.json
```

The orchestrator subscribes to `SPARKPLUG_TCK/RESULT`, sends a
`NEW_TEST` command on `SPARKPLUG_TCK/TEST_CONTROL`, drives a
spec-compliant edge node lifecycle, and captures every per-ID verdict
emitted by the upstream extension.

## Diff against the Go bench

```sh
go build ./cmd/sparkplug-tck-bench ./cmd/sparkplug-tck-diff
./sparkplug-tck-bench -json >go.json
./sparkplug-tck-diff -java java.json -go go.json
```

The diff reports agreement % across IDs both engines emitted a verdict
for. As of writing, `edge SessionEstablishmentTest` reaches **95.7%
agreement** (67/70 IDs) — disagreements indicate either a Go scenario
that's too lenient or an upstream test that's stricter than the spec
text itself.
