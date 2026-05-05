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

The diff splits per-ID outcomes into three buckets:

- **Logic agreement** — both engines PASSed or both FAILed the same ID.
  This is the headline metric. As of writing, all 11 upstream tests
  reach **100.0% logic agreement** (243/243 IDs both sides graded).
- **Logic conflict** — one engine PASS, the other FAIL. A real
  disagreement that needs investigation.
- **Coverage Δ** — one side emitted NE while the other graded. Reflects
  the structural mismatch between Java's per-test scoping (some IDs are
  NOT_EXECUTED for tests that don't exercise them) and Go's per-profile
  scoping (one verdict per ID across all scenarios in a profile). Not a
  logic conflict — flagged separately so it doesn't drag the agreement
  number down.
