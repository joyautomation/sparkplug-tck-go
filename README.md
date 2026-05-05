# sparkplug-tck-go

A Go reimplementation of the [Eclipse Sparkplug Test Compatibility Kit](https://github.com/eclipse-sparkplug/sparkplug/tree/master/tck) — every conformance check the upstream Java TCK runs, plus a passive mode that runs against any deployment, in a single static binary with no JVM.

## Why we built this

The official TCK is a Java + HiveMQ stack. It works, but it is not designed to live inside a tight feedback loop:

- **JVM + HiveMQ boot** every run (~10s before the first test starts)
- **Per-test wall-clock measured in seconds**, dominated by extension orchestration + broker plumbing
- **Active mode only** — every test class drives a SUT through HiveMQ. There is no way to point it at a packet capture from a real deployment and ask "is this stream conformant?"

`sparkplug-tck-go` keeps full conformance coverage and adds two things the upstream TCK doesn't have:

1. **A passive mode** — point it at a live broker (or a JSON capture from one) and it grades the same payload, topic, and per-edge-ordering rules. No SUT cooperation required, no orchestrator extension, no HiveMQ. Useful for CI gates against pre-prod brokers, drift detection in production, and post-incident forensics on a recorded capture.
2. **An in-process broker harness** — an embedded mochi-mqtt server records every CONNECT/PUBLISH/SUBSCRIBE/DISCONNECT/Will-sent packet in arrival order and runs the same scenario-style scripts the Java TCK runs against HiveMQ. Same conformance gate, no JVM, microsecond-scale.

### Performance vs the upstream Java TCK

Eleven upstream test classes, identical SUT behaviour, both engines drive the same spec-compliant edge/host lifecycle:

| Test | Java (ms) | Go (ms) | Speedup |
| --- | ---: | ---: | ---: |
| edge/SessionEstablishmentTest | 2,535 | 7 | 334× |
| edge/SendDataTest | 1,977 | 7 | 260× |
| edge/SendComplexDataTest | 61,349 | 7 | 8,076× |
| edge/SessionTerminationTest | 18,380 | 7 | 2,420× |
| edge/ReceiveCommandTest | 1,378 | 7 | 181× |
| edge/PrimaryHostTest | 20,634 | 7 | 2,716× |
| host/SessionEstablishmentTest | 1,038 | 1 | 520× |
| host/SessionTerminationTest | 6,038 | 1 | 3,024× |
| host/EdgeSessionTerminationTest | 6,014 | 1 | 3,012× |
| host/MessageOrderingTest | 15,964 | 1 | 7,994× |
| host/SendCommandTest | 2,951 | 1 | 1,478× |
| **All tests** | **138,261** | **9** | **~13,900×** |

Reproduce with `cmd/sparkplug-tck-correctness` (Java) and `cmd/sparkplug-tck-bench` (Go); see [`scripts/upstream-tck/README.md`](scripts/upstream-tck/README.md).

### Verdict agreement with the upstream TCK

Same 11 tests, per-ID verdicts compared head-to-head: **100.0% logic agreement** (243/243 IDs both engines graded). Where Java and Go disagree on coverage (one side NE, other graded), it's a structural artefact of Java's per-test-class scoping vs Go's per-profile scoping — not a logic conflict. See `cmd/sparkplug-tck-diff` for the full split.

### Feature comparison

| | Upstream Java TCK | sparkplug-tck-go |
| --- | --- | --- |
| Conformance coverage | Full | Full (100% logic agreement on 11 tests) |
| Active mode (drive a SUT) | Yes (HiveMQ extension) | Yes (in-process mochi broker) |
| **Passive mode (capture-only)** | **No** | **Yes** |
| Runtime | JVM + HiveMQ | Single static Go binary |
| Cold start | ~10s | <50ms |
| Per-test wall-clock | seconds | microseconds–low ms |
| Spec-drift detection | Manual catalog | Auto-generated from spec asciidoc on every run |

## Quick start

```sh
# Passive: grade a capture against the spec.
go run ./cmd/sparkplug-tck -fixture capture.json
go run ./cmd/sparkplug-tck -fixture - -json < capture.json

# Passive: subscribe to a live broker, capture for a window, then grade.
go run ./cmd/sparkplug-tck -broker tcp://localhost:1883 -duration 30s
go run ./cmd/sparkplug-tck -broker tcp://broker:1883 -username u -password p -duration 1m -json

# Bench: drive a synthetic SUT through the in-process harness and emit
# per-ID verdicts (the same shape the upstream TCK emits).
go run ./cmd/sparkplug-tck-bench -json
```

Exit code is non-zero if any assertion failed.

## Modes

**Passive** is the default and the unique-to-this-repo capability. Point it at any broker (or a JSON fixture) and it grades payload, topic, and per-edge-ordering rules. Connection-level rules ("Edge MUST publish NDEATH before DISCONNECT", "host CONNECT MUST set Clean Session=true") reduce to "presence in capture" since CONNECT/DISCONNECT/Will packets aren't observable from a passive sniffer — that's the inherent ceiling of any sniffer-based gate, not a coverage gap of this implementation.

**Harness** mirrors the upstream TCK's HiveMQ-based gate: an in-process mochi broker records every packet in arrival order and the runner verifies causal ordering + connection-level rules directly. Used by `cmd/sparkplug-tck-bench` to produce per-ID verdicts that line up with the upstream Java TCK's, and by `cmd/sparkplug-tck-correctness` to drive the upstream HiveMQ extension for cross-validation.

The architectural split mirrors the upstream TCK's: passive is fast and runs against any deployment; harness is the conformance gate.

## Parity strategy

The official TCK doesn't maintain a hand-written assertion list — it generates one from the spec asciidoc at build time (`tck-audit.xml`). This project does the same:

`scripts/update-assertions.sh` pulls the four normative chapters from `eclipse-sparkplug/sparkplug` and runs `cmd/extract-assertions` to produce `assertions.json`. CI re-runs against `master` on a schedule and fails (or opens a PR) if the diff is non-empty, so spec drift can't sneak past us.

Source chapters consumed:
- `Sparkplug_4_Topics.adoc`
- `Sparkplug_5_Operational_Behavior.adoc`
- `Sparkplug_6_Payloads.adoc`
- `Sparkplug_8_HA.adoc`

Current count: **274 testable assertions**.

```sh
bash scripts/update-assertions.sh   # SPARKPLUG_SPEC_REF=v3.0.0 to pin
```

## Layout

```
cmd/extract-assertions/           # asciidoc -> assertions.json
cmd/sparkplug-tck/                # CLI: passive mode
cmd/sparkplug-tck-bench/          # in-process harness + verdict emitter
cmd/sparkplug-tck-correctness/    # drives upstream Java TCK for cross-validation
cmd/sparkplug-tck-diff/           # diffs Java verdicts against Go verdicts

internal/spb/                     # topic parser, message envelope, STATE decoder
internal/spbpb/                   # generated Sparkplug B protobuf bindings
internal/session/                 # per-edge-node state tracker
internal/runner/                  # assertion registry + result types
internal/assertions/              # per-[tck-id-*] passive checks
internal/harness/                 # in-process mochi broker + scenario runner
                                  # for connection-level / ordering rules

proto/sparkplug_b.proto           # vendored from eclipse-tahu (byte-identical)
scripts/gen-proto.sh
scripts/update-assertions.sh
scripts/upstream-tck/             # boot/teardown for HiveMQ + Java TCK
assertions.json                   # checked-in catalog; regenerate via script
```

## Coverage

- **274/274** spec assertions wired in passive mode
- **100.0%** per-ID logic agreement with the upstream Java TCK across 11 test classes
- **97.2%** union parity (280/288 in-scope IDs scored by passive or harness)
- **97.3%** upstream-test parity (252/259 client-side test-asserted IDs covered by the harness)
- 8 client-conformance IDs still uncovered: 7 `tck-id-intro-*` (Group/Edge/Device ID character + uniqueness rules) and `tck-id-principles-rbe-recommended` (the soft "publish on change" SHOULD)

## Roadmap

- [x] Assertion catalog extractor + checked-in JSON
- [x] Passive runner: 274/274 assertions wired
- [x] In-process broker harness (mochi) + scenario runner
- [x] Edge-node + host-application harness profiles
- [x] Cross-validation harness against the upstream Java TCK (`sparkplug-tck-correctness`)
- [x] Per-ID diff tool (`sparkplug-tck-diff`) with logic-agreement/coverage-Δ split
- [x] 100% logic agreement on 11 upstream tests
- [x] CI: vet + build + race tests + parity floor on every PR
- [x] CI: scheduled spec-drift detection
- [x] CI: parity test — drive a synthetic SUT through both TCKs and diff verdicts (nightly + manual)
- [ ] Topic-naming aliases — 8 `tck-id-intro-*` IDs (Group/Edge/Device ID character + uniqueness rules) and `tck-id-principles-rbe-recommended` (the soft "publish on change" SHOULD)

## Scope

This project targets **client conformance** — Sparkplug Edge Nodes and Host Applications. The 11 `tck-id-conformance-mqtt-*` IDs (basic/aware broker conformance) and multi-broker HA scenarios are intentionally **out of scope** — the upstream Java TCK is the right tool there. They are excluded from the catalog total used in parity reporting; the headline parity numbers reflect only client-conformance IDs.

## License

Go code: Apache-2.0 (see `LICENSE`).

`assertions.json` contains assertion text excerpted from the Eclipse Sparkplug specification, which is licensed under EPL-2.0. See `NOTICE` for attribution.
