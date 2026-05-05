# sparkplug-tck-go

A Go reimplementation of the [Eclipse Sparkplug Test Compatibility Kit](https://github.com/eclipse-sparkplug/sparkplug/tree/master/tck).

The official TCK is a Java/HiveMQ stack: ~10s JVM warmup, heavyweight to run as a CI gate. This is the same conformance check, single static binary, no JVM.

## Status

Early but executable. Assertion catalog is extracted; the runner, session tracker, MQTT capture, and 54 of 274 assertions are wired. The CLI runs against either a JSON fixture or a live MQTT broker.

## Parity strategy

The official TCK doesn't maintain a hand-written assertion list — it generates one from the spec asciidoc at build time (`tck-audit.xml`). This project does the same thing directly:

`scripts/update-assertions.sh` pulls the four normative chapters from `eclipse-sparkplug/sparkplug` and runs `cmd/extract-assertions` to produce `assertions.json`. CI re-runs it against `master` on a schedule and fails (or opens a PR) if the diff is non-empty, so spec drift can't sneak past us.

Source chapters consumed:
- `Sparkplug_4_Topics.adoc`
- `Sparkplug_5_Operational_Behavior.adoc`
- `Sparkplug_6_Payloads.adoc`
- `Sparkplug_8_HA.adoc`

Current count: **274 testable assertions**.

## Layout

```
cmd/extract-assertions/   # asciidoc -> assertions.json
cmd/sparkplug-tck/        # CLI: runs the suite against a captured fixture
internal/spb/             # topic parser, message envelope, STATE decoder
internal/spbpb/           # generated Sparkplug B protobuf bindings
internal/session/         # per-edge-node state tracker
internal/runner/          # assertion registry + result types
internal/assertions/      # individual [tck-id-*] checks (importing this
                          # package wires every check into the runner)
proto/sparkplug_b.proto   # vendored from eclipse-tahu (byte-identical)
scripts/gen-proto.sh
scripts/update-assertions.sh
assertions.json           # checked-in catalog; regenerate via script
```

## Running the CLI

Two input modes:

```sh
# Offline: replay a recorded fixture (one entry per MQTT message — base64
# Sparkplug protobuf or STATE bytes).
go run ./cmd/sparkplug-tck -fixture capture.json
go run ./cmd/sparkplug-tck -fixture - -json < capture.json

# Online: subscribe to a live broker, capture for a duration, then assert.
go run ./cmd/sparkplug-tck -broker tcp://localhost:1883 -duration 30s
go run ./cmd/sparkplug-tck -broker tcp://broker:1883 -username u -password p -duration 1m -json
```

Exit code is non-zero if any assertion failed.

## Implemented assertions

NBIRTH / NDEATH lifecycle:
```
tck-id-topic-structure-namespace-a
tck-id-topics-nbirth-mqtt
tck-id-topics-nbirth-seq-num
tck-id-payloads-nbirth-seq
tck-id-payloads-nbirth-timestamp
tck-id-payloads-nbirth-bdseq
tck-id-payloads-ndeath-seq
tck-id-payloads-ndeath-bdseq
tck-id-payloads-ndeath-will-message-qos
tck-id-payloads-ndeath-will-message-retain
tck-id-payloads-sequence-num-incrementing
tck-id-topics-nbirth-bdseq-included
tck-id-topics-nbirth-bdseq-matching
```

DBIRTH / DDATA / DDEATH / NDATA envelope (QoS, retain, seq, timestamp):
```
tck-id-payloads-dbirth-qos
tck-id-payloads-dbirth-retain
tck-id-payloads-dbirth-seq
tck-id-payloads-dbirth-timestamp
tck-id-payloads-ndata-qos
tck-id-payloads-ndata-retain
tck-id-payloads-ndata-seq
tck-id-payloads-ndata-timestamp
tck-id-payloads-ddata-qos
tck-id-payloads-ddata-retain
tck-id-payloads-ddata-seq
tck-id-payloads-ddata-timestamp
tck-id-payloads-ddeath-seq
tck-id-payloads-ddeath-timestamp
```

Per-edge ordering:
```
tck-id-payloads-dbirth-order
tck-id-payloads-ndata-order
tck-id-payloads-ddata-order
```

Host STATE (3.x JSON envelope, retain rules, topic shape — chapter 4 + 5 IDs):
```
tck-id-host-topic-phid-birth-qos
tck-id-host-topic-phid-birth-retain
tck-id-host-topic-phid-birth-payload
tck-id-host-topic-phid-birth-topic
tck-id-host-topic-phid-death-qos
tck-id-host-topic-phid-death-retain
tck-id-host-topic-phid-death-payload
tck-id-host-topic-phid-death-topic
tck-id-operational-behavior-host-application-connect-birth-qos
tck-id-operational-behavior-host-application-connect-birth-retained
tck-id-operational-behavior-host-application-connect-birth-payload
tck-id-operational-behavior-host-application-connect-birth-topic
tck-id-operational-behavior-host-application-connect-will-qos
tck-id-operational-behavior-host-application-connect-will-retained
tck-id-operational-behavior-host-application-connect-will-payload
tck-id-operational-behavior-host-application-connect-will-topic
tck-id-operational-behavior-host-application-death-payload
tck-id-message-flow-phid-sparkplug-state-publish-payload
tck-id-payloads-state-birth-payload
tck-id-payloads-state-will-message-qos
tck-id-payloads-state-will-message-retain
tck-id-payloads-state-will-message-payload
```

Rebirth metric on NBIRTH (Node Control/Rebirth must be Boolean, false, no alias):
```
tck-id-operational-behavior-data-commands-rebirth-name
tck-id-operational-behavior-data-commands-rebirth-value
tck-id-operational-behavior-data-commands-rebirth-datatype
tck-id-operational-behavior-data-commands-rebirth-name-aliases
```

54 of 274. Many of the remaining assertions reduce to additional
`messageRule` entries (see `internal/assertions/message_rules.go`).

## Regenerating the catalog

```sh
bash scripts/update-assertions.sh
```

Set `SPARKPLUG_SPEC_REF` to pin a tag instead of `master`.

## Roadmap

- [x] Assertion catalog extractor + checked-in JSON
- [x] Sparkplug B Go protobuf bindings (vendored from eclipse-tahu)
- [x] Topic parser, message envelope, session state tracker
- [x] Assertion runner + result types
- [x] First batch of NBIRTH/NDEATH/sequence assertions (9)
- [x] Fixture-driven CLI for offline runs
- [x] MQTT harness — live capture against a broker (paho client)
- [x] Second batch: DBIRTH/NDATA/DDATA/DDEATH envelope + ordering (17)
- [x] Third batch: host STATE envelope/topic + rebirth metric (20)
- [ ] Remaining payload-level checks (~80 mechanical)
- [ ] Alias rules + sequencing checks not yet covered
- [ ] Edge-node profile parity with HiveMQ TCK
- [ ] Host-application profile parity
- [ ] CI: scheduled spec-drift detection
- [ ] CI: parity test — same fixture run through both TCKs, diff results

## License

Go code: Apache-2.0 (see `LICENSE`).

`assertions.json` contains assertion text excerpted from the Eclipse Sparkplug specification, which is licensed under EPL-2.0. See `NOTICE` for attribution.
