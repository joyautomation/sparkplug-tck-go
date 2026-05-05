# sparkplug-tck-go

A Go reimplementation of the [Eclipse Sparkplug Test Compatibility Kit](https://github.com/eclipse-sparkplug/sparkplug/tree/master/tck).

The official TCK is a Java/HiveMQ stack: ~10s JVM warmup, heavyweight to run as a CI gate. This is the same conformance check, single static binary, no JVM.

## Status

Early but executable. Assertion catalog is extracted; the runner, session tracker, MQTT capture, and 100 of 274 assertions are wired. The CLI runs against either a JSON fixture or a live MQTT broker.

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
tck-id-payloads-sequence-num-always-included
tck-id-payloads-sequence-num-req-nbirth
tck-id-topics-nbirth-bdseq-included
tck-id-topics-nbirth-bdseq-matching
```

Per-metric structural rules (datatype + name):
```
tck-id-payloads-metric-datatype-req
tck-id-payloads-metric-datatype-value
tck-id-payloads-metric-datatype-value-type
tck-id-payloads-name-requirement
```

DataSet structural rules (types/columns parallel arrays, type enum):
```
tck-id-payloads-dataset-parameter-type-req
tck-id-payloads-dataset-types-num
tck-id-payloads-dataset-column-num-headers
tck-id-payloads-dataset-column-size
tck-id-payloads-dataset-types-def
tck-id-payloads-dataset-types-type
tck-id-payloads-dataset-types-value
```

PropertySet + PropertyValue structural rules:
```
tck-id-payloads-propertyset-keys-array-size
tck-id-payloads-propertyset-values-array-size
tck-id-payloads-metric-propertyvalue-type-req
tck-id-payloads-metric-propertyvalue-type-type
tck-id-payloads-metric-propertyvalue-type-value
```

Topic-structure + message-flow aliases (chapters 4 + 5 restate chapter 6
constraints under their own ID namespaces):
```
tck-id-topic-structure-namespace-valid-group-id
tck-id-topic-structure-namespace-valid-edge-node-id
tck-id-topic-structure-namespace-valid-device-id
tck-id-topic-structure-namespace-device-id-associated-message-types
tck-id-topic-structure-namespace-device-id-non-associated-message-types
tck-id-topic-structure-namespace-unique-edge-node-descriptor
tck-id-topic-structure-namespace-unique-device-id
tck-id-topic-structure-namespace-duplicate-device-id-across-edge-node
tck-id-message-flow-edge-node-birth-publish-{nbirth-payload,nbirth-payload-bdSeq,nbirth-payload-seq,nbirth-qos,nbirth-retained,nbirth-topic,connect}
tck-id-message-flow-edge-node-birth-publish-will-message{,-payload,-payload-bdSeq,-qos,-topic,-will-retained}
tck-id-message-flow-device-birth-publish-dbirth-{payload,payload-seq,qos,retained,topic,match-edge-node-topic}
tck-id-message-flow-device-birth-publish-nbirth-wait
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

NCMD / DCMD envelope (QoS=0, retain=false, no seq, timestamp present) plus
chapter-4 topic-* and chapter-5 verb aliases, plus host-application-death
QoS/retain/topic aliases:
```
tck-id-payloads-ncmd-{qos,retain,seq,timestamp}
tck-id-payloads-dcmd-{qos,retain,seq,timestamp}
tck-id-topics-ncmd-{mqtt,payload,timestamp,topic}
tck-id-topics-dcmd-{mqtt,payload,timestamp,topic}
tck-id-operational-behavior-data-commands-ncmd-verb
tck-id-operational-behavior-data-commands-dcmd-verb
tck-id-operational-behavior-host-application-death-{qos,retained,topic}
```

Templates (per-Template structural + per-parameter rules; cross-shape
member/parameter parity deferred until a session-level template registry):
```
tck-id-payloads-template-is-definition
tck-id-payloads-template-is-definition-{definition,instance}
tck-id-payloads-template-instance-is-definition
tck-id-payloads-template-ref-{definition,instance}
tck-id-payloads-template-instance-ref
tck-id-payloads-template-parameter-{name-required,name-type}
tck-id-payloads-template-parameter-type-{req,value}
tck-id-payloads-template-parameter-value
tck-id-payloads-template-parameter-value-type
```

Metric alias rules (BIRTH name+alias binding, DATA/CMD alias-only,
per-edge-node uniqueness) and PropertySet "Quality" property:
```
tck-id-payloads-alias-birth-requirement
tck-id-payloads-alias-data-cmd-requirement
tck-id-payloads-alias-uniqueness
tck-id-payloads-propertyset-quality-value-{type,value}
```

Chapter-4 [tck-id-topics-*] aliases for QoS/retain/timestamp/seq/topic
across DBIRTH, NDATA, DDATA, DDEATH, NDEATH, plus per-edge sequence-inc
aliases and state-presence aliases:
```
tck-id-topics-{dbirth,ndata,ddata,ddeath}-mqtt
tck-id-topics-{nbirth,dbirth,ndata,ddata,ndeath,ddeath}-topic
tck-id-topics-{nbirth,dbirth,ndata,ddata}-timestamp
tck-id-topics-{dbirth,ndata,ddata,ddeath}-seq[-num]
tck-id-topics-ndeath-seq
tck-id-topics-{ndata,ddata,ndeath}-payload
tck-id-payloads-nbirth-{qos,retain}
tck-id-payloads-ddeath-seq-number
tck-id-payloads-{ndata,ddata,dbirth,ddeath}-seq-inc
tck-id-payloads-state-{birth,subscribe,will-message}
```

173 of 274. Many of the remaining assertions reduce to additional
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
