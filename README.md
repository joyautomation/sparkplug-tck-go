# sparkplug-tck-go

A Go reimplementation of the [Eclipse Sparkplug Test Compatibility Kit](https://github.com/eclipse-sparkplug/sparkplug/tree/master/tck).

The official TCK is a Java/HiveMQ stack: ~10s JVM warmup, heavyweight to run as a CI gate. This is the same conformance check, single static binary, no JVM.

## Status

Early but executable. Assertion catalog is extracted; the runner, session tracker, and the first 9 assertions are wired. The CLI accepts a JSON fixture today; a live MQTT harness is the next chunk.

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

Today it consumes a JSON fixture (one entry per MQTT message — base64-encoded
Sparkplug protobuf or STATE bytes). The MQTT harness will subscribe to a live
broker in the next iteration.

```sh
go run ./cmd/sparkplug-tck -fixture path/to/capture.json
go run ./cmd/sparkplug-tck -fixture - -json < capture.json   # JSON results to stdout
```

Exit code is non-zero if any assertion failed.

## Implemented assertions

```
tck-id-topic-structure-namespace-a
tck-id-topics-nbirth-mqtt
tck-id-topics-nbirth-seq-num
tck-id-payloads-nbirth-seq
tck-id-payloads-nbirth-timestamp
tck-id-payloads-nbirth-bdseq
tck-id-payloads-ndeath-seq
tck-id-payloads-ndeath-bdseq
tck-id-payloads-sequence-num-incrementing
```

9 of 274. Adding more is a matter of `runner.Register` + a small function;
see `internal/assertions/assertions.go`.

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
- [ ] MQTT harness — live capture against a broker (paho client, retained STATE)
- [ ] Payload-level checks at scale (108 mostly-mechanical assertions)
- [ ] Sequencing checks: rebirth, STATE, alias rules (63)
- [ ] Edge-node profile parity with HiveMQ TCK
- [ ] Host-application profile parity
- [ ] CI: scheduled spec-drift detection
- [ ] CI: parity test — same fixture run through both TCKs, diff results

## License

Go code: Apache-2.0 (see `LICENSE`).

`assertions.json` contains assertion text excerpted from the Eclipse Sparkplug specification, which is licensed under EPL-2.0. See `NOTICE` for attribution.
