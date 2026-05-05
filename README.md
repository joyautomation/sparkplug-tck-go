# sparkplug-tck-go

A Go reimplementation of the [Eclipse Sparkplug Test Compatibility Kit](https://github.com/eclipse-sparkplug/sparkplug/tree/master/tck).

The official TCK is a Java/HiveMQ stack: ~10s JVM warmup, heavyweight to run as a CI gate. This is the same conformance check, single static binary, no JVM.

## Status

Early. Assertion catalog is extracted; harness and per-assertion checks are not yet implemented.

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
scripts/update-assertions.sh
assertions.json           # checked-in catalog; regenerate via script
```

## Regenerating the catalog

```sh
bash scripts/update-assertions.sh
```

Set `SPARKPLUG_SPEC_REF` to pin a tag instead of `master`.

## Roadmap

- [x] Assertion catalog extractor + checked-in JSON
- [ ] MQTT harness (subscribe `spBv1.0/#` + `STATE/#`, capture session per edge node)
- [ ] Assertion runner: payload-level checks (108 of these — mostly mechanical)
- [ ] Sequencing checks: rebirth, STATE, alias rules (the hard 63)
- [ ] Edge-node profile parity with HiveMQ TCK
- [ ] Host-application profile parity
- [ ] CI: scheduled spec-drift detection
- [ ] CI: parity test — same fixture run through both TCKs, diff results

## License

Go code: Apache-2.0 (see `LICENSE`).

`assertions.json` contains assertion text excerpted from the Eclipse Sparkplug specification, which is licensed under EPL-2.0. See `NOTICE` for attribution.
