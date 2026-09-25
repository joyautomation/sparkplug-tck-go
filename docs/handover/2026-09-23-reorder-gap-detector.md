# `findSeqGaps` fails a host that reordered correctly

> **Resolved** in #9 (merged 2026-09-25). `findSeqGaps` now buffers seqs that arrive ahead of the awaited one
> (`d1899fc`), with the table test below in `internal/harness/scenarios_host_ordering_test.go`. The same commit
> widens recovery matching to any of the edge's DBIRTH/NDATA/DDATA/DDEATH topics. Confirmed end to end in the
> `ignition` repo: with the simulator's swap restored, Mantle passes all four `host-reordering-*` assertions.

Found 2026-09-23 while grading Mantle (the Ignition Sparkplug host module) against the host-application
profile. **The kit reported a conformance failure against a host that behaved exactly to spec.**

## The bug

`findSeqGaps` in `internal/harness/scenarios_host_ordering.go` tracks only `next` — the expected sequence
number — and advances it unconditionally on every data message:

```go
if seq != s.next && isData {
    gaps = append(gaps, seqGap{ missing: s.next, observed: seq, ... })
}
s.next = (seq + 1) % 256     // <- unconditional
```

It keeps no memory of sequence numbers already seen. So an edge publishing `…6, 8, 7, 9…` — legal Sparkplug,
and precisely what a host's reorder buffer exists for — registers as **three** gaps:

| observed | `s.next` | recorded | filled later? |
|---|---|---|---|
| 8 | 7 | gap, missing 7 | yes, 7 arrives |
| 7 | 9 | gap, missing 9 | yes, 9 arrives |
| 9 | 8 | gap, missing 8 | **never — 8 arrived first** |

The third gap can never be filled, so `tck-id-operational-behavior-host-reordering-rebirth` fails with:

```
seq gap (missing=8) but host published no NCMD/Rebirth within 30s and no recovery observed
```

The host under test had buffered 8, accepted 7, applied both in order, and correctly did **not** request a
rebirth. There was nothing to rebirth for. The failure is entirely an artefact of the detector.

## Why it matters

This is the assertion that grades the single most important thing a Sparkplug host does with out-of-order
data. Right now the kit can only pass it when the gap is a genuine drop; a host that reorders — the correct
behaviour — gets marked non-conformant. Anyone using this to certify a host will either get a false failure
or, worse, tune their host until the kit is happy.

## Suggested fix

Make the tracker reorder-tolerant: remember what has been seen, and only call something a gap if the
expected sequence number has *not* already arrived.

```go
type state struct {
    next uint64
    seen map[uint64]bool   // or a [256]bool ring, since seq wraps at 256
    ...
}
```

- On every data message, mark `seen[seq]`.
- If `seq == s.next`, advance: `for s.seen[s.next] { s.next = (s.next + 1) % 256 }`.
- If `seq != s.next`, record a gap **only if `!s.seen[s.next]`**, and do not clobber `s.next` with `seq+1`
  when `seq > s.next` — the host is still legitimately waiting on `s.next`.
- A `seq` below `s.next` is a late arrival: mark it seen and re-run the advance loop.

Wrap-around at 256 needs care: "below" is modular, so compare within a window rather than numerically.

## Test to add

There is no test touching `findSeqGaps` today (`grep -l findSeqGaps internal/harness/*_test.go` is empty),
which is how this survived. A table test in `internal/harness/scenarios_host_ordering_test.go` would pin it:

| stream | expected gaps |
|---|---|
| `0(NBIRTH),1,2,3` | none |
| `0,1,3,2` (swap) | **one**, filled by 2 |
| `0,1,3,4` (drop 2) | one, never filled |
| `0,1,3,2,4` | one, filled |
| wrap: `254,255,0,1` | none |
| wrap with swap: `254,0,255,1` | one, filled |

The swap cases are the regression; today the second row yields three.

## How it was found, if you want to reproduce end to end

In the `ignition` repo, `scripts/tck-conformance.sh` runs this kit's host profile against a live Mantle. Its
`EdgeSimulator` has a `SIM_CONFORMANCE=1` mode; it now *drops* a sequence number rather than swapping two,
specifically to avoid this bug — see the comment on `dropOneMessage` in
`mantle/gateway/src/test/java/com/joyautomation/ignition/mantle/sim/EdgeSimulator.java`. Restoring the swap
reproduces the false failure.

Write-up with the full trace: `ignition/docs/releasing.md`, "A bug found in our own TCK".

## Note

The repo had uncommitted work on `scenarios_birth.go` and a new `scenarios_birth_test.go` when this was
written. Nothing here touches those files.
