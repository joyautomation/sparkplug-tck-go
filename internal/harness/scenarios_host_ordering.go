package harness

import (
	"fmt"
	"slices"
	"time"

	"github.com/joyautomation/sparkplug-tck-go/internal/runner"
	"github.com/joyautomation/sparkplug-tck-go/internal/spbpb"
	"google.golang.org/protobuf/proto"
)

// Host message-ordering scenarios — strict form of the four
// tck-id-operational-behavior-host-reordering-* assertions. The Host
// Application is expected to start an internal Reorder Timeout when an
// NDATA/DDATA arrives with an out-of-order seq, and either
//   (a) terminate the timer if the missing message arrives before it
//       elapses (the *-success case), or
//   (b) publish NCMD/Rebirth to the edge if it doesn't (the *-rebirth
//       case, which also implies *-param and *-start).
//
// We can't observe the timer directly, but we can observe its
// behavioural effects on the wire: a gap in the seq stream followed by
// either the missing seq arriving (success) or NCMD/Rebirth being
// published (rebirth).

// hostReorderWindow is the upper bound we use to bound "how long the
// host might wait" before counting a missing recovery as failure or a
// rebirth as on-time. The spec is silent on a default value; the
// upstream Java TCK uses a configurable param (typically a few seconds).
const hostReorderWindow = 30 * time.Second

func HostMessageOrdering(b *Broker) []runner.Result {
	const idParam = "tck-id-operational-behavior-host-reordering-param"
	const idStart = "tck-id-operational-behavior-host-reordering-start"
	const idRebirth = "tck-id-operational-behavior-host-reordering-rebirth"
	const idSuccess = "tck-id-operational-behavior-host-reordering-success"

	gaps := findSeqGaps(b.Events())
	if len(gaps) == 0 {
		na := "no out-of-order NDATA/DDATA observed in scenario"
		return []runner.Result{
			runner.NA(idParam, na),
			runner.NA(idStart, na),
			runner.NA(idRebirth, na),
			runner.NA(idSuccess, na),
		}
	}

	var out []runner.Result
	events := b.Events()
	for _, g := range gaps {
		// Walk events after the gap looking for either:
		//   (a) the missing seq arriving on the same edge -> recovered
		//   (b) NCMD/Rebirth on this edge -> host rebirthed
		var rebirthAt, recoveredAt time.Time
		for _, e := range events {
			if !e.At.After(g.at) {
				continue
			}
			if e.At.Sub(g.at) > hostReorderWindow {
				break
			}
			if e.Type != EvPublish {
				continue
			}
			if e.Topic == g.ncmdTopic && payloadHasRebirthTrue(e.Payload) {
				if rebirthAt.IsZero() {
					rebirthAt = e.At
				}
				continue
			}
			// The missing seq can arrive on any of the edge's seq-bearing
			// topics — another device's DDATA, or a DBIRTH/DDEATH — not
			// just the topic that arrived out of order.
			if g.isEdgeSeqTopic(e.Topic) && payloadSeq(e.Payload)%256 == g.missing {
				if recoveredAt.IsZero() {
					recoveredAt = e.At
				}
			}
		}

		subj := g.ncmdTopic + fmt.Sprintf(" (gap before seq=%d)", g.observed)
		switch {
		case !recoveredAt.IsZero() && rebirthAt.IsZero():
			// Missing arrived; host did not rebirth -> timer terminated normally.
			out = append(out, runner.Pass(idSuccess, subj))
		case !recoveredAt.IsZero() && !rebirthAt.IsZero() && rebirthAt.Before(recoveredAt):
			// Host rebirthed before the missing arrived -> rebirth path,
			// success doesn't apply for this gap.
			out = append(out,
				runner.Pass(idParam, subj),
				runner.Pass(idStart, subj),
				runner.Pass(idRebirth, subj),
			)
		case !rebirthAt.IsZero():
			out = append(out,
				runner.Pass(idParam, subj),
				runner.Pass(idStart, subj),
				runner.Pass(idRebirth, subj),
			)
		default:
			out = append(out, runner.Fail(idRebirth, subj,
				fmt.Sprintf("seq gap (missing=%d) but host published no NCMD/Rebirth within %s and no recovery observed",
					g.missing, hostReorderWindow)))
		}
	}
	return out
}

// seqGap describes a detected gap in an edge's NDATA/DDATA seq stream.
type seqGap struct {
	at        time.Time // time of the out-of-order publish
	missing   uint64    // the seq number the host is waiting on
	observed  uint64    // the seq number that arrived (out of order)
	ncmdTopic string    // spBv1.0/<group>/NCMD/<edge>
	group     string
	edge      string
}

// isEdgeSeqTopic reports whether t is a message from the gap's edge that
// consumes a seq number after NBIRTH: DBIRTH, NDATA, DDATA or DDEATH.
func (g seqGap) isEdgeSeqTopic(t string) bool {
	var grp, edge string
	switch {
	case isNDATATopic(t):
		grp, edge = topicParts2(t)
	case isDDATATopic(t), isDBIRTHTopic(t), isDDEATHTopic(t):
		grp, edge, _ = topicParts3(t)
	default:
		return false
	}
	return grp == g.group && edge == g.edge
}

// findSeqGaps walks NBIRTH/DBIRTH/NDATA/DDATA/DDEATH events grouped by
// (group,edge) and returns each instance where seq jumped non-monotonically.
// Per Sparkplug B 3.0 §6.4.6, NBIRTH establishes seq=0 and EVERY subsequent
// data-bearing message — DBIRTH, NDATA, DDATA, DDEATH — increments the same
// seq counter by 1 modulo 256. Tracking only NDATA/DDATA misses DBIRTH's
// consumption of seq=1, which would mis-flag a normal NBIRTH(0)/DBIRTH(1)/
// DDATA(2) stream as a gap and demand a rebirth from a well-behaved edge.
//
// We treat the gap as detected only on data-bearing topics (NDATA/DDATA),
// because those are the ones a host could plausibly act on with a rebirth;
// a DBIRTH out-of-order gap is rarer in practice and the assertions below
// frame "host-reordering" in terms of NDATA/DDATA recovery.
//
// The tracker models a reordering host: seqs that arrive ahead of the one
// it's waiting on are buffered, and a gap is recorded once per awaited seq,
// never for a seq that has already arrived. So …6, 8, 7, 9… is one gap
// (missing 7, filled), not three — a host that reorders correctly must not
// be graded as if 8 went missing.
func findSeqGaps(events []Event) []seqGap {
	type key struct{ group, edge string }
	type state struct {
		next    uint64    // seq the host is waiting on
		started bool      // an NBIRTH (or first message) has set next
		seen    [256]bool // seqs that arrived ahead of next, awaiting it
		gapped  bool      // a gap has already been recorded for next
	}
	st := map[key]*state{}
	var gaps []seqGap
	for _, e := range events {
		if e.Type != EvPublish {
			continue
		}
		t := e.Topic
		var grp, edge string
		var isBirth, isData bool
		switch {
		case isNBIRTHTopic(t):
			grp, edge = topicParts2(t)
			isBirth = true
		case isDBIRTHTopic(t), isDDEATHTopic(t):
			grp, edge, _ = topicParts3(t)
		case isNDATATopic(t):
			grp, edge = topicParts2(t)
			isData = true
		case isDDATATopic(t):
			grp, edge, _ = topicParts3(t)
			isData = true
		default:
			continue
		}
		k := key{grp, edge}
		s := st[k]
		if s == nil {
			s = &state{}
			st[k] = s
		}
		seq := payloadSeq(e.Payload) % 256
		if isBirth || !s.started {
			// NBIRTH establishes seq=0; next expected is 1. A stream we
			// joined mid-flight starts from whatever arrived first.
			*s = state{next: (seq + 1) % 256, started: true}
			continue
		}

		// Forward distance from the seq the host is waiting on. Within
		// half the ring it's ahead (the host must buffer it); otherwise
		// it's a duplicate or a late copy of something already applied.
		switch ahead := (seq + 256 - s.next) % 256; {
		case ahead == 0:
			// The awaited seq arrived: apply it and anything buffered
			// behind it, the way a reordering host drains its buffer.
			s.next = (s.next + 1) % 256
			s.gapped = false
			for s.seen[s.next] {
				s.seen[s.next] = false
				s.next = (s.next + 1) % 256
			}
			// Still holding buffered seqs means a further hole: the host
			// is now waiting on the new next.
			if slices.Contains(s.seen[:], true) {
				gaps = append(gaps, newSeqGap(e.At, s.next, seq, grp, edge))
				s.gapped = true
			}
		case ahead < 128:
			s.seen[seq] = true
			if isData && !s.gapped {
				gaps = append(gaps, newSeqGap(e.At, s.next, seq, grp, edge))
				s.gapped = true
			}
		}
	}
	return gaps
}

func newSeqGap(at time.Time, missing, observed uint64, grp, edge string) seqGap {
	return seqGap{
		at:        at,
		missing:   missing,
		observed:  observed,
		ncmdTopic: "spBv1.0/" + grp + "/NCMD/" + edge,
		group:     grp,
		edge:      edge,
	}
}

func payloadSeq(raw []byte) uint64 {
	var p spbpb.Payload
	if err := proto.Unmarshal(raw, &p); err != nil {
		return 0
	}
	return p.GetSeq()
}

func splitTopic(t string) []string {
	out := make([]string, 0, 5)
	last := 0
	for i := 0; i < len(t); i++ {
		if t[i] == '/' {
			out = append(out, t[last:i])
			last = i + 1
		}
	}
	out = append(out, t[last:])
	return out
}

func topicParts2(t string) (group, edge string) {
	parts := splitTopic(t)
	if len(parts) >= 4 {
		return parts[1], parts[3]
	}
	return "", ""
}

func topicParts3(t string) (group, edge, dev string) {
	parts := splitTopic(t)
	if len(parts) >= 5 {
		return parts[1], parts[3], parts[4]
	}
	return "", "", ""
}
