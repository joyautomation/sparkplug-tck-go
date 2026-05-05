package harness

import (
	"fmt"
	"strings"

	"github.com/joyautomation/sparkplug-tck-go/internal/runner"
	"github.com/joyautomation/sparkplug-tck-go/internal/spbpb"
	"google.golang.org/protobuf/proto"
)

// bdSeq-bound scenarios — strict checks that pair the Will payload at
// CONNECT with the NBIRTH PUBLISH that follows it. Passive captures
// can't observe both values together (the Will only fires on unclean
// disconnect), so these are harness-only.

// EdgeBdSeqMatchesWill: every NBIRTH the edge publishes MUST carry a
// bdSeq metric whose value equals the bdSeq from the Will payload of
// the same CONNECT. Strict form of tck-id-payloads-nbirth-bdseq-repeat
// and tck-id-topics-nbirth-bdseq-matching.
func EdgeBdSeqMatchesWill(b *Broker) []runner.Result {
	const id = "tck-id-payloads-nbirth-bdseq-repeat"
	events := b.Events()
	willBdSeq := map[string]uint64{}   // clientID -> bdSeq from latest CONNECT Will
	willSeen := map[string]bool{}      // clientID had an NDEATH-shape Will
	birthChecked := map[string]bool{}  // first NBIRTH per clientID — only score that one
	var out []runner.Result
	for _, e := range events {
		switch {
		case e.Type == EvConnect && e.Will != nil && isNDEATHTopic(e.Will.Topic):
			seq, ok := bdSeqFromPayload(e.Will.Payload)
			if !ok {
				continue
			}
			willBdSeq[e.ClientID] = seq
			willSeen[e.ClientID] = true
			birthChecked[e.ClientID] = false
		case e.Type == EvPublish && isNBIRTHTopic(e.Topic):
			if !willSeen[e.ClientID] || birthChecked[e.ClientID] {
				continue
			}
			birthChecked[e.ClientID] = true
			subj := e.ClientID + " " + e.Topic
			seq, ok := bdSeqFromPayload(e.Payload)
			if !ok {
				out = append(out, runner.Fail(id, subj,
					"NBIRTH missing bdSeq metric"))
				continue
			}
			want := willBdSeq[e.ClientID]
			if seq != want {
				out = append(out, runner.Fail(id, subj,
					fmt.Sprintf("NBIRTH bdSeq=%d, Will bdSeq=%d (must match)",
						seq, want)))
			} else {
				out = append(out, runner.Pass(id, subj))
			}
		}
	}
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no NBIRTH paired with NDEATH-Will CONNECT")}
	}
	return out
}

// EdgeBdSeqIncrements: across multiple CONNECTs from the same edge
// (same clientID), the bdSeq advertised in each CONNECT Will MUST
// increment by 1 (mod 256) from the previous CONNECT. Strict form of
// tck-id-topics-nbirth-bdseq-increment — passive captures only see one
// session's bdSeq at a time.
func EdgeBdSeqIncrements(b *Broker) []runner.Result {
	const id = "tck-id-topics-nbirth-bdseq-increment"
	prev := map[string]uint64{}
	count := map[string]int{}
	var out []runner.Result
	for _, e := range b.Events() {
		if e.Type != EvConnect || e.Will == nil || !isNDEATHTopic(e.Will.Topic) {
			continue
		}
		seq, ok := bdSeqFromPayload(e.Will.Payload)
		if !ok {
			continue
		}
		count[e.ClientID]++
		if count[e.ClientID] == 1 {
			prev[e.ClientID] = seq
			continue
		}
		want := (prev[e.ClientID] + 1) % 256
		subj := fmt.Sprintf("%s session #%d", e.ClientID, count[e.ClientID])
		if seq != want {
			out = append(out, runner.Fail(id, subj,
				fmt.Sprintf("CONNECT bdSeq=%d, expected %d (prev=%d, +1 mod 256)",
					seq, want, prev[e.ClientID])))
		} else {
			out = append(out, runner.Pass(id, subj))
		}
		prev[e.ClientID] = seq
	}
	if len(out) == 0 {
		return []runner.Result{runner.NA(id,
			"no client reconnected with NDEATH-Will CONNECTs in scenario")}
	}
	return out
}

// bdSeqFromPayload pulls the integer bdSeq metric out of a Sparkplug
// protobuf payload. Returns (value, true) if the metric exists with a
// recognised integer-valued shape.
func bdSeqFromPayload(raw []byte) (uint64, bool) {
	var p spbpb.Payload
	if err := proto.Unmarshal(raw, &p); err != nil {
		return 0, false
	}
	for _, m := range p.GetMetrics() {
		if m.GetName() != "bdSeq" {
			continue
		}
		switch v := m.Value.(type) {
		case *spbpb.Payload_Metric_LongValue:
			return v.LongValue, true
		case *spbpb.Payload_Metric_IntValue:
			return uint64(v.IntValue), true
		}
		return 0, false
	}
	return 0, false
}

func isNBIRTHTopic(t string) bool {
	parts := strings.Split(t, "/")
	return len(parts) == 4 && parts[0] == "spBv1.0" && parts[2] == "NBIRTH"
}
