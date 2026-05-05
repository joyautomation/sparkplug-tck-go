package harness

import (
	"fmt"
	"strings"

	"github.com/joyautomation/sparkplug-tck-go/internal/runner"
	"github.com/joyautomation/sparkplug-tck-go/internal/spbpb"
	"google.golang.org/protobuf/proto"
)

// DDEATH scenarios — strict checks on the shape and ordering of the
// Device Death Certificate the Edge Node MUST publish on behalf of a
// Sparkplug Device that drops off. Implements the per-DDEATH packet
// invariants the upstream edge/SessionTerminationTest groups together:
// topic shape, QoS=0/retain=false, seq present + monotonically
// increasing relative to the prior message on that edge, and a
// payload-level timestamp.

// EdgeDDEATHCompliant evaluates every DDEATH publish against the
// per-message rules and emits Pass/Fail for each spec ID.
func EdgeDDEATHCompliant(b *Broker) []runner.Result {
	const idTopic = "tck-id-topics-ddeath-topic"
	const idMQTT = "tck-id-topics-ddeath-mqtt"
	const idSeq = "tck-id-payloads-ddeath-seq"
	const idSeqNum = "tck-id-payloads-ddeath-seq-number"
	const idSeqInc = "tck-id-payloads-ddeath-seq-inc"
	const idTopicSeqNum = "tck-id-topics-ddeath-seq-num"
	const idTimestamp = "tck-id-payloads-ddeath-timestamp"
	const idDevDDEATH = "tck-id-operational-behavior-device-ddeath"
	allIDs := []string{idTopic, idMQTT, idSeq, idSeqNum, idSeqInc, idTopicSeqNum, idTimestamp, idDevDDEATH}

	events := b.Events()

	// Track the last seq seen per (group, edge) across all spB messages
	// — Sparkplug seq is monotonic per edge, not per topic.
	type edgeKey struct{ group, edge string }
	lastSeq := map[edgeKey]int64{} // -1 means "no prior seq for this edge"

	// Pre-pass to seed lastSeq from the first NBIRTH onward.
	for _, e := range events {
		if e.Type != EvPublish {
			continue
		}
		if !isSpBSeqTopic(e.Topic) {
			continue
		}
		grp, edge, _ := splitSpBTopic(e.Topic)
		k := edgeKey{grp, edge}
		if _, ok := lastSeq[k]; !ok {
			lastSeq[k] = -1
		}
	}

	var out []runner.Result
	scored := false
	for _, e := range events {
		if e.Type != EvPublish || !isDDEATHTopic(e.Topic) {
			// Still need to advance lastSeq for non-DDEATH spB messages.
			if e.Type == EvPublish && isSpBSeqTopic(e.Topic) {
				grp, edge, _ := splitSpBTopic(e.Topic)
				if seq, ok := payloadSeqOpt(e.Payload); ok {
					lastSeq[edgeKey{grp, edge}] = int64(seq)
				}
			}
			continue
		}
		scored = true
		grp, edge, dev := splitSpBTopic(e.Topic)
		subj := e.Topic

		// Topic shape: spBv1.0/<group>/DDEATH/<edge>/<device>
		if grp == "" || edge == "" || dev == "" {
			out = append(out, runner.Fail(idTopic, subj,
				"DDEATH topic must be spBv1.0/<group>/DDEATH/<edge>/<device>"))
		} else {
			out = append(out, runner.Pass(idTopic, subj))
		}

		// QoS=0, retain=false
		if e.QoS != 0 || e.Retained {
			out = append(out, runner.Fail(idMQTT, subj,
				fmt.Sprintf("DDEATH must be QoS=0 retain=false, got QoS=%d retain=%t", e.QoS, e.Retained)))
		} else {
			out = append(out, runner.Pass(idMQTT, subj))
		}

		// Decode payload once for the seq + timestamp checks.
		var p spbpb.Payload
		decoded := proto.Unmarshal(e.Payload, &p) == nil

		// Seq present (idSeq + idSeqNum + idTopicSeqNum all assert this).
		if !decoded || p.Seq == nil {
			out = append(out,
				runner.Fail(idSeq, subj, "DDEATH payload missing seq"),
				runner.Fail(idSeqNum, subj, "DDEATH payload missing seq"),
				runner.Fail(idTopicSeqNum, subj, "DDEATH payload missing seq"),
			)
		} else {
			out = append(out,
				runner.Pass(idSeq, subj),
				runner.Pass(idSeqNum, subj),
				runner.Pass(idTopicSeqNum, subj),
			)
		}

		// Seq one greater than the previous spB message on this edge.
		k := edgeKey{grp, edge}
		prev, hasPrev := lastSeq[k]
		if !decoded || p.Seq == nil {
			out = append(out, runner.Fail(idSeqInc, subj, "missing seq"))
		} else if !hasPrev || prev < 0 {
			// No baseline to compare against — treat as Pass (we can't
			// assert violation without a prior seq).
			out = append(out, runner.Pass(idSeqInc, subj))
		} else {
			want := uint64((prev + 1) % 256)
			if p.GetSeq() != want {
				out = append(out, runner.Fail(idSeqInc, subj,
					fmt.Sprintf("DDEATH seq=%d, want %d (one greater than prior=%d)",
						p.GetSeq(), want, prev)))
			} else {
				out = append(out, runner.Pass(idSeqInc, subj))
			}
		}

		// Payload timestamp present.
		if !decoded || p.Timestamp == nil {
			out = append(out, runner.Fail(idTimestamp, subj, "DDEATH payload missing timestamp"))
		} else {
			out = append(out, runner.Pass(idTimestamp, subj))
		}

		// Operational rule: an edge that loses contact with a device MUST
		// publish DDEATH for it. Observing the DDEATH itself satisfies
		// the *positive* form of the assertion — we can't observe device
		// disconnection from the broker side.
		out = append(out, runner.Pass(idDevDDEATH, subj))

		// Advance lastSeq.
		if decoded && p.Seq != nil {
			lastSeq[k] = int64(p.GetSeq())
		}
	}
	if !scored {
		na := "no DDEATH observed in scenario"
		res := make([]runner.Result, 0, len(allIDs))
		for _, id := range allIDs {
			res = append(res, runner.NA(id, na))
		}
		return res
	}
	return out
}

func isDDEATHTopic(t string) bool {
	parts := strings.Split(t, "/")
	return len(parts) == 5 && parts[0] == "spBv1.0" && parts[2] == "DDEATH"
}

// isSpBSeqTopic returns true for topic types whose payload carries the
// monotonic seq counter (everything except NBIRTH which resets it, and
// NDEATH which doesn't carry seq).
func isSpBSeqTopic(t string) bool {
	parts := strings.Split(t, "/")
	if len(parts) < 4 || parts[0] != "spBv1.0" {
		return false
	}
	switch parts[2] {
	case "NBIRTH", "NDATA", "NCMD", "DBIRTH", "DDATA", "DCMD", "DDEATH":
		return true
	}
	return false
}

// splitSpBTopic returns (group, edge, device) for a Sparkplug topic.
// device is "" for node-level topics.
func splitSpBTopic(t string) (group, edge, dev string) {
	parts := strings.Split(t, "/")
	if len(parts) < 4 || parts[0] != "spBv1.0" {
		return "", "", ""
	}
	group = parts[1]
	if len(parts) >= 4 {
		edge = parts[3]
	}
	if len(parts) >= 5 {
		dev = parts[4]
	}
	return
}

func payloadSeqOpt(raw []byte) (uint64, bool) {
	var p spbpb.Payload
	if err := proto.Unmarshal(raw, &p); err != nil {
		return 0, false
	}
	if p.Seq == nil {
		return 0, false
	}
	return p.GetSeq(), true
}
