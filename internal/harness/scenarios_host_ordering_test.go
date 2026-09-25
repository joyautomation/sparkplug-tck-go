package harness

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/joyautomation/sparkplug-tck-go/internal/runner"
	"github.com/joyautomation/sparkplug-tck-go/internal/spbpb"
)

func seqPayload(seq uint64) []byte {
	raw, _ := proto.Marshal(&spbpb.Payload{Seq: &seq})
	return raw
}

// seqStream turns a list of seqs into PUBLISH events on edge G/N. The
// first entry is an NBIRTH when birth is set, the rest are NDATA.
func seqStream(birth bool, seqs ...uint64) []Event {
	t0 := time.Unix(0, 0)
	var out []Event
	for i, s := range seqs {
		topic := "spBv1.0/G/NDATA/N"
		if i == 0 && birth {
			topic = "spBv1.0/G/NBIRTH/N"
		}
		out = append(out, Event{
			At:      t0.Add(time.Duration(i) * time.Second),
			Type:    EvPublish,
			Topic:   topic,
			Payload: seqPayload(s),
		})
	}
	return out
}

func TestFindSeqGaps(t *testing.T) {
	type gap struct{ missing, observed uint64 }
	cases := []struct {
		name  string
		birth bool
		seqs  []uint64
		want  []gap
	}{
		{"in order", true, []uint64{0, 1, 2, 3}, nil},
		{"swap", true, []uint64{0, 1, 3, 2}, []gap{{2, 3}}},
		{"drop", true, []uint64{0, 1, 3, 4}, []gap{{2, 3}}},
		{"swap then continue", true, []uint64{0, 1, 3, 2, 4}, []gap{{2, 3}}},
		{"handover trace 6,8,7,9", false, []uint64{6, 8, 7, 9}, []gap{{7, 8}}},
		{"wrap", false, []uint64{254, 255, 0, 1}, nil},
		{"wrap with swap", false, []uint64{254, 0, 255, 1}, []gap{{255, 0}}},
		{"duplicate is not a gap", true, []uint64{0, 1, 1, 2}, nil},
		// Two holes: once 2 fills, the host is still waiting on 3.
		{"two holes", true, []uint64{0, 1, 4, 2, 3}, []gap{{2, 4}, {3, 2}}},
		{"rebirth resets", true, []uint64{0, 1, 3, 0, 1}, []gap{{2, 3}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			events := seqStream(tc.birth, tc.seqs...)
			if tc.name == "rebirth resets" {
				events[3].Topic = "spBv1.0/G/NBIRTH/N"
			}
			got := findSeqGaps(events)
			if len(got) != len(tc.want) {
				t.Fatalf("got %d gaps %+v, want %v", len(got), got, tc.want)
			}
			for i, g := range got {
				if g.missing != tc.want[i].missing || g.observed != tc.want[i].observed {
					t.Errorf("gap %d: missing=%d observed=%d, want %+v", i, g.missing, g.observed, tc.want[i])
				}
			}
		})
	}
}

// A host that buffers a swapped pair and applies it without rebirthing is
// behaving to spec; the kit must grade it as reorder success, not failure.
func TestHostMessageOrdering_SwapIsSuccess(t *testing.T) {
	b := &Broker{events: seqStream(false, 6, 8, 7, 9)}
	res := HostMessageOrdering(b)
	if len(res) != 1 {
		t.Fatalf("got %d results %+v, want 1", len(res), res)
	}
	r := res[0]
	if r.AssertionID != "tck-id-operational-behavior-host-reordering-success" || r.Status != runner.StatusPass {
		t.Fatalf("got %+v, want reordering-success PASS", r)
	}
}

// The missing seq can arrive on a different device's DDATA than the one
// that arrived out of order; seq is per edge, not per device.
func TestHostMessageOrdering_RecoveryOnOtherDevice(t *testing.T) {
	events := seqStream(true, 0, 1, 3, 2, 4)
	events[1].Topic = "spBv1.0/G/DBIRTH/N/A"
	events[2].Topic = "spBv1.0/G/DDATA/N/A" // seq 3, out of order
	events[3].Topic = "spBv1.0/G/DDATA/N/B" // seq 2, fills the gap
	events[4].Topic = "spBv1.0/G/DDATA/N/A"
	b := &Broker{events: events}
	res := HostMessageOrdering(b)
	if len(res) != 1 {
		t.Fatalf("got %d results %+v, want 1", len(res), res)
	}
	r := res[0]
	if r.AssertionID != "tck-id-operational-behavior-host-reordering-success" || r.Status != runner.StatusPass {
		t.Fatalf("got %+v, want reordering-success PASS", r)
	}
}
