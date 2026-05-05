package harness

import (
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"google.golang.org/protobuf/proto"

	"github.com/joyautomation/sparkplug-tck-go/internal/runner"
	"github.com/joyautomation/sparkplug-tck-go/internal/spbpb"
)

// nbirth builds an NBIRTH protobuf with a bdSeq metric. We deliberately
// keep this separate from edgeWill so a test can pass mismatched values
// to exercise the matching/increment rules.
func nbirth(bdSeq uint64) []byte {
	dt := uint32(spbpb.DataType_UInt64)
	name := "bdSeq"
	val := bdSeq
	zero := uint64(0)
	ts := uint64(time.Now().UnixMilli())
	p := &spbpb.Payload{
		Timestamp: &ts,
		Seq:       &zero,
		Metrics: []*spbpb.Payload_Metric{{
			Name:      &name,
			Datatype:  &dt,
			Timestamp: &ts,
			Value:     &spbpb.Payload_Metric_LongValue{LongValue: val},
		}},
	}
	raw, _ := proto.Marshal(p)
	return raw
}

func TestEdgeBdSeqMatchesWill_Pass(t *testing.T) {
	b, _ := NewBroker()
	defer b.Close()
	c := connectEdge(t, b, "edge-match", "spBv1.0/G/NDEATH/N")
	defer c.Disconnect(200)
	c.Publish("spBv1.0/G/NBIRTH/N", 0, false, nbirth(0)).WaitTimeout(2 * time.Second)
	time.Sleep(50 * time.Millisecond)

	res := EdgeBdSeqMatchesWill(b)
	if len(res) != 1 || res[0].Status != runner.StatusPass {
		t.Errorf("expected pass, got %+v", res)
	}
}

func TestEdgeBdSeqMatchesWill_Mismatch(t *testing.T) {
	b, _ := NewBroker()
	defer b.Close()
	// Will declares bdSeq=0, NBIRTH publishes bdSeq=42 — the edge
	// rolled mid-session. tck-id-payloads-nbirth-bdseq-repeat fail.
	c := connectEdge(t, b, "edge-mismatch", "spBv1.0/G/NDEATH/N")
	defer c.Disconnect(200)
	c.Publish("spBv1.0/G/NBIRTH/N", 0, false, nbirth(42)).WaitTimeout(2 * time.Second)
	time.Sleep(50 * time.Millisecond)

	res := EdgeBdSeqMatchesWill(b)
	if len(res) != 1 || res[0].Status != runner.StatusFail {
		t.Errorf("expected fail for mismatched bdSeq, got %+v", res)
	}
}

func TestEdgeBdSeqIncrements_Pass(t *testing.T) {
	b, _ := NewBroker()
	defer b.Close()
	// Reconnect three times, advancing bdSeq each session — same
	// clientID so the harness associates them.
	for seq := uint64(0); seq < 3; seq++ {
		opts := mqtt.NewClientOptions().
			AddBroker(b.URL()).
			SetClientID("edge-inc").
			SetCleanSession(true).
			SetConnectTimeout(2 * time.Second).
			SetBinaryWill("spBv1.0/G/NDEATH/N", edgeWill(seq), 1, false)
		c := mqtt.NewClient(opts)
		c.Connect().WaitTimeout(2 * time.Second)
		c.Disconnect(100)
		time.Sleep(40 * time.Millisecond)
	}

	res := EdgeBdSeqIncrements(b)
	for _, r := range res {
		if r.Status == runner.StatusFail {
			t.Errorf("unexpected fail: %+v", r)
		}
	}
	if len(res) != 2 { // sessions #2 and #3 are scored; #1 has no prior
		t.Errorf("expected 2 results (sessions 2+3), got %d: %+v", len(res), res)
	}
}

func TestEdgeBdSeqIncrements_Skip(t *testing.T) {
	b, _ := NewBroker()
	defer b.Close()
	// First session bdSeq=0, second session bdSeq=5 (jumped, not +1).
	for _, seq := range []uint64{0, 5} {
		opts := mqtt.NewClientOptions().
			AddBroker(b.URL()).
			SetClientID("edge-skip").
			SetCleanSession(true).
			SetConnectTimeout(2 * time.Second).
			SetBinaryWill("spBv1.0/G/NDEATH/N", edgeWill(seq), 1, false)
		c := mqtt.NewClient(opts)
		c.Connect().WaitTimeout(2 * time.Second)
		c.Disconnect(100)
		time.Sleep(40 * time.Millisecond)
	}

	res := EdgeBdSeqIncrements(b)
	if len(res) != 1 || res[0].Status != runner.StatusFail {
		t.Errorf("expected single fail for skipped bdSeq, got %+v", res)
	}
}
