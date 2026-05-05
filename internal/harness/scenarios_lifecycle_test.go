package harness

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/joyautomation/sparkplug-tck-go/internal/runner"
	"github.com/joyautomation/sparkplug-tck-go/internal/spbpb"
)

func TestWillNotFiredOnCleanDisconnect_Pass(t *testing.T) {
	b, _ := NewBroker()
	defer b.Close()
	c := connectEdge(t, b, "edge-clean", "spBv1.0/G/NDEATH/N")
	c.Disconnect(200) // clean DISCONNECT — Will MUST NOT fire
	time.Sleep(100 * time.Millisecond)

	res := WillNotFiredOnCleanDisconnect(b)
	if len(res) != 1 || res[0].Status != runner.StatusPass {
		t.Errorf("expected pass, got %+v", res)
	}
}

func TestWillNotFiredOnCleanDisconnect_NA_OnUnclean(t *testing.T) {
	b, _ := NewBroker()
	defer b.Close()
	connectEdge(t, b, "edge-uncl", "spBv1.0/G/NDEATH/N")
	time.Sleep(50 * time.Millisecond)
	killBrokerSide(t, b, "edge-uncl") // unclean — Will fires; rule doesn't apply
	time.Sleep(100 * time.Millisecond)

	res := WillNotFiredOnCleanDisconnect(b)
	if len(res) != 1 || res[0].Status != runner.StatusNotApplicable {
		t.Errorf("expected NA for unclean disconnect, got %+v", res)
	}
}

// rebirthCmd builds a Rebirth NCMD payload — single boolean metric
// "Node Control/Rebirth" = true, no seq/timestamp required by the
// scenario.
func rebirthCmd() []byte {
	dt := uint32(spbpb.DataType_Boolean)
	name := "Node Control/Rebirth"
	tru := true
	p := &spbpb.Payload{Metrics: []*spbpb.Payload_Metric{{
		Name:     &name,
		Datatype: &dt,
		Value:    &spbpb.Payload_Metric_BooleanValue{BooleanValue: tru},
	}}}
	raw, _ := proto.Marshal(p)
	return raw
}

func TestEdgeRespondsToRebirth_Pass(t *testing.T) {
	b, _ := NewBroker()
	defer b.Close()
	c := connectEdge(t, b, "edge-reb", "spBv1.0/G/NDEATH/N")
	defer c.Disconnect(200)
	c.Publish("spBv1.0/G/NBIRTH/N", 0, false, nbirth(0)).WaitTimeout(time.Second)
	time.Sleep(50 * time.Millisecond)

	// Inject the NCMD/Rebirth via the broker's inline publish — the
	// recorder sees it as EvPublish from the inline client.
	if err := b.Publish("spBv1.0/G/NCMD/N", rebirthCmd(), false, 1); err != nil {
		t.Fatalf("inject rebirth: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	// Edge responds with a fresh NBIRTH within the deadline.
	c.Publish("spBv1.0/G/NBIRTH/N", 0, false, nbirth(0)).WaitTimeout(time.Second)
	time.Sleep(50 * time.Millisecond)

	res := EdgeRespondsToRebirth(b)
	if len(res) != 1 || res[0].Status != runner.StatusPass {
		t.Errorf("expected pass, got %+v", res)
	}
}

func TestEdgeRespondsToRebirth_NoResponse(t *testing.T) {
	b, _ := NewBroker()
	defer b.Close()
	c := connectEdge(t, b, "edge-reb-bad", "spBv1.0/G/NDEATH/N")
	defer c.Disconnect(200)
	c.Publish("spBv1.0/G/NBIRTH/N", 0, false, nbirth(0)).WaitTimeout(time.Second)
	time.Sleep(50 * time.Millisecond)

	if err := b.Publish("spBv1.0/G/NCMD/N", rebirthCmd(), false, 1); err != nil {
		t.Fatalf("inject rebirth: %v", err)
	}
	// No response NBIRTH published.
	time.Sleep(100 * time.Millisecond)

	res := EdgeRespondsToRebirth(b)
	if len(res) != 1 || res[0].Status != runner.StatusFail {
		t.Errorf("expected fail (no NBIRTH after rebirth), got %+v", res)
	}
}
