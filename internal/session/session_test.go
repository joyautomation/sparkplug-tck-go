package session

import (
	"testing"
	"time"

	"github.com/joyautomation/sparkplug-tck-go/internal/spb"
	"github.com/joyautomation/sparkplug-tck-go/internal/spbpb"
)

func u64(v uint64) *uint64 { return &v }
func str(v string) *string { return &v }

func node(group, n string, mt spb.MessageType, seq *uint64, metrics ...*spbpb.Payload_Metric) spb.Message {
	return spb.Message{
		Topic: spb.Topic{
			Namespace:  spb.Namespace,
			EdgeNodeID: spb.EdgeNodeID{Group: group, Node: n},
			Type:       mt,
		},
		Payload: &spbpb.Payload{Seq: seq, Metrics: metrics},
		At:      time.Now(),
	}
}

func TestApply_NBIRTH_then_NDEATH(t *testing.T) {
	tr := New()
	tr.Apply(node("G1", "N1", spb.NBIRTH, u64(0),
		&spbpb.Payload_Metric{Name: str("bdSeq"), Value: &spbpb.Payload_Metric_LongValue{LongValue: 7}},
		&spbpb.Payload_Metric{Name: str("temp"), Alias: u64(101)},
	))
	es := tr.Edges[spb.EdgeNodeID{Group: "G1", Node: "N1"}]
	if es == nil || !es.Online || es.BirthCount != 1 {
		t.Fatalf("after NBIRTH: state = %+v", es)
	}
	if es.BirthBdSeq == nil || *es.BirthBdSeq != 7 {
		t.Errorf("bdSeq = %v, want 7", es.BirthBdSeq)
	}
	if got := es.NodeAliases[101]; got != "temp" {
		t.Errorf("alias 101 -> %q, want temp", got)
	}
	if es.LastSeq != 0 {
		t.Errorf("LastSeq = %d, want 0", es.LastSeq)
	}

	tr.Apply(node("G1", "N1", spb.NDEATH, nil))
	if es.Online || es.DeathCount != 1 {
		t.Errorf("after NDEATH: %+v", es)
	}
}

func TestApply_DBIRTH_DDATA_DDEATH(t *testing.T) {
	tr := New()
	tr.Apply(node("G", "N", spb.NBIRTH, u64(0)))
	tr.Apply(spb.Message{
		Topic:   spb.Topic{Namespace: spb.Namespace, EdgeNodeID: spb.EdgeNodeID{Group: "G", Node: "N"}, Type: spb.DBIRTH, Device: "D1"},
		Payload: &spbpb.Payload{Seq: u64(1)},
	})
	tr.Apply(spb.Message{
		Topic:   spb.Topic{Namespace: spb.Namespace, EdgeNodeID: spb.EdgeNodeID{Group: "G", Node: "N"}, Type: spb.DDATA, Device: "D1"},
		Payload: &spbpb.Payload{Seq: u64(2)},
	})
	tr.Apply(spb.Message{
		Topic:   spb.Topic{Namespace: spb.Namespace, EdgeNodeID: spb.EdgeNodeID{Group: "G", Node: "N"}, Type: spb.DDEATH, Device: "D1"},
		Payload: &spbpb.Payload{Seq: u64(3)},
	})
	es := tr.Edges[spb.EdgeNodeID{Group: "G", Node: "N"}]
	d := es.Devices["D1"]
	if d == nil || d.BirthCount != 1 || d.DeathCount != 1 || d.DataCount != 1 || d.Online {
		t.Errorf("device state wrong: %+v", d)
	}
	if es.LastSeq != 3 {
		t.Errorf("LastSeq = %d, want 3", es.LastSeq)
	}
}

func TestApply_Host_STATE_3x(t *testing.T) {
	tr := New()
	tr.Apply(spb.Message{
		Topic: spb.Topic{Namespace: spb.Namespace, Type: spb.STATE, Host: "host1"},
		State: &spb.StatePayload{Online: true, Timestamp: 1234},
	})
	h := tr.Hosts["host1"]
	if h == nil || !h.Online || h.Legacy {
		t.Fatalf("host state wrong: %+v", h)
	}
	if len(h.History) != 1 || h.History[0].Timestamp != 1234 {
		t.Errorf("history wrong: %+v", h.History)
	}
}

func TestApply_Host_STATE_legacy(t *testing.T) {
	tr := New()
	tr.Apply(spb.Message{
		Topic: spb.Topic{Namespace: spb.Namespace, Type: spb.STATE, Host: "host2"},
		State: &spb.StatePayload{Online: false, Legacy: true},
	})
	h := tr.Hosts["host2"]
	if h == nil || h.Online || !h.Legacy {
		t.Fatalf("legacy state wrong: %+v", h)
	}
}
