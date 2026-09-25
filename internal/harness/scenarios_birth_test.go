package harness

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/joyautomation/sparkplug-tck-go/internal/runner"
	"github.com/joyautomation/sparkplug-tck-go/internal/spbpb"
)

// birthMetric describes one metric in a test NBIRTH payload: a name and
// an optional alias (nil = un-aliased).
type birthMetric struct {
	name  string
	alias *uint64
}

func aliasOf(a uint64) *uint64 { return &a }

func birthPayload(metrics ...birthMetric) []byte {
	dt := uint32(spbpb.DataType_Int64)
	ts := uint64(time.Now().UnixMilli())
	zero := uint64(0)
	p := &spbpb.Payload{
		Timestamp: &ts,
		Seq:       &zero,
	}
	for i := range metrics {
		m := &spbpb.Payload_Metric{
			Name:      &metrics[i].name,
			Alias:     metrics[i].alias,
			Datatype:  &dt,
			Timestamp: &ts,
			Value:     &spbpb.Payload_Metric_LongValue{LongValue: uint64(i)},
		}
		p.Metrics = append(p.Metrics, m)
	}
	raw, _ := proto.Marshal(p)
	return raw
}

func aliasReqResult(t *testing.T, res []runner.Result) runner.Result {
	t.Helper()
	for _, r := range res {
		if r.AssertionID == "tck-id-payloads-alias-birth-requirement" {
			return r
		}
	}
	t.Fatalf("tck-id-payloads-alias-birth-requirement missing from %+v", res)
	return runner.Result{}
}

// A spec-conformant aliased edge: every metric aliased except Node
// Control/Rebirth, which the spec REQUIRES to stay un-aliased in NBIRTH
// [tck-id-operational-behavior-data-commands-rebirth-name-aliases]. The
// alias-birth-requirement check must not count that against the edge.
func TestEdgeBirthMetricNaming_AliasedBirthWithUnaliasedRebirth_Pass(t *testing.T) {
	b, _ := NewBroker()
	defer b.Close()
	c := connectEdge(t, b, "edge-alias-ok", "spBv1.0/G/NDEATH/N")
	defer c.Disconnect(200)

	c.Publish("spBv1.0/G/NBIRTH/N", 0, false, birthPayload(
		birthMetric{name: "Node Control/Rebirth"},
		birthMetric{name: "Temperature", alias: aliasOf(1)},
		birthMetric{name: "Pressure", alias: aliasOf(2)},
	)).WaitTimeout(2 * time.Second)
	time.Sleep(50 * time.Millisecond)

	r := aliasReqResult(t, EdgeBirthMetricNaming(b))
	if r.Status != runner.StatusPass {
		t.Errorf("expected pass for aliased birth with un-aliased Node Control/Rebirth, got %+v", r)
	}
}

// A data metric missing its alias while the edge uses aliases must still
// fail — the carve-out is for Node Control/Rebirth only.
func TestEdgeBirthMetricNaming_DataMetricMissingAlias_Fail(t *testing.T) {
	b, _ := NewBroker()
	defer b.Close()
	c := connectEdge(t, b, "edge-alias-miss", "spBv1.0/G/NDEATH/N")
	defer c.Disconnect(200)

	c.Publish("spBv1.0/G/NBIRTH/N", 0, false, birthPayload(
		birthMetric{name: "Node Control/Rebirth"},
		birthMetric{name: "Temperature", alias: aliasOf(1)},
		birthMetric{name: "Pressure"},
	)).WaitTimeout(2 * time.Second)
	time.Sleep(50 * time.Millisecond)

	r := aliasReqResult(t, EdgeBirthMetricNaming(b))
	if r.Status != runner.StatusFail {
		t.Errorf("expected fail for un-aliased data metric, got %+v", r)
	}
}

// Regression: the violation must be caught even when the un-aliased
// metric appears BEFORE the edge's first aliased metric. The old
// single-pass logic only flagged metrics after alias use was observed.
func TestEdgeBirthMetricNaming_UnaliasedMetricBeforeFirstAlias_Fail(t *testing.T) {
	b, _ := NewBroker()
	defer b.Close()
	c := connectEdge(t, b, "edge-alias-order", "spBv1.0/G/NDEATH/N")
	defer c.Disconnect(200)

	c.Publish("spBv1.0/G/NBIRTH/N", 0, false, birthPayload(
		birthMetric{name: "Pressure"},
		birthMetric{name: "Temperature", alias: aliasOf(1)},
	)).WaitTimeout(2 * time.Second)
	time.Sleep(50 * time.Millisecond)

	r := aliasReqResult(t, EdgeBirthMetricNaming(b))
	if r.Status != runner.StatusFail {
		t.Errorf("expected fail for un-aliased metric preceding first aliased metric, got %+v", r)
	}
}

// No aliases anywhere: the requirement doesn't apply and must pass, and
// a bare Node Control/Rebirth stays legal.
func TestEdgeBirthMetricNaming_NoAliases_Pass(t *testing.T) {
	b, _ := NewBroker()
	defer b.Close()
	c := connectEdge(t, b, "edge-no-alias", "spBv1.0/G/NDEATH/N")
	defer c.Disconnect(200)

	c.Publish("spBv1.0/G/NBIRTH/N", 0, false, birthPayload(
		birthMetric{name: "Node Control/Rebirth"},
		birthMetric{name: "Temperature"},
	)).WaitTimeout(2 * time.Second)
	time.Sleep(50 * time.Millisecond)

	r := aliasReqResult(t, EdgeBirthMetricNaming(b))
	if r.Status != runner.StatusPass {
		t.Errorf("expected pass when no aliases are used, got %+v", r)
	}
}
