package assertions

import (
	"strings"
	"testing"

	"github.com/joyautomation/sparkplug-tck-go/internal/runner"
	"github.com/joyautomation/sparkplug-tck-go/internal/spb"
	"github.com/joyautomation/sparkplug-tck-go/internal/spbpb"
)

// Helpers --------------------------------------------------------------

func u64(v uint64) *uint64 { return &v }
func u32(v uint32) *uint32 { return &v }
func str(v string) *string { return &v }

func bdSeqMetric(v uint64) *spbpb.Payload_Metric {
	return &spbpb.Payload_Metric{
		Name:  str("bdSeq"),
		Value: &spbpb.Payload_Metric_LongValue{LongValue: v},
	}
}

// stdRebirthMetric is the well-formed Node Control/Rebirth metric every
// NBIRTH must carry. Tests that don't care about rebirth get it for free
// via the nbirth helper; rebirth-specific tests build their own.
func stdRebirthMetric() *spbpb.Payload_Metric {
	return &spbpb.Payload_Metric{
		Name:     str(rebirthMetricName),
		Datatype: u32(uint32(spbpb.DataType_Boolean)),
		Value:    &spbpb.Payload_Metric_BooleanValue{BooleanValue: false},
	}
}

func nbirth(group, n string, seq uint64, ts uint64, bdSeq uint64, qos byte, retain bool) spb.Message {
	return spb.Message{
		Topic: spb.Topic{Namespace: spb.Namespace, EdgeNodeID: spb.EdgeNodeID{Group: group, Node: n}, Type: spb.NBIRTH},
		Payload: &spbpb.Payload{
			Seq:       u64(seq),
			Timestamp: u64(ts),
			Metrics:   []*spbpb.Payload_Metric{bdSeqMetric(bdSeq), stdRebirthMetric()},
		},
		QoS:      qos,
		Retained: retain,
	}
}

func ndata(group, n string, seq uint64) spb.Message {
	return spb.Message{
		Topic:   spb.Topic{Namespace: spb.Namespace, EdgeNodeID: spb.EdgeNodeID{Group: group, Node: n}, Type: spb.NDATA},
		Payload: &spbpb.Payload{Seq: u64(seq), Timestamp: u64(1)},
	}
}

func ndeath(group, n string, bdSeq *uint64) spb.Message {
	p := &spbpb.Payload{}
	if bdSeq != nil {
		p.Metrics = []*spbpb.Payload_Metric{bdSeqMetric(*bdSeq)}
	}
	// NDEATH is the broker-published Will: QoS=1, retain=false.
	return spb.Message{
		Topic:   spb.Topic{Namespace: spb.Namespace, EdgeNodeID: spb.EdgeNodeID{Group: group, Node: n}, Type: spb.NDEATH},
		Payload: p,
		QoS:     1,
	}
}

// resultByID returns the first Result for a given assertion ID — handy when
// a test expects exactly one outcome.
func resultByID(t *testing.T, results []runner.Result, id string) runner.Result {
	t.Helper()
	for _, r := range results {
		if r.AssertionID == id {
			return r
		}
	}
	t.Fatalf("no result for %s", id)
	return runner.Result{}
}

// resultsByID returns every Result for a given assertion ID.
func resultsByID(results []runner.Result, id string) []runner.Result {
	var out []runner.Result
	for _, r := range results {
		if r.AssertionID == id {
			out = append(out, r)
		}
	}
	return out
}

// Tests ----------------------------------------------------------------

func TestRegistry_AllRegistered(t *testing.T) {
	want := []string{
		"tck-id-topic-structure-namespace-a",
		"tck-id-topics-nbirth-mqtt",
		"tck-id-topics-nbirth-seq-num",
		"tck-id-payloads-nbirth-seq",
		"tck-id-payloads-nbirth-timestamp",
		"tck-id-payloads-nbirth-bdseq",
		"tck-id-payloads-ndeath-seq",
		"tck-id-payloads-ndeath-bdseq",
		"tck-id-payloads-sequence-num-incrementing",
	}
	got := map[string]bool{}
	for _, a := range runner.All() {
		got[a.ID] = true
	}
	for _, id := range want {
		if !got[id] {
			t.Errorf("assertion %q missing from registry", id)
		}
	}
}

func TestHappyPath_NBIRTH_NDATA_NDEATH(t *testing.T) {
	msgs := []spb.Message{
		nbirth("G", "N", 0, 1000, 42, 0, false),
		ndata("G", "N", 1),
		ndata("G", "N", 2),
		ndeath("G", "N", u64(42)),
	}
	res := runner.RunAll(runner.NewCapture(msgs))

	for _, r := range res {
		if r.Status == runner.StatusFail {
			t.Errorf("unexpected fail: %s [%s]: %s", r.AssertionID, r.Subject, r.Detail)
		}
	}
}

func TestNamespaceA_Fail(t *testing.T) {
	m := nbirth("G", "N", 0, 1, 1, 0, false)
	m.Topic.Namespace = "spBv2.0"
	res := runner.RunAll(runner.NewCapture([]spb.Message{m}))
	r := resultByID(t, res, "tck-id-topic-structure-namespace-a")
	if r.Status != runner.StatusFail {
		t.Fatalf("expected fail, got %+v", r)
	}
	if !strings.Contains(r.Detail, "spBv2.0") {
		t.Errorf("detail missing offending namespace: %q", r.Detail)
	}
}

func TestNbirthMQTT_Fail_QoS(t *testing.T) {
	m := nbirth("G", "N", 0, 1, 1, 1, false)
	res := runner.RunAll(runner.NewCapture([]spb.Message{m}))
	r := resultByID(t, res, "tck-id-topics-nbirth-mqtt")
	if r.Status != runner.StatusFail || !strings.Contains(r.Detail, "QoS") {
		t.Errorf("expected QoS fail, got %+v", r)
	}
}

func TestNbirthMQTT_Fail_Retain(t *testing.T) {
	m := nbirth("G", "N", 0, 1, 1, 0, true)
	res := runner.RunAll(runner.NewCapture([]spb.Message{m}))
	r := resultByID(t, res, "tck-id-topics-nbirth-mqtt")
	if r.Status != runner.StatusFail || !strings.Contains(r.Detail, "retain") {
		t.Errorf("expected retain fail, got %+v", r)
	}
}

func TestNbirthSeqNum_Fail_NotZero(t *testing.T) {
	m := nbirth("G", "N", 5, 1, 1, 0, false)
	res := runner.RunAll(runner.NewCapture([]spb.Message{m}))
	r := resultByID(t, res, "tck-id-topics-nbirth-seq-num")
	if r.Status != runner.StatusFail {
		t.Fatalf("expected fail, got %+v", r)
	}
}

func TestNbirthBdSeq_Fail_Missing(t *testing.T) {
	m := nbirth("G", "N", 0, 1, 0, 0, false)
	m.Payload.Metrics = nil // strip bdSeq metric
	res := runner.RunAll(runner.NewCapture([]spb.Message{m}))
	r := resultByID(t, res, "tck-id-payloads-nbirth-bdseq")
	if r.Status != runner.StatusFail {
		t.Fatalf("expected fail, got %+v", r)
	}
}

func TestNbirthTimestamp_Fail_Missing(t *testing.T) {
	m := nbirth("G", "N", 0, 0, 1, 0, false)
	m.Payload.Timestamp = nil
	res := runner.RunAll(runner.NewCapture([]spb.Message{m}))
	r := resultByID(t, res, "tck-id-payloads-nbirth-timestamp")
	if r.Status != runner.StatusFail {
		t.Fatalf("expected fail, got %+v", r)
	}
}

func TestNdeathNoSeq_Fail_HasSeq(t *testing.T) {
	d := ndeath("G", "N", u64(1))
	d.Payload.Seq = u64(99)
	res := runner.RunAll(runner.NewCapture([]spb.Message{d}))
	r := resultByID(t, res, "tck-id-payloads-ndeath-seq")
	if r.Status != runner.StatusFail {
		t.Fatalf("expected fail, got %+v", r)
	}
}

func TestNdeathBdSeqMatches_Mismatch(t *testing.T) {
	msgs := []spb.Message{
		nbirth("G", "N", 0, 1, 5, 0, false),
		ndeath("G", "N", u64(7)),
	}
	res := runner.RunAll(runner.NewCapture(msgs))
	r := resultByID(t, res, "tck-id-payloads-ndeath-bdseq")
	if r.Status != runner.StatusFail || !strings.Contains(r.Detail, "5") {
		t.Errorf("expected mismatch fail, got %+v", r)
	}
}

func TestSeqIncrementing_Wrap_OK(t *testing.T) {
	msgs := []spb.Message{nbirth("G", "N", 0, 1, 1, 0, false)}
	for i := uint64(1); i <= 300; i++ {
		msgs = append(msgs, ndata("G", "N", i%256))
	}
	res := runner.RunAll(runner.NewCapture(msgs))
	r := resultByID(t, res, "tck-id-payloads-sequence-num-incrementing")
	if r.Status != runner.StatusPass {
		t.Errorf("expected pass across the 255->0 wrap, got %+v", r)
	}
}

func TestSeqIncrementing_Gap_Fail(t *testing.T) {
	msgs := []spb.Message{
		nbirth("G", "N", 0, 1, 1, 0, false),
		ndata("G", "N", 1),
		ndata("G", "N", 5), // gap
	}
	res := runner.RunAll(runner.NewCapture(msgs))
	r := resultByID(t, res, "tck-id-payloads-sequence-num-incrementing")
	if r.Status != runner.StatusFail {
		t.Fatalf("expected fail, got %+v", r)
	}
}

func TestNoMessages_AllNA(t *testing.T) {
	res := runner.RunAll(runner.NewCapture(nil))
	for _, r := range resultsByID(res, "tck-id-topics-nbirth-mqtt") {
		if r.Status != runner.StatusNotApplicable {
			t.Errorf("expected NA on empty capture, got %+v", r)
		}
	}
}
