package assertions

import (
	"strings"
	"testing"

	"github.com/joyautomation/sparkplug-tck-go/internal/runner"
	"github.com/joyautomation/sparkplug-tck-go/internal/spb"
	"github.com/joyautomation/sparkplug-tck-go/internal/spbpb"
)

// Test helpers specific to device-level messages — node-level helpers live
// in assertions_test.go.

func dbirth(group, n, dev string, seq uint64, ts uint64, qos byte, retain bool) spb.Message {
	return spb.Message{
		Topic: spb.Topic{
			Namespace:  spb.Namespace,
			EdgeNodeID: spb.EdgeNodeID{Group: group, Node: n},
			Type:       spb.DBIRTH,
			Device:     dev,
		},
		Payload:  &spbpb.Payload{Seq: u64(seq), Timestamp: u64(ts)},
		QoS:      qos,
		Retained: retain,
	}
}

func ddata(group, n, dev string, seq uint64) spb.Message {
	return spb.Message{
		Topic:   spb.Topic{Namespace: spb.Namespace, EdgeNodeID: spb.EdgeNodeID{Group: group, Node: n}, Type: spb.DDATA, Device: dev},
		Payload: &spbpb.Payload{Seq: u64(seq), Timestamp: u64(1)},
	}
}

func ddeath(group, n, dev string, seq uint64) spb.Message {
	return spb.Message{
		Topic:   spb.Topic{Namespace: spb.Namespace, EdgeNodeID: spb.EdgeNodeID{Group: group, Node: n}, Type: spb.DDEATH, Device: dev},
		Payload: &spbpb.Payload{Seq: u64(seq), Timestamp: u64(1)},
	}
}

// --- envelope rules: DBIRTH ---

func TestDBIRTH_QoS_Retain(t *testing.T) {
	bad := dbirth("G", "N", "D", 1, 1, 1, false) // wrong QoS
	res := runner.RunAll(runner.NewCapture([]spb.Message{bad}))
	if r := resultByID(t, res, "tck-id-payloads-dbirth-qos"); r.Status != runner.StatusFail {
		t.Errorf("dbirth-qos: expected fail, got %+v", r)
	}

	bad2 := dbirth("G", "N", "D", 1, 1, 0, true) // retained
	res = runner.RunAll(runner.NewCapture([]spb.Message{bad2}))
	if r := resultByID(t, res, "tck-id-payloads-dbirth-retain"); r.Status != runner.StatusFail {
		t.Errorf("dbirth-retain: expected fail, got %+v", r)
	}
}

func TestDBIRTH_MissingSeq_MissingTimestamp(t *testing.T) {
	m := dbirth("G", "N", "D", 0, 1, 0, false)
	m.Payload.Seq = nil
	m.Payload.Timestamp = nil
	res := runner.RunAll(runner.NewCapture([]spb.Message{m}))
	if r := resultByID(t, res, "tck-id-payloads-dbirth-seq"); r.Status != runner.StatusFail {
		t.Errorf("dbirth-seq: expected fail, got %+v", r)
	}
	if r := resultByID(t, res, "tck-id-payloads-dbirth-timestamp"); r.Status != runner.StatusFail {
		t.Errorf("dbirth-timestamp: expected fail, got %+v", r)
	}
}

// --- envelope rules: NDATA / DDATA / DDEATH ---

func TestNDATA_Envelope(t *testing.T) {
	bad := ndata("G", "N", 1)
	bad.QoS = 2
	bad.Retained = true
	bad.Payload.Timestamp = nil
	res := runner.RunAll(runner.NewCapture([]spb.Message{bad}))
	for _, id := range []string{
		"tck-id-payloads-ndata-qos",
		"tck-id-payloads-ndata-retain",
		"tck-id-payloads-ndata-timestamp",
	} {
		if r := resultByID(t, res, id); r.Status != runner.StatusFail {
			t.Errorf("%s: expected fail, got %+v", id, r)
		}
	}
}

func TestDDATA_Envelope_OK(t *testing.T) {
	good := ddata("G", "N", "D", 7)
	res := runner.RunAll(runner.NewCapture([]spb.Message{good}))
	for _, id := range []string{
		"tck-id-payloads-ddata-qos",
		"tck-id-payloads-ddata-retain",
		"tck-id-payloads-ddata-seq",
		"tck-id-payloads-ddata-timestamp",
	} {
		if r := resultByID(t, res, id); r.Status != runner.StatusPass {
			t.Errorf("%s: expected pass, got %+v", id, r)
		}
	}
}

func TestDDEATH_Envelope_OK(t *testing.T) {
	good := ddeath("G", "N", "D", 9)
	res := runner.RunAll(runner.NewCapture([]spb.Message{good}))
	for _, id := range []string{"tck-id-payloads-ddeath-seq", "tck-id-payloads-ddeath-timestamp"} {
		if r := resultByID(t, res, id); r.Status != runner.StatusPass {
			t.Errorf("%s: expected pass, got %+v", id, r)
		}
	}
}

// --- ordering ---

func TestOrder_HappyPath_NBIRTH_DBIRTH_DDATA_NDATA(t *testing.T) {
	msgs := []spb.Message{
		nbirth("G", "N", 0, 1, 1, 0, false),
		dbirth("G", "N", "D", 1, 1, 0, false),
		ddata("G", "N", "D", 2),
		ndata("G", "N", 3),
	}
	res := runner.RunAll(runner.NewCapture(msgs))
	for _, id := range []string{
		"tck-id-payloads-dbirth-order",
		"tck-id-payloads-ndata-order",
		"tck-id-payloads-ddata-order",
	} {
		if r := resultByID(t, res, id); r.Status != runner.StatusPass {
			t.Errorf("%s: expected pass, got %+v", id, r)
		}
	}
}

func TestOrder_DBIRTH_AfterData_Fails(t *testing.T) {
	msgs := []spb.Message{
		nbirth("G", "N", 0, 1, 1, 0, false),
		ndata("G", "N", 1),
		dbirth("G", "N", "D", 2, 1, 0, false), // late DBIRTH
	}
	res := runner.RunAll(runner.NewCapture(msgs))
	r := resultByID(t, res, "tck-id-payloads-dbirth-order")
	if r.Status != runner.StatusFail || !strings.Contains(r.Detail, "after data") {
		t.Errorf("expected dbirth-after-data fail, got %+v", r)
	}
}

func TestOrder_NDATA_BeforeBirth_Fails(t *testing.T) {
	msgs := []spb.Message{
		ndata("G", "N", 0), // no NBIRTH yet
		nbirth("G", "N", 0, 1, 1, 0, false),
	}
	res := runner.RunAll(runner.NewCapture(msgs))
	r := resultByID(t, res, "tck-id-payloads-ndata-order")
	if r.Status != runner.StatusFail || !strings.Contains(r.Detail, "before NBIRTH") {
		t.Errorf("expected ndata-before-birth fail, got %+v", r)
	}
}

func TestOrder_DDATA_BeforeBirth_Fails(t *testing.T) {
	msgs := []spb.Message{
		ddata("G", "N", "D", 0),
		nbirth("G", "N", 0, 1, 1, 0, false),
	}
	res := runner.RunAll(runner.NewCapture(msgs))
	r := resultByID(t, res, "tck-id-payloads-ddata-order")
	if r.Status != runner.StatusFail || !strings.Contains(r.Detail, "before NBIRTH") {
		t.Errorf("expected ddata-before-birth fail, got %+v", r)
	}
}
