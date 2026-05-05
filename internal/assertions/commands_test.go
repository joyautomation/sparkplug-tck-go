package assertions

import (
	"strings"
	"testing"

	"github.com/joyautomation/sparkplug-tck-go/internal/runner"
	"github.com/joyautomation/sparkplug-tck-go/internal/spb"
	"github.com/joyautomation/sparkplug-tck-go/internal/spbpb"
)

// ncmd builds a well-formed NCMD: QoS=0, retain=false, timestamp set, no seq.
func ncmd(group, n string) spb.Message {
	return spb.Message{
		Topic: spb.Topic{Namespace: spb.Namespace, EdgeNodeID: spb.EdgeNodeID{Group: group, Node: n}, Type: spb.NCMD},
		Payload: &spbpb.Payload{
			Timestamp: u64(1000),
			Metrics: []*spbpb.Payload_Metric{
				{
					Name:     str("Some/Tag"),
					Datatype: u32(uint32(spbpb.DataType_Int32)),
					Value:    &spbpb.Payload_Metric_IntValue{IntValue: 1},
				},
			},
		},
	}
}

// dcmd builds a well-formed DCMD.
func dcmd(group, n, d string) spb.Message {
	return spb.Message{
		Topic: spb.Topic{Namespace: spb.Namespace, EdgeNodeID: spb.EdgeNodeID{Group: group, Node: n}, Device: d, Type: spb.DCMD},
		Payload: &spbpb.Payload{
			Timestamp: u64(1000),
			Metrics: []*spbpb.Payload_Metric{
				{
					Name:     str("Some/Tag"),
					Datatype: u32(uint32(spbpb.DataType_Int32)),
					Value:    &spbpb.Payload_Metric_IntValue{IntValue: 1},
				},
			},
		},
	}
}

func TestCommands_AllRegistered(t *testing.T) {
	want := []string{
		"tck-id-payloads-ncmd-qos",
		"tck-id-payloads-ncmd-retain",
		"tck-id-payloads-ncmd-seq",
		"tck-id-payloads-ncmd-timestamp",
		"tck-id-payloads-dcmd-qos",
		"tck-id-payloads-dcmd-retain",
		"tck-id-payloads-dcmd-seq",
		"tck-id-payloads-dcmd-timestamp",
		"tck-id-topics-ncmd-mqtt",
		"tck-id-topics-ncmd-payload",
		"tck-id-topics-ncmd-timestamp",
		"tck-id-topics-ncmd-topic",
		"tck-id-topics-dcmd-mqtt",
		"tck-id-topics-dcmd-payload",
		"tck-id-topics-dcmd-timestamp",
		"tck-id-topics-dcmd-topic",
		"tck-id-operational-behavior-data-commands-ncmd-verb",
		"tck-id-operational-behavior-data-commands-dcmd-verb",
		"tck-id-operational-behavior-host-application-death-qos",
		"tck-id-operational-behavior-host-application-death-retained",
		"tck-id-operational-behavior-host-application-death-topic",
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

func TestCommands_HappyPath(t *testing.T) {
	msgs := []spb.Message{ncmd("G", "N"), dcmd("G", "N", "D")}
	res := runner.RunAll(runner.NewCapture(msgs))
	for _, id := range []string{
		"tck-id-payloads-ncmd-qos",
		"tck-id-payloads-ncmd-retain",
		"tck-id-payloads-ncmd-seq",
		"tck-id-payloads-ncmd-timestamp",
		"tck-id-payloads-dcmd-qos",
		"tck-id-payloads-dcmd-retain",
		"tck-id-payloads-dcmd-seq",
		"tck-id-payloads-dcmd-timestamp",
		"tck-id-topics-ncmd-mqtt",
		"tck-id-topics-ncmd-payload",
		"tck-id-topics-ncmd-timestamp",
		"tck-id-topics-ncmd-topic",
		"tck-id-topics-dcmd-mqtt",
		"tck-id-topics-dcmd-payload",
		"tck-id-topics-dcmd-timestamp",
		"tck-id-topics-dcmd-topic",
		"tck-id-operational-behavior-data-commands-ncmd-verb",
		"tck-id-operational-behavior-data-commands-dcmd-verb",
	} {
		r := resultByID(t, res, id)
		if r.Status != runner.StatusPass {
			t.Errorf("%s: expected pass, got %+v", id, r)
		}
	}
}

func TestNCMD_QoSFail(t *testing.T) {
	m := ncmd("G", "N")
	m.QoS = 1
	res := runner.RunAll(runner.NewCapture([]spb.Message{m}))
	r := resultByID(t, res, "tck-id-payloads-ncmd-qos")
	if r.Status != runner.StatusFail || !strings.Contains(r.Detail, "QoS") {
		t.Errorf("expected QoS fail, got %+v", r)
	}
}

func TestNCMD_RetainFail(t *testing.T) {
	m := ncmd("G", "N")
	m.Retained = true
	res := runner.RunAll(runner.NewCapture([]spb.Message{m}))
	r := resultByID(t, res, "tck-id-payloads-ncmd-retain")
	if r.Status != runner.StatusFail {
		t.Errorf("expected retain fail, got %+v", r)
	}
}

func TestNCMD_SeqPresent_Fails(t *testing.T) {
	m := ncmd("G", "N")
	m.Payload.Seq = u64(5)
	res := runner.RunAll(runner.NewCapture([]spb.Message{m}))
	r := resultByID(t, res, "tck-id-payloads-ncmd-seq")
	if r.Status != runner.StatusFail || !strings.Contains(r.Detail, "must be absent") {
		t.Errorf("expected seq-absent fail, got %+v", r)
	}
}

func TestDCMD_TimestampMissing_Fails(t *testing.T) {
	m := dcmd("G", "N", "D")
	m.Payload.Timestamp = nil
	res := runner.RunAll(runner.NewCapture([]spb.Message{m}))
	r := resultByID(t, res, "tck-id-payloads-dcmd-timestamp")
	if r.Status != runner.StatusFail {
		t.Errorf("expected timestamp fail, got %+v", r)
	}
}

func TestNoCommands_NA(t *testing.T) {
	res := runner.RunAll(runner.NewCapture([]spb.Message{nbirth("G", "N", 0, 1, 1, 0, false)}))
	r := resultByID(t, res, "tck-id-payloads-ncmd-qos")
	if r.Status != runner.StatusNotApplicable {
		t.Errorf("expected NA when no NCMD present, got %+v", r)
	}
}
