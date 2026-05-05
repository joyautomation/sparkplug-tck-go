package assertions

import (
	"testing"

	"github.com/joyautomation/sparkplug-tck-go/internal/runner"
	"github.com/joyautomation/sparkplug-tck-go/internal/spb"
	"github.com/joyautomation/sparkplug-tck-go/internal/spbpb"
)

func TestDataPublish_AllRegistered(t *testing.T) {
	want := []string{
		"tck-id-operational-behavior-data-publish-nbirth-order",
		"tck-id-operational-behavior-data-publish-dbirth-order",
		"tck-id-operational-behavior-data-publish-nbirth-values",
		"tck-id-operational-behavior-data-publish-dbirth-values",
		"tck-id-operational-behavior-data-publish-nbirth",
		"tck-id-operational-behavior-data-publish-dbirth",
		"tck-id-operational-behavior-data-publish-nbirth-change",
		"tck-id-operational-behavior-data-publish-dbirth-change",
		"tck-id-payloads-template-definition-is-definition",
		"tck-id-payloads-template-definition-ref",
		"tck-id-payloads-template-definition-nbirth-only",
		"tck-id-payloads-template-definition-nbirth",
		"tck-id-payloads-template-definition-parameters",
		"tck-id-payloads-template-definition-parameters-default",
		"tck-id-payloads-template-definition-members",
		"tck-id-payloads-template-version",
		"tck-id-payloads-template-dataset-value",
		"tck-id-payloads-timestamp-in-UTC",
		"tck-id-payloads-metric-timestamp-in-UTC",
		"tck-id-payloads-ndeath-will-message",
		"tck-id-topic-structure",
		"tck-id-payloads-metric-datatype-not-req",
		"tck-id-payloads-name-birth-data-requirement",
		"tck-id-payloads-name-cmd-requirement",
		// NCMD rebirth from rebirth.go chunk-11 additions.
		"tck-id-operational-behavior-data-commands-ncmd-rebirth-name",
		"tck-id-operational-behavior-data-commands-ncmd-rebirth-value",
		"tck-id-operational-behavior-data-commands-ncmd-rebirth-verb",
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

func TestNBIRTHValues_MissingValueFails(t *testing.T) {
	m := nbirth("G", "N", 0, 1, 1, 0, false)
	// Append a metric with no value and no isnull flag.
	m.Payload.Metrics = append(m.Payload.Metrics, &spbpb.Payload_Metric{
		Name:      str("Broken"),
		Timestamp: u64(1),
		Datatype:  u32(uint32(spbpb.DataType_Int32)),
	})
	res := runner.RunAll(runner.NewCapture([]spb.Message{m}))
	r := resultByID(t, res, "tck-id-operational-behavior-data-publish-nbirth-values")
	if r.Status != runner.StatusFail {
		t.Errorf("expected fail for missing value, got %+v", r)
	}
}

func TestNBIRTHOrder_OutOfOrderFails(t *testing.T) {
	m := nbirth("G", "N", 0, 1, 1, 0, false)
	m.Payload.Metrics = append(m.Payload.Metrics,
		&spbpb.Payload_Metric{
			Name:      str("A"),
			Timestamp: u64(100),
			Datatype:  u32(uint32(spbpb.DataType_Int32)),
			Value:     &spbpb.Payload_Metric_IntValue{IntValue: 1},
		},
		&spbpb.Payload_Metric{
			Name:      str("B"),
			Timestamp: u64(50), // earlier than previous
			Datatype:  u32(uint32(spbpb.DataType_Int32)),
			Value:     &spbpb.Payload_Metric_IntValue{IntValue: 2},
		},
	)
	res := runner.RunAll(runner.NewCapture([]spb.Message{m}))
	r := resultByID(t, res, "tck-id-operational-behavior-data-publish-nbirth-order")
	if r.Status != runner.StatusFail {
		t.Errorf("expected fail for out-of-order metric timestamps, got %+v", r)
	}
}

func TestMetricDatatypeNotReq_DataMetricWithDatatypeFails(t *testing.T) {
	// ncmd helper sets Datatype on metric — should fail the SHOULD-NOT rule.
	m := ncmd("G", "N")
	res := runner.RunAll(runner.NewCapture([]spb.Message{m}))
	rs := resultsByID(res, "tck-id-payloads-metric-datatype-not-req")
	hasFail := false
	for _, r := range rs {
		if r.Status == runner.StatusFail {
			hasFail = true
			break
		}
	}
	if !hasFail {
		t.Errorf("expected fail when datatype set on NCMD metric, got %+v", rs)
	}
}

func TestNameBirthDataRequirement_MissingTimestampFails(t *testing.T) {
	m := nbirth("G", "N", 0, 1, 1, 0, false)
	m.Payload.Metrics = append(m.Payload.Metrics, &spbpb.Payload_Metric{
		Name:     str("NoTs"),
		Datatype: u32(uint32(spbpb.DataType_Int32)),
		Value:    &spbpb.Payload_Metric_IntValue{IntValue: 1},
	})
	res := runner.RunAll(runner.NewCapture([]spb.Message{m}))
	rs := resultsByID(res, "tck-id-payloads-name-birth-data-requirement")
	hasFail := false
	for _, r := range rs {
		if r.Status == runner.StatusFail {
			hasFail = true
		}
	}
	if !hasFail {
		t.Errorf("expected fail when metric in NBIRTH lacks timestamp, got %+v", rs)
	}
}

func TestTopicStructure_BadNamespaceFails(t *testing.T) {
	m := nbirth("G", "N", 0, 1, 1, 0, false)
	m.Topic.Namespace = "spBv2.0"
	res := runner.RunAll(runner.NewCapture([]spb.Message{m}))
	r := resultByID(t, res, "tck-id-topic-structure")
	if r.Status != runner.StatusFail {
		t.Errorf("expected fail for non-spBv1.0 namespace, got %+v", r)
	}
}

func TestNDEATHWillMessagePresence_NA(t *testing.T) {
	m := nbirth("G", "N", 0, 1, 1, 0, false)
	res := runner.RunAll(runner.NewCapture([]spb.Message{m}))
	r := resultByID(t, res, "tck-id-payloads-ndeath-will-message")
	if r.Status != runner.StatusNotApplicable {
		t.Errorf("expected NA without NDEATH, got %+v", r)
	}
}

// NCMD rebirth tests ---------------------------------------------------

func ncmdRebirth(group, n string, value bool) spb.Message {
	return spb.Message{
		Topic: spb.Topic{Namespace: spb.Namespace, EdgeNodeID: spb.EdgeNodeID{Group: group, Node: n}, Type: spb.NCMD},
		Payload: &spbpb.Payload{
			Timestamp: u64(1000),
			Metrics: []*spbpb.Payload_Metric{
				{
					Name:      str(rebirthMetricName),
					Timestamp: u64(1000),
					Datatype:  u32(uint32(spbpb.DataType_Boolean)),
					Value:     &spbpb.Payload_Metric_BooleanValue{BooleanValue: value},
				},
			},
		},
	}
}

func TestNCMDRebirth_HappyPath(t *testing.T) {
	res := runner.RunAll(runner.NewCapture([]spb.Message{ncmdRebirth("G", "N", true)}))
	for _, id := range []string{
		"tck-id-operational-behavior-data-commands-ncmd-rebirth-name",
		"tck-id-operational-behavior-data-commands-ncmd-rebirth-value",
		"tck-id-operational-behavior-data-commands-ncmd-rebirth-verb",
	} {
		r := resultByID(t, res, id)
		if r.Status != runner.StatusPass {
			t.Errorf("%s: expected pass, got %+v", id, r)
		}
	}
}

func TestNCMDRebirth_FalseValueFails(t *testing.T) {
	res := runner.RunAll(runner.NewCapture([]spb.Message{ncmdRebirth("G", "N", false)}))
	r := resultByID(t, res, "tck-id-operational-behavior-data-commands-ncmd-rebirth-value")
	if r.Status != runner.StatusFail {
		t.Errorf("expected fail for value=false, got %+v", r)
	}
}

func TestNCMDRebirth_NoRebirth_NA(t *testing.T) {
	res := runner.RunAll(runner.NewCapture([]spb.Message{ncmd("G", "N")}))
	r := resultByID(t, res, "tck-id-operational-behavior-data-commands-ncmd-rebirth-name")
	if r.Status != runner.StatusNotApplicable {
		t.Errorf("expected NA without NCMD Rebirth, got %+v", r)
	}
}
