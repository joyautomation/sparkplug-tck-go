package assertions

import (
	"testing"

	"github.com/joyautomation/sparkplug-tck-go/internal/runner"
	"github.com/joyautomation/sparkplug-tck-go/internal/spb"
	"github.com/joyautomation/sparkplug-tck-go/internal/spbpb"
)

func TestBirthMetricAliases_AllRegistered(t *testing.T) {
	want := []string{
		"tck-id-topics-nbirth-metrics",
		"tck-id-topics-dbirth-metrics",
		"tck-id-topics-nbirth-metric-reqs",
		"tck-id-topics-dbirth-metric-reqs",
		"tck-id-topics-nbirth-rebirth-metric",
		"tck-id-topics-nbirth-templates",
		"tck-id-topics-nbirth-bdseq-increment",
		"tck-id-payloads-nbirth-rebirth-req",
		"tck-id-payloads-nbirth-bdseq-repeat",
		"tck-id-payloads-nbirth-edge-node-descriptor",
		"tck-id-payloads-template-instance-members",
		"tck-id-payloads-template-instance-members-birth",
		"tck-id-payloads-template-instance-members-data",
		"tck-id-payloads-template-instance-parameters",
		"tck-id-payloads-ndeath-will-message-publisher",
		"tck-id-payloads-ndeath-will-message-publisher-disconnect-mqtt311",
		"tck-id-payloads-ndeath-will-message-publisher-disconnect-mqtt50",
		"tck-id-operational-behavior-data-commands-ncmd-metric-name",
		"tck-id-operational-behavior-data-commands-ncmd-metric-value",
		"tck-id-operational-behavior-data-commands-dcmd-metric-name",
		"tck-id-operational-behavior-data-commands-dcmd-metric-value",
	}
	got := map[string]bool{}
	for _, a := range runner.All() {
		got[a.ID] = true
	}
	for _, id := range want {
		if !got[id] {
			t.Errorf("alias %q missing from registry", id)
		}
	}
}

func TestNBIRTHMetrics_HappyPath(t *testing.T) {
	res := runner.RunAll(runner.NewCapture([]spb.Message{nbirth("G", "N", 0, 1, 1, 0, false)}))
	for _, id := range []string{
		"tck-id-topics-nbirth-metrics",
		"tck-id-topics-nbirth-metric-reqs",
		"tck-id-topics-nbirth-rebirth-metric",
		"tck-id-payloads-nbirth-rebirth-req",
		"tck-id-payloads-nbirth-edge-node-descriptor",
	} {
		r := resultByID(t, res, id)
		if r.Status != runner.StatusPass {
			t.Errorf("%s: expected pass, got %+v", id, r)
		}
	}
}

func TestNBIRTHMetrics_MissingDatatypeFails(t *testing.T) {
	m := nbirth("G", "N", 0, 1, 1, 0, false)
	m.Payload.Metrics = append(m.Payload.Metrics, &spbpb.Payload_Metric{
		Name:      str("NoType"),
		Timestamp: u64(1),
		Value:     &spbpb.Payload_Metric_IntValue{IntValue: 1},
	})
	res := runner.RunAll(runner.NewCapture([]spb.Message{m}))
	r := resultByID(t, res, "tck-id-topics-nbirth-metrics")
	if r.Status != runner.StatusFail {
		t.Errorf("expected fail for missing datatype on NBIRTH metric, got %+v", r)
	}
}

func TestNCMDMetricValue_IncompatibleFails(t *testing.T) {
	m := ncmd("G", "N")
	// Override metric: declared as Boolean but carries Int value.
	m.Payload.Metrics = []*spbpb.Payload_Metric{{
		Name:     str("Bad"),
		Datatype: u32(uint32(spbpb.DataType_Boolean)),
		Value:    &spbpb.Payload_Metric_IntValue{IntValue: 1},
	}}
	res := runner.RunAll(runner.NewCapture([]spb.Message{m}))
	rs := resultsByID(res, "tck-id-operational-behavior-data-commands-ncmd-metric-value")
	hasFail := false
	for _, r := range rs {
		if r.Status == runner.StatusFail {
			hasFail = true
		}
	}
	if !hasFail {
		t.Errorf("expected fail for value/datatype mismatch, got %+v", rs)
	}
}

func TestNDEATHPublisherAliases_NA(t *testing.T) {
	res := runner.RunAll(runner.NewCapture([]spb.Message{nbirth("G", "N", 0, 1, 1, 0, false)}))
	r := resultByID(t, res, "tck-id-payloads-ndeath-will-message-publisher")
	if r.Status != runner.StatusNotApplicable {
		t.Errorf("expected NA without NDEATH, got %+v", r)
	}
}

func TestTemplateInstance_PassThroughOnInstance(t *testing.T) {
	// Build an NBIRTH carrying a Template Instance.
	tmpl := &spbpb.Payload_Template{
		IsDefinition: boolp(false),
		TemplateRef:  str("MyType"),
	}
	m := nbirth("G", "N", 0, 1, 1, 0, false)
	m.Payload.Metrics = append(m.Payload.Metrics, &spbpb.Payload_Metric{
		Name:      str("Inst"),
		Timestamp: u64(1),
		Datatype:  u32(uint32(spbpb.DataType_Template)),
		Value:     &spbpb.Payload_Metric_TemplateValue{TemplateValue: tmpl},
	})
	res := runner.RunAll(runner.NewCapture([]spb.Message{m}))
	for _, id := range []string{
		"tck-id-payloads-template-instance-members",
		"tck-id-payloads-template-instance-members-birth",
		"tck-id-payloads-template-instance-members-data",
		"tck-id-payloads-template-instance-parameters",
	} {
		r := resultByID(t, res, id)
		if r.Status != runner.StatusPass {
			t.Errorf("%s: expected pass for Template Instance, got %+v", id, r)
		}
	}
}
