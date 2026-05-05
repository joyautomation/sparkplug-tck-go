package assertions

import (
	"strings"
	"testing"

	"github.com/joyautomation/sparkplug-tck-go/internal/runner"
	"github.com/joyautomation/sparkplug-tck-go/internal/spb"
	"github.com/joyautomation/sparkplug-tck-go/internal/spbpb"
)

func rebirthMetric(value bool, dt spbpb.DataType, alias *uint64) *spbpb.Payload_Metric {
	m := &spbpb.Payload_Metric{
		Name:     str(rebirthMetricName),
		Datatype: u32(uint32(dt)),
		Value:    &spbpb.Payload_Metric_BooleanValue{BooleanValue: value},
	}
	m.Alias = alias
	return m
}

// nbirthWithRebirth builds an NBIRTH that contains exactly the rebirth
// metric supplied (or none if nil). It strips the default rebirth metric
// added by the shared nbirth helper so each test pins the metric it cares
// about.
func nbirthWithRebirth(group, n string, m *spbpb.Payload_Metric) spb.Message {
	msg := nbirth(group, n, 0, 1, 1, 0, false)
	filtered := msg.Payload.Metrics[:0]
	for _, met := range msg.Payload.Metrics {
		if met.GetName() != rebirthMetricName {
			filtered = append(filtered, met)
		}
	}
	msg.Payload.Metrics = filtered
	if m != nil {
		msg.Payload.Metrics = append(msg.Payload.Metrics, m)
	}
	return msg
}

func TestRebirth_HappyPath(t *testing.T) {
	msg := nbirthWithRebirth("G", "N", rebirthMetric(false, spbpb.DataType_Boolean, nil))
	res := runner.RunAll(runner.NewCapture([]spb.Message{msg}))
	for _, id := range []string{
		"tck-id-operational-behavior-data-commands-rebirth-name",
		"tck-id-operational-behavior-data-commands-rebirth-value",
		"tck-id-operational-behavior-data-commands-rebirth-datatype",
		"tck-id-operational-behavior-data-commands-rebirth-name-aliases",
	} {
		if r := resultByID(t, res, id); r.Status != runner.StatusPass {
			t.Errorf("%s: expected pass, got %+v", id, r)
		}
	}
}

func TestRebirth_Missing_Fails(t *testing.T) {
	msg := nbirthWithRebirth("G", "N", nil)
	res := runner.RunAll(runner.NewCapture([]spb.Message{msg}))
	r := resultByID(t, res, "tck-id-operational-behavior-data-commands-rebirth-name")
	if r.Status != runner.StatusFail || !strings.Contains(r.Detail, "missing") {
		t.Errorf("expected missing fail, got %+v", r)
	}
	// dependent assertions should be NA when metric absent
	for _, id := range []string{
		"tck-id-operational-behavior-data-commands-rebirth-value",
		"tck-id-operational-behavior-data-commands-rebirth-datatype",
		"tck-id-operational-behavior-data-commands-rebirth-name-aliases",
	} {
		if r := resultByID(t, res, id); r.Status != runner.StatusNotApplicable {
			t.Errorf("%s: expected NA when rebirth absent, got %+v", id, r)
		}
	}
}

func TestRebirth_ValueTrue_Fails(t *testing.T) {
	msg := nbirthWithRebirth("G", "N", rebirthMetric(true, spbpb.DataType_Boolean, nil))
	res := runner.RunAll(runner.NewCapture([]spb.Message{msg}))
	r := resultByID(t, res, "tck-id-operational-behavior-data-commands-rebirth-value")
	if r.Status != runner.StatusFail || !strings.Contains(r.Detail, "true") {
		t.Errorf("expected value=true fail, got %+v", r)
	}
}

func TestRebirth_WrongDatatype_Fails(t *testing.T) {
	msg := nbirthWithRebirth("G", "N", rebirthMetric(false, spbpb.DataType_Int32, nil))
	res := runner.RunAll(runner.NewCapture([]spb.Message{msg}))
	r := resultByID(t, res, "tck-id-operational-behavior-data-commands-rebirth-datatype")
	if r.Status != runner.StatusFail || !strings.Contains(r.Detail, "Boolean") {
		t.Errorf("expected datatype fail, got %+v", r)
	}
}

func TestRebirth_HasAlias_Fails(t *testing.T) {
	alias := uint64(7)
	msg := nbirthWithRebirth("G", "N", rebirthMetric(false, spbpb.DataType_Boolean, &alias))
	res := runner.RunAll(runner.NewCapture([]spb.Message{msg}))
	r := resultByID(t, res, "tck-id-operational-behavior-data-commands-rebirth-name-aliases")
	if r.Status != runner.StatusFail || !strings.Contains(r.Detail, "alias") {
		t.Errorf("expected alias fail, got %+v", r)
	}
}

func TestRebirth_NoNBIRTH_NA(t *testing.T) {
	res := runner.RunAll(runner.NewCapture(nil))
	r := resultByID(t, res, "tck-id-operational-behavior-data-commands-rebirth-name")
	if r.Status != runner.StatusNotApplicable {
		t.Errorf("expected NA, got %+v", r)
	}
}
