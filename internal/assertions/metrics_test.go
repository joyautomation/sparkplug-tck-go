package assertions

import (
	"strings"
	"testing"

	"github.com/joyautomation/sparkplug-tck-go/internal/runner"
	"github.com/joyautomation/sparkplug-tck-go/internal/spb"
	"github.com/joyautomation/sparkplug-tck-go/internal/spbpb"
)

func TestMetricDatatype_HappyPath(t *testing.T) {
	res := runner.RunAll(runner.NewCapture([]spb.Message{nbirth("G", "N", 0, 1, 1, 0, false)}))
	for _, id := range []string{
		"tck-id-payloads-metric-datatype-req",
		"tck-id-payloads-metric-datatype-value",
		"tck-id-payloads-metric-datatype-value-type",
	} {
		if r := resultByID(t, res, id); r.Status != runner.StatusPass {
			t.Errorf("%s: expected pass, got %+v", id, r)
		}
	}
}

func TestMetricDatatype_MissingDatatype_Fails(t *testing.T) {
	m := nbirth("G", "N", 0, 1, 1, 0, false)
	// strip the datatype off the bdSeq metric
	m.Payload.Metrics[0].Datatype = nil
	res := runner.RunAll(runner.NewCapture([]spb.Message{m}))
	r := resultByID(t, res, "tck-id-payloads-metric-datatype-req")
	if r.Status != runner.StatusFail || !strings.Contains(r.Detail, "missing datatype") {
		t.Errorf("expected fail, got %+v", r)
	}
}

func TestMetricDatatype_InvalidValue_Fails(t *testing.T) {
	m := nbirth("G", "N", 0, 1, 1, 0, false)
	m.Payload.Metrics[0].Datatype = u32(99) // out-of-range
	res := runner.RunAll(runner.NewCapture([]spb.Message{m}))
	r := resultByID(t, res, "tck-id-payloads-metric-datatype-value")
	if r.Status != runner.StatusFail || !strings.Contains(r.Detail, "valid enum") {
		t.Errorf("expected fail, got %+v", r)
	}
}

func TestMetricName_RequiredUnlessAlias(t *testing.T) {
	m := nbirth("G", "N", 0, 1, 1, 0, false)
	// Add a metric with neither name nor alias.
	m.Payload.Metrics = append(m.Payload.Metrics, &spbpb.Payload_Metric{
		Datatype: u32(uint32(spbpb.DataType_Int32)),
		Value:    &spbpb.Payload_Metric_IntValue{IntValue: 1},
	})
	res := runner.RunAll(runner.NewCapture([]spb.Message{m}))
	r := resultByID(t, res, "tck-id-payloads-name-requirement")
	if r.Status != runner.StatusFail || !strings.Contains(r.Detail, "neither name nor alias") {
		t.Errorf("expected fail, got %+v", r)
	}

	// Alias-only is acceptable.
	alias := uint64(7)
	m2 := nbirth("G", "N", 0, 1, 1, 0, false)
	m2.Payload.Metrics = append(m2.Payload.Metrics, &spbpb.Payload_Metric{
		Alias:    &alias,
		Datatype: u32(uint32(spbpb.DataType_Int32)),
		Value:    &spbpb.Payload_Metric_IntValue{IntValue: 1},
	})
	res = runner.RunAll(runner.NewCapture([]spb.Message{m2}))
	if r := resultByID(t, res, "tck-id-payloads-name-requirement"); r.Status != runner.StatusPass {
		t.Errorf("alias-only should pass, got %+v", r)
	}
}

func TestSeqAlwaysIncluded_NDATA_Missing_Fails(t *testing.T) {
	m := ndata("G", "N", 1)
	m.Payload.Seq = nil
	res := runner.RunAll(runner.NewCapture([]spb.Message{m}))
	r := resultByID(t, res, "tck-id-payloads-sequence-num-always-included")
	if r.Status != runner.StatusFail {
		t.Errorf("expected fail, got %+v", r)
	}
}

func TestSeqAlwaysIncluded_NDEATH_Excluded(t *testing.T) {
	// NDEATH carries no seq; the assertion must not flag it.
	d := ndeath("G", "N", u64(1))
	res := runner.RunAll(runner.NewCapture([]spb.Message{d}))
	r := resultByID(t, res, "tck-id-payloads-sequence-num-always-included")
	if r.Status != runner.StatusNotApplicable {
		t.Errorf("expected NA (only NDEATH in capture), got %+v", r)
	}
}

func TestNbirthSeqAlias_Reports(t *testing.T) {
	m := nbirth("G", "N", 0, 1, 1, 0, false)
	res := runner.RunAll(runner.NewCapture([]spb.Message{m}))
	if r := resultByID(t, res, "tck-id-payloads-sequence-num-req-nbirth"); r.Status != runner.StatusPass {
		t.Errorf("expected pass via alias, got %+v", r)
	}
}
