package assertions

import (
	"strings"
	"testing"

	"github.com/joyautomation/sparkplug-tck-go/internal/runner"
	"github.com/joyautomation/sparkplug-tck-go/internal/spb"
	"github.com/joyautomation/sparkplug-tck-go/internal/spbpb"
)

func aliasedMetric(name string, alias uint64) *spbpb.Payload_Metric {
	return &spbpb.Payload_Metric{
		Name:     str(name),
		Alias:    u64(alias),
		Datatype: u32(uint32(spbpb.DataType_Int32)),
		Value:    &spbpb.Payload_Metric_IntValue{IntValue: 1},
	}
}

func TestAlias_AllRegistered(t *testing.T) {
	want := []string{
		"tck-id-payloads-alias-birth-requirement",
		"tck-id-payloads-alias-data-cmd-requirement",
		"tck-id-payloads-alias-uniqueness",
		"tck-id-payloads-propertyset-quality-value-type",
		"tck-id-payloads-propertyset-quality-value-value",
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

func TestAliasBirth_AliasedMetricMissingName_Fails(t *testing.T) {
	m := nbirth("G", "N", 0, 1, 1, 0, false)
	m.Payload.Metrics = append(m.Payload.Metrics, &spbpb.Payload_Metric{
		Alias:    u64(7),
		Datatype: u32(uint32(spbpb.DataType_Int32)),
		Value:    &spbpb.Payload_Metric_IntValue{IntValue: 1},
	})
	res := runner.RunAll(runner.NewCapture([]spb.Message{m}))
	r := resultByID(t, res, "tck-id-payloads-alias-birth-requirement")
	if r.Status != runner.StatusFail || !strings.Contains(r.Detail, "missing name") {
		t.Errorf("expected missing-name fail, got %+v", r)
	}
}

func TestAliasBirth_NoAliasedMetrics_NA(t *testing.T) {
	m := nbirth("G", "N", 0, 1, 1, 0, false)
	res := runner.RunAll(runner.NewCapture([]spb.Message{m}))
	r := resultByID(t, res, "tck-id-payloads-alias-birth-requirement")
	if r.Status != runner.StatusNotApplicable {
		t.Errorf("expected NA, got %+v", r)
	}
}

func TestAliasData_AliasedMetricWithName_Fails(t *testing.T) {
	birth := nbirth("G", "N", 0, 1, 1, 0, false)
	birth.Payload.Metrics = append(birth.Payload.Metrics, aliasedMetric("Tag1", 7))
	data := ndata("G", "N", 1)
	data.Payload.Metrics = []*spbpb.Payload_Metric{
		{Name: str("Tag1"), Alias: u64(7),
			Datatype: u32(uint32(spbpb.DataType_Int32)),
			Value:    &spbpb.Payload_Metric_IntValue{IntValue: 2}},
	}
	res := runner.RunAll(runner.NewCapture([]spb.Message{birth, data}))
	r := resultByID(t, res, "tck-id-payloads-alias-data-cmd-requirement")
	if r.Status != runner.StatusFail || !strings.Contains(r.Detail, "must omit name") {
		t.Errorf("expected omit-name fail, got %+v", r)
	}
}

func TestAliasUniqueness_DuplicateAlias_Fails(t *testing.T) {
	m := nbirth("G", "N", 0, 1, 1, 0, false)
	m.Payload.Metrics = append(m.Payload.Metrics,
		aliasedMetric("Tag1", 5),
		aliasedMetric("Tag2", 5), // same alias, different name
	)
	res := runner.RunAll(runner.NewCapture([]spb.Message{m}))
	results := resultsByID(res, "tck-id-payloads-alias-uniqueness")
	var fail *runner.Result
	for i := range results {
		if results[i].Status == runner.StatusFail {
			fail = &results[i]
			break
		}
	}
	if fail == nil || !strings.Contains(fail.Detail, "already bound") {
		t.Errorf("expected uniqueness fail among %v", results)
	}
}

func TestQualityProperty_HappyPath(t *testing.T) {
	ps := &spbpb.Payload_PropertySet{
		Keys: []string{"Quality"},
		Values: []*spbpb.Payload_PropertyValue{
			{Type: u32(uint32(spbpb.DataType_Int32)),
				Value: &spbpb.Payload_PropertyValue_IntValue{IntValue: 192}},
		},
	}
	res := runner.RunAll(runner.NewCapture([]spb.Message{metricWithPropertySet(ps)}))
	for _, id := range []string{
		"tck-id-payloads-propertyset-quality-value-type",
		"tck-id-payloads-propertyset-quality-value-value",
	} {
		if r := resultByID(t, res, id); r.Status != runner.StatusPass {
			t.Errorf("%s: expected pass, got %+v", id, r)
		}
	}
}

func TestQualityProperty_WrongType_Fails(t *testing.T) {
	ps := &spbpb.Payload_PropertySet{
		Keys: []string{"Quality"},
		Values: []*spbpb.Payload_PropertyValue{
			{Type: u32(uint32(spbpb.DataType_String)),
				Value: &spbpb.Payload_PropertyValue_StringValue{StringValue: "GOOD"}},
		},
	}
	res := runner.RunAll(runner.NewCapture([]spb.Message{metricWithPropertySet(ps)}))
	r := resultByID(t, res, "tck-id-payloads-propertyset-quality-value-type")
	if r.Status != runner.StatusFail {
		t.Errorf("expected fail, got %+v", r)
	}
}

func TestQualityProperty_BadCode_Fails(t *testing.T) {
	ps := &spbpb.Payload_PropertySet{
		Keys: []string{"Quality"},
		Values: []*spbpb.Payload_PropertyValue{
			{Type: u32(uint32(spbpb.DataType_Int32)),
				Value: &spbpb.Payload_PropertyValue_IntValue{IntValue: 42}},
		},
	}
	res := runner.RunAll(runner.NewCapture([]spb.Message{metricWithPropertySet(ps)}))
	r := resultByID(t, res, "tck-id-payloads-propertyset-quality-value-value")
	if r.Status != runner.StatusFail || !strings.Contains(r.Detail, "want 0|192|500") {
		t.Errorf("expected bad-code fail, got %+v", r)
	}
}
