package assertions

import (
	"strings"
	"testing"

	"github.com/joyautomation/sparkplug-tck-go/internal/runner"
	"github.com/joyautomation/sparkplug-tck-go/internal/spb"
	"github.com/joyautomation/sparkplug-tck-go/internal/spbpb"
)

// metricWithDataSet wraps an NBIRTH around one metric carrying the given
// DataSet value. Tests use it to inject specific DataSet shapes.
func metricWithDataSet(ds *spbpb.Payload_DataSet) spb.Message {
	m := nbirth("G", "N", 0, 1, 1, 0, false)
	m.Payload.Metrics = append(m.Payload.Metrics, &spbpb.Payload_Metric{
		Name:     str("ds"),
		Datatype: u32(uint32(spbpb.DataType_DataSet)),
		Value:    &spbpb.Payload_Metric_DatasetValue{DatasetValue: ds},
	})
	return m
}

func metricWithPropertySet(ps *spbpb.Payload_PropertySet) spb.Message {
	m := nbirth("G", "N", 0, 1, 1, 0, false)
	m.Payload.Metrics = append(m.Payload.Metrics, &spbpb.Payload_Metric{
		Name:       str("p"),
		Datatype:   u32(uint32(spbpb.DataType_Int32)),
		Value:      &spbpb.Payload_Metric_IntValue{IntValue: 1},
		Properties: ps,
	})
	return m
}

func TestDataSet_HappyPath(t *testing.T) {
	ds := &spbpb.Payload_DataSet{
		NumOfColumns: u64(2),
		Columns:      []string{"a", "b"},
		Types:        []uint32{uint32(spbpb.DataType_Int32), uint32(spbpb.DataType_String)},
	}
	res := runner.RunAll(runner.NewCapture([]spb.Message{metricWithDataSet(ds)}))
	for _, id := range []string{
		"tck-id-payloads-dataset-parameter-type-req",
		"tck-id-payloads-dataset-types-num",
		"tck-id-payloads-dataset-column-num-headers",
		"tck-id-payloads-dataset-column-size",
		"tck-id-payloads-dataset-types-def",
		"tck-id-payloads-dataset-types-type",
		"tck-id-payloads-dataset-types-value",
	} {
		if r := resultByID(t, res, id); r.Status != runner.StatusPass {
			t.Errorf("%s: expected pass, got %+v", id, r)
		}
	}
}

func TestDataSet_TypesMissing_Fails(t *testing.T) {
	ds := &spbpb.Payload_DataSet{NumOfColumns: u64(0), Columns: nil, Types: nil}
	res := runner.RunAll(runner.NewCapture([]spb.Message{metricWithDataSet(ds)}))
	r := resultByID(t, res, "tck-id-payloads-dataset-parameter-type-req")
	if r.Status != runner.StatusFail || !strings.Contains(r.Detail, "empty/missing") {
		t.Errorf("expected fail, got %+v", r)
	}
}

func TestDataSet_LengthMismatch_Fails(t *testing.T) {
	ds := &spbpb.Payload_DataSet{
		NumOfColumns: u64(2),
		Columns:      []string{"a", "b"},
		Types:        []uint32{uint32(spbpb.DataType_Int32)}, // mismatched
	}
	res := runner.RunAll(runner.NewCapture([]spb.Message{metricWithDataSet(ds)}))
	r := resultByID(t, res, "tck-id-payloads-dataset-types-num")
	if r.Status != runner.StatusFail {
		t.Errorf("expected fail, got %+v", r)
	}
}

func TestDataSet_ColumnSizeMismatch_Fails(t *testing.T) {
	ds := &spbpb.Payload_DataSet{
		NumOfColumns: u64(3), // claims 3 but has 2
		Columns:      []string{"a", "b"},
		Types:        []uint32{uint32(spbpb.DataType_Int32), uint32(spbpb.DataType_String)},
	}
	res := runner.RunAll(runner.NewCapture([]spb.Message{metricWithDataSet(ds)}))
	r := resultByID(t, res, "tck-id-payloads-dataset-column-size")
	if r.Status != runner.StatusFail {
		t.Errorf("expected fail, got %+v", r)
	}
}

func TestDataSet_InvalidTypeValue_Fails(t *testing.T) {
	ds := &spbpb.Payload_DataSet{
		NumOfColumns: u64(1),
		Columns:      []string{"a"},
		Types:        []uint32{99}, // out of range
	}
	res := runner.RunAll(runner.NewCapture([]spb.Message{metricWithDataSet(ds)}))
	r := resultByID(t, res, "tck-id-payloads-dataset-types-value")
	if r.Status != runner.StatusFail || !strings.Contains(r.Detail, "valid enum") {
		t.Errorf("expected fail, got %+v", r)
	}
}

func TestPropertySet_HappyPath(t *testing.T) {
	ps := &spbpb.Payload_PropertySet{
		Keys: []string{"unit", "quality"},
		Values: []*spbpb.Payload_PropertyValue{
			{Type: u32(uint32(spbpb.DataType_String)), Value: &spbpb.Payload_PropertyValue_StringValue{StringValue: "psi"}},
			{Type: u32(uint32(spbpb.DataType_Int32)), Value: &spbpb.Payload_PropertyValue_IntValue{IntValue: 192}},
		},
	}
	res := runner.RunAll(runner.NewCapture([]spb.Message{metricWithPropertySet(ps)}))
	for _, id := range []string{
		"tck-id-payloads-propertyset-keys-array-size",
		"tck-id-payloads-propertyset-values-array-size",
		"tck-id-payloads-metric-propertyvalue-type-req",
		"tck-id-payloads-metric-propertyvalue-type-type",
		"tck-id-payloads-metric-propertyvalue-type-value",
	} {
		if r := resultByID(t, res, id); r.Status != runner.StatusPass {
			t.Errorf("%s: expected pass, got %+v", id, r)
		}
	}
}

func TestPropertySet_KeyValueMismatch_Fails(t *testing.T) {
	ps := &spbpb.Payload_PropertySet{
		Keys: []string{"a", "b"},
		Values: []*spbpb.Payload_PropertyValue{
			{Type: u32(uint32(spbpb.DataType_Int32)), Value: &spbpb.Payload_PropertyValue_IntValue{IntValue: 1}},
		},
	}
	res := runner.RunAll(runner.NewCapture([]spb.Message{metricWithPropertySet(ps)}))
	r := resultByID(t, res, "tck-id-payloads-propertyset-keys-array-size")
	if r.Status != runner.StatusFail {
		t.Errorf("expected fail, got %+v", r)
	}
}

func TestPropertyValue_TypeMissing_Fails(t *testing.T) {
	ps := &spbpb.Payload_PropertySet{
		Keys: []string{"a"},
		Values: []*spbpb.Payload_PropertyValue{
			{Value: &spbpb.Payload_PropertyValue_IntValue{IntValue: 1}}, // no Type
		},
	}
	res := runner.RunAll(runner.NewCapture([]spb.Message{metricWithPropertySet(ps)}))
	r := resultByID(t, res, "tck-id-payloads-metric-propertyvalue-type-req")
	if r.Status != runner.StatusFail || !strings.Contains(r.Detail, "missing type") {
		t.Errorf("expected fail, got %+v", r)
	}
}

func TestPropertyValue_InvalidType_Fails(t *testing.T) {
	ps := &spbpb.Payload_PropertySet{
		Keys: []string{"a"},
		Values: []*spbpb.Payload_PropertyValue{
			{Type: u32(99), Value: &spbpb.Payload_PropertyValue_IntValue{IntValue: 1}},
		},
	}
	res := runner.RunAll(runner.NewCapture([]spb.Message{metricWithPropertySet(ps)}))
	r := resultByID(t, res, "tck-id-payloads-metric-propertyvalue-type-value")
	if r.Status != runner.StatusFail || !strings.Contains(r.Detail, "valid enum") {
		t.Errorf("expected fail, got %+v", r)
	}
}

func TestDataSet_NoMessages_NA(t *testing.T) {
	res := runner.RunAll(runner.NewCapture([]spb.Message{nbirth("G", "N", 0, 1, 1, 0, false)}))
	r := resultByID(t, res, "tck-id-payloads-dataset-types-num")
	if r.Status != runner.StatusNotApplicable {
		t.Errorf("expected NA when no DataSet metrics present, got %+v", r)
	}
}
