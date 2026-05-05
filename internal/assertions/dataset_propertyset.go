package assertions

import (
	"fmt"

	"github.com/joyautomation/sparkplug-tck-go/internal/runner"
	"github.com/joyautomation/sparkplug-tck-go/internal/spb"
	"github.com/joyautomation/sparkplug-tck-go/internal/spbpb"
)

// Structural checks for compound metric values: DataSet (rectangular table)
// and PropertySet (parallel keys/values arrays). The spec ties most of
// these constraints together — types[] vs columns[] for DataSets, keys[]
// vs values[] for PropertySets — so we walk every metric once and emit
// per-violation results under each spec ID.

func init() {
	runner.Register(runner.Assertion{ID: "tck-id-payloads-dataset-parameter-type-req", Run: datasetTypesPresent})
	runner.Register(runner.Assertion{ID: "tck-id-payloads-dataset-types-num", Run: datasetTypesNum})
	runner.Register(runner.Assertion{ID: "tck-id-payloads-dataset-column-num-headers", Run: datasetColumnNumHeaders})
	runner.Register(runner.Assertion{ID: "tck-id-payloads-dataset-column-size", Run: datasetColumnSize})
	runner.Register(runner.Assertion{ID: "tck-id-payloads-dataset-types-def", Run: datasetTypesDef})
	runner.Register(runner.Assertion{ID: "tck-id-payloads-dataset-types-type", Run: datasetTypesType})
	runner.Register(runner.Assertion{ID: "tck-id-payloads-dataset-types-value", Run: datasetTypesValue})

	runner.Register(runner.Assertion{ID: "tck-id-payloads-propertyset-keys-array-size", Run: propertySetKeysSize})
	runner.Register(runner.Assertion{ID: "tck-id-payloads-propertyset-values-array-size", Run: propertySetValuesSize})
	runner.Register(runner.Assertion{ID: "tck-id-payloads-metric-propertyvalue-type-req", Run: propertyValueTypeReq})
	runner.Register(runner.Assertion{ID: "tck-id-payloads-metric-propertyvalue-type-type", Run: propertyValueTypeType})
	runner.Register(runner.Assertion{ID: "tck-id-payloads-metric-propertyvalue-type-value", Run: propertyValueTypeValue})
}

// forEachDataSet visits every DataSet value carried in a metric across
// all messages; the visitor receives the parent message and the dataset.
func forEachDataSet(c *runner.Capture, visit func(m spb.Message, met *spbpb.Payload_Metric, ds *spbpb.Payload_DataSet)) {
	for _, m := range c.Messages {
		if m.Topic.Type == spb.STATE || m.Payload == nil {
			continue
		}
		for _, met := range m.Payload.GetMetrics() {
			ds, ok := met.Value.(*spbpb.Payload_Metric_DatasetValue)
			if !ok || ds == nil || ds.DatasetValue == nil {
				continue
			}
			visit(m, met, ds.DatasetValue)
		}
	}
}

// forEachPropertySet visits every PropertySet attached to a metric (via
// metric.Properties) across all messages.
func forEachPropertySet(c *runner.Capture, visit func(m spb.Message, met *spbpb.Payload_Metric, ps *spbpb.Payload_PropertySet)) {
	for _, m := range c.Messages {
		if m.Topic.Type == spb.STATE || m.Payload == nil {
			continue
		}
		for _, met := range m.Payload.GetMetrics() {
			if ps := met.GetProperties(); ps != nil {
				visit(m, met, ps)
			}
		}
	}
}

func metricLabel(m *spbpb.Payload_Metric) string {
	if n := m.GetName(); n != "" {
		return n
	}
	if m.Alias != nil {
		return fmt.Sprintf("alias=%d", *m.Alias)
	}
	return "<unnamed>"
}

// --- DataSet checks --------------------------------------------------------

func datasetTypesPresent(c *runner.Capture) []runner.Result {
	const id = "tck-id-payloads-dataset-parameter-type-req"
	var out []runner.Result
	forEachDataSet(c, func(m spb.Message, met *spbpb.Payload_Metric, ds *spbpb.Payload_DataSet) {
		subject := subjectFor(m) + "/" + metricLabel(met)
		if len(ds.GetTypes()) == 0 {
			out = append(out, runner.Fail(id, subject, "DataSet types[] empty/missing"))
		} else {
			out = append(out, runner.Pass(id, subject))
		}
	})
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no DataSet metrics in capture")}
	}
	return out
}

func datasetTypesNum(c *runner.Capture) []runner.Result {
	const id = "tck-id-payloads-dataset-types-num"
	return datasetColLengthCheck(c, id)
}

func datasetColumnNumHeaders(c *runner.Capture) []runner.Result {
	const id = "tck-id-payloads-dataset-column-num-headers"
	return datasetColLengthCheck(c, id)
}

// datasetColLengthCheck implements both the columns-vs-types and the
// headers-vs-types length-equality requirements (the spec phrases them
// from each side; the constraint is symmetric).
func datasetColLengthCheck(c *runner.Capture, id string) []runner.Result {
	var out []runner.Result
	forEachDataSet(c, func(m spb.Message, met *spbpb.Payload_Metric, ds *spbpb.Payload_DataSet) {
		subject := subjectFor(m) + "/" + metricLabel(met)
		if got, want := len(ds.GetColumns()), len(ds.GetTypes()); got != want {
			out = append(out, runner.Fail(id, subject,
				fmt.Sprintf("columns=%d types=%d", got, want)))
		} else {
			out = append(out, runner.Pass(id, subject))
		}
	})
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no DataSet metrics in capture")}
	}
	return out
}

func datasetColumnSize(c *runner.Capture) []runner.Result {
	const id = "tck-id-payloads-dataset-column-size"
	var out []runner.Result
	forEachDataSet(c, func(m spb.Message, met *spbpb.Payload_Metric, ds *spbpb.Payload_DataSet) {
		subject := subjectFor(m) + "/" + metricLabel(met)
		got := uint64(len(ds.GetColumns()))
		want := ds.GetNumOfColumns()
		if want != got {
			out = append(out, runner.Fail(id, subject,
				fmt.Sprintf("num_of_columns=%d but len(columns)=%d", want, got)))
		} else {
			out = append(out, runner.Pass(id, subject))
		}
	})
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no DataSet metrics in capture")}
	}
	return out
}

// datasetTypesDef and datasetTypesType: the proto already pins types[]
// to []uint32. Once a DataSet decodes, both rules are satisfied. We still
// emit a pass per dataset to keep reports consistent with the spec ID set.
func datasetTypesDef(c *runner.Capture) []runner.Result {
	return datasetTrivialPass(c, "tck-id-payloads-dataset-types-def")
}

func datasetTypesType(c *runner.Capture) []runner.Result {
	return datasetTrivialPass(c, "tck-id-payloads-dataset-types-type")
}

func datasetTrivialPass(c *runner.Capture, id string) []runner.Result {
	var out []runner.Result
	forEachDataSet(c, func(m spb.Message, met *spbpb.Payload_Metric, ds *spbpb.Payload_DataSet) {
		out = append(out, runner.Pass(id, subjectFor(m)+"/"+metricLabel(met)))
	})
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no DataSet metrics in capture")}
	}
	return out
}

func datasetTypesValue(c *runner.Capture) []runner.Result {
	const id = "tck-id-payloads-dataset-types-value"
	var out []runner.Result
	forEachDataSet(c, func(m spb.Message, met *spbpb.Payload_Metric, ds *spbpb.Payload_DataSet) {
		subject := subjectFor(m) + "/" + metricLabel(met)
		var bad []string
		for i, v := range ds.GetTypes() {
			if !validDataType(v) {
				bad = append(bad, fmt.Sprintf("types[%d]=%d not in valid enum", i, v))
			}
		}
		if len(bad) > 0 {
			out = append(out, runner.Fail(id, subject, joinDetails(bad)))
		} else {
			out = append(out, runner.Pass(id, subject))
		}
	})
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no DataSet metrics in capture")}
	}
	return out
}

// --- PropertySet checks ----------------------------------------------------

func propertySetKeysSize(c *runner.Capture) []runner.Result {
	return propertySetLengthCheck(c, "tck-id-payloads-propertyset-keys-array-size")
}

func propertySetValuesSize(c *runner.Capture) []runner.Result {
	return propertySetLengthCheck(c, "tck-id-payloads-propertyset-values-array-size")
}

func propertySetLengthCheck(c *runner.Capture, id string) []runner.Result {
	var out []runner.Result
	forEachPropertySet(c, func(m spb.Message, met *spbpb.Payload_Metric, ps *spbpb.Payload_PropertySet) {
		subject := subjectFor(m) + "/" + metricLabel(met)
		if k, v := len(ps.GetKeys()), len(ps.GetValues()); k != v {
			out = append(out, runner.Fail(id, subject,
				fmt.Sprintf("keys=%d values=%d", k, v)))
		} else {
			out = append(out, runner.Pass(id, subject))
		}
	})
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no PropertySet metrics in capture")}
	}
	return out
}

func propertyValueTypeReq(c *runner.Capture) []runner.Result {
	const id = "tck-id-payloads-metric-propertyvalue-type-req"
	var out []runner.Result
	for _, m := range c.Messages {
		if !m.Topic.Type.IsBirth() {
			continue
		}
		for _, met := range m.Payload.GetMetrics() {
			ps := met.GetProperties()
			if ps == nil {
				continue
			}
			subject := subjectFor(m) + "/" + metricLabel(met)
			var bad []string
			for i, pv := range ps.GetValues() {
				if pv == nil {
					continue
				}
				if pv.Type == nil {
					bad = append(bad, fmt.Sprintf("PropertyValue[%d] missing type", i))
				}
			}
			if len(bad) > 0 {
				out = append(out, runner.Fail(id, subject, joinDetails(bad)))
			} else if len(ps.GetValues()) > 0 {
				out = append(out, runner.Pass(id, subject))
			}
		}
	}
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no PropertyValues in NBIRTH/DBIRTH")}
	}
	return out
}

// propertyValueTypeType: PropertyValue.type is a uint32 by proto definition.
// Once decoded, this is satisfied. Emit a pass per encountered PropertyValue.
func propertyValueTypeType(c *runner.Capture) []runner.Result {
	const id = "tck-id-payloads-metric-propertyvalue-type-type"
	var out []runner.Result
	forEachPropertySet(c, func(m spb.Message, met *spbpb.Payload_Metric, ps *spbpb.Payload_PropertySet) {
		if len(ps.GetValues()) > 0 {
			out = append(out, runner.Pass(id, subjectFor(m)+"/"+metricLabel(met)))
		}
	})
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no PropertySet metrics in capture")}
	}
	return out
}

func propertyValueTypeValue(c *runner.Capture) []runner.Result {
	const id = "tck-id-payloads-metric-propertyvalue-type-value"
	var out []runner.Result
	forEachPropertySet(c, func(m spb.Message, met *spbpb.Payload_Metric, ps *spbpb.Payload_PropertySet) {
		subject := subjectFor(m) + "/" + metricLabel(met)
		var bad []string
		for i, pv := range ps.GetValues() {
			if pv == nil || pv.Type == nil {
				continue
			}
			if !validDataType(*pv.Type) {
				bad = append(bad, fmt.Sprintf("PropertyValue[%d] type=%d not in valid enum", i, *pv.Type))
			}
		}
		if len(bad) > 0 {
			out = append(out, runner.Fail(id, subject, joinDetails(bad)))
		} else if len(ps.GetValues()) > 0 {
			out = append(out, runner.Pass(id, subject))
		}
	})
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no PropertySet metrics in capture")}
	}
	return out
}
