package assertions

import (
	"fmt"

	"github.com/joyautomation/sparkplug-tck-go/internal/runner"
	"github.com/joyautomation/sparkplug-tck-go/internal/spb"
	"github.com/joyautomation/sparkplug-tck-go/internal/spbpb"
)

// Per-metric checks that apply to every metric inside birth/data payloads.
//
//   tck-id-payloads-metric-datatype-req:
//     "The datatype MUST be included with each metric definition in
//      NBIRTH and DBIRTH messages."
//
//   tck-id-payloads-metric-datatype-value:
//     "The datatype MUST be one of the enumerated values [DataType]."
//
//   tck-id-payloads-metric-datatype-value-type:
//     "The datatype MUST be an unsigned 32-bit integer." Trivially true once
//     the proto parses, so this assertion only confirms presence + decode.
//
//   tck-id-payloads-name-requirement:
//     "The name MUST be included with every metric unless aliases are being used."

func init() {
	runner.Register(runner.Assertion{ID: "tck-id-payloads-metric-datatype-req", Run: metricDatatypeReq})
	runner.Register(runner.Assertion{ID: "tck-id-payloads-metric-datatype-value", Run: metricDatatypeValue})
	runner.Register(runner.Assertion{ID: "tck-id-payloads-metric-datatype-value-type", Run: metricDatatypeValueType})
	runner.Register(runner.Assertion{ID: "tck-id-payloads-name-requirement", Run: metricNameRequirement})
}

// validDataType reports whether v matches one of the spec's enumerated
// DataType values (Unknown is reserved as the zero value and not allowed
// for a metric).
func validDataType(v uint32) bool {
	dt := spbpb.DataType(v)
	switch dt {
	case spbpb.DataType_Unknown:
		return false
	}
	// Accept anything in the enumerated range. The proto enum spans 1..34
	// (including the Array variants); future extensions add to the upper
	// bound, so cap loosely.
	if v >= 1 && v <= 34 {
		return true
	}
	// Vendor extensions may use higher values; the spec text technically
	// limits to the enumerated set, so stay strict.
	return false
}

// metricDatatypeReq: every metric in NBIRTH/DBIRTH must carry a non-zero
// datatype.
func metricDatatypeReq(c *runner.Capture) []runner.Result {
	const id = "tck-id-payloads-metric-datatype-req"
	var out []runner.Result
	for _, m := range c.Messages {
		if !m.Topic.Type.IsBirth() {
			continue
		}
		subject := subjectFor(m)
		var bad []string
		for _, met := range m.Payload.GetMetrics() {
			if met.Datatype == nil || *met.Datatype == 0 {
				bad = append(bad, fmt.Sprintf("metric %q missing datatype", met.GetName()))
			}
		}
		if len(bad) > 0 {
			out = append(out, runner.Fail(id, subject, joinDetails(bad)))
		} else {
			out = append(out, runner.Pass(id, subject))
		}
	}
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no NBIRTH/DBIRTH messages in capture")}
	}
	return out
}

// metricDatatypeValue: the datatype enum must be a valid value.
func metricDatatypeValue(c *runner.Capture) []runner.Result {
	const id = "tck-id-payloads-metric-datatype-value"
	var out []runner.Result
	for _, m := range c.Messages {
		if !m.Topic.Type.IsBirth() {
			continue
		}
		subject := subjectFor(m)
		var bad []string
		for _, met := range m.Payload.GetMetrics() {
			if met.Datatype == nil {
				continue // covered by datatype-req
			}
			if !validDataType(*met.Datatype) {
				bad = append(bad, fmt.Sprintf("metric %q datatype=%d not in valid enum", met.GetName(), *met.Datatype))
			}
		}
		if len(bad) > 0 {
			out = append(out, runner.Fail(id, subject, joinDetails(bad)))
		} else {
			out = append(out, runner.Pass(id, subject))
		}
	}
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no NBIRTH/DBIRTH messages in capture")}
	}
	return out
}

// metricDatatypeValueType: proto field is uint32 by definition. If the
// payload decoded, this is satisfied — emit one pass per birth message.
func metricDatatypeValueType(c *runner.Capture) []runner.Result {
	const id = "tck-id-payloads-metric-datatype-value-type"
	var out []runner.Result
	for _, m := range c.Messages {
		if !m.Topic.Type.IsBirth() {
			continue
		}
		out = append(out, runner.Pass(id, subjectFor(m)))
	}
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no birth messages in capture")}
	}
	return out
}

// metricNameRequirement: name MUST be included unless an alias is present.
// Applies to all edge messages with metrics (NBIRTH/DBIRTH/NDATA/DDATA/etc).
func metricNameRequirement(c *runner.Capture) []runner.Result {
	const id = "tck-id-payloads-name-requirement"
	var out []runner.Result
	for _, m := range c.Messages {
		if m.Topic.Type == spb.STATE || m.Payload == nil {
			continue
		}
		if len(m.Payload.GetMetrics()) == 0 {
			continue
		}
		subject := subjectFor(m)
		var bad []string
		for i, met := range m.Payload.GetMetrics() {
			hasName := met.Name != nil && *met.Name != ""
			hasAlias := met.Alias != nil
			if !hasName && !hasAlias {
				bad = append(bad, fmt.Sprintf("metric[%d] has neither name nor alias", i))
			}
		}
		if len(bad) > 0 {
			out = append(out, runner.Fail(id, subject, joinDetails(bad)))
		} else {
			out = append(out, runner.Pass(id, subject))
		}
	}
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no metric-bearing messages in capture")}
	}
	return out
}

func joinDetails(s []string) string {
	out := ""
	for i, v := range s {
		if i > 0 {
			out += "; "
		}
		out += v
	}
	return out
}
