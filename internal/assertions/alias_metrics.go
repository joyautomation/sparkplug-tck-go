package assertions

import (
	"fmt"

	"github.com/joyautomation/sparkplug-tck-go/internal/runner"
	"github.com/joyautomation/sparkplug-tck-go/internal/spb"
	"github.com/joyautomation/sparkplug-tck-go/internal/spbpb"
)

// Metric alias rules.
//
//   * birth-requirement: every metric in NBIRTH/DBIRTH MUST include both
//     name AND alias.
//   * data-cmd-requirement: every metric in N/DDATA + N/DCMD MUST include
//     only an alias; the metric name MUST be excluded.
//   * uniqueness: aliases are unique across an Edge Node's entire metric set.

func init() {
	runner.Register(runner.Assertion{ID: "tck-id-payloads-alias-birth-requirement", Run: aliasBirthRequirement})
	runner.Register(runner.Assertion{ID: "tck-id-payloads-alias-data-cmd-requirement", Run: aliasDataCmdRequirement})
	runner.Register(runner.Assertion{ID: "tck-id-payloads-alias-uniqueness", Run: aliasUniqueness})
}

// aliasBirthRequirement: every aliased metric in BIRTH must publish the
// alias→name binding. We interpret the spec's "MUST include both a metric
// name and alias" as a per-metric rule that fires when aliasing is in use:
// if a BIRTH metric carries an alias, it MUST also carry the name; if a
// BIRTH metric carries a name but no alias (e.g. bdSeq), it isn't aliased
// and the rule doesn't apply.
func aliasBirthRequirement(c *runner.Capture) []runner.Result {
	const id = "tck-id-payloads-alias-birth-requirement"
	var out []runner.Result
	for _, m := range c.Messages {
		if !m.Topic.Type.IsBirth() || m.Payload == nil {
			continue
		}
		for _, met := range m.Payload.GetMetrics() {
			if met.Alias == nil {
				continue // not aliased, rule doesn't apply
			}
			subj := subjectFor(m) + "/" + metricLabel(met)
			if met.Name == nil || *met.Name == "" {
				out = append(out, runner.Fail(id, subj,
					fmt.Sprintf("aliased metric (alias=%d) missing name in BIRTH", *met.Alias)))
			} else {
				out = append(out, runner.Pass(id, subj))
			}
		}
	}
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no aliased metrics in NBIRTH/DBIRTH")}
	}
	return out
}

// aliasDataCmdRequirement: in N/DDATA + N/DCMD, an aliased metric MUST NOT
// repeat the name (the alias is the canonical handle once BIRTH established
// the binding). Metrics without an alias are out of scope — the spec's
// "only include an alias and the metric name MUST be excluded" wording
// targets aliased metrics specifically.
func aliasDataCmdRequirement(c *runner.Capture) []runner.Result {
	const id = "tck-id-payloads-alias-data-cmd-requirement"
	var out []runner.Result
	for _, m := range c.Messages {
		t := m.Topic.Type
		if t != spb.NDATA && t != spb.DDATA && t != spb.NCMD && t != spb.DCMD {
			continue
		}
		if m.Payload == nil {
			continue
		}
		for _, met := range m.Payload.GetMetrics() {
			if met.Alias == nil {
				continue
			}
			subj := subjectFor(m) + "/" + metricLabel(met)
			if met.Name != nil && *met.Name != "" {
				out = append(out, runner.Fail(id, subj,
					fmt.Sprintf("aliased metric in %s must omit name (got %q)", t, *met.Name)))
			} else {
				out = append(out, runner.Pass(id, subj))
			}
		}
	}
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no aliased metrics in NDATA/DDATA/NCMD/DCMD")}
	}
	return out
}

// aliasUniqueness: per edge node, aliases declared in NBIRTH/DBIRTH must be
// unique. We bind alias→name within each edge-node descriptor and report a
// fail when the same alias is reused with a different name.
func aliasUniqueness(c *runner.Capture) []runner.Result {
	const id = "tck-id-payloads-alias-uniqueness"
	type binding struct{ name string }
	per := map[string]map[uint64]binding{} // edge → alias → first-seen name
	var out []runner.Result
	for _, m := range c.Messages {
		if !m.Topic.Type.IsBirth() || m.Payload == nil {
			continue
		}
		edge := m.Topic.EdgeNodeID.String()
		seen, ok := per[edge]
		if !ok {
			seen = map[uint64]binding{}
			per[edge] = seen
		}
		for _, met := range m.Payload.GetMetrics() {
			if met.Alias == nil {
				continue
			}
			a := *met.Alias
			name := ""
			if met.Name != nil {
				name = *met.Name
			}
			subj := edge + "/" + metricLabel(met)
			if prev, dup := seen[a]; dup {
				if prev.name != name {
					out = append(out, runner.Fail(id, subj,
						fmt.Sprintf("alias=%d already bound to %q, now %q", a, prev.name, name)))
					continue
				}
			} else {
				seen[a] = binding{name: name}
			}
			out = append(out, runner.Pass(id, subj))
		}
	}
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no metrics with aliases in NBIRTH/DBIRTH")}
	}
	return out
}

// PropertySet quality codes: the well-known "Quality" property has a fixed
// shape — type=Int32 (3), value ∈ {0, 192, 500}. The spec calls it out as
// a special case of PropertyValue rules.
const propertyKeyQuality = "Quality"

func init() {
	runner.Register(runner.Assertion{ID: "tck-id-payloads-propertyset-quality-value-type", Run: qualityValueType})
	runner.Register(runner.Assertion{ID: "tck-id-payloads-propertyset-quality-value-value", Run: qualityValueValue})
}

func qualityValueType(c *runner.Capture) []runner.Result {
	const id = "tck-id-payloads-propertyset-quality-value-type"
	var out []runner.Result
	forEachPropertySet(c, func(m spb.Message, met *spbpb.Payload_Metric, ps *spbpb.Payload_PropertySet) {
		for i, k := range ps.GetKeys() {
			if k != propertyKeyQuality {
				continue
			}
			values := ps.GetValues()
			if i >= len(values) {
				continue
			}
			pv := values[i]
			if pv == nil {
				continue
			}
			subj := subjectFor(m) + "/" + metricLabel(met) + "/Quality"
			if pv.Type == nil || *pv.Type != uint32(spbpb.DataType_Int32) {
				got := uint32(0)
				if pv.Type != nil {
					got = *pv.Type
				}
				out = append(out, runner.Fail(id, subj,
					fmt.Sprintf("Quality property type=%d, want %d (Int32)", got, spbpb.DataType_Int32)))
			} else {
				out = append(out, runner.Pass(id, subj))
			}
		}
	})
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no Quality property values in capture")}
	}
	return out
}

func qualityValueValue(c *runner.Capture) []runner.Result {
	const id = "tck-id-payloads-propertyset-quality-value-value"
	var out []runner.Result
	forEachPropertySet(c, func(m spb.Message, met *spbpb.Payload_Metric, ps *spbpb.Payload_PropertySet) {
		for i, k := range ps.GetKeys() {
			if k != propertyKeyQuality {
				continue
			}
			values := ps.GetValues()
			if i >= len(values) {
				continue
			}
			pv := values[i]
			if pv == nil {
				continue
			}
			subj := subjectFor(m) + "/" + metricLabel(met) + "/Quality"
			iv, ok := pv.Value.(*spbpb.Payload_PropertyValue_IntValue)
			if !ok || iv == nil {
				out = append(out, runner.Fail(id, subj, "Quality property value must be int"))
				continue
			}
			switch iv.IntValue {
			case 0, 192, 500:
				out = append(out, runner.Pass(id, subj))
			default:
				out = append(out, runner.Fail(id, subj,
					fmt.Sprintf("Quality value=%d, want 0|192|500", iv.IntValue)))
			}
		}
	})
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no Quality property values in capture")}
	}
	return out
}
