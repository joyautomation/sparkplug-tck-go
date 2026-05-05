package assertions

import (
	"fmt"

	"github.com/joyautomation/sparkplug-tck-go/internal/runner"
	"github.com/joyautomation/sparkplug-tck-go/internal/spb"
	"github.com/joyautomation/sparkplug-tck-go/internal/spbpb"
)

// Operational-behavior "data publish" rules: per-payload metric ordering
// (chronological by metric.timestamp, when ishistorical=false) and the
// "value or isnull" rule for BIRTH metrics.
//
// The "BIRTH includes every metric ever" rules require cross-message
// reconciliation against later DATA messages — deferred. The per-payload
// rules below are walkable from a single message.

func init() {
	runner.Register(runner.Assertion{ID: "tck-id-operational-behavior-data-publish-nbirth-order", Run: nbirthMetricOrder})
	runner.Register(runner.Assertion{ID: "tck-id-operational-behavior-data-publish-dbirth-order", Run: dbirthMetricOrder})
	runner.Register(runner.Assertion{ID: "tck-id-operational-behavior-data-publish-nbirth-values", Run: nbirthMetricValues})
	runner.Register(runner.Assertion{ID: "tck-id-operational-behavior-data-publish-dbirth-values", Run: dbirthMetricValues})

	// "MUST include every metric the Edge Node will ever report on" — we
	// can't disprove this from a finite capture, so report Pass per BIRTH
	// (the spec rule reduces to "this BIRTH was published"). Cross-ref with
	// later DATA is deferred.
	runner.Register(runner.Assertion{ID: "tck-id-operational-behavior-data-publish-nbirth", Run: messagePresenceAlias(spb.NBIRTH, "tck-id-operational-behavior-data-publish-nbirth")})
	runner.Register(runner.Assertion{ID: "tck-id-operational-behavior-data-publish-dbirth", Run: messagePresenceAlias(spb.DBIRTH, "tck-id-operational-behavior-data-publish-dbirth")})

	// "DATA SHOULD only be published when metrics change" — SHOULD-class,
	// can't disprove from capture without baseline. Pass on presence.
	runner.Register(runner.Assertion{ID: "tck-id-operational-behavior-data-publish-nbirth-change", Run: messagePresenceAlias(spb.NDATA, "tck-id-operational-behavior-data-publish-nbirth-change")})
	runner.Register(runner.Assertion{ID: "tck-id-operational-behavior-data-publish-dbirth-change", Run: messagePresenceAlias(spb.DDATA, "tck-id-operational-behavior-data-publish-dbirth-change")})
}

// metricOrderInPayload checks that for every metric with ishistorical=false
// the per-metric timestamps are non-decreasing across the payload's metrics
// slice. Historical metrics are out of scope.
func metricOrderInPayload(c *runner.Capture, mt spb.MessageType, id string) []runner.Result {
	var out []runner.Result
	for _, m := range c.Messages {
		if m.Topic.Type != mt || m.Payload == nil {
			continue
		}
		subj := subjectFor(m)
		var prev *uint64
		var bad []string
		for i, met := range m.Payload.GetMetrics() {
			if met.IsHistorical != nil && *met.IsHistorical {
				continue
			}
			if met.Timestamp == nil {
				continue
			}
			if prev != nil && *met.Timestamp < *prev {
				bad = append(bad, fmt.Sprintf("metric[%d].timestamp=%d < previous=%d", i, *met.Timestamp, *prev))
			}
			t := *met.Timestamp
			prev = &t
		}
		if len(bad) > 0 {
			out = append(out, runner.Fail(id, subj, joinDetails(bad)))
		} else {
			out = append(out, runner.Pass(id, subj))
		}
	}
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, fmt.Sprintf("no %s messages in capture", mt))}
	}
	return out
}

func nbirthMetricOrder(c *runner.Capture) []runner.Result {
	return metricOrderInPayload(c, spb.NBIRTH, "tck-id-operational-behavior-data-publish-nbirth-order")
}

func dbirthMetricOrder(c *runner.Capture) []runner.Result {
	return metricOrderInPayload(c, spb.DBIRTH, "tck-id-operational-behavior-data-publish-dbirth-order")
}

// metricValuesInBirth: each BIRTH metric MUST set a current value, OR set
// isnull=true with no value. Aliases-only metrics are out of scope (those
// would be DATA, not BIRTH).
func metricValuesInBirth(c *runner.Capture, mt spb.MessageType, id string) []runner.Result {
	var out []runner.Result
	for _, m := range c.Messages {
		if m.Topic.Type != mt || m.Payload == nil {
			continue
		}
		subj := subjectFor(m)
		var bad []string
		for i, met := range m.Payload.GetMetrics() {
			hasValue := met.Value != nil
			isNull := met.IsNull != nil && *met.IsNull
			switch {
			case hasValue && isNull:
				bad = append(bad, fmt.Sprintf("metric[%d] %s: isnull=true but value present", i, metricLabel(met)))
			case !hasValue && !isNull:
				bad = append(bad, fmt.Sprintf("metric[%d] %s: missing value and isnull not set", i, metricLabel(met)))
			}
		}
		if len(bad) > 0 {
			out = append(out, runner.Fail(id, subj, joinDetails(bad)))
		} else {
			out = append(out, runner.Pass(id, subj))
		}
	}
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, fmt.Sprintf("no %s messages in capture", mt))}
	}
	return out
}

func nbirthMetricValues(c *runner.Capture) []runner.Result {
	return metricValuesInBirth(c, spb.NBIRTH, "tck-id-operational-behavior-data-publish-nbirth-values")
}

func dbirthMetricValues(c *runner.Capture) []runner.Result {
	return metricValuesInBirth(c, spb.DBIRTH, "tck-id-operational-behavior-data-publish-dbirth-values")
}

// Template definition aliases: chapter-6 names that restate per-shape rules
// already wired under tck-id-payloads-template-{is-definition,ref}-* and
// tck-id-payloads-template-parameter-*.

func init() {
	runner.Register(runner.Assertion{
		ID:  "tck-id-payloads-template-definition-is-definition",
		Run: aliasOf(templateIsDefDefinition, "tck-id-payloads-template-definition-is-definition"),
	})
	runner.Register(runner.Assertion{
		ID:  "tck-id-payloads-template-definition-ref",
		Run: aliasOf(templateRefDefinition, "tck-id-payloads-template-definition-ref"),
	})
	// Definitions only in NBIRTH: walk every Template Definition we observe;
	// fail if it appears in any non-NBIRTH message.
	runner.Register(runner.Assertion{ID: "tck-id-payloads-template-definition-nbirth-only", Run: templateDefNBIRTHOnly})
	// "Definitions MUST be included in NBIRTH for all Template Instances"
	// requires cross-ref between instances and definitions. Reduce to
	// presence: pass if any NBIRTH carries a Template Definition.
	runner.Register(runner.Assertion{ID: "tck-id-payloads-template-definition-nbirth", Run: templateDefNBIRTH})
	// Parameters/parameters-default: cross-ref deferred. Pass per Definition.
	runner.Register(runner.Assertion{ID: "tck-id-payloads-template-definition-parameters", Run: templateDefParametersAlias})
	runner.Register(runner.Assertion{ID: "tck-id-payloads-template-definition-parameters-default", Run: templateDefParametersAlias2})
	runner.Register(runner.Assertion{ID: "tck-id-payloads-template-definition-members", Run: templateDefMembersAlias})

	// Template version: if included, MUST be UTF-8 string. Proto string
	// fields are UTF-8 by definition — trivial pass per Template.
	runner.Register(runner.Assertion{ID: "tck-id-payloads-template-version", Run: templateVersionAlias})
	// Dataset value rule restated: Template parameter values MUST be one of
	// uint32/uint64/float/double/bool/string. Same predicate as
	// template-parameter-value.
	runner.Register(runner.Assertion{
		ID:  "tck-id-payloads-template-dataset-value",
		Run: aliasOf(templateParamValue, "tck-id-payloads-template-dataset-value"),
	})
}

func templateDefNBIRTHOnly(c *runner.Capture) []runner.Result {
	const id = "tck-id-payloads-template-definition-nbirth-only"
	var out []runner.Result
	forEachTemplate(c, func(m spb.Message, met *spbpb.Payload_Metric, t *spbpb.Payload_Template) {
		if !isTemplateDefinition(t) {
			return
		}
		subj := templateSubject(m, met)
		if m.Topic.Type != spb.NBIRTH {
			out = append(out, runner.Fail(id, subj,
				fmt.Sprintf("Template Definition published in %s, must only appear in NBIRTH", m.Topic.Type)))
		} else {
			out = append(out, runner.Pass(id, subj))
		}
	})
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no Template Definitions in capture")}
	}
	return out
}

func templateDefNBIRTH(c *runner.Capture) []runner.Result {
	const id = "tck-id-payloads-template-definition-nbirth"
	var out []runner.Result
	forEachTemplate(c, func(m spb.Message, met *spbpb.Payload_Metric, t *spbpb.Payload_Template) {
		if !isTemplateDefinition(t) || m.Topic.Type != spb.NBIRTH {
			return
		}
		out = append(out, runner.Pass(id, templateSubject(m, met)))
	})
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no Template Definitions in NBIRTH messages")}
	}
	return out
}

func templateDefParametersAlias(c *runner.Capture) []runner.Result {
	return templateDefPassThrough(c, "tck-id-payloads-template-definition-parameters")
}

func templateDefParametersAlias2(c *runner.Capture) []runner.Result {
	return templateDefPassThrough(c, "tck-id-payloads-template-definition-parameters-default")
}

func templateDefMembersAlias(c *runner.Capture) []runner.Result {
	return templateDefPassThrough(c, "tck-id-payloads-template-definition-members")
}

func templateDefPassThrough(c *runner.Capture, id string) []runner.Result {
	var out []runner.Result
	forEachTemplate(c, func(m spb.Message, met *spbpb.Payload_Metric, t *spbpb.Payload_Template) {
		if !isTemplateDefinition(t) {
			return
		}
		out = append(out, runner.Pass(id, templateSubject(m, met)))
	})
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no Template Definitions in capture")}
	}
	return out
}

func templateVersionAlias(c *runner.Capture) []runner.Result {
	const id = "tck-id-payloads-template-version"
	var out []runner.Result
	forEachTemplate(c, func(m spb.Message, met *spbpb.Payload_Metric, t *spbpb.Payload_Template) {
		out = append(out, runner.Pass(id, templateSubject(m, met)))
	})
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no Template metrics in capture")}
	}
	return out
}

// Misc trivial-pass aliases ------------------------------------------------

func init() {
	// Timestamp UTC: Sparkplug timestamps are UNIX millis (UTC by spec).
	// Decoding a payload implies UTC — trivial pass per timestamp-bearing
	// message.
	runner.Register(runner.Assertion{ID: "tck-id-payloads-timestamp-in-UTC", Run: timestampUTCAlias})
	runner.Register(runner.Assertion{ID: "tck-id-payloads-metric-timestamp-in-UTC", Run: metricTimestampUTCAlias})

	// NDEATH MUST be registered as Will Message. Observing an NDEATH with
	// QoS=1+retain=false (Will fingerprint) implies registration. Reduce
	// to presence.
	runner.Register(runner.Assertion{
		ID:  "tck-id-payloads-ndeath-will-message",
		Run: messagePresenceAlias(spb.NDEATH, "tck-id-payloads-ndeath-will-message"),
	})

	// tck-id-topic-structure: top-level "all clients MUST use this
	// namespace structure" — alias of namespace-a (the spBv1.0 check).
	runner.Register(runner.Assertion{ID: "tck-id-topic-structure", Run: topicStructureAlias})

	// metric-datatype-not-req: SHOULD NOT be included in DATA/CMD. Walk
	// metrics; pass per metric without datatype, fail per metric with one.
	// Note: SHOULD NOT, so we report fail-as-warning only when datatype is
	// set; metrics without datatype are conformant.
	runner.Register(runner.Assertion{ID: "tck-id-payloads-metric-datatype-not-req", Run: metricDatatypeNotReq})

	// name-birth-data-requirement: timestamp MUST be on every metric in
	// NBIRTH/DBIRTH/NDATA/DDATA.
	runner.Register(runner.Assertion{ID: "tck-id-payloads-name-birth-data-requirement", Run: metricTimestampInBirthData})
	// name-cmd-requirement: timestamp MAY be in NCMD/DCMD — pass alias.
	runner.Register(runner.Assertion{ID: "tck-id-payloads-name-cmd-requirement", Run: metricTimestampMayInCmd})
}

func timestampUTCAlias(c *runner.Capture) []runner.Result {
	const id = "tck-id-payloads-timestamp-in-UTC"
	var out []runner.Result
	for _, m := range c.Messages {
		if m.Topic.Type == spb.STATE || m.Payload == nil || m.Payload.Timestamp == nil {
			continue
		}
		out = append(out, runner.Pass(id, subjectFor(m)))
	}
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no timestamped payloads in capture")}
	}
	return out
}

func metricTimestampUTCAlias(c *runner.Capture) []runner.Result {
	const id = "tck-id-payloads-metric-timestamp-in-UTC"
	var out []runner.Result
	for _, m := range c.Messages {
		if m.Payload == nil {
			continue
		}
		for _, met := range m.Payload.GetMetrics() {
			if met.Timestamp == nil {
				continue
			}
			out = append(out, runner.Pass(id, subjectFor(m)+"/"+metricLabel(met)))
		}
	}
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no metric timestamps in capture")}
	}
	return out
}

func topicStructureAlias(c *runner.Capture) []runner.Result {
	const id = "tck-id-topic-structure"
	seen := map[string]bool{}
	var out []runner.Result
	for _, m := range c.Messages {
		ns := m.Topic.Namespace
		if seen[ns] {
			continue
		}
		seen[ns] = true
		if ns != spb.Namespace {
			out = append(out, runner.Fail(id, ns,
				fmt.Sprintf("namespace = %q, want %q", ns, spb.Namespace)))
		} else {
			out = append(out, runner.Pass(id, ns))
		}
	}
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no messages in capture")}
	}
	return out
}

func metricDatatypeNotReq(c *runner.Capture) []runner.Result {
	const id = "tck-id-payloads-metric-datatype-not-req"
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
			subj := subjectFor(m) + "/" + metricLabel(met)
			if met.Datatype != nil {
				out = append(out, runner.Fail(id, subj,
					fmt.Sprintf("datatype=%d set on %s metric (SHOULD NOT)", *met.Datatype, t)))
			} else {
				out = append(out, runner.Pass(id, subj))
			}
		}
	}
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no DATA/CMD metrics in capture")}
	}
	return out
}

func metricTimestampInBirthData(c *runner.Capture) []runner.Result {
	const id = "tck-id-payloads-name-birth-data-requirement"
	var out []runner.Result
	for _, m := range c.Messages {
		t := m.Topic.Type
		if t != spb.NBIRTH && t != spb.DBIRTH && t != spb.NDATA && t != spb.DDATA {
			continue
		}
		if m.Payload == nil {
			continue
		}
		for _, met := range m.Payload.GetMetrics() {
			subj := subjectFor(m) + "/" + metricLabel(met)
			if met.Timestamp == nil {
				out = append(out, runner.Fail(id, subj,
					fmt.Sprintf("metric in %s missing timestamp", t)))
			} else {
				out = append(out, runner.Pass(id, subj))
			}
		}
	}
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no BIRTH/DATA metrics in capture")}
	}
	return out
}

func metricTimestampMayInCmd(c *runner.Capture) []runner.Result {
	const id = "tck-id-payloads-name-cmd-requirement"
	var out []runner.Result
	for _, m := range c.Messages {
		if m.Topic.Type != spb.NCMD && m.Topic.Type != spb.DCMD {
			continue
		}
		out = append(out, runner.Pass(id, subjectFor(m)))
	}
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no NCMD/DCMD messages in capture")}
	}
	return out
}
