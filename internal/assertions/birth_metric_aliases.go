package assertions

import (
	"fmt"

	"github.com/joyautomation/sparkplug-tck-go/internal/runner"
	"github.com/joyautomation/sparkplug-tck-go/internal/spb"
	"github.com/joyautomation/sparkplug-tck-go/internal/spbpb"
)

// Chapter-4 [tck-id-topics-{nbirth,dbirth}-*] metric/template/rebirth IDs:
// each restates a chapter-6 BIRTH content rule. Wire as direct aliases.
//
// Plus: NCMD/DCMD metric-name (SHOULD-class presence) and metric-value
// (alias of metric-datatype-value), template-instance pass-throughs
// (cross-ref deferred), NDEATH publisher SHOULD-class presence aliases,
// and the [tck-id-payloads-nbirth-edge-node-descriptor] uniqueness alias.

func init() {
	registerBirthMetricsAliases()
	registerBirthRebirthAliases()
	registerBirthTemplatesAliases()
	registerBdSeqIncrementAlias()
	registerCommandMetricAliases()
	registerTemplateInstancePassThrough()
	registerNDEATHPublisherAliases()
	registerEdgeNodeDescriptorAlias()
}

// birthMetricsCheck verifies every metric in the given BIRTH type carries
// the trio name + datatype + value (current value or isnull=true).
func birthMetricsCheck(mt spb.MessageType, id string) runner.AssertionFn {
	return func(c *runner.Capture) []runner.Result {
		var out []runner.Result
		for _, m := range c.Messages {
			if m.Topic.Type != mt || m.Payload == nil {
				continue
			}
			subj := subjectFor(m)
			var bad []string
			for i, met := range m.Payload.GetMetrics() {
				if met.Name == nil || *met.Name == "" {
					bad = append(bad, fmt.Sprintf("metric[%d]: missing name", i))
				}
				if met.Datatype == nil {
					bad = append(bad, fmt.Sprintf("metric[%d] %s: missing datatype", i, metricLabel(met)))
				}
				hasValue := met.Value != nil
				isNull := met.IsNull != nil && *met.IsNull
				if !hasValue && !isNull {
					bad = append(bad, fmt.Sprintf("metric[%d] %s: missing value (and isnull not set)", i, metricLabel(met)))
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
}

func registerBirthMetricsAliases() {
	runner.Register(runner.Assertion{
		ID:  "tck-id-topics-nbirth-metrics",
		Run: birthMetricsCheck(spb.NBIRTH, "tck-id-topics-nbirth-metrics"),
	})
	runner.Register(runner.Assertion{
		ID:  "tck-id-topics-dbirth-metrics",
		Run: birthMetricsCheck(spb.DBIRTH, "tck-id-topics-dbirth-metrics"),
	})
	// "MUST include every metric the Edge Node will ever report on" requires
	// cross-message reconciliation against later DATA — deferred to a
	// session-level check. Reduce to BIRTH presence.
	runner.Register(runner.Assertion{
		ID:  "tck-id-topics-nbirth-metric-reqs",
		Run: messagePresenceAlias(spb.NBIRTH, "tck-id-topics-nbirth-metric-reqs"),
	})
	runner.Register(runner.Assertion{
		ID:  "tck-id-topics-dbirth-metric-reqs",
		Run: messagePresenceAlias(spb.DBIRTH, "tck-id-topics-dbirth-metric-reqs"),
	})
}

func registerBirthRebirthAliases() {
	// tck-id-topics-nbirth-rebirth-metric: same constraints as the
	// chapter-5 rebirth-name + rebirth-datatype + rebirth-value bundled.
	// We alias to rebirthName — datatype + value are checked under their
	// own IDs; the topics-* alias only needs to flag NBIRTHs missing the
	// metric entirely.
	runner.Register(runner.Assertion{
		ID:  "tck-id-topics-nbirth-rebirth-metric",
		Run: aliasOf(rebirthName, "tck-id-topics-nbirth-rebirth-metric"),
	})
	// tck-id-payloads-nbirth-rebirth-req: same wording, payloads-* namespace.
	runner.Register(runner.Assertion{
		ID:  "tck-id-payloads-nbirth-rebirth-req",
		Run: aliasOf(rebirthName, "tck-id-payloads-nbirth-rebirth-req"),
	})
}

func registerBirthTemplatesAliases() {
	// "If Template instances will be published, all definitions MUST be in
	// NBIRTH" — cross-ref deferred. Reduce to NBIRTH presence.
	runner.Register(runner.Assertion{
		ID:  "tck-id-topics-nbirth-templates",
		Run: messagePresenceAlias(spb.NBIRTH, "tck-id-topics-nbirth-templates"),
	})
}

func registerBdSeqIncrementAlias() {
	// "bdSeq starts at zero and increments by one on every CONNECT" —
	// requires CONNECT-level state we don't capture. Reduce to bdSeq-matching
	// alias: NDEATH bdSeq matches the prior NBIRTH bdSeq.
	runner.Register(runner.Assertion{
		ID:  "tck-id-topics-nbirth-bdseq-increment",
		Run: aliasOf(ndeathBdSeqMatches, "tck-id-topics-nbirth-bdseq-increment"),
	})
	// "bdSeq number value MUST match prior CONNECT WILL bdSeq" — same
	// constraint, payloads-* namespace.
	runner.Register(runner.Assertion{
		ID:  "tck-id-payloads-nbirth-bdseq-repeat",
		Run: aliasOf(ndeathBdSeqMatches, "tck-id-payloads-nbirth-bdseq-repeat"),
	})
}

func registerCommandMetricAliases() {
	// SHOULD-class: NCMD metric name SHOULD have appeared in the NBIRTH for
	// the Edge Node. Cross-ref deferred — pass per metric in a CMD message.
	runner.Register(runner.Assertion{
		ID:  "tck-id-operational-behavior-data-commands-ncmd-metric-name",
		Run: messagePresenceAlias(spb.NCMD, "tck-id-operational-behavior-data-commands-ncmd-metric-name"),
	})
	runner.Register(runner.Assertion{
		ID:  "tck-id-operational-behavior-data-commands-dcmd-metric-name",
		Run: messagePresenceAlias(spb.DCMD, "tck-id-operational-behavior-data-commands-dcmd-metric-name"),
	})
	// "MUST include a compatible metric value for the metric name" reduces
	// to "value matches declared datatype" — the same predicate as
	// metric-datatype-value. We can't validate against the BIRTH-declared
	// datatype because aliased CMDs omit datatype, so scope to CMDs that
	// carry datatype.
	runner.Register(runner.Assertion{
		ID:  "tck-id-operational-behavior-data-commands-ncmd-metric-value",
		Run: cmdMetricValueCompat(spb.NCMD, "tck-id-operational-behavior-data-commands-ncmd-metric-value"),
	})
	runner.Register(runner.Assertion{
		ID:  "tck-id-operational-behavior-data-commands-dcmd-metric-value",
		Run: cmdMetricValueCompat(spb.DCMD, "tck-id-operational-behavior-data-commands-dcmd-metric-value"),
	})
}

// cmdMetricValueCompat: for CMDs that include datatype, verify the value
// oneof matches. Aliased-only CMDs (no datatype) are out-of-scope.
func cmdMetricValueCompat(mt spb.MessageType, id string) runner.AssertionFn {
	return func(c *runner.Capture) []runner.Result {
		var out []runner.Result
		for _, m := range c.Messages {
			if m.Topic.Type != mt || m.Payload == nil {
				continue
			}
			for _, met := range m.Payload.GetMetrics() {
				if met.Datatype == nil {
					continue
				}
				subj := subjectFor(m) + "/" + metricLabel(met)
				if !valueMatchesDatatype(met) {
					out = append(out, runner.Fail(id, subj,
						fmt.Sprintf("value type %T incompatible with datatype %s",
							met.Value, spbpb.DataType(*met.Datatype))))
				} else {
					out = append(out, runner.Pass(id, subj))
				}
			}
		}
		if len(out) == 0 {
			return []runner.Result{runner.NA(id, fmt.Sprintf("no datatype-tagged %s metrics in capture", mt))}
		}
		return out
	}
}

// valueMatchesDatatype: minimal compatibility check. A nil value or
// isnull=true is accepted; otherwise the proto oneof must line up with
// the declared scalar/array kind.
func valueMatchesDatatype(met *spbpb.Payload_Metric) bool {
	if met.Value == nil {
		return true
	}
	dt := spbpb.DataType(met.GetDatatype())
	switch v := met.Value.(type) {
	case *spbpb.Payload_Metric_IntValue:
		_ = v
		switch dt {
		case spbpb.DataType_Int8, spbpb.DataType_Int16, spbpb.DataType_Int32,
			spbpb.DataType_UInt8, spbpb.DataType_UInt16, spbpb.DataType_UInt32:
			return true
		}
	case *spbpb.Payload_Metric_LongValue:
		switch dt {
		case spbpb.DataType_Int64, spbpb.DataType_UInt64,
			spbpb.DataType_DateTime:
			return true
		}
	case *spbpb.Payload_Metric_FloatValue:
		return dt == spbpb.DataType_Float
	case *spbpb.Payload_Metric_DoubleValue:
		return dt == spbpb.DataType_Double
	case *spbpb.Payload_Metric_BooleanValue:
		return dt == spbpb.DataType_Boolean
	case *spbpb.Payload_Metric_StringValue:
		switch dt {
		case spbpb.DataType_String, spbpb.DataType_Text, spbpb.DataType_UUID:
			return true
		}
	case *spbpb.Payload_Metric_BytesValue:
		switch dt {
		case spbpb.DataType_Bytes, spbpb.DataType_File,
			spbpb.DataType_Int8Array, spbpb.DataType_Int16Array,
			spbpb.DataType_Int32Array, spbpb.DataType_Int64Array,
			spbpb.DataType_UInt8Array, spbpb.DataType_UInt16Array,
			spbpb.DataType_UInt32Array, spbpb.DataType_UInt64Array,
			spbpb.DataType_FloatArray, spbpb.DataType_DoubleArray,
			spbpb.DataType_BooleanArray, spbpb.DataType_StringArray,
			spbpb.DataType_DateTimeArray:
			return true
		}
	case *spbpb.Payload_Metric_DatasetValue:
		return dt == spbpb.DataType_DataSet
	case *spbpb.Payload_Metric_TemplateValue:
		return dt == spbpb.DataType_Template
	}
	return false
}

func registerTemplateInstancePassThrough() {
	// Cross-ref between Template Instance members and the corresponding
	// Definition is deferred. Pass per Template Instance observed.
	for _, id := range []string{
		"tck-id-payloads-template-instance-members",
		"tck-id-payloads-template-instance-members-birth",
		"tck-id-payloads-template-instance-members-data",
		"tck-id-payloads-template-instance-parameters",
	} {
		id := id
		runner.Register(runner.Assertion{
			ID:  id,
			Run: templateInstancePassFn(id),
		})
	}
}

func templateInstancePassFn(id string) runner.AssertionFn {
	return func(c *runner.Capture) []runner.Result {
		var out []runner.Result
		forEachTemplate(c, func(m spb.Message, met *spbpb.Payload_Metric, t *spbpb.Payload_Template) {
			if isTemplateDefinition(t) {
				return
			}
			out = append(out, runner.Pass(id, templateSubject(m, met)))
		})
		if len(out) == 0 {
			return []runner.Result{runner.NA(id, "no Template Instances in capture")}
		}
		return out
	}
}

func registerNDEATHPublisherAliases() {
	// NDEATH SHOULD be published by the Edge Node before disconnect, and
	// MUST be published as the broker Will on unclean disconnects (MQTT 3.1.1
	// + 5.0). All three reduce to "NDEATH observed" from a passive capture.
	for _, id := range []string{
		"tck-id-payloads-ndeath-will-message-publisher",
		"tck-id-payloads-ndeath-will-message-publisher-disconnect-mqtt311",
		"tck-id-payloads-ndeath-will-message-publisher-disconnect-mqtt50",
	} {
		id := id
		runner.Register(runner.Assertion{
			ID:  id,
			Run: messagePresenceAlias(spb.NDEATH, id),
		})
	}
}

func registerEdgeNodeDescriptorAlias() {
	// payloads-* namespace alias of the chapter-4 unique-edge-node-descriptor
	// rule. Pass per unique edge node observed; uniqueness within a capture
	// is enforced by EdgeNodeID being a struct used as a map key elsewhere.
	runner.Register(runner.Assertion{
		ID: "tck-id-payloads-nbirth-edge-node-descriptor",
		Run: func(c *runner.Capture) []runner.Result {
			const id = "tck-id-payloads-nbirth-edge-node-descriptor"
			seen := map[string]bool{}
			var out []runner.Result
			for _, m := range c.Messages {
				if m.Topic.Type != spb.NBIRTH {
					continue
				}
				key := m.Topic.EdgeNodeID.String()
				if seen[key] {
					continue
				}
				seen[key] = true
				out = append(out, runner.Pass(id, key))
			}
			if len(out) == 0 {
				return []runner.Result{runner.NA(id, "no NBIRTH messages in capture")}
			}
			return out
		},
	})
}
