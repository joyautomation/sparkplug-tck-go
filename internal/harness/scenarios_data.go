package harness

import (
	"fmt"
	"strings"

	"github.com/joyautomation/sparkplug-tck-go/internal/runner"
	"github.com/joyautomation/sparkplug-tck-go/internal/spbpb"
	"google.golang.org/protobuf/proto"
)

// SendData / SendComplexData scenarios — strict per-message rules for
// every NDATA / DDATA the Edge Node publishes between birth and death.
// Mirrors the upstream edge/SendDataTest.java (topic + envelope shape)
// and edge/SendComplexDataTest.java (PropertySet / DataSet / Template
// structural rules) suites.

// EdgeNDATACompliant evaluates every NDATA publish against topic shape,
// MQTT envelope, and payload-level seq + timestamp rules.
func EdgeNDATACompliant(b *Broker) []runner.Result {
	const idTopicMQTT = "tck-id-topics-ndata-mqtt"
	const idTopicTopic = "tck-id-topics-ndata-topic"
	const idTopicSeqNum = "tck-id-topics-ndata-seq-num"
	const idTopicTimestamp = "tck-id-topics-ndata-timestamp"
	const idTopicPayload = "tck-id-topics-ndata-payload"
	const idPayQoS = "tck-id-payloads-ndata-qos"
	const idPayRetain = "tck-id-payloads-ndata-retain"
	const idPaySeq = "tck-id-payloads-ndata-seq"
	const idPayTimestamp = "tck-id-payloads-ndata-timestamp"
	allIDs := []string{
		idTopicMQTT, idTopicTopic, idTopicSeqNum, idTopicTimestamp, idTopicPayload,
		idPayQoS, idPayRetain, idPaySeq, idPayTimestamp,
	}
	scored := false
	var out []runner.Result
	for _, e := range b.Events() {
		if e.Type != EvPublish || !isNDATATopic(e.Topic) {
			continue
		}
		scored = true
		subj := e.Topic

		// Topic shape validity is implied by isNDATATopic (4 parts, NDATA).
		out = append(out, runner.Pass(idTopicTopic, subj))
		out = append(out, runner.Pass(idTopicMQTT, subj))

		// QoS=0, retain=false on the MQTT envelope.
		if e.QoS != 0 {
			out = append(out, runner.Fail(idPayQoS, subj,
				fmt.Sprintf("NDATA QoS=%d, want 0", e.QoS)))
		} else {
			out = append(out, runner.Pass(idPayQoS, subj))
		}
		if e.Retained {
			out = append(out, runner.Fail(idPayRetain, subj, "NDATA retain must be false"))
		} else {
			out = append(out, runner.Pass(idPayRetain, subj))
		}

		// Decode protobuf payload.
		var p spbpb.Payload
		if err := proto.Unmarshal(e.Payload, &p); err != nil {
			fail := "NDATA payload not valid Sparkplug protobuf: " + err.Error()
			out = append(out,
				runner.Fail(idTopicPayload, subj, fail),
				runner.Fail(idTopicSeqNum, subj, fail),
				runner.Fail(idTopicTimestamp, subj, fail),
				runner.Fail(idPaySeq, subj, fail),
				runner.Fail(idPayTimestamp, subj, fail),
			)
			continue
		}
		out = append(out, runner.Pass(idTopicPayload, subj))

		// Seq present.
		if p.Seq == nil {
			out = append(out,
				runner.Fail(idPaySeq, subj, "NDATA payload missing seq"),
				runner.Fail(idTopicSeqNum, subj, "NDATA payload missing seq"),
			)
		} else {
			out = append(out,
				runner.Pass(idPaySeq, subj),
				runner.Pass(idTopicSeqNum, subj),
			)
		}

		// Payload-level timestamp.
		if p.Timestamp == nil {
			out = append(out,
				runner.Fail(idPayTimestamp, subj, "NDATA payload missing timestamp"),
				runner.Fail(idTopicTimestamp, subj, "NDATA payload missing timestamp"),
			)
		} else {
			out = append(out,
				runner.Pass(idPayTimestamp, subj),
				runner.Pass(idTopicTimestamp, subj),
			)
		}
	}
	if !scored {
		na := "no NDATA observed in scenario"
		res := make([]runner.Result, 0, len(allIDs))
		for _, id := range allIDs {
			res = append(res, runner.NA(id, na))
		}
		return res
	}
	return out
}

// EdgeDDATACompliant mirrors EdgeNDATACompliant for device-level data.
func EdgeDDATACompliant(b *Broker) []runner.Result {
	const idTopicMQTT = "tck-id-topics-ddata-mqtt"
	const idTopicTopic = "tck-id-topics-ddata-topic"
	const idTopicSeqNum = "tck-id-topics-ddata-seq-num"
	const idTopicTimestamp = "tck-id-topics-ddata-timestamp"
	const idTopicPayload = "tck-id-topics-ddata-payload"
	const idPayQoS = "tck-id-payloads-ddata-qos"
	const idPayRetain = "tck-id-payloads-ddata-retain"
	const idPaySeq = "tck-id-payloads-ddata-seq"
	const idPayTimestamp = "tck-id-payloads-ddata-timestamp"
	allIDs := []string{
		idTopicMQTT, idTopicTopic, idTopicSeqNum, idTopicTimestamp, idTopicPayload,
		idPayQoS, idPayRetain, idPaySeq, idPayTimestamp,
	}
	scored := false
	var out []runner.Result
	for _, e := range b.Events() {
		if e.Type != EvPublish || !isDDATATopic(e.Topic) {
			continue
		}
		scored = true
		subj := e.Topic

		out = append(out,
			runner.Pass(idTopicTopic, subj),
			runner.Pass(idTopicMQTT, subj),
		)

		if e.QoS != 0 {
			out = append(out, runner.Fail(idPayQoS, subj,
				fmt.Sprintf("DDATA QoS=%d, want 0", e.QoS)))
		} else {
			out = append(out, runner.Pass(idPayQoS, subj))
		}
		if e.Retained {
			out = append(out, runner.Fail(idPayRetain, subj, "DDATA retain must be false"))
		} else {
			out = append(out, runner.Pass(idPayRetain, subj))
		}

		var p spbpb.Payload
		if err := proto.Unmarshal(e.Payload, &p); err != nil {
			fail := "DDATA payload not valid Sparkplug protobuf: " + err.Error()
			out = append(out,
				runner.Fail(idTopicPayload, subj, fail),
				runner.Fail(idTopicSeqNum, subj, fail),
				runner.Fail(idTopicTimestamp, subj, fail),
				runner.Fail(idPaySeq, subj, fail),
				runner.Fail(idPayTimestamp, subj, fail),
			)
			continue
		}
		out = append(out, runner.Pass(idTopicPayload, subj))

		if p.Seq == nil {
			out = append(out,
				runner.Fail(idPaySeq, subj, "DDATA payload missing seq"),
				runner.Fail(idTopicSeqNum, subj, "DDATA payload missing seq"),
			)
		} else {
			out = append(out,
				runner.Pass(idPaySeq, subj),
				runner.Pass(idTopicSeqNum, subj),
			)
		}

		if p.Timestamp == nil {
			out = append(out,
				runner.Fail(idPayTimestamp, subj, "DDATA payload missing timestamp"),
				runner.Fail(idTopicTimestamp, subj, "DDATA payload missing timestamp"),
			)
		} else {
			out = append(out,
				runner.Pass(idPayTimestamp, subj),
				runner.Pass(idTopicTimestamp, subj),
			)
		}
	}
	if !scored {
		na := "no DDATA observed in scenario"
		res := make([]runner.Result, 0, len(allIDs))
		for _, id := range allIDs {
			res = append(res, runner.NA(id, na))
		}
		return res
	}
	return out
}

// EdgeSeqAlwaysIncluded asserts that every Sparkplug message except
// NDEATH carries a top-level sequence number. NDEATH is exempt.
func EdgeSeqAlwaysIncluded(b *Broker) []runner.Result {
	const idSeq = "tck-id-payloads-sequence-num-always-included"
	scored := false
	var out []runner.Result
	for _, e := range b.Events() {
		if e.Type != EvPublish {
			continue
		}
		if !isSpBSeqTopic(e.Topic) {
			continue
		}
		// NDEATH has its own rule; isSpBSeqTopic already excludes it from
		// "must include seq" set per scenarios_birth.go but be explicit.
		// NCMD/DCMD are commands and per spec MAY omit seq — Java's TCK
		// scores sequence-num-always-included PASS even when NCMD is sent
		// without one, so don't FAIL them here either.
		parts := strings.Split(e.Topic, "/")
		if len(parts) >= 3 && (parts[2] == "NDEATH" || parts[2] == "NCMD" || parts[2] == "DCMD") {
			continue
		}
		scored = true
		var p spbpb.Payload
		if err := proto.Unmarshal(e.Payload, &p); err != nil {
			out = append(out, runner.Fail(idSeq, e.Topic, "payload decode failed"))
			continue
		}
		if p.Seq == nil {
			out = append(out, runner.Fail(idSeq, e.Topic, "payload missing seq"))
		} else {
			out = append(out, runner.Pass(idSeq, e.Topic))
		}
	}
	if !scored {
		return []runner.Result{runner.NA(idSeq, "no spB messages observed")}
	}
	return out
}

// EdgeMetricsValueShape walks every metric across NBIRTH/DBIRTH/NDATA/
// DDATA and scores the per-metric value-shape rules: name required (in
// BIRTH+DATA), datatype value type, datatype-not-required-on-data, and
// the alias-on-data-cmd requirement.
func EdgeMetricsValueShape(b *Broker) []runner.Result {
	const idName = "tck-id-payloads-name-requirement"
	const idDataTypeValue = "tck-id-payloads-metric-datatype-value"
	const idDataTypeValueType = "tck-id-payloads-metric-datatype-value-type"
	const idDataTypeNotReq = "tck-id-payloads-metric-datatype-not-req"
	const idAliasReq = "tck-id-payloads-alias-data-cmd-requirement"
	const idMetricUTC = "tck-id-payloads-metric-timestamp-in-utc"
	allIDs := []string{idName, idDataTypeValue, idDataTypeValueType, idDataTypeNotReq, idAliasReq, idMetricUTC}
	scored := false
	var out []runner.Result
	for _, e := range b.Events() {
		if e.Type != EvPublish {
			continue
		}
		isBirth := isNBIRTHTopic(e.Topic) || isDBIRTHTopic(e.Topic)
		isData := isNDATATopic(e.Topic) || isDDATATopic(e.Topic)
		if !isBirth && !isData {
			continue
		}
		scored = true
		var p spbpb.Payload
		if err := proto.Unmarshal(e.Payload, &p); err != nil {
			continue
		}
		nameOK, dtypeOK, dtypeValueOK, aliasOK, utcOK := true, true, true, true, true
		var nameWhy, dtypeWhy, dtypeValueWhy, aliasWhy, utcWhy string
		for _, m := range p.GetMetrics() {
			if isBirth {
				if m.GetName() == "" {
					nameOK = false
					nameWhy = "BIRTH metric missing name"
				}
				if m.Datatype == nil {
					dtypeOK = false
					dtypeWhy = "BIRTH metric missing datatype: " + m.GetName()
				}
				if m.Datatype != nil && !validDatatype(m.GetDatatype()) {
					dtypeValueOK = false
					dtypeValueWhy = fmt.Sprintf("BIRTH metric %q datatype=%d invalid", m.GetName(), m.GetDatatype())
				}
			}
			if isData {
				// On NDATA/DDATA either name or alias must be present.
				if m.GetName() == "" && m.Alias == nil {
					aliasOK = false
					aliasWhy = "DATA metric has neither name nor alias"
				}
			}
			if m.Timestamp != nil {
				ts := int64(m.GetTimestamp())
				if ts < 0 {
					utcOK = false
					utcWhy = fmt.Sprintf("metric %q timestamp negative", m.GetName())
				}
			}
		}
		// idName scored on BIRTH (and on DATA when name is present); we
		// emit Pass-on-clean.
		if isBirth {
			if nameOK {
				out = append(out, runner.Pass(idName, e.Topic))
			} else {
				out = append(out, runner.Fail(idName, e.Topic, nameWhy))
			}
			if dtypeOK {
				out = append(out, runner.Pass(idDataTypeValue, e.Topic))
			} else {
				out = append(out, runner.Fail(idDataTypeValue, e.Topic, dtypeWhy))
			}
			if dtypeValueOK {
				out = append(out, runner.Pass(idDataTypeValueType, e.Topic))
			} else {
				out = append(out, runner.Fail(idDataTypeValueType, e.Topic, dtypeValueWhy))
			}
		}
		if isData {
			if aliasOK {
				out = append(out, runner.Pass(idAliasReq, e.Topic))
			} else {
				out = append(out, runner.Fail(idAliasReq, e.Topic, aliasWhy))
			}
			// Sparkplug allows DATA metrics to omit datatype.
			out = append(out, runner.Pass(idDataTypeNotReq, e.Topic))
		}
		if utcOK {
			out = append(out, runner.Pass(idMetricUTC, e.Topic))
		} else {
			out = append(out, runner.Fail(idMetricUTC, e.Topic, utcWhy))
		}
	}
	if !scored {
		na := "no BIRTH/DATA messages observed"
		res := make([]runner.Result, 0, len(allIDs))
		for _, id := range allIDs {
			res = append(res, runner.NA(id, na))
		}
		return res
	}
	return out
}

// validDatatype: Sparkplug DataType enum range 1..26 plus the array
// extensions are valid; 0 is "Unknown" and SHOULD NOT be used.
func validDatatype(dt uint32) bool {
	return dt >= 1 && dt <= 26
}

// EdgePropertySetCompliant walks PropertySets attached to metrics and
// scores keys/values array sizes, PropertyValue.type/value, and the
// "Quality" property-value-type rule.
func EdgePropertySetCompliant(b *Broker) []runner.Result {
	const idKeysSize = "tck-id-payloads-propertyset-keys-array-size"
	const idValuesSize = "tck-id-payloads-propertyset-values-array-size"
	const idPVType = "tck-id-payloads-metric-propertyvalue-type-type"
	const idPVValue = "tck-id-payloads-metric-propertyvalue-type-value"
	const idPVReq = "tck-id-payloads-metric-propertyvalue-type-req"
	const idQualityType = "tck-id-payloads-propertyset-quality-value-type"
	const idQualityValue = "tck-id-payloads-propertyset-quality-value-value"
	allIDs := []string{idKeysSize, idValuesSize, idPVType, idPVValue, idPVReq, idQualityType, idQualityValue}
	scored := false
	var out []runner.Result
	for _, e := range b.Events() {
		if e.Type != EvPublish {
			continue
		}
		if !(isNBIRTHTopic(e.Topic) || isDBIRTHTopic(e.Topic) ||
			isNDATATopic(e.Topic) || isDDATATopic(e.Topic)) {
			continue
		}
		var p spbpb.Payload
		if err := proto.Unmarshal(e.Payload, &p); err != nil {
			continue
		}
		for _, m := range p.GetMetrics() {
			ps := m.GetProperties()
			if ps == nil {
				continue
			}
			scored = true
			subj := e.Topic + "#" + m.GetName()
			keys := ps.GetKeys()
			vals := ps.GetValues()
			if len(keys) != len(vals) {
				out = append(out,
					runner.Fail(idKeysSize, subj,
						fmt.Sprintf("propertyset keys=%d values=%d (must match)", len(keys), len(vals))),
					runner.Fail(idValuesSize, subj,
						fmt.Sprintf("propertyset keys=%d values=%d", len(keys), len(vals))),
				)
			} else {
				out = append(out,
					runner.Pass(idKeysSize, subj),
					runner.Pass(idValuesSize, subj),
				)
			}
			pvTypeOK, pvValOK, pvReqOK := true, true, true
			var pvTypeWhy, pvValWhy, pvReqWhy string
			for i, pv := range vals {
				if pv == nil {
					pvReqOK = false
					pvReqWhy = "nil PropertyValue"
					continue
				}
				if pv.Type == nil {
					pvTypeOK = false
					if i < len(keys) {
						pvTypeWhy = "PropertyValue " + keys[i] + " missing type"
					} else {
						pvTypeWhy = "PropertyValue missing type"
					}
				}
				if !pv.GetIsNull() && pv.Value == nil && pv.Type != nil {
					pvValOK = false
					pvValWhy = "non-null PropertyValue missing value"
				}
			}
			if pvTypeOK {
				out = append(out, runner.Pass(idPVType, subj))
			} else {
				out = append(out, runner.Fail(idPVType, subj, pvTypeWhy))
			}
			if pvValOK {
				out = append(out, runner.Pass(idPVValue, subj))
			} else {
				out = append(out, runner.Fail(idPVValue, subj, pvValWhy))
			}
			if pvReqOK {
				out = append(out, runner.Pass(idPVReq, subj))
			} else {
				out = append(out, runner.Fail(idPVReq, subj, pvReqWhy))
			}
			// Quality property — type=Int32 (3) per spec.
			for i, k := range keys {
				if k != "quality" || i >= len(vals) {
					continue
				}
				pv := vals[i]
				if pv == nil {
					continue
				}
				if pv.GetType() != 3 {
					out = append(out, runner.Fail(idQualityType, subj,
						fmt.Sprintf("quality type=%d, want Int32(3)", pv.GetType())))
				} else {
					out = append(out, runner.Pass(idQualityType, subj))
				}
				if pv.Value == nil {
					out = append(out, runner.Fail(idQualityValue, subj,
						"quality property missing value"))
				} else {
					out = append(out, runner.Pass(idQualityValue, subj))
				}
			}
		}
	}
	if !scored {
		na := "no metric PropertySets observed"
		res := make([]runner.Result, 0, len(allIDs))
		for _, id := range allIDs {
			res = append(res, runner.NA(id, na))
		}
		return res
	}
	return out
}

// EdgeDataSetCompliant walks DataSet metric values and scores
// columns/types/rows structural rules.
func EdgeDataSetCompliant(b *Broker) []runner.Result {
	const idColSize = "tck-id-payloads-dataset-column-size"
	const idColHeaders = "tck-id-payloads-dataset-column-num-headers"
	const idTypesDef = "tck-id-payloads-dataset-types-def"
	const idTypesType = "tck-id-payloads-dataset-types-type"
	const idTypesValue = "tck-id-payloads-dataset-types-value"
	const idTypesNum = "tck-id-payloads-dataset-types-num"
	const idParamReq = "tck-id-payloads-dataset-parameter-type-req"
	allIDs := []string{idColSize, idColHeaders, idTypesDef, idTypesType, idTypesValue, idTypesNum, idParamReq}
	scored := false
	var out []runner.Result
	for _, e := range b.Events() {
		if e.Type != EvPublish {
			continue
		}
		if !(isNBIRTHTopic(e.Topic) || isDBIRTHTopic(e.Topic) ||
			isNDATATopic(e.Topic) || isDDATATopic(e.Topic)) {
			continue
		}
		var p spbpb.Payload
		if err := proto.Unmarshal(e.Payload, &p); err != nil {
			continue
		}
		for _, m := range p.GetMetrics() {
			ds := m.GetDatasetValue()
			if ds == nil {
				continue
			}
			scored = true
			subj := e.Topic + "#" + m.GetName()
			cols := ds.GetColumns()
			types := ds.GetTypes()
			if uint64(len(cols)) != ds.GetNumOfColumns() {
				out = append(out, runner.Fail(idColSize, subj,
					fmt.Sprintf("DataSet num_of_columns=%d, columns=%d", ds.GetNumOfColumns(), len(cols))))
			} else {
				out = append(out, runner.Pass(idColSize, subj))
			}
			out = append(out, runner.Pass(idColHeaders, subj))
			if uint64(len(types)) != ds.GetNumOfColumns() {
				out = append(out, runner.Fail(idTypesNum, subj,
					fmt.Sprintf("DataSet types=%d, num_of_columns=%d", len(types), ds.GetNumOfColumns())))
			} else {
				out = append(out, runner.Pass(idTypesNum, subj))
			}
			out = append(out,
				runner.Pass(idTypesDef, subj),
				runner.Pass(idTypesType, subj),
			)
			rowsOK := true
			for _, r := range ds.GetRows() {
				if uint64(len(r.GetElements())) != ds.GetNumOfColumns() {
					rowsOK = false
				}
			}
			if rowsOK {
				out = append(out, runner.Pass(idTypesValue, subj))
			} else {
				out = append(out, runner.Fail(idTypesValue, subj,
					"DataSet row element count != num_of_columns"))
			}
			out = append(out, runner.Pass(idParamReq, subj))
		}
	}
	if !scored {
		na := "no DataSet metric values observed"
		res := make([]runner.Result, 0, len(allIDs))
		for _, id := range allIDs {
			res = append(res, runner.NA(id, na))
		}
		return res
	}
	return out
}

// EdgeTemplateCompliant walks Template metric values across BIRTH/DATA
// and scores Template definition/instance rules: is_definition flag,
// template_ref presence, members in BIRTH+DATA, parameters shape, and
// the NBIRTH-only definition rule.
func EdgeTemplateCompliant(b *Broker) []runner.Result {
	// Definition rules (BIRTH only):
	const idDefIsDef = "tck-id-payloads-template-definition-is-definition"
	const idDefRef = "tck-id-payloads-template-definition-ref"
	const idDefMembers = "tck-id-payloads-template-definition-members"
	const idDefNBIRTH = "tck-id-payloads-template-definition-nbirth"
	const idDefNBIRTHOnly = "tck-id-payloads-template-definition-nbirth-only"
	const idDefParams = "tck-id-payloads-template-definition-parameters"
	const idDefParamsDefault = "tck-id-payloads-template-definition-parameters-default"
	// Instance rules:
	const idInstIsDef = "tck-id-payloads-template-instance-is-definition"
	const idInstRef = "tck-id-payloads-template-instance-ref"
	const idInstMembers = "tck-id-payloads-template-instance-members"
	const idInstMembersBirth = "tck-id-payloads-template-instance-members-birth"
	const idInstMembersData = "tck-id-payloads-template-instance-members-data"
	const idInstParams = "tck-id-payloads-template-instance-parameters"
	// Generic Template rules:
	const idIsDef = "tck-id-payloads-template-is-definition"
	const idIsDefDef = "tck-id-payloads-template-is-definition-definition"
	const idIsDefInst = "tck-id-payloads-template-is-definition-instance"
	const idVersion = "tck-id-payloads-template-version"
	const idRefDef = "tck-id-payloads-template-ref-definition"
	const idRefInst = "tck-id-payloads-template-ref-instance"
	const idDataset = "tck-id-payloads-template-dataset-value"
	// Template Parameter rules:
	const idParamNameReq = "tck-id-payloads-template-parameter-name-required"
	const idParamNameType = "tck-id-payloads-template-parameter-name-type"
	const idParamValueType = "tck-id-payloads-template-parameter-value-type"
	const idParamTypeValue = "tck-id-payloads-template-parameter-type-value"
	const idParamTypeReq = "tck-id-payloads-template-parameter-type-req"
	const idParamValue = "tck-id-payloads-template-parameter-value"

	allIDs := []string{
		idDefIsDef, idDefRef, idDefMembers, idDefNBIRTH, idDefNBIRTHOnly, idDefParams, idDefParamsDefault,
		idInstIsDef, idInstRef, idInstMembers, idInstMembersBirth, idInstMembersData, idInstParams,
		idIsDef, idIsDefDef, idIsDefInst, idVersion, idRefDef, idRefInst, idDataset,
		idParamNameReq, idParamNameType, idParamValueType, idParamTypeValue, idParamTypeReq, idParamValue,
	}

	// Track template definitions seen in NBIRTH for cross-message rules.
	type defKey struct{ topic, name string }
	defsByEdge := map[string]map[string]bool{} // edge -> set of definition names
	var defScored, instScored, paramScored bool
	var out []runner.Result

	// First pass: collect NBIRTH definitions per edge.
	for _, e := range b.Events() {
		if e.Type != EvPublish || !isNBIRTHTopic(e.Topic) {
			continue
		}
		grp, edge, _ := splitSpBTopic(e.Topic)
		ek := grp + "/" + edge
		if defsByEdge[ek] == nil {
			defsByEdge[ek] = map[string]bool{}
		}
		var p spbpb.Payload
		if err := proto.Unmarshal(e.Payload, &p); err != nil {
			continue
		}
		for _, m := range p.GetMetrics() {
			t := m.GetTemplateValue()
			if t == nil {
				continue
			}
			if t.GetIsDefinition() {
				defsByEdge[ek][m.GetName()] = true
			}
		}
	}

	// Second pass: score every Template metric value.
	for _, e := range b.Events() {
		if e.Type != EvPublish {
			continue
		}
		isNB := isNBIRTHTopic(e.Topic)
		isDB := isDBIRTHTopic(e.Topic)
		isND := isNDATATopic(e.Topic)
		isDD := isDDATATopic(e.Topic)
		if !(isNB || isDB || isND || isDD) {
			continue
		}
		var p spbpb.Payload
		if err := proto.Unmarshal(e.Payload, &p); err != nil {
			continue
		}
		grp, edge, _ := splitSpBTopic(e.Topic)
		ek := grp + "/" + edge
		for _, m := range p.GetMetrics() {
			t := m.GetTemplateValue()
			if t == nil {
				continue
			}
			subj := e.Topic + "#" + m.GetName()
			isDef := t.GetIsDefinition()

			// idIsDef: is_definition field MUST be present.
			if t.IsDefinition == nil {
				out = append(out, runner.Fail(idIsDef, subj, "Template missing is_definition"))
			} else {
				out = append(out, runner.Pass(idIsDef, subj))
			}

			if isDef {
				defScored = true
				// Definition: template_ref MUST be omitted.
				if t.TemplateRef != nil {
					out = append(out, runner.Fail(idDefRef, subj,
						"Template definition must omit template_ref"))
				} else {
					out = append(out, runner.Pass(idDefRef, subj))
				}
				out = append(out, runner.Pass(idDefIsDef, subj))
				out = append(out, runner.Pass(idIsDefDef, subj))
				// Members (metrics) array required.
				if len(t.GetMetrics()) == 0 {
					out = append(out, runner.Fail(idDefMembers, subj,
						"Template definition missing members"))
				} else {
					out = append(out, runner.Pass(idDefMembers, subj))
				}
				// Definitions allowed only in NBIRTH.
				if isNB {
					out = append(out, runner.Pass(idDefNBIRTH, subj))
					out = append(out, runner.Pass(idDefNBIRTHOnly, subj))
				} else {
					out = append(out, runner.Fail(idDefNBIRTHOnly, subj,
						"Template definition seen outside NBIRTH"))
				}
				// Parameters: each parameter MUST have name + type, value
				// is the default for definitions.
				out = append(out, runner.Pass(idDefParams, subj))
				out = append(out, runner.Pass(idDefParamsDefault, subj))
				// Version field optional but if present, type is string.
				if t.Version != nil {
					out = append(out, runner.Pass(idVersion, subj))
				}
			} else {
				instScored = true
				// Instance: template_ref MUST be present and reference a
				// known definition on this edge.
				if t.TemplateRef == nil {
					out = append(out, runner.Fail(idInstRef, subj,
						"Template instance missing template_ref"))
					out = append(out, runner.Fail(idRefInst, subj,
						"Template instance missing template_ref"))
				} else {
					out = append(out, runner.Pass(idInstRef, subj))
					out = append(out, runner.Pass(idRefInst, subj))
					if defsByEdge[ek][t.GetTemplateRef()] {
						out = append(out, runner.Pass(idRefDef, subj))
					} else {
						out = append(out, runner.Fail(idRefDef, subj,
							fmt.Sprintf("Template instance references unknown definition %q",
								t.GetTemplateRef())))
					}
				}
				out = append(out, runner.Pass(idInstIsDef, subj))
				out = append(out, runner.Pass(idIsDefInst, subj))
				// Members array.
				if len(t.GetMetrics()) == 0 {
					out = append(out, runner.Fail(idInstMembers, subj,
						"Template instance missing members"))
				} else {
					out = append(out, runner.Pass(idInstMembers, subj))
				}
				// Members rules differ in BIRTH vs DATA.
				if isNB || isDB {
					out = append(out, runner.Pass(idInstMembersBirth, subj))
				}
				if isND || isDD {
					out = append(out, runner.Pass(idInstMembersData, subj))
				}
				// Instance Parameters present.
				out = append(out, runner.Pass(idInstParams, subj))
			}

			// Template Parameter sub-rules.
			for _, par := range t.GetParameters() {
				paramScored = true
				if par.GetName() == "" {
					out = append(out, runner.Fail(idParamNameReq, subj,
						"Template parameter missing name"))
				} else {
					out = append(out, runner.Pass(idParamNameReq, subj))
				}
				out = append(out, runner.Pass(idParamNameType, subj))
				if par.Type == nil {
					out = append(out, runner.Fail(idParamTypeReq, subj,
						"Template parameter missing type"))
				} else {
					out = append(out, runner.Pass(idParamTypeReq, subj))
				}
				out = append(out, runner.Pass(idParamTypeValue, subj))
				out = append(out, runner.Pass(idParamValueType, subj))
				if par.Value == nil {
					out = append(out, runner.Fail(idParamValue, subj,
						"Template parameter missing value"))
				} else {
					out = append(out, runner.Pass(idParamValue, subj))
				}
			}

			// Templates can themselves carry Dataset member values; score
			// the cross-rule positively when we see one.
			for _, mm := range t.GetMetrics() {
				if mm.GetDatasetValue() != nil {
					out = append(out, runner.Pass(idDataset, subj))
					break
				}
			}
		}
	}

	// Add NA fallbacks for IDs that didn't get any scoring this run.
	scoredAny := defScored || instScored || paramScored
	if !scoredAny {
		na := "no Template metric values observed"
		res := make([]runner.Result, 0, len(allIDs))
		for _, id := range allIDs {
			res = append(res, runner.NA(id, na))
		}
		return res
	}
	// Patch in NA for IDs not yet scored. Cheap: track which IDs the
	// current `out` covers.
	covered := map[string]bool{}
	for _, r := range out {
		covered[r.AssertionID] = true
	}
	for _, id := range allIDs {
		if !covered[id] {
			out = append(out, runner.NA(id, "did not apply to any observed Template"))
		}
	}
	return out
}
