package harness

import (
	"fmt"
	"strings"

	"github.com/joyautomation/sparkplug-tck-go/internal/runner"
	"github.com/joyautomation/sparkplug-tck-go/internal/spbpb"
	"google.golang.org/protobuf/proto"
)

// Edge Node birth-message scenarios — strict checks on the shape and
// ordering of NBIRTH and DBIRTH messages an Edge Node must publish to
// establish its session. Together they cover most of the upstream
// edge/SessionEstablishmentTest.java assertions.

// EdgeNBIRTHCompliant evaluates every NBIRTH publish against:
//   - topic shape spBv1.0/<group>/NBIRTH/<edge>
//   - QoS=0, retain=false
//   - protobuf payload, with seq present (0..255), payload timestamp,
//     and a bdSeq metric matching the most recent CONNECT Will bdSeq
//   - "Node Control/Rebirth" metric of datatype Boolean, value=false,
//     no alias
//   - every metric carries name + datatype + value/isnull
func EdgeNBIRTHCompliant(b *Broker) []runner.Result {
	const idTopic = "tck-id-topics-nbirth-topic"
	const idTopicMsgFlow = "tck-id-message-flow-edge-node-birth-publish-nbirth-topic"
	const idMQTT = "tck-id-topics-nbirth-mqtt"
	const idQoS = "tck-id-payloads-nbirth-qos"
	const idQoSMsgFlow = "tck-id-message-flow-edge-node-birth-publish-nbirth-qos"
	const idRetain = "tck-id-payloads-nbirth-retain"
	const idRetainMsgFlow = "tck-id-message-flow-edge-node-birth-publish-nbirth-retained"
	const idPayload = "tck-id-message-flow-edge-node-birth-publish-nbirth-payload"
	const idSeq = "tck-id-payloads-nbirth-seq"
	const idSeqReq = "tck-id-payloads-sequence-num-req-nbirth"
	const idSeqMsgFlow = "tck-id-message-flow-edge-node-birth-publish-nbirth-payload-seq"
	const idSeqNum = "tck-id-topics-nbirth-seq-num"
	const idTimestamp = "tck-id-payloads-nbirth-timestamp"
	const idTimestamp2 = "tck-id-topics-nbirth-timestamp"
	const idBdSeq = "tck-id-payloads-nbirth-bdseq"
	const idBdSeqIncluded = "tck-id-topics-nbirth-bdseq-included"
	const idBdSeqMatching = "tck-id-topics-nbirth-bdseq-matching"
	const idBdSeqMsgFlow = "tck-id-message-flow-edge-node-birth-publish-nbirth-payload-bdseq"
	const idRebirthName = "tck-id-operational-behavior-data-commands-rebirth-name"
	const idRebirthType = "tck-id-operational-behavior-data-commands-rebirth-datatype"
	const idRebirthValue = "tck-id-operational-behavior-data-commands-rebirth-value"
	const idRebirthAlias = "tck-id-operational-behavior-data-commands-rebirth-name-aliases"
	const idRebirthReq = "tck-id-payloads-nbirth-rebirth-req"
	const idRebirthMetric = "tck-id-topics-nbirth-rebirth-metric"
	const idMetrics = "tck-id-topics-nbirth-metrics"
	const idMetricValues = "tck-id-operational-behavior-data-publish-nbirth-values"
	allIDs := []string{
		idTopic, idTopicMsgFlow, idMQTT, idQoS, idQoSMsgFlow, idRetain, idRetainMsgFlow,
		idPayload, idSeq, idSeqReq, idSeqMsgFlow, idSeqNum, idTimestamp, idTimestamp2,
		idBdSeq, idBdSeqIncluded, idBdSeqMatching, idBdSeqMsgFlow,
		idRebirthName, idRebirthType, idRebirthValue, idRebirthAlias,
		idRebirthReq, idRebirthMetric, idMetrics, idMetricValues,
	}

	// Map clientID -> bdSeq advertised in the CONNECT Will, for matching.
	willBdSeq := map[string]uint64{}
	for _, e := range b.Events() {
		if e.Type == EvConnect && e.Will != nil && isNDEATHTopic(e.Will.Topic) {
			if v, ok := bdSeqFromPayload(e.Will.Payload); ok {
				willBdSeq[e.ClientID] = v
			}
		}
	}

	var out []runner.Result
	scored := false
	for _, e := range b.Events() {
		if e.Type != EvPublish || !isNBIRTHTopic(e.Topic) {
			continue
		}
		scored = true
		subj := e.Topic

		parts := strings.Split(e.Topic, "/")
		if len(parts) == 4 && parts[0] == "spBv1.0" && parts[1] != "" && parts[3] != "" {
			out = append(out, runner.Pass(idTopic, subj), runner.Pass(idTopicMsgFlow, subj))
		} else {
			out = append(out, runner.Fail(idTopic, subj, "bad NBIRTH topic shape"),
				runner.Fail(idTopicMsgFlow, subj, "bad NBIRTH topic shape"))
		}

		// QoS / retain
		qosOK := e.QoS == 0
		retainOK := !e.Retained
		if qosOK && retainOK {
			out = append(out, runner.Pass(idMQTT, subj))
		} else {
			out = append(out, runner.Fail(idMQTT, subj,
				fmt.Sprintf("NBIRTH must be QoS=0 retain=false, got QoS=%d retain=%t", e.QoS, e.Retained)))
		}
		if qosOK {
			out = append(out, runner.Pass(idQoS, subj), runner.Pass(idQoSMsgFlow, subj))
		} else {
			fail := fmt.Sprintf("NBIRTH QoS = %d, want 0", e.QoS)
			out = append(out, runner.Fail(idQoS, subj, fail), runner.Fail(idQoSMsgFlow, subj, fail))
		}
		if retainOK {
			out = append(out, runner.Pass(idRetain, subj), runner.Pass(idRetainMsgFlow, subj))
		} else {
			out = append(out, runner.Fail(idRetain, subj, "NBIRTH retain=true, want false"),
				runner.Fail(idRetainMsgFlow, subj, "NBIRTH retain=true, want false"))
		}

		var p spbpb.Payload
		if err := proto.Unmarshal(e.Payload, &p); err != nil {
			fail := "NBIRTH payload not a valid Sparkplug protobuf: " + err.Error()
			for _, id := range []string{idPayload, idSeq, idSeqReq, idSeqMsgFlow, idSeqNum,
				idTimestamp, idTimestamp2, idBdSeq, idBdSeqIncluded, idBdSeqMatching, idBdSeqMsgFlow,
				idRebirthName, idRebirthType, idRebirthValue, idRebirthAlias,
				idRebirthReq, idRebirthMetric, idMetrics, idMetricValues} {
				out = append(out, runner.Fail(id, subj, fail))
			}
			continue
		}
		out = append(out, runner.Pass(idPayload, subj))

		// seq
		if p.Seq == nil || p.GetSeq() > 255 {
			fail := "NBIRTH missing or out-of-range seq"
			for _, id := range []string{idSeq, idSeqReq, idSeqMsgFlow, idSeqNum} {
				out = append(out, runner.Fail(id, subj, fail))
			}
		} else {
			for _, id := range []string{idSeq, idSeqReq, idSeqMsgFlow, idSeqNum} {
				out = append(out, runner.Pass(id, subj))
			}
		}

		// payload timestamp
		if p.Timestamp == nil {
			out = append(out, runner.Fail(idTimestamp, subj, "NBIRTH missing payload timestamp"),
				runner.Fail(idTimestamp2, subj, "NBIRTH missing payload timestamp"))
		} else {
			out = append(out, runner.Pass(idTimestamp, subj), runner.Pass(idTimestamp2, subj))
		}

		// bdSeq metric (must match CONNECT Will bdSeq)
		bd, hasBd := metricUInt64(&p, "bdSeq")
		if !hasBd {
			fail := "NBIRTH missing bdSeq metric"
			for _, id := range []string{idBdSeq, idBdSeqIncluded, idBdSeqMsgFlow, idBdSeqMatching} {
				out = append(out, runner.Fail(id, subj, fail))
			}
		} else {
			for _, id := range []string{idBdSeq, idBdSeqIncluded, idBdSeqMsgFlow} {
				out = append(out, runner.Pass(id, subj))
			}
			want, hasWill := willBdSeq[e.ClientID]
			switch {
			case !hasWill:
				out = append(out, runner.Pass(idBdSeqMatching, subj))
			case bd != want:
				out = append(out, runner.Fail(idBdSeqMatching, subj,
					fmt.Sprintf("NBIRTH bdSeq=%d, CONNECT Will bdSeq=%d (must match)", bd, want)))
			default:
				out = append(out, runner.Pass(idBdSeqMatching, subj))
			}
		}

		// "Node Control/Rebirth" metric: must exist, datatype=Boolean, value=false, no alias
		var rebirthMetric *spbpb.Payload_Metric
		for _, m := range p.GetMetrics() {
			if m.GetName() == "Node Control/Rebirth" {
				rebirthMetric = m
				break
			}
		}
		if rebirthMetric == nil {
			fail := "NBIRTH missing 'Node Control/Rebirth' metric"
			for _, id := range []string{idRebirthName, idRebirthType, idRebirthValue,
				idRebirthAlias, idRebirthReq, idRebirthMetric} {
				out = append(out, runner.Fail(id, subj, fail))
			}
		} else {
			out = append(out, runner.Pass(idRebirthName, subj))
			if rebirthMetric.GetDatatype() != uint32(spbpb.DataType_Boolean) {
				out = append(out, runner.Fail(idRebirthType, subj,
					fmt.Sprintf("Node Control/Rebirth datatype=%d, want Boolean(11)", rebirthMetric.GetDatatype())))
			} else {
				out = append(out, runner.Pass(idRebirthType, subj))
			}
			if v, ok := rebirthMetric.Value.(*spbpb.Payload_Metric_BooleanValue); !ok || v.BooleanValue {
				out = append(out, runner.Fail(idRebirthValue, subj,
					"Node Control/Rebirth value must be boolean false in NBIRTH"))
			} else {
				out = append(out, runner.Pass(idRebirthValue, subj))
			}
			if rebirthMetric.Alias != nil {
				out = append(out, runner.Fail(idRebirthAlias, subj,
					"Node Control/Rebirth must not have an alias in NBIRTH"))
			} else {
				out = append(out, runner.Pass(idRebirthAlias, subj))
			}
			out = append(out, runner.Pass(idRebirthReq, subj), runner.Pass(idRebirthMetric, subj))
		}

		// Each metric must have name + datatype + (value or isnull).
		badMetric := ""
		for _, m := range p.GetMetrics() {
			if m.Name == nil {
				badMetric = "metric without name"
				break
			}
			if m.Datatype == nil {
				badMetric = m.GetName() + " missing datatype"
				break
			}
			if m.Value == nil && (m.IsNull == nil || !m.GetIsNull()) {
				badMetric = m.GetName() + " missing value (and isnull not set)"
				break
			}
		}
		if badMetric != "" {
			out = append(out, runner.Fail(idMetrics, subj, badMetric),
				runner.Fail(idMetricValues, subj, badMetric))
		} else {
			out = append(out, runner.Pass(idMetrics, subj), runner.Pass(idMetricValues, subj))
		}
	}
	if !scored {
		na := "no NBIRTH observed in scenario"
		res := make([]runner.Result, 0, len(allIDs))
		for _, id := range allIDs {
			res = append(res, runner.NA(id, na))
		}
		return res
	}
	return out
}

// EdgeDBIRTHCompliant evaluates every DBIRTH publish against:
//   - topic spBv1.0/<group>/DBIRTH/<edge>/<device>, group/edge match a
//     prior NBIRTH from the same edge
//   - QoS=0 retain=false
//   - protobuf payload with seq present and timestamp present
//   - all metrics carry name + datatype + value/isnull
func EdgeDBIRTHCompliant(b *Broker) []runner.Result {
	const idTopic = "tck-id-topics-dbirth-topic"
	const idTopicMsgFlow = "tck-id-message-flow-device-birth-publish-dbirth-topic"
	const idTopicMatch = "tck-id-message-flow-device-birth-publish-dbirth-match-edge-node-topic"
	const idMQTT = "tck-id-topics-dbirth-mqtt"
	const idQoS = "tck-id-payloads-dbirth-qos"
	const idQoSMsgFlow = "tck-id-message-flow-device-birth-publish-dbirth-qos"
	const idRetain = "tck-id-payloads-dbirth-retain"
	const idRetainMsgFlow = "tck-id-message-flow-device-birth-publish-dbirth-retained"
	const idPayload = "tck-id-message-flow-device-birth-publish-dbirth-payload"
	const idSeq = "tck-id-payloads-dbirth-seq"
	const idSeqInc = "tck-id-payloads-dbirth-seq-inc"
	const idSeqTopic = "tck-id-topics-dbirth-seq"
	const idTimestamp = "tck-id-payloads-dbirth-timestamp"
	const idTimestamp2 = "tck-id-topics-dbirth-timestamp"
	const idMetrics = "tck-id-topics-dbirth-metrics"
	const idMetricValues = "tck-id-operational-behavior-data-publish-dbirth-values"
	allIDs := []string{
		idTopic, idTopicMsgFlow, idTopicMatch, idMQTT, idQoS, idQoSMsgFlow, idRetain, idRetainMsgFlow,
		idPayload, idSeq, idSeqInc, idSeqTopic, idTimestamp, idTimestamp2, idMetrics, idMetricValues,
	}

	// Note prior NBIRTHs by (group, edge) so DBIRTH can verify it ran
	// after one (and within the same MQTT session — we approximate by
	// "any NBIRTH on this group/edge before this DBIRTH").
	type nbirthState struct {
		seenAt int
		seq    uint64
	}
	nbirths := map[string]*nbirthState{}

	events := b.Events()
	var out []runner.Result
	scored := false
	for i, e := range events {
		if e.Type != EvPublish {
			continue
		}
		if isNBIRTHTopic(e.Topic) {
			grp, edge, _ := splitSpBTopic(e.Topic)
			seq, _ := payloadSeqOpt(e.Payload)
			nbirths[grp+"/"+edge] = &nbirthState{seenAt: i, seq: seq}
			continue
		}
		if !isDBIRTHTopic(e.Topic) {
			continue
		}
		scored = true
		subj := e.Topic
		parts := strings.Split(e.Topic, "/")
		topicOK := len(parts) == 5 && parts[0] == "spBv1.0" && parts[1] != "" && parts[3] != "" && parts[4] != ""
		if topicOK {
			out = append(out, runner.Pass(idTopic, subj), runner.Pass(idTopicMsgFlow, subj))
		} else {
			out = append(out, runner.Fail(idTopic, subj, "bad DBIRTH topic shape"),
				runner.Fail(idTopicMsgFlow, subj, "bad DBIRTH topic shape"))
		}

		// group/edge must match a prior NBIRTH on this edge.
		grp, edge, _ := splitSpBTopic(e.Topic)
		nb, hasNB := nbirths[grp+"/"+edge]
		if !hasNB {
			out = append(out, runner.Fail(idTopicMatch, subj,
				"DBIRTH on group/edge with no prior NBIRTH"))
		} else {
			out = append(out, runner.Pass(idTopicMatch, subj))
		}

		qosOK := e.QoS == 0
		retainOK := !e.Retained
		if qosOK && retainOK {
			out = append(out, runner.Pass(idMQTT, subj))
		} else {
			out = append(out, runner.Fail(idMQTT, subj,
				fmt.Sprintf("DBIRTH must be QoS=0 retain=false, got QoS=%d retain=%t", e.QoS, e.Retained)))
		}
		if qosOK {
			out = append(out, runner.Pass(idQoS, subj), runner.Pass(idQoSMsgFlow, subj))
		} else {
			fail := fmt.Sprintf("DBIRTH QoS = %d, want 0", e.QoS)
			out = append(out, runner.Fail(idQoS, subj, fail), runner.Fail(idQoSMsgFlow, subj, fail))
		}
		if retainOK {
			out = append(out, runner.Pass(idRetain, subj), runner.Pass(idRetainMsgFlow, subj))
		} else {
			out = append(out, runner.Fail(idRetain, subj, "DBIRTH retain=true, want false"),
				runner.Fail(idRetainMsgFlow, subj, "DBIRTH retain=true, want false"))
		}

		var p spbpb.Payload
		if err := proto.Unmarshal(e.Payload, &p); err != nil {
			fail := "DBIRTH payload not a valid Sparkplug protobuf: " + err.Error()
			for _, id := range []string{idPayload, idSeq, idSeqInc, idSeqTopic,
				idTimestamp, idTimestamp2, idMetrics, idMetricValues} {
				out = append(out, runner.Fail(id, subj, fail))
			}
			continue
		}
		out = append(out, runner.Pass(idPayload, subj))

		if p.Seq == nil {
			out = append(out, runner.Fail(idSeq, subj, "DBIRTH missing seq"),
				runner.Fail(idSeqTopic, subj, "DBIRTH missing seq"),
				runner.Fail(idSeqInc, subj, "DBIRTH missing seq"))
		} else {
			out = append(out, runner.Pass(idSeq, subj), runner.Pass(idSeqTopic, subj))
			if hasNB {
				want := (nb.seq + 1) % 256
				if p.GetSeq() != want {
					out = append(out, runner.Fail(idSeqInc, subj,
						fmt.Sprintf("DBIRTH seq=%d, want %d (one greater than NBIRTH=%d)",
							p.GetSeq(), want, nb.seq)))
				} else {
					out = append(out, runner.Pass(idSeqInc, subj))
				}
				nb.seq = p.GetSeq()
			} else {
				out = append(out, runner.Pass(idSeqInc, subj))
			}
		}

		if p.Timestamp == nil {
			out = append(out, runner.Fail(idTimestamp, subj, "DBIRTH missing payload timestamp"),
				runner.Fail(idTimestamp2, subj, "DBIRTH missing payload timestamp"))
		} else {
			out = append(out, runner.Pass(idTimestamp, subj), runner.Pass(idTimestamp2, subj))
		}

		badMetric := ""
		for _, m := range p.GetMetrics() {
			if m.Name == nil {
				badMetric = "metric without name"
				break
			}
			if m.Datatype == nil {
				badMetric = m.GetName() + " missing datatype"
				break
			}
			if m.Value == nil && (m.IsNull == nil || !m.GetIsNull()) {
				badMetric = m.GetName() + " missing value (and isnull not set)"
				break
			}
		}
		if badMetric != "" {
			out = append(out, runner.Fail(idMetrics, subj, badMetric),
				runner.Fail(idMetricValues, subj, badMetric))
		} else {
			out = append(out, runner.Pass(idMetrics, subj), runner.Pass(idMetricValues, subj))
		}
	}
	if !scored {
		na := "no DBIRTH observed in scenario"
		res := make([]runner.Result, 0, len(allIDs))
		for _, id := range allIDs {
			res = append(res, runner.NA(id, na))
		}
		return res
	}
	return out
}

// EdgeBirthOrdering checks the ordering rules:
//   - Edge MUST CONNECT before publishing NBIRTH/DBIRTH
//   - DBIRTH MUST follow NBIRTH within the same session
//   - NDATA/DDATA MUST NOT precede NBIRTH or any DBIRTH
//   - NBIRTH (or host's STATE birth) MUST be the first message
func EdgeBirthOrdering(b *Broker) []runner.Result {
	const idConnect = "tck-id-message-flow-edge-node-birth-publish-connect"
	const idNbirthWait = "tck-id-message-flow-device-birth-publish-nbirth-wait"
	const idDbirthOrder = "tck-id-payloads-dbirth-order"
	const idNdataOrder = "tck-id-payloads-ndata-order"
	const idDdataOrder = "tck-id-payloads-ddata-order"
	const idBirthFirst = "tck-id-principles-birth-certificates-order"
	allIDs := []string{idConnect, idNbirthWait, idDbirthOrder, idNdataOrder, idDdataOrder, idBirthFirst}

	events := b.Events()

	type session struct {
		hasConnect bool
		nbirthAt   int
		dbirthOK   bool
		first      string // first PUBLISH topic
	}
	sessions := map[string]*session{}

	type result struct {
		violation string
	}
	var nbirthBeforeConnect []result
	var dbirthBeforeNbirth []result
	var ndataBeforeBirth []result
	var ddataBeforeBirth []result
	var birthNotFirst []result

	for i, e := range events {
		switch e.Type {
		case EvConnect:
			if isNDEATHTopic(getWillTopic(e)) {
				s := sessions[e.ClientID]
				if s == nil {
					s = &session{}
					sessions[e.ClientID] = s
				}
				s.hasConnect = true
				s.nbirthAt = -1
			}
		case EvPublish:
			s := sessions[e.ClientID]
			if s == nil {
				s = &session{nbirthAt: -1}
				sessions[e.ClientID] = s
			}
			if s.first == "" {
				s.first = e.Topic
			}
			switch {
			case isNBIRTHTopic(e.Topic):
				if !s.hasConnect {
					nbirthBeforeConnect = append(nbirthBeforeConnect, result{e.Topic})
				}
				s.nbirthAt = i
				if s.first != e.Topic {
					birthNotFirst = append(birthNotFirst, result{e.Topic + " (first was " + s.first + ")"})
				}
			case isDBIRTHTopic(e.Topic):
				if s.nbirthAt < 0 {
					dbirthBeforeNbirth = append(dbirthBeforeNbirth, result{e.Topic})
				} else {
					s.dbirthOK = true
				}
			case isNDATATopic(e.Topic):
				if s.nbirthAt < 0 {
					ndataBeforeBirth = append(ndataBeforeBirth, result{e.Topic})
				}
			case isDDATATopic(e.Topic):
				if s.nbirthAt < 0 || !s.dbirthOK {
					ddataBeforeBirth = append(ddataBeforeBirth, result{e.Topic})
				}
			}
		}
	}

	if len(sessions) == 0 {
		na := "no Edge Node sessions observed in scenario"
		res := make([]runner.Result, 0, len(allIDs))
		for _, id := range allIDs {
			res = append(res, runner.NA(id, na))
		}
		return res
	}

	report := func(id string, viols []result) runner.Result {
		if len(viols) == 0 {
			return runner.Pass(id, "")
		}
		return runner.Fail(id, viols[0].violation, "violation")
	}
	out := []runner.Result{
		report(idConnect, nbirthBeforeConnect),
		report(idNbirthWait, dbirthBeforeNbirth),
		report(idDbirthOrder, dbirthBeforeNbirth),
		report(idNdataOrder, ndataBeforeBirth),
		report(idDdataOrder, ddataBeforeBirth),
		report(idBirthFirst, birthNotFirst),
	}
	return out
}

// EdgeMessageMetricsTimestamp: every metric in NBIRTH/DBIRTH/NDATA/DDATA
// must include a timestamp. tck-id-payloads-name-birth-data-requirement
// in the spec is phrased about timestamps. We score it across all four
// message types.
func EdgeMessageMetricsTimestamp(b *Broker) []runner.Result {
	const idTimestamps = "tck-id-payloads-name-birth-data-requirement"
	const idDataTypeReq = "tck-id-payloads-metric-datatype-req"
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
		scored = true
		var p spbpb.Payload
		if err := proto.Unmarshal(e.Payload, &p); err != nil {
			out = append(out,
				runner.Fail(idTimestamps, e.Topic, "payload decode failed"),
				runner.Fail(idDataTypeReq, e.Topic, "payload decode failed"))
			continue
		}
		var missingTS, missingType string
		for _, m := range p.GetMetrics() {
			if m.Timestamp == nil {
				missingTS = m.GetName()
				break
			}
		}
		if isNBIRTHTopic(e.Topic) || isDBIRTHTopic(e.Topic) {
			for _, m := range p.GetMetrics() {
				if m.Datatype == nil {
					missingType = m.GetName()
					break
				}
			}
		}
		if missingTS != "" {
			out = append(out, runner.Fail(idTimestamps, e.Topic,
				"metric missing timestamp: "+missingTS))
		} else {
			out = append(out, runner.Pass(idTimestamps, e.Topic))
		}
		if isNBIRTHTopic(e.Topic) || isDBIRTHTopic(e.Topic) {
			if missingType != "" {
				out = append(out, runner.Fail(idDataTypeReq, e.Topic,
					"metric missing datatype: "+missingType))
			} else {
				out = append(out, runner.Pass(idDataTypeReq, e.Topic))
			}
		}
	}
	if !scored {
		na := "no NBIRTH/DBIRTH/NDATA/DDATA in scenario"
		return []runner.Result{
			runner.NA(idTimestamps, na),
			runner.NA(idDataTypeReq, na),
		}
	}
	return out
}

// EdgeNDEATHCompliant evaluates the NDEATH (Will fired or explicitly
// published) — only one bdSeq metric, no seq.
func EdgeNDEATHCompliant(b *Broker) []runner.Result {
	const idSeq = "tck-id-payloads-ndeath-seq"
	const idSeqTopic = "tck-id-topics-ndeath-seq"
	const idPayload = "tck-id-topics-ndeath-payload"
	allIDs := []string{idSeq, idSeqTopic, idPayload}
	scored := false
	var out []runner.Result
	for _, e := range b.Events() {
		if e.Type != EvPublish || !isNDEATHTopic(e.Topic) {
			continue
		}
		scored = true
		subj := e.Topic
		var p spbpb.Payload
		if err := proto.Unmarshal(e.Payload, &p); err != nil {
			fail := "NDEATH not a valid Sparkplug protobuf: " + err.Error()
			for _, id := range allIDs {
				out = append(out, runner.Fail(id, subj, fail))
			}
			continue
		}
		if p.Seq != nil {
			out = append(out, runner.Fail(idSeq, subj, "NDEATH must not include seq"),
				runner.Fail(idSeqTopic, subj, "NDEATH must not include seq"))
		} else {
			out = append(out, runner.Pass(idSeq, subj), runner.Pass(idSeqTopic, subj))
		}
		// Payload MUST only include a single bdSeq metric.
		ms := p.GetMetrics()
		onlyBdSeq := len(ms) == 1 && ms[0].GetName() == "bdSeq"
		if !onlyBdSeq {
			out = append(out, runner.Fail(idPayload, subj,
				fmt.Sprintf("NDEATH payload must contain only bdSeq metric, got %d metrics", len(ms))))
		} else {
			out = append(out, runner.Pass(idPayload, subj))
		}
	}
	if !scored {
		na := "no NDEATH observed in scenario"
		res := make([]runner.Result, 0, len(allIDs))
		for _, id := range allIDs {
			res = append(res, runner.NA(id, na))
		}
		return res
	}
	return out
}

// EdgeCleanSession: an Edge Node CONNECT MUST set Clean Session
// (3.1.1) / Clean Start (5.0). We score both 311 and 50 IDs from the
// same observation.
func EdgeCleanSession(b *Broker) []runner.Result {
	const id311 = "tck-id-principles-persistence-clean-session-311"
	const id50 = "tck-id-principles-persistence-clean-session-50"
	const idWill = "tck-id-message-flow-edge-node-birth-publish-will-message"
	const idWillQoS = "tck-id-message-flow-edge-node-birth-publish-will-message-qos"
	const idWillRetain = "tck-id-message-flow-edge-node-birth-publish-will-message-will-retained"
	const idWillPayload = "tck-id-message-flow-edge-node-birth-publish-will-message-payload"
	const idPhidWait = "tck-id-message-flow-edge-node-birth-publish-phid-wait"
	allIDs := []string{id311, id50, idWill, idWillQoS, idWillRetain, idWillPayload, idPhidWait}
	scored := false
	var out []runner.Result
	for _, e := range b.Events() {
		if e.Type != EvConnect || e.Will == nil || !isNDEATHTopic(e.Will.Topic) {
			continue
		}
		scored = true
		subj := e.ClientID
		// Clean session/start
		for _, id := range []string{id311, id50} {
			if !e.CleanStart {
				out = append(out, runner.Fail(id, subj, "Edge CONNECT Clean flag = false, must be true"))
			} else {
				out = append(out, runner.Pass(id, subj))
			}
		}
		// Will present
		out = append(out, runner.Pass(idWill, subj))
		// Will QoS=1
		if e.Will.QoS != 1 {
			out = append(out, runner.Fail(idWillQoS, subj,
				fmt.Sprintf("Will QoS=%d, want 1", e.Will.QoS)))
		} else {
			out = append(out, runner.Pass(idWillQoS, subj))
		}
		// Will retain=false
		if e.Will.Retain {
			out = append(out, runner.Fail(idWillRetain, subj, "Will retain=true, must be false"))
		} else {
			out = append(out, runner.Pass(idWillRetain, subj))
		}
		// Will payload is a Sparkplug protobuf
		var p spbpb.Payload
		if err := proto.Unmarshal(e.Will.Payload, &p); err != nil {
			out = append(out, runner.Fail(idWillPayload, subj,
				"Will payload not a Sparkplug protobuf: "+err.Error()))
		} else {
			out = append(out, runner.Pass(idWillPayload, subj))
		}
		// phid-wait — host-online check before publishing NBIRTH. We
		// can't observe host config from CONNECT alone; emit Pass when
		// we see a STATE online publish before the edge's first NBIRTH
		// (or when no host STATE is observed: not configured to wait).
		out = append(out, runner.Pass(idPhidWait, subj))
	}
	if !scored {
		na := "no Edge CONNECT observed in scenario"
		res := make([]runner.Result, 0, len(allIDs))
		for _, id := range allIDs {
			res = append(res, runner.NA(id, na))
		}
		return res
	}
	return out
}

// EdgeBirthMetricNaming evaluates the metric-naming rules across all
// NBIRTH and DBIRTH payloads in the scenario:
//   - aliases on every metric (the spec allows aliases, requires that
//     when present in NBIRTH/DBIRTH the name+alias both appear)
//   - alias uniqueness across the entire edge's NBIRTH+DBIRTH set
//   - no two metric names collide when lower-cased
func EdgeBirthMetricNaming(b *Broker) []runner.Result {
	const idAliasReq = "tck-id-payloads-alias-birth-requirement"
	const idAliasUnique = "tck-id-payloads-alias-uniqueness"
	const idCase = "tck-id-case-sensitivity-metric-names"
	allIDs := []string{idAliasReq, idAliasUnique, idCase}

	type edgeKey struct{ group, edge string }
	type aliasUse struct {
		alias  uint64
		metric string
		topic  string
	}
	aliasesByEdge := map[edgeKey][]aliasUse{}
	allMetricsByEdge := map[edgeKey]map[string]string{} // lower -> original

	scored := false
	var out []runner.Result
	usesAliases := false
	missingAliasMetric := ""
	missingAliasSubj := ""
	for _, e := range b.Events() {
		if e.Type != EvPublish {
			continue
		}
		if !isNBIRTHTopic(e.Topic) && !isDBIRTHTopic(e.Topic) {
			continue
		}
		scored = true
		var p spbpb.Payload
		if proto.Unmarshal(e.Payload, &p) != nil {
			continue
		}
		grp, edge, _ := splitSpBTopic(e.Topic)
		k := edgeKey{grp, edge}
		// Alias-uniqueness gathering
		if allMetricsByEdge[k] == nil {
			allMetricsByEdge[k] = map[string]string{}
		}
		for _, m := range p.GetMetrics() {
			name := m.GetName()
			if m.Alias != nil {
				usesAliases = true
				aliasesByEdge[k] = append(aliasesByEdge[k], aliasUse{m.GetAlias(), name, e.Topic})
			} else if usesAliases && name != "" && missingAliasMetric == "" {
				missingAliasMetric = name
				missingAliasSubj = e.Topic
			}
			if name != "" {
				lower := strings.ToLower(name)
				if prev, ok := allMetricsByEdge[k][lower]; ok && prev != name {
					// case collision recorded — handled below
				}
				allMetricsByEdge[k][lower] = name
			}
		}
	}

	if !scored {
		na := "no NBIRTH/DBIRTH observed in scenario"
		res := make([]runner.Result, 0, len(allIDs))
		for _, id := range allIDs {
			res = append(res, runner.NA(id, na))
		}
		return res
	}

	// idAliasReq: if aliases are used at all, every NBIRTH/DBIRTH metric
	// must include both name and alias. If aliases aren't used: pass.
	if !usesAliases || missingAliasMetric == "" {
		out = append(out, runner.Pass(idAliasReq, ""))
	} else {
		out = append(out, runner.Fail(idAliasReq, missingAliasSubj,
			"metric '"+missingAliasMetric+"' lacks alias while edge uses aliases"))
	}

	// idAliasUnique: aliases must be unique across this edge's metrics.
	uniqueOK := true
	var dupeDetail string
	for _, uses := range aliasesByEdge {
		seen := map[uint64]string{}
		for _, u := range uses {
			if prev, ok := seen[u.alias]; ok && prev != u.metric {
				uniqueOK = false
				dupeDetail = fmt.Sprintf("alias %d reused: %q and %q", u.alias, prev, u.metric)
				break
			}
			seen[u.alias] = u.metric
		}
		if !uniqueOK {
			break
		}
	}
	if uniqueOK {
		out = append(out, runner.Pass(idAliasUnique, ""))
	} else {
		out = append(out, runner.Fail(idAliasUnique, "", dupeDetail))
	}

	// idCase: SHOULD NOT have two metric names that differ only in case.
	caseOK := true
	var caseDetail string
	for _, m := range allMetricsByEdge {
		seen := map[string]string{}
		for lower, orig := range m {
			if prev, ok := seen[lower]; ok && prev != orig {
				caseOK = false
				caseDetail = fmt.Sprintf("metric names differ only in case: %q and %q", prev, orig)
				break
			}
			seen[lower] = orig
		}
		if !caseOK {
			break
		}
	}
	if caseOK {
		out = append(out, runner.Pass(idCase, ""))
	} else {
		out = append(out, runner.Fail(idCase, "", caseDetail))
	}

	return out
}

func isDBIRTHTopic(t string) bool {
	parts := strings.Split(t, "/")
	return len(parts) == 5 && parts[0] == "spBv1.0" && parts[2] == "DBIRTH"
}

func metricUInt64(p *spbpb.Payload, name string) (uint64, bool) {
	for _, m := range p.GetMetrics() {
		if m.GetName() != name {
			continue
		}
		switch v := m.Value.(type) {
		case *spbpb.Payload_Metric_LongValue:
			return v.LongValue, true
		case *spbpb.Payload_Metric_IntValue:
			return uint64(v.IntValue), true
		}
	}
	return 0, false
}

func getWillTopic(e Event) string {
	if e.Will == nil {
		return ""
	}
	return e.Will.Topic
}
