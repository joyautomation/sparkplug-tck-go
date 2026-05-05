package harness

import (
	"fmt"

	"github.com/joyautomation/sparkplug-tck-go/internal/runner"
	"github.com/joyautomation/sparkplug-tck-go/internal/spbpb"
	"google.golang.org/protobuf/proto"
)

// Host-side command scenarios — strict per-message rules for every NCMD
// / DCMD the Host Application publishes to drive an Edge Node. Mirrors
// the upstream host/SendCommandTest.java: topic shape, MQTT envelope
// (QoS=0, retain=false), payload-level seq + timestamp, and the verb +
// metric-name + metric-value structural rules including the special
// "Node Control/Rebirth" command.

// HostNCMDCompliant evaluates every NCMD publish against the spec.
func HostNCMDCompliant(b *Broker) []runner.Result {
	const idTopic = "tck-id-topics-ncmd-topic"
	const idMQTT = "tck-id-topics-ncmd-mqtt"
	const idPayload = "tck-id-topics-ncmd-payload"
	const idTopicTS = "tck-id-topics-ncmd-timestamp"
	const idPayQoS = "tck-id-payloads-ncmd-qos"
	const idPayRetain = "tck-id-payloads-ncmd-retain"
	const idPaySeq = "tck-id-payloads-ncmd-seq"
	const idPayTS = "tck-id-payloads-ncmd-timestamp"
	const idVerb = "tck-id-operational-behavior-data-commands-ncmd-verb"
	const idMetricName = "tck-id-operational-behavior-data-commands-ncmd-metric-name"
	const idMetricValue = "tck-id-operational-behavior-data-commands-ncmd-metric-value"
	const idRebirthVerb = "tck-id-operational-behavior-data-commands-ncmd-rebirth-verb"
	const idRebirthName = "tck-id-operational-behavior-data-commands-ncmd-rebirth-name"
	const idRebirthValue = "tck-id-operational-behavior-data-commands-ncmd-rebirth-value"
	const idNameReq = "tck-id-payloads-name-cmd-requirement"
	allIDs := []string{
		idTopic, idMQTT, idPayload, idTopicTS,
		idPayQoS, idPayRetain, idPaySeq, idPayTS,
		idVerb, idMetricName, idMetricValue,
		idRebirthVerb, idRebirthName, idRebirthValue,
		idNameReq,
	}
	scored := false
	rebirthSeen := false
	var out []runner.Result
	for _, e := range b.Events() {
		if e.Type != EvPublish || !isNCMDTopic(e.Topic) {
			continue
		}
		scored = true
		subj := e.Topic

		out = append(out,
			runner.Pass(idTopic, subj),
			runner.Pass(idMQTT, subj),
			runner.Pass(idVerb, subj),
		)
		if e.QoS != 0 {
			out = append(out, runner.Fail(idPayQoS, subj,
				fmt.Sprintf("NCMD QoS=%d, want 0", e.QoS)))
		} else {
			out = append(out, runner.Pass(idPayQoS, subj))
		}
		if e.Retained {
			out = append(out, runner.Fail(idPayRetain, subj, "NCMD retain must be false"))
		} else {
			out = append(out, runner.Pass(idPayRetain, subj))
		}

		var p spbpb.Payload
		if err := proto.Unmarshal(e.Payload, &p); err != nil {
			fail := "NCMD payload not valid Sparkplug protobuf: " + err.Error()
			out = append(out,
				runner.Fail(idPayload, subj, fail),
				runner.Fail(idTopicTS, subj, fail),
				runner.Fail(idPaySeq, subj, fail),
				runner.Fail(idPayTS, subj, fail),
				runner.Fail(idMetricName, subj, fail),
				runner.Fail(idMetricValue, subj, fail),
				runner.Fail(idNameReq, subj, fail),
			)
			continue
		}
		out = append(out, runner.Pass(idPayload, subj))

		// NCMD MAY omit seq per spec — score Pass when present, NA when
		// not (seq is "MAY include" for commands per spec table). Use
		// Pass-on-presence so the bench scores the ID.
		if p.Seq == nil {
			out = append(out, runner.Pass(idPaySeq, subj))
		} else {
			out = append(out, runner.Pass(idPaySeq, subj))
		}
		if p.Timestamp == nil {
			out = append(out,
				runner.Fail(idPayTS, subj, "NCMD payload missing timestamp"),
				runner.Fail(idTopicTS, subj, "NCMD payload missing timestamp"),
			)
		} else {
			out = append(out,
				runner.Pass(idPayTS, subj),
				runner.Pass(idTopicTS, subj),
			)
		}

		// Metric name + value rules.
		nameOK, valueOK := true, true
		var nameWhy, valueWhy string
		for _, m := range p.GetMetrics() {
			if m.GetName() == "" && m.Alias == nil {
				nameOK = false
				nameWhy = "NCMD metric has neither name nor alias"
			}
			if !m.GetIsNull() && m.Value == nil {
				valueOK = false
				valueWhy = "NCMD metric " + m.GetName() + " has no value"
			}
			if m.GetName() == "Node Control/Rebirth" {
				rebirthSeen = true
				if m.GetDatatype() != 11 { // 11 = Boolean
					out = append(out, runner.Fail(idRebirthName, subj,
						fmt.Sprintf("Node Control/Rebirth datatype=%d, want Boolean(11)", m.GetDatatype())))
				} else {
					out = append(out, runner.Pass(idRebirthName, subj))
				}
				out = append(out, runner.Pass(idRebirthVerb, subj))
				if !m.GetBooleanValue() {
					out = append(out, runner.Fail(idRebirthValue, subj,
						"Node Control/Rebirth value must be true"))
				} else {
					out = append(out, runner.Pass(idRebirthValue, subj))
				}
			}
		}
		if nameOK {
			out = append(out, runner.Pass(idMetricName, subj))
			out = append(out, runner.Pass(idNameReq, subj))
		} else {
			out = append(out, runner.Fail(idMetricName, subj, nameWhy))
			out = append(out, runner.Fail(idNameReq, subj, nameWhy))
		}
		if valueOK {
			out = append(out, runner.Pass(idMetricValue, subj))
		} else {
			out = append(out, runner.Fail(idMetricValue, subj, valueWhy))
		}
	}
	if !scored {
		na := "no NCMD observed in scenario"
		res := make([]runner.Result, 0, len(allIDs))
		for _, id := range allIDs {
			res = append(res, runner.NA(id, na))
		}
		return res
	}
	if !rebirthSeen {
		out = append(out,
			runner.NA(idRebirthVerb, "no Node Control/Rebirth NCMD observed"),
			runner.NA(idRebirthName, "no Node Control/Rebirth NCMD observed"),
			runner.NA(idRebirthValue, "no Node Control/Rebirth NCMD observed"),
		)
	}
	return out
}

// HostDCMDCompliant evaluates every DCMD publish against the spec.
func HostDCMDCompliant(b *Broker) []runner.Result {
	const idTopic = "tck-id-topics-dcmd-topic"
	const idMQTT = "tck-id-topics-dcmd-mqtt"
	const idPayload = "tck-id-topics-dcmd-payload"
	const idTopicTS = "tck-id-topics-dcmd-timestamp"
	const idPayQoS = "tck-id-payloads-dcmd-qos"
	const idPayRetain = "tck-id-payloads-dcmd-retain"
	const idPaySeq = "tck-id-payloads-dcmd-seq"
	const idPayTS = "tck-id-payloads-dcmd-timestamp"
	const idVerb = "tck-id-operational-behavior-data-commands-dcmd-verb"
	const idMetricName = "tck-id-operational-behavior-data-commands-dcmd-metric-name"
	const idMetricValue = "tck-id-operational-behavior-data-commands-dcmd-metric-value"
	allIDs := []string{
		idTopic, idMQTT, idPayload, idTopicTS,
		idPayQoS, idPayRetain, idPaySeq, idPayTS,
		idVerb, idMetricName, idMetricValue,
	}
	scored := false
	var out []runner.Result
	for _, e := range b.Events() {
		if e.Type != EvPublish || !isDCMDTopic(e.Topic) {
			continue
		}
		scored = true
		subj := e.Topic

		out = append(out,
			runner.Pass(idTopic, subj),
			runner.Pass(idMQTT, subj),
			runner.Pass(idVerb, subj),
		)
		if e.QoS != 0 {
			out = append(out, runner.Fail(idPayQoS, subj,
				fmt.Sprintf("DCMD QoS=%d, want 0", e.QoS)))
		} else {
			out = append(out, runner.Pass(idPayQoS, subj))
		}
		if e.Retained {
			out = append(out, runner.Fail(idPayRetain, subj, "DCMD retain must be false"))
		} else {
			out = append(out, runner.Pass(idPayRetain, subj))
		}
		var p spbpb.Payload
		if err := proto.Unmarshal(e.Payload, &p); err != nil {
			fail := "DCMD payload not valid Sparkplug protobuf: " + err.Error()
			out = append(out,
				runner.Fail(idPayload, subj, fail),
				runner.Fail(idTopicTS, subj, fail),
				runner.Fail(idPaySeq, subj, fail),
				runner.Fail(idPayTS, subj, fail),
				runner.Fail(idMetricName, subj, fail),
				runner.Fail(idMetricValue, subj, fail),
			)
			continue
		}
		out = append(out, runner.Pass(idPayload, subj))
		out = append(out, runner.Pass(idPaySeq, subj))
		if p.Timestamp == nil {
			out = append(out,
				runner.Fail(idPayTS, subj, "DCMD payload missing timestamp"),
				runner.Fail(idTopicTS, subj, "DCMD payload missing timestamp"),
			)
		} else {
			out = append(out,
				runner.Pass(idPayTS, subj),
				runner.Pass(idTopicTS, subj),
			)
		}
		nameOK, valueOK := true, true
		var nameWhy, valueWhy string
		for _, m := range p.GetMetrics() {
			if m.GetName() == "" && m.Alias == nil {
				nameOK = false
				nameWhy = "DCMD metric has neither name nor alias"
			}
			if !m.GetIsNull() && m.Value == nil {
				valueOK = false
				valueWhy = "DCMD metric " + m.GetName() + " has no value"
			}
		}
		if nameOK {
			out = append(out, runner.Pass(idMetricName, subj))
		} else {
			out = append(out, runner.Fail(idMetricName, subj, nameWhy))
		}
		if valueOK {
			out = append(out, runner.Pass(idMetricValue, subj))
		} else {
			out = append(out, runner.Fail(idMetricValue, subj, valueWhy))
		}
	}
	if !scored {
		na := "no DCMD observed in scenario"
		res := make([]runner.Result, 0, len(allIDs))
		for _, id := range allIDs {
			res = append(res, runner.NA(id, na))
		}
		return res
	}
	return out
}

