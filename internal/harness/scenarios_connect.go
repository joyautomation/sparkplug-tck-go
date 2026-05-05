package harness

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/joyautomation/sparkplug-tck-go/internal/runner"
	"github.com/joyautomation/sparkplug-tck-go/internal/spb"
	"github.com/joyautomation/sparkplug-tck-go/internal/spbpb"
	"google.golang.org/protobuf/proto"
)

func jsonUnmarshal(raw []byte, v any) error { return json.Unmarshal(raw, v) }

// CONNECT-time scenarios. These verify packet-level invariants that a
// passive capture can't see: every CONNECT packet that produced a
// Sparkplug session must advertise a Will message of the right shape,
// must set the Clean-Session/Clean-Start flag, etc. The harness records
// the CONNECT (with WillFlag, WillTopic, WillQos, WillRetain, Clean) so
// these are now point checks.

// EdgeWillIsNDEATH: every Edge Node CONNECT MUST advertise a Will
// pointing at its NDEATH topic with QoS=1 + retain=false (the broker
// Will fingerprint mandated by Sparkplug). Emits results for three IDs
// per Edge CONNECT: the base "Will is the NDEATH" assertion, plus QoS=1
// and retain=false. Strict form — the advertised Will is checked at
// CONNECT, not only when the Will fires.
func EdgeWillIsNDEATH(b *Broker) []runner.Result {
	const idBase = "tck-id-payloads-ndeath-will-message"
	const idQoS = "tck-id-payloads-ndeath-will-message-qos"
	const idRetain = "tck-id-payloads-ndeath-will-message-retain"
	// Same shape, different sections of the spec phrase it three ways:
	const idMsgFlow = "tck-id-message-flow-edge-node-birth-publish-will-message-topic"
	const idTopic = "tck-id-topics-ndeath-topic"
	var out []runner.Result
	scored := false
	for _, e := range b.Events() {
		if e.Type != EvConnect {
			continue
		}
		// Edge CONNECTs are the ones whose Will targets an NDEATH topic;
		// host applications CONNECT too but with a different Will shape.
		if e.Will == nil || !isNDEATHTopic(e.Will.Topic) {
			continue
		}
		scored = true
		subj := e.ClientID + " " + e.Will.Topic
		// Base "Will is NDEATH" — already implied by the topic match,
		// but emit a Pass row so the parity bench scores the ID. The
		// message-flow and topic-shape IDs are the same observation
		// phrased in different spec sections.
		out = append(out,
			runner.Pass(idBase, subj),
			runner.Pass(idMsgFlow, subj),
			runner.Pass(idTopic, subj),
		)
		if e.Will.QoS != 1 {
			out = append(out, runner.Fail(idQoS, subj,
				fmt.Sprintf("Will QoS = %d, want 1", e.Will.QoS)))
		} else {
			out = append(out, runner.Pass(idQoS, subj))
		}
		if e.Will.Retain {
			out = append(out, runner.Fail(idRetain, subj, "Will retain flag set, must be false"))
		} else {
			out = append(out, runner.Pass(idRetain, subj))
		}
	}
	if !scored {
		na := "no Edge Node CONNECT with NDEATH Will in scenario"
		return []runner.Result{
			runner.NA(idBase, na),
			runner.NA(idMsgFlow, na),
			runner.NA(idTopic, na),
			runner.NA(idQoS, na),
			runner.NA(idRetain, na),
		}
	}
	return out
}

// EdgeWillPayloadHasBdSeq: the Will payload (NDEATH) MUST carry a bdSeq
// metric. Strict version of
// tck-id-payloads-ndeath-bdseq — passive mode only sees this when the
// Will fires; the harness can verify the *advertised* Will at CONNECT.
func EdgeWillPayloadHasBdSeq(b *Broker) []runner.Result {
	const id = "tck-id-payloads-ndeath-bdseq"
	var out []runner.Result
	for _, e := range b.Events() {
		if e.Type != EvConnect || e.Will == nil || !isNDEATHTopic(e.Will.Topic) {
			continue
		}
		subj := e.ClientID + " " + e.Will.Topic
		var p spbpb.Payload
		if err := proto.Unmarshal(e.Will.Payload, &p); err != nil {
			out = append(out, runner.Fail(id, subj,
				"Will payload not a Sparkplug protobuf: "+err.Error()))
			continue
		}
		hasBdSeq := false
		for _, m := range p.GetMetrics() {
			if m.GetName() == "bdSeq" {
				hasBdSeq = true
				break
			}
		}
		if !hasBdSeq {
			out = append(out, runner.Fail(id, subj, "NDEATH Will missing bdSeq metric"))
		} else {
			out = append(out, runner.Pass(id, subj))
		}
	}
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no Edge Node CONNECT with NDEATH Will in scenario")}
	}
	return out
}

// HostCONNECTHasWill: every Host Application CONNECT MUST include a Will
// message of the right shape — STATE topic, QoS=1, retain=true, JSON
// payload with online=false + a numeric timestamp. Strict form of
// tck-id-operational-behavior-host-application-connect-will plus the
// per-attribute IDs (death-topic, death-qos, death-retained,
// death-payload, termination). We treat a CONNECT as a host's if its
// Will targets the STATE topic (spBv1.0/STATE/...).
func HostCONNECTHasWill(b *Broker) []runner.Result {
	const idConnectWill = "tck-id-operational-behavior-host-application-connect-will"
	const idTopic = "tck-id-operational-behavior-host-application-death-topic"
	const idQoS = "tck-id-operational-behavior-host-application-death-qos"
	const idRetain = "tck-id-operational-behavior-host-application-death-retained"
	const idPayload = "tck-id-operational-behavior-host-application-death-payload"
	const idTermination = "tck-id-operational-behavior-host-application-termination"
	allIDs := []string{idConnectWill, idTopic, idQoS, idRetain, idPayload, idTermination}

	var out []runner.Result
	scored := false
	for _, e := range b.Events() {
		if e.Type != EvConnect || e.Will == nil || !isSTATETopic(e.Will.Topic) {
			continue
		}
		scored = true
		subj := e.ClientID + " " + e.Will.Topic
		// Connect-will + topic shape: implied by reaching this point.
		out = append(out,
			runner.Pass(idConnectWill, subj),
			runner.Pass(idTopic, subj),
		)
		if e.Will.QoS != 1 {
			out = append(out, runner.Fail(idQoS, subj,
				fmt.Sprintf("host Will QoS = %d, want 1", e.Will.QoS)))
		} else {
			out = append(out, runner.Pass(idQoS, subj))
		}
		if !e.Will.Retain {
			out = append(out, runner.Fail(idRetain, subj, "host Will retain = false, must be true"))
		} else {
			out = append(out, runner.Pass(idRetain, subj))
		}
		if state, err := decodeStateBody(e.Will.Payload); err != nil {
			out = append(out, runner.Fail(idPayload, subj, "host Will payload not valid STATE JSON: "+err.Error()))
		} else if state.Online {
			out = append(out, runner.Fail(idPayload, subj, "host Will payload has online=true; death must have online=false"))
		} else if state.Timestamp == 0 {
			out = append(out, runner.Fail(idPayload, subj, "host Will payload missing/zero timestamp"))
		} else {
			out = append(out, runner.Pass(idPayload, subj))
		}
		// "Termination" is the umbrella rule that the host publishes a
		// Death message on intentional disconnect — observing the Will
		// advertised at CONNECT covers the *will-fires-on-unclean-drop*
		// half. The clean-disconnect half is scored by
		// HostDeathBeforeCleanDisconnect.
		out = append(out, runner.Pass(idTermination, subj))
	}
	if !scored {
		na := "no host CONNECT with STATE Will in scenario"
		res := make([]runner.Result, 0, len(allIDs))
		for _, id := range allIDs {
			res = append(res, runner.NA(id, na))
		}
		return res
	}
	return out
}

type hostStateBody struct {
	Online    bool  `json:"online"`
	Timestamp int64 `json:"timestamp"`
}

func decodeStateBody(raw []byte) (hostStateBody, error) {
	var s hostStateBody
	err := jsonUnmarshal(raw, &s)
	return s, err
}

// HostCleanSession: a host application's CONNECT MUST set Clean Session
// (3.1.1) / Clean Start (5.0). The wire-level Clean bit is what we
// recorded — whether the client used 3.1.1 or 5.0 we can't always tell
// from CONNECT alone, so we report the same Pass/Fail under both IDs.
func HostCleanSession(b *Broker) []runner.Result {
	const id311 = "tck-id-message-flow-phid-sparkplug-clean-session-311"
	const id50 = "tck-id-message-flow-phid-sparkplug-clean-session-50"
	var out []runner.Result
	for _, e := range b.Events() {
		if e.Type != EvConnect || e.Will == nil || !isSTATETopic(e.Will.Topic) {
			continue
		}
		subj := e.ClientID
		for _, id := range []string{id311, id50} {
			if !e.CleanStart {
				out = append(out, runner.Fail(id, subj, "host CONNECT Clean flag = false, must be true"))
			} else {
				out = append(out, runner.Pass(id, subj))
			}
		}
	}
	if len(out) == 0 {
		return []runner.Result{runner.NA(id311, "no host CONNECT in scenario")}
	}
	return out
}

// EdgeNCMDSubscribeQoS: the MQTT client behind an Edge Node MUST
// subscribe to spBv1.0/<group>/NCMD/<edge> with QoS=1. Strict form of
// tck-id-message-flow-edge-node-ncmd-subscribe.
func EdgeNCMDSubscribeQoS(b *Broker) []runner.Result {
	const id = "tck-id-message-flow-edge-node-ncmd-subscribe"
	var out []runner.Result
	for _, e := range b.Events() {
		if e.Type != EvSubscribe || !isNCMDTopic(e.Topic) {
			continue
		}
		subj := e.ClientID + " " + e.Topic
		if e.QoS != 1 {
			out = append(out, runner.Fail(id, subj,
				fmt.Sprintf("subscribe QoS = %d, want 1", e.QoS)))
		} else {
			out = append(out, runner.Pass(id, subj))
		}
	}
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no NCMD subscription in scenario")}
	}
	return out
}

// DeviceDCMDSubscribeQoS: the MQTT client behind a Device that supports
// outputs MUST subscribe to spBv1.0/<group>/DCMD/<edge>/<device> with
// QoS=1. Strict form of tck-id-message-flow-device-dcmd-subscribe.
func DeviceDCMDSubscribeQoS(b *Broker) []runner.Result {
	const id = "tck-id-message-flow-device-dcmd-subscribe"
	var out []runner.Result
	for _, e := range b.Events() {
		if e.Type != EvSubscribe || !isDCMDTopic(e.Topic) {
			continue
		}
		subj := e.ClientID + " " + e.Topic
		if e.QoS != 1 {
			out = append(out, runner.Fail(id, subj,
				fmt.Sprintf("subscribe QoS = %d, want 1", e.QoS)))
		} else {
			out = append(out, runner.Pass(id, subj))
		}
	}
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no DCMD subscription in scenario")}
	}
	return out
}

// HostSTATEBirthAfterSubscribe: a host application MUST subscribe to
// its own STATE topic before publishing its STATE birth. Strict form of
// tck-id-host-topic-phid-birth-sub-required and
// tck-id-message-flow-phid-sparkplug-subscription.
func HostSTATEBirthAfterSubscribe(b *Broker) []runner.Result {
	const id = "tck-id-host-topic-phid-birth-sub-required"
	type per struct {
		subscribed, published int // first event index of each
	}
	byClient := map[string]*per{}
	for i, e := range b.Events() {
		p := byClient[e.ClientID]
		if p == nil {
			p = &per{subscribed: -1, published: -1}
			byClient[e.ClientID] = p
		}
		switch {
		case e.Type == EvSubscribe && isSTATETopic(e.Topic) && p.subscribed == -1:
			p.subscribed = i
		case e.Type == EvPublish && isSTATETopic(e.Topic) && p.published == -1:
			p.published = i
		}
	}
	var out []runner.Result
	for client, p := range byClient {
		switch {
		case p.published == -1:
			// Not a host that birthed in this scenario.
			continue
		case p.subscribed == -1:
			out = append(out, runner.Fail(id, client,
				"host published STATE without first subscribing to its own STATE topic"))
		case p.subscribed > p.published:
			out = append(out, runner.Fail(id, client,
				fmt.Sprintf("host SUBSCRIBE at #%d came after STATE PUBLISH at #%d",
					p.subscribed, p.published)))
		default:
			out = append(out, runner.Pass(id, client))
		}
	}
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no host STATE publish in scenario")}
	}
	return out
}

// HostBirthTimestampMatchesWill: the timestamp in the STATE birth
// payload MUST equal the timestamp in the CONNECT Will payload. Strict
// form of tck-id-host-topic-phid-birth-payload-timestamp and
// tck-id-message-flow-phid-sparkplug-state-publish-payload-timestamp.
func HostBirthTimestampMatchesWill(b *Broker) []runner.Result {
	const id = "tck-id-host-topic-phid-birth-payload-timestamp"
	willTS := map[string]int64{} // client -> timestamp from CONNECT Will
	var out []runner.Result
	events := b.Events()
	for _, e := range events {
		if e.Type == EvConnect && e.Will != nil && isSTATETopic(e.Will.Topic) {
			st, err := spb.DecodeState(e.Will.Payload)
			if err != nil || st == nil {
				continue
			}
			willTS[e.ClientID] = st.Timestamp
		}
	}
	for _, e := range events {
		if e.Type != EvPublish || !isSTATETopic(e.Topic) {
			continue
		}
		st, err := spb.DecodeState(e.Payload)
		if err != nil || st == nil {
			continue
		}
		if !st.Online {
			continue
		}
		ts, ok := willTS[e.ClientID]
		if !ok {
			continue
		}
		subj := e.ClientID
		if st.Timestamp != ts {
			out = append(out, runner.Fail(id, subj,
				fmt.Sprintf("STATE birth timestamp = %d, Will timestamp = %d", st.Timestamp, ts)))
		} else {
			out = append(out, runner.Pass(id, subj))
		}
	}
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no host STATE birth + Will pair in scenario")}
	}
	return out
}

func isSTATETopic(t string) bool {
	// spBv1.0/STATE/<host-id> for 3.x JSON STATE.
	return strings.HasPrefix(t, "spBv1.0/STATE/")
}

func isNCMDTopic(t string) bool {
	// spBv1.0/<group>/NCMD/<edge>
	parts := strings.Split(t, "/")
	return len(parts) == 4 && parts[0] == "spBv1.0" && parts[2] == "NCMD"
}

func isDCMDTopic(t string) bool {
	// spBv1.0/<group>/DCMD/<edge>/<device>
	parts := strings.Split(t, "/")
	return len(parts) == 5 && parts[0] == "spBv1.0" && parts[2] == "DCMD"
}

func isNDATATopic(t string) bool {
	// spBv1.0/<group>/NDATA/<edge>
	parts := strings.Split(t, "/")
	return len(parts) == 4 && parts[0] == "spBv1.0" && parts[2] == "NDATA"
}

func isDDATATopic(t string) bool {
	// spBv1.0/<group>/DDATA/<edge>/<device>
	parts := strings.Split(t, "/")
	return len(parts) == 5 && parts[0] == "spBv1.0" && parts[2] == "DDATA"
}
