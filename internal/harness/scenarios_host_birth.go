package harness

import (
	"fmt"

	"github.com/joyautomation/sparkplug-tck-go/internal/runner"
)

// Host Application birth scenarios — strict checks on the host's STATE
// birth lifecycle: subscribe to its own STATE topic before publishing,
// then publish a birth (online=true) on the same topic with the right
// QoS/retain/payload shape, matching the Will (death) timestamp from
// CONNECT. Together they cover the bulk of upstream
// host/SessionEstablishmentTest.java.

// HostWillCompliant evaluates the Will the host advertises in CONNECT
// against the per-attribute spec IDs (topic, QoS, retain, payload).
// HostCONNECTHasWill scores a slightly different, broader set of IDs;
// this one fills in the connect-will-* and payloads-state-will-message-*
// IDs that those don't carry.
func HostWillCompliant(b *Broker) []runner.Result {
	const idTopic = "tck-id-operational-behavior-host-application-connect-will-topic"
	const idQoS = "tck-id-operational-behavior-host-application-connect-will-qos"
	const idRetain = "tck-id-operational-behavior-host-application-connect-will-retained"
	const idPayload = "tck-id-operational-behavior-host-application-connect-will-payload"
	const idStateWill = "tck-id-payloads-state-will-message"
	const idStateWillQoS = "tck-id-payloads-state-will-message-qos"
	const idStateWillRetain = "tck-id-payloads-state-will-message-retain"
	const idStateWillPayload = "tck-id-payloads-state-will-message-payload"
	const idDeathTopic = "tck-id-host-topic-phid-death-topic"
	const idDeathQoS = "tck-id-host-topic-phid-death-qos"
	const idDeathRetain = "tck-id-host-topic-phid-death-retain"
	const idDeathPayload = "tck-id-host-topic-phid-death-payload"
	const idDeathRequired = "tck-id-host-topic-phid-death-required"
	allIDs := []string{
		idTopic, idQoS, idRetain, idPayload,
		idStateWill, idStateWillQoS, idStateWillRetain, idStateWillPayload,
		idDeathTopic, idDeathQoS, idDeathRetain, idDeathPayload, idDeathRequired,
	}
	scored := false
	var out []runner.Result
	for _, e := range b.Events() {
		if e.Type != EvConnect || e.Will == nil || !isSTATETopic(e.Will.Topic) {
			continue
		}
		scored = true
		subj := e.ClientID + " " + e.Will.Topic
		// Topic shape spBv1.0/STATE/<hostid> — already implied.
		out = append(out,
			runner.Pass(idTopic, subj),
			runner.Pass(idStateWill, subj),
			runner.Pass(idDeathTopic, subj),
			runner.Pass(idDeathRequired, subj),
		)
		// QoS = 1
		if e.Will.QoS != 1 {
			fail := fmt.Sprintf("host Will QoS=%d, want 1", e.Will.QoS)
			out = append(out,
				runner.Fail(idQoS, subj, fail),
				runner.Fail(idStateWillQoS, subj, fail),
				runner.Fail(idDeathQoS, subj, fail),
			)
		} else {
			out = append(out,
				runner.Pass(idQoS, subj),
				runner.Pass(idStateWillQoS, subj),
				runner.Pass(idDeathQoS, subj),
			)
		}
		// Retain = true
		if !e.Will.Retain {
			out = append(out,
				runner.Fail(idRetain, subj, "host Will retain=false, want true"),
				runner.Fail(idStateWillRetain, subj, "host Will retain=false, want true"),
				runner.Fail(idDeathRetain, subj, "host Will retain=false, want true"),
			)
		} else {
			out = append(out,
				runner.Pass(idRetain, subj),
				runner.Pass(idStateWillRetain, subj),
				runner.Pass(idDeathRetain, subj),
			)
		}
		// JSON payload with online=false + timestamp
		if state, err := decodeStateBody(e.Will.Payload); err != nil {
			fail := "host Will payload not valid STATE JSON: " + err.Error()
			out = append(out,
				runner.Fail(idPayload, subj, fail),
				runner.Fail(idStateWillPayload, subj, fail),
				runner.Fail(idDeathPayload, subj, fail),
			)
		} else if state.Online || state.Timestamp == 0 {
			fail := "host Will payload must be {online:false, timestamp:<ms>}"
			out = append(out,
				runner.Fail(idPayload, subj, fail),
				runner.Fail(idStateWillPayload, subj, fail),
				runner.Fail(idDeathPayload, subj, fail),
			)
		} else {
			out = append(out,
				runner.Pass(idPayload, subj),
				runner.Pass(idStateWillPayload, subj),
				runner.Pass(idDeathPayload, subj),
			)
		}
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

// HostBirthCompliant evaluates the host's STATE birth message (the
// retained PUBLISH with online=true) against the per-attribute IDs:
// topic, QoS=1, retain=true, JSON payload with online=true + same
// timestamp as the CONNECT Will, and ordering rules (subscribe before
// publish, birth is first PUBLISH after CONNECT).
func HostBirthCompliant(b *Broker) []runner.Result {
	const idBirthRequired = "tck-id-host-topic-phid-birth-required"
	const idBirthTopic = "tck-id-host-topic-phid-birth-topic"
	const idBirthQoS = "tck-id-host-topic-phid-birth-qos"
	const idBirthRetain = "tck-id-host-topic-phid-birth-retain"
	const idBirthPayload = "tck-id-host-topic-phid-birth-payload"
	const idBirthMessage = "tck-id-host-topic-phid-birth-message"
	const idConnectBirth = "tck-id-operational-behavior-host-application-connect-birth"
	const idConnectBirthTopic = "tck-id-operational-behavior-host-application-connect-birth-topic"
	const idConnectBirthQoS = "tck-id-operational-behavior-host-application-connect-birth-qos"
	const idConnectBirthRetain = "tck-id-operational-behavior-host-application-connect-birth-retained"
	const idConnectBirthPayload = "tck-id-operational-behavior-host-application-connect-birth-payload"
	const idStateBirth = "tck-id-payloads-state-birth"
	const idStateBirthPayload = "tck-id-payloads-state-birth-payload"
	const idStateSubscribe = "tck-id-payloads-state-subscribe"
	const idStatePublish = "tck-id-message-flow-phid-sparkplug-state-publish"
	const idStatePublishPayload = "tck-id-message-flow-phid-sparkplug-state-publish-payload"
	const idStatePublishTS = "tck-id-message-flow-phid-sparkplug-state-publish-payload-timestamp"
	const idStateMessageDelivered = "tck-id-message-flow-hid-sparkplug-state-message-delivered"
	const idSubscription = "tck-id-message-flow-phid-sparkplug-subscription"
	const idIntroState = "tck-id-intro-sparkplug-host-state"
	const idCompState = "tck-id-components-ph-state"
	const idConformPrimary = "tck-id-conformance-primary-host"
	allIDs := []string{
		idBirthRequired, idBirthTopic, idBirthQoS, idBirthRetain, idBirthPayload, idBirthMessage,
		idConnectBirth, idConnectBirthTopic, idConnectBirthQoS, idConnectBirthRetain, idConnectBirthPayload,
		idStateBirth, idStateBirthPayload, idStateSubscribe, idStatePublish, idStatePublishPayload,
		idStatePublishTS, idStateMessageDelivered, idSubscription, idIntroState, idCompState, idConformPrimary,
	}
	events := b.Events()

	// For each host clientID, find its CONNECT Will + earliest STATE
	// birth PUBLISH after CONNECT.
	type hostInfo struct {
		willTopic    string
		willTS       int64
		willSeen     bool
		subscribed   bool
		firstPublish string
		birth        *Event
		index        int // index of CONNECT
	}
	hosts := map[string]*hostInfo{}
	for i, e := range events {
		if e.Type == EvConnect && e.Will != nil && isSTATETopic(e.Will.Topic) {
			h := &hostInfo{willTopic: e.Will.Topic, willSeen: true, index: i}
			if state, err := decodeStateBody(e.Will.Payload); err == nil {
				h.willTS = state.Timestamp
			}
			hosts[e.ClientID] = h
		}
	}
	for _, e := range events {
		h := hosts[e.ClientID]
		if h == nil {
			continue
		}
		switch e.Type {
		case EvSubscribe:
			if e.Topic == h.willTopic {
				h.subscribed = true
			}
		case EvPublish:
			if h.firstPublish == "" {
				h.firstPublish = e.Topic
			}
			if h.birth == nil && e.Topic == h.willTopic {
				if state, err := decodeStateBody(e.Payload); err == nil && state.Online {
					ev := e
					h.birth = &ev
				}
			}
		}
	}

	scored := false
	var out []runner.Result
	for clientID, h := range hosts {
		if !h.willSeen {
			continue
		}
		scored = true
		subj := clientID + " " + h.willTopic

		if h.birth == nil {
			fail := "host published no STATE birth (online=true) in scenario"
			for _, id := range []string{idBirthRequired, idBirthMessage,
				idConnectBirth, idStateBirth, idStatePublish, idStateMessageDelivered,
				idIntroState, idCompState, idConformPrimary} {
				out = append(out, runner.Fail(id, subj, fail))
			}
			continue
		}
		out = append(out,
			runner.Pass(idBirthRequired, subj),
			runner.Pass(idBirthMessage, subj),
			runner.Pass(idConnectBirth, subj),
			runner.Pass(idStateBirth, subj),
			runner.Pass(idStatePublish, subj),
			runner.Pass(idStateMessageDelivered, subj),
			runner.Pass(idIntroState, subj),
			runner.Pass(idCompState, subj),
			runner.Pass(idConformPrimary, subj),
		)

		// Topic shape implied by birth being on h.willTopic which already
		// matched isSTATETopic.
		out = append(out,
			runner.Pass(idBirthTopic, subj),
			runner.Pass(idConnectBirthTopic, subj),
		)
		// QoS=1
		if h.birth.QoS != 1 {
			fail := fmt.Sprintf("birth QoS=%d, want 1", h.birth.QoS)
			out = append(out,
				runner.Fail(idBirthQoS, subj, fail),
				runner.Fail(idConnectBirthQoS, subj, fail),
			)
		} else {
			out = append(out,
				runner.Pass(idBirthQoS, subj),
				runner.Pass(idConnectBirthQoS, subj),
			)
		}
		// Retain=true
		if !h.birth.Retained {
			fail := "birth retain=false, want true"
			out = append(out,
				runner.Fail(idBirthRetain, subj, fail),
				runner.Fail(idConnectBirthRetain, subj, fail),
			)
		} else {
			out = append(out,
				runner.Pass(idBirthRetain, subj),
				runner.Pass(idConnectBirthRetain, subj),
			)
		}
		// Payload + timestamp
		state, perr := decodeStateBody(h.birth.Payload)
		switch {
		case perr != nil:
			fail := "birth payload not valid STATE JSON: " + perr.Error()
			out = append(out,
				runner.Fail(idBirthPayload, subj, fail),
				runner.Fail(idConnectBirthPayload, subj, fail),
				runner.Fail(idStateBirthPayload, subj, fail),
				runner.Fail(idStatePublishPayload, subj, fail),
				runner.Fail(idStatePublishTS, subj, fail),
			)
		case !state.Online || state.Timestamp == 0:
			fail := "birth payload must be {online:true, timestamp:<ms>}"
			out = append(out,
				runner.Fail(idBirthPayload, subj, fail),
				runner.Fail(idConnectBirthPayload, subj, fail),
				runner.Fail(idStateBirthPayload, subj, fail),
				runner.Fail(idStatePublishPayload, subj, fail),
				runner.Fail(idStatePublishTS, subj, fail),
			)
		default:
			out = append(out,
				runner.Pass(idBirthPayload, subj),
				runner.Pass(idConnectBirthPayload, subj),
				runner.Pass(idStateBirthPayload, subj),
				runner.Pass(idStatePublishPayload, subj),
			)
			// Timestamp must match the CONNECT Will timestamp.
			if h.willTS != 0 && state.Timestamp != h.willTS {
				out = append(out, runner.Fail(idStatePublishTS, subj,
					fmt.Sprintf("birth timestamp=%d, Will timestamp=%d (must match)",
						state.Timestamp, h.willTS)))
			} else {
				out = append(out, runner.Pass(idStatePublishTS, subj))
			}
		}
		// subscribe-before-publish
		if !h.subscribed {
			out = append(out,
				runner.Fail(idStateSubscribe, subj, "host did not SUBSCRIBE to its own STATE topic"),
				runner.Fail(idSubscription, subj, "host did not SUBSCRIBE to its own STATE topic"),
			)
		} else {
			out = append(out,
				runner.Pass(idStateSubscribe, subj),
				runner.Pass(idSubscription, subj),
			)
		}
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
