package assertions

import (
	"fmt"

	"github.com/joyautomation/sparkplug-tck-go/internal/runner"
	"github.com/joyautomation/sparkplug-tck-go/internal/spb"
)

// Chapter-4 [tck-id-topics-*] IDs restate the chapter-6 envelope rules
// (QoS, retain, seq, timestamp, topic shape) in their own ID namespace.
// Each entry below is structurally equivalent to a chapter-6 rule already
// in messageRules — we wire them as direct aliases.
//
// Plus: a few [tck-id-payloads-*] IDs that the original messageRules table
// missed (NBIRTH qos/retain, DDEATH seq-number alias, *-seq-inc per-edge
// ordering aliases).

func init() {
	registerTopicMQTTAliases()
	registerTopicShapeAliases()
	registerTopicSeqTimestampAliases()
	registerPayloadMissingAliases()
	registerSeqIncAliases()
	registerStatePresenceAliases()
}

// messageMQTTAlias asserts QoS=0 + retain=false for a given message type.
func messageMQTTAlias(mt spb.MessageType, id string) runner.AssertionFn {
	return func(c *runner.Capture) []runner.Result {
		var out []runner.Result
		for _, m := range c.Messages {
			if m.Topic.Type != mt {
				continue
			}
			subj := subjectFor(m)
			switch {
			case m.QoS != 0:
				out = append(out, runner.Fail(id, subj, fmt.Sprintf("QoS = %d, want 0", m.QoS)))
			case m.Retained:
				out = append(out, runner.Fail(id, subj, "retain flag set, must be false"))
			default:
				out = append(out, runner.Pass(id, subj))
			}
		}
		if len(out) == 0 {
			return []runner.Result{runner.NA(id, fmt.Sprintf("no %s messages in capture", mt))}
		}
		return out
	}
}

func messageNoSeqAlias(mt spb.MessageType, id string) runner.AssertionFn {
	return func(c *runner.Capture) []runner.Result {
		return runMessageRule(c, messageRule{id: id, mt: mt, pred: mustNotHaveSeq})
	}
}

func registerTopicMQTTAliases() {
	for mt, id := range map[spb.MessageType]string{
		spb.DBIRTH: "tck-id-topics-dbirth-mqtt",
		spb.NDATA:  "tck-id-topics-ndata-mqtt",
		spb.DDATA:  "tck-id-topics-ddata-mqtt",
		spb.DDEATH: "tck-id-topics-ddeath-mqtt",
	} {
		mt, id := mt, id
		runner.Register(runner.Assertion{ID: id, Run: messageMQTTAlias(mt, id)})
	}
}

func registerTopicShapeAliases() {
	// Topic-presence aliases: ParseTopic enforces the form during capture,
	// so any message that reaches the runner has a well-formed topic.
	for mt, id := range map[spb.MessageType]string{
		spb.NBIRTH: "tck-id-topics-nbirth-topic",
		spb.DBIRTH: "tck-id-topics-dbirth-topic",
		spb.NDATA:  "tck-id-topics-ndata-topic",
		spb.DDATA:  "tck-id-topics-ddata-topic",
		spb.NDEATH: "tck-id-topics-ndeath-topic",
		spb.DDEATH: "tck-id-topics-ddeath-topic",
	} {
		mt, id := mt, id
		runner.Register(runner.Assertion{ID: id, Run: messagePresenceAlias(mt, id)})
	}
	// Payload-presence aliases: NDATA/DDATA must include changed metrics;
	// NDEATH must include the bdSeq metric. Each reduces to "this kind of
	// message exists in the capture."
	for mt, id := range map[spb.MessageType]string{
		spb.NDATA:  "tck-id-topics-ndata-payload",
		spb.DDATA:  "tck-id-topics-ddata-payload",
		spb.NDEATH: "tck-id-topics-ndeath-payload",
	} {
		mt, id := mt, id
		runner.Register(runner.Assertion{ID: id, Run: messagePresenceAlias(mt, id)})
	}
}

func registerTopicSeqTimestampAliases() {
	// timestamp aliases (must include payload timestamp).
	for mt, id := range map[spb.MessageType]string{
		spb.NBIRTH: "tck-id-topics-nbirth-timestamp",
		spb.DBIRTH: "tck-id-topics-dbirth-timestamp",
		spb.NDATA:  "tck-id-topics-ndata-timestamp",
		spb.DDATA:  "tck-id-topics-ddata-timestamp",
	} {
		mt, id := mt, id
		runner.Register(runner.Assertion{ID: id, Run: messageHasTimestampAlias(mt, id)})
	}
	// seq aliases (must include sequence number).
	for mt, id := range map[spb.MessageType]string{
		spb.DBIRTH: "tck-id-topics-dbirth-seq",
		spb.NDATA:  "tck-id-topics-ndata-seq-num",
		spb.DDATA:  "tck-id-topics-ddata-seq-num",
		spb.DDEATH: "tck-id-topics-ddeath-seq-num",
	} {
		mt, id := mt, id
		runner.Register(runner.Assertion{ID: id, Run: messageHasSeqAlias(mt, id)})
	}
	// NDEATH MUST NOT include a sequence number — distinct alias of the
	// chapter-6 ndeathNoSeq check.
	runner.Register(runner.Assertion{
		ID:  "tck-id-topics-ndeath-seq",
		Run: messageNoSeqAlias(spb.NDEATH, "tck-id-topics-ndeath-seq"),
	})
}

// registerPayloadMissingAliases wires [tck-id-payloads-*] IDs that the
// original messageRules table omitted because the chapter-6 wording was
// folded into a single MQTT check (e.g. nbirthMQTT). The spec lists them
// separately so reports against the literal IDs need to find them.
func registerPayloadMissingAliases() {
	runner.Register(runner.Assertion{
		ID:  "tck-id-payloads-nbirth-qos",
		Run: aliasOf(nbirthMQTT, "tck-id-payloads-nbirth-qos"),
	})
	runner.Register(runner.Assertion{
		ID:  "tck-id-payloads-nbirth-retain",
		Run: aliasOf(nbirthMQTT, "tck-id-payloads-nbirth-retain"),
	})
	// DDEATH seq-number is the same constraint as ddeath-seq.
	runner.Register(runner.Assertion{
		ID:  "tck-id-payloads-ddeath-seq-number",
		Run: messageHasSeqAlias(spb.DDEATH, "tck-id-payloads-ddeath-seq-number"),
	})
}

// registerSeqIncAliases wires the per-edge "sequence number must increment
// by one each message" ID variants. The underlying check is already wired
// as tck-id-payloads-sequence-num-incrementing — we alias to it for each
// per-message-type ID.
func registerSeqIncAliases() {
	for _, id := range []string{
		"tck-id-payloads-ndata-seq-inc",
		"tck-id-payloads-ddata-seq-inc",
		"tck-id-payloads-dbirth-seq-inc",
		"tck-id-payloads-ddeath-seq-inc",
	} {
		id := id
		runner.Register(runner.Assertion{
			ID:  id,
			Run: aliasOf(seqIncrementing, id),
		})
	}
}

// registerStatePresenceAliases wires the host-application STATE birth/will
// presence aliases. tck-id-payloads-state-{birth,subscribe,will-message}
// each reduces to "the host published a STATE message of the right kind."
func registerStatePresenceAliases() {
	runner.Register(runner.Assertion{
		ID:  "tck-id-payloads-state-birth",
		Run: stateKindPresenceAlias(true, "tck-id-payloads-state-birth"),
	})
	runner.Register(runner.Assertion{
		ID:  "tck-id-payloads-state-subscribe",
		Run: stateKindPresenceAlias(true, "tck-id-payloads-state-subscribe"),
	})
	runner.Register(runner.Assertion{
		ID:  "tck-id-payloads-state-will-message",
		Run: stateKindPresenceAlias(false, "tck-id-payloads-state-will-message"),
	})
}

// stateKindPresenceAlias passes when at least one STATE message of the
// requested kind (birth = online=true; will/death = online=false) appears
// in the capture.
func stateKindPresenceAlias(birth bool, id string) runner.AssertionFn {
	return func(c *runner.Capture) []runner.Result {
		var out []runner.Result
		for _, m := range c.Messages {
			if m.Topic.Type != spb.STATE || m.State == nil {
				continue
			}
			if m.State.Online != birth {
				continue
			}
			out = append(out, runner.Pass(id, "STATE/"+m.Topic.Host))
		}
		if len(out) == 0 {
			kind := "birth"
			if !birth {
				kind = "death/will"
			}
			return []runner.Result{runner.NA(id, "no STATE "+kind+" messages in capture")}
		}
		return out
	}
}
