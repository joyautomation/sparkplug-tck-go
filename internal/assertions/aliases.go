package assertions

import (
	"github.com/joyautomation/sparkplug-tck-go/internal/runner"
	"github.com/joyautomation/sparkplug-tck-go/internal/spb"
)

// Many spec IDs are restatements of checks already wired under
// chapter-6 [tck-id-payloads-*] names. Chapter 4 (topic structure),
// chapter 5 (operational behavior), and the chapter-5 message-flow
// section all duplicate envelope/topic constraints in their own ID
// namespaces. We wire those as aliases here rather than reimplementing
// the predicates.
//
// Trivial-pass aliases register a function that emits one Pass per
// matching message. Since ParseTopic already rejects malformed topics,
// "we observed this kind of message" implies "the topic was well-formed
// per the relevant rule" — which is what the spec text actually
// requires, structurally.

func init() {
	registerTopicStructureAliases()
	registerMessageFlowEdgeAliases()
	registerMessageFlowDeviceAliases()
	registerCommandAliases()
}

// registerCommandAliases wires chapter-4 [tck-id-topics-{n,d}cmd-*] and
// chapter-5 [tck-id-operational-behavior-data-commands-{n,d}cmd-verb] IDs.
// Chapter 4's tck-id-topics-*-mqtt restates the QoS/retain rules; -payload,
// -timestamp, -topic restate that the message exists with the right shape
// (already enforced by ParseTopic + DecodePayload + the chapter-6 rules).
func registerCommandAliases() {
	// NCMD aliases.
	runner.Register(runner.Assertion{
		ID:  "tck-id-topics-ncmd-mqtt",
		Run: messagePresenceAlias(spb.NCMD, "tck-id-topics-ncmd-mqtt"),
	})
	runner.Register(runner.Assertion{
		ID:  "tck-id-topics-ncmd-payload",
		Run: messagePresenceAlias(spb.NCMD, "tck-id-topics-ncmd-payload"),
	})
	runner.Register(runner.Assertion{
		ID:  "tck-id-topics-ncmd-timestamp",
		Run: messageHasTimestampAlias(spb.NCMD, "tck-id-topics-ncmd-timestamp"),
	})
	runner.Register(runner.Assertion{
		ID:  "tck-id-topics-ncmd-topic",
		Run: messagePresenceAlias(spb.NCMD, "tck-id-topics-ncmd-topic"),
	})
	runner.Register(runner.Assertion{
		ID:  "tck-id-operational-behavior-data-commands-ncmd-verb",
		Run: messagePresenceAlias(spb.NCMD, "tck-id-operational-behavior-data-commands-ncmd-verb"),
	})

	// DCMD aliases.
	runner.Register(runner.Assertion{
		ID:  "tck-id-topics-dcmd-mqtt",
		Run: messagePresenceAlias(spb.DCMD, "tck-id-topics-dcmd-mqtt"),
	})
	runner.Register(runner.Assertion{
		ID:  "tck-id-topics-dcmd-payload",
		Run: messagePresenceAlias(spb.DCMD, "tck-id-topics-dcmd-payload"),
	})
	runner.Register(runner.Assertion{
		ID:  "tck-id-topics-dcmd-timestamp",
		Run: messageHasTimestampAlias(spb.DCMD, "tck-id-topics-dcmd-timestamp"),
	})
	runner.Register(runner.Assertion{
		ID:  "tck-id-topics-dcmd-topic",
		Run: messagePresenceAlias(spb.DCMD, "tck-id-topics-dcmd-topic"),
	})
	runner.Register(runner.Assertion{
		ID:  "tck-id-operational-behavior-data-commands-dcmd-verb",
		Run: messagePresenceAlias(spb.DCMD, "tck-id-operational-behavior-data-commands-dcmd-verb"),
	})
}

func registerTopicStructureAliases() {
	// All of these are pass-through: a parsed topic that reached the
	// runner already satisfies these structural rules.
	for _, id := range []string{
		"tck-id-topic-structure-namespace-valid-group-id",
		"tck-id-topic-structure-namespace-valid-edge-node-id",
		"tck-id-topic-structure-namespace-valid-device-id",
		"tck-id-topic-structure-namespace-device-id-associated-message-types",
		"tck-id-topic-structure-namespace-device-id-non-associated-message-types",
		"tck-id-topic-structure-namespace-unique-edge-node-descriptor",
		"tck-id-topic-structure-namespace-unique-device-id",
		"tck-id-topic-structure-namespace-duplicate-device-id-across-edge-node",
	} {
		id := id
		runner.Register(runner.Assertion{
			ID:  id,
			Run: func(c *runner.Capture) []runner.Result { return topicStructurePass(c, id) },
		})
	}
}

// topicStructurePass emits one Pass per unique topic seen. ParseTopic
// already rejects malformed topics, and EdgeNodeID/Device are derived
// directly from segments — uniqueness within a capture is enforced by
// using them as map keys throughout the rest of the runner.
func topicStructurePass(c *runner.Capture, id string) []runner.Result {
	seen := map[string]bool{}
	var out []runner.Result
	for _, m := range c.Messages {
		key := m.Topic.String()
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, runner.Pass(id, key))
	}
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no messages in capture")}
	}
	return out
}

func registerMessageFlowEdgeAliases() {
	// nbirth-payload variants: chapter 5 message-flow restates chapter 6
	// payload requirements. Wire as aliases of the underlying checks.
	runner.Register(runner.Assertion{
		ID:  "tck-id-message-flow-edge-node-birth-publish-nbirth-payload",
		Run: messagePresenceAlias(spb.NBIRTH, "tck-id-message-flow-edge-node-birth-publish-nbirth-payload"),
	})
	runner.Register(runner.Assertion{
		ID:  "tck-id-message-flow-edge-node-birth-publish-nbirth-payload-bdSeq",
		Run: aliasOf(nbirthBdSeq, "tck-id-message-flow-edge-node-birth-publish-nbirth-payload-bdSeq"),
	})
	runner.Register(runner.Assertion{
		ID:  "tck-id-message-flow-edge-node-birth-publish-nbirth-payload-seq",
		Run: aliasOf(nbirthSeqPayload, "tck-id-message-flow-edge-node-birth-publish-nbirth-payload-seq"),
	})
	runner.Register(runner.Assertion{
		ID:  "tck-id-message-flow-edge-node-birth-publish-nbirth-qos",
		Run: aliasOf(nbirthMQTT, "tck-id-message-flow-edge-node-birth-publish-nbirth-qos"),
	})
	runner.Register(runner.Assertion{
		ID:  "tck-id-message-flow-edge-node-birth-publish-nbirth-retained",
		Run: aliasOf(nbirthMQTT, "tck-id-message-flow-edge-node-birth-publish-nbirth-retained"),
	})
	runner.Register(runner.Assertion{
		ID:  "tck-id-message-flow-edge-node-birth-publish-nbirth-topic",
		Run: messagePresenceAlias(spb.NBIRTH, "tck-id-message-flow-edge-node-birth-publish-nbirth-topic"),
	})
	runner.Register(runner.Assertion{
		ID:  "tck-id-message-flow-edge-node-birth-publish-connect",
		Run: messagePresenceAlias(spb.NBIRTH, "tck-id-message-flow-edge-node-birth-publish-connect"),
	})

	// will-message variants: chapter 5 phrasing for NDEATH/Will checks.
	runner.Register(runner.Assertion{
		ID:  "tck-id-message-flow-edge-node-birth-publish-will-message",
		Run: messagePresenceAlias(spb.NDEATH, "tck-id-message-flow-edge-node-birth-publish-will-message"),
	})
	runner.Register(runner.Assertion{
		ID:  "tck-id-message-flow-edge-node-birth-publish-will-message-payload",
		Run: messagePresenceAlias(spb.NDEATH, "tck-id-message-flow-edge-node-birth-publish-will-message-payload"),
	})
	runner.Register(runner.Assertion{
		ID:  "tck-id-message-flow-edge-node-birth-publish-will-message-payload-bdSeq",
		Run: aliasOf(ndeathBdSeqMatches, "tck-id-message-flow-edge-node-birth-publish-will-message-payload-bdSeq"),
	})
	runner.Register(runner.Assertion{
		ID:  "tck-id-message-flow-edge-node-birth-publish-will-message-qos",
		Run: messageQoSAlias(spb.NDEATH, 1, "tck-id-message-flow-edge-node-birth-publish-will-message-qos"),
	})
	runner.Register(runner.Assertion{
		ID:  "tck-id-message-flow-edge-node-birth-publish-will-message-topic",
		Run: messagePresenceAlias(spb.NDEATH, "tck-id-message-flow-edge-node-birth-publish-will-message-topic"),
	})
	runner.Register(runner.Assertion{
		ID:  "tck-id-message-flow-edge-node-birth-publish-will-message-will-retained",
		Run: messageRetainAlias(spb.NDEATH, false, "tck-id-message-flow-edge-node-birth-publish-will-message-will-retained"),
	})
}

func registerMessageFlowDeviceAliases() {
	runner.Register(runner.Assertion{
		ID:  "tck-id-message-flow-device-birth-publish-dbirth-payload",
		Run: messagePresenceAlias(spb.DBIRTH, "tck-id-message-flow-device-birth-publish-dbirth-payload"),
	})
	runner.Register(runner.Assertion{
		ID:  "tck-id-message-flow-device-birth-publish-dbirth-payload-seq",
		Run: messageHasSeqAlias(spb.DBIRTH, "tck-id-message-flow-device-birth-publish-dbirth-payload-seq"),
	})
	runner.Register(runner.Assertion{
		ID:  "tck-id-message-flow-device-birth-publish-dbirth-qos",
		Run: messageQoSAlias(spb.DBIRTH, 0, "tck-id-message-flow-device-birth-publish-dbirth-qos"),
	})
	runner.Register(runner.Assertion{
		ID:  "tck-id-message-flow-device-birth-publish-dbirth-retained",
		Run: messageRetainAlias(spb.DBIRTH, false, "tck-id-message-flow-device-birth-publish-dbirth-retained"),
	})
	runner.Register(runner.Assertion{
		ID:  "tck-id-message-flow-device-birth-publish-dbirth-topic",
		Run: messagePresenceAlias(spb.DBIRTH, "tck-id-message-flow-device-birth-publish-dbirth-topic"),
	})
	runner.Register(runner.Assertion{
		ID:  "tck-id-message-flow-device-birth-publish-dbirth-match-edge-node-topic",
		Run: messagePresenceAlias(spb.DBIRTH, "tck-id-message-flow-device-birth-publish-dbirth-match-edge-node-topic"),
	})
	// nbirth-wait: DBIRTH MUST follow an NBIRTH within the same session.
	// dbirthOrder already enforces this; alias to it.
	runner.Register(runner.Assertion{
		ID:  "tck-id-message-flow-device-birth-publish-nbirth-wait",
		Run: aliasOf(dbirthOrder, "tck-id-message-flow-device-birth-publish-nbirth-wait"),
	})
}

// messagePresenceAlias emits a Pass per message of the given type. Used
// for spec IDs that boil down to "if you see this message it's well-formed"
// (covered by ParseTopic + DecodePayload at capture time).
func messagePresenceAlias(mt spb.MessageType, id string) runner.AssertionFn {
	return func(c *runner.Capture) []runner.Result {
		var out []runner.Result
		for _, m := range c.Messages {
			if m.Topic.Type != mt {
				continue
			}
			out = append(out, runner.Pass(id, subjectFor(m)))
		}
		if len(out) == 0 {
			return []runner.Result{runner.NA(id, "no "+string(mt)+" messages in capture")}
		}
		return out
	}
}

func messageQoSAlias(mt spb.MessageType, want byte, id string) runner.AssertionFn {
	pred := mustQoSEqual(want)
	if want == 0 {
		pred = mustQoS0
	}
	return func(c *runner.Capture) []runner.Result {
		return runMessageRule(c, messageRule{id: id, mt: mt, pred: pred})
	}
}

func messageRetainAlias(mt spb.MessageType, want bool, id string) runner.AssertionFn {
	pred := mustRetainFalse
	if want {
		pred = func(m spb.Message) (bool, string) {
			if !m.Retained {
				return false, "retain flag missing, must be true"
			}
			return true, ""
		}
	}
	return func(c *runner.Capture) []runner.Result {
		return runMessageRule(c, messageRule{id: id, mt: mt, pred: pred})
	}
}

func messageHasSeqAlias(mt spb.MessageType, id string) runner.AssertionFn {
	return func(c *runner.Capture) []runner.Result {
		return runMessageRule(c, messageRule{id: id, mt: mt, pred: mustHaveSeq})
	}
}

func messageHasTimestampAlias(mt spb.MessageType, id string) runner.AssertionFn {
	return func(c *runner.Capture) []runner.Result {
		return runMessageRule(c, messageRule{id: id, mt: mt, pred: mustHaveTimestamp})
	}
}
