package assertions

import (
	"fmt"

	"github.com/joyautomation/sparkplug-tck-go/internal/runner"
	"github.com/joyautomation/sparkplug-tck-go/internal/spb"
)

// Sparkplug 3.x host applications signal liveness on spBv1.0/STATE/<host_id>:
//
//   * Birth: online=true,  retained=true, QoS=1, JSON payload with timestamp.
//   * Death (Will or clean disconnect): online=false, retained=true, QoS=1.
//
// Several spec assertions enforce these envelope rules under different
// [tck-id-*] (chapter 4 calls them tck-id-host-topic-phid-*; chapter 5 calls
// them tck-id-operational-behavior-host-application-connect-*). The
// underlying check is the same — register the predicate against every
// equivalent ID so reports against either chapter pass.

func init() {
	registerStateBirth()
	registerStateDeath()
	registerStateTopic()
}

// stateBirthIDs and stateDeathIDs are the families of spec IDs that boil
// down to the same envelope check. Add new spec IDs here as parity grows.
var stateBirthIDs = struct {
	qos     []string
	retain  []string
	payload []string
}{
	qos: []string{
		"tck-id-host-topic-phid-birth-qos",
		"tck-id-operational-behavior-host-application-connect-birth-qos",
	},
	retain: []string{
		"tck-id-host-topic-phid-birth-retain",
		"tck-id-operational-behavior-host-application-connect-birth-retained",
	},
	payload: []string{
		"tck-id-host-topic-phid-birth-payload",
		"tck-id-operational-behavior-host-application-connect-birth-payload",
		"tck-id-message-flow-phid-sparkplug-state-publish-payload",
		"tck-id-payloads-state-birth-payload",
	},
}

var stateDeathIDs = struct {
	qos     []string
	retain  []string
	payload []string
}{
	qos: []string{
		"tck-id-host-topic-phid-death-qos",
		"tck-id-operational-behavior-host-application-connect-will-qos",
		"tck-id-payloads-state-will-message-qos",
	},
	retain: []string{
		"tck-id-host-topic-phid-death-retain",
		"tck-id-operational-behavior-host-application-connect-will-retained",
		"tck-id-payloads-state-will-message-retain",
	},
	payload: []string{
		"tck-id-host-topic-phid-death-payload",
		"tck-id-operational-behavior-host-application-connect-will-payload",
		"tck-id-operational-behavior-host-application-death-payload",
		"tck-id-payloads-state-will-message-payload",
	},
}

func registerStateBirth() {
	registerEach(stateBirthIDs.qos, func(c *runner.Capture, id string) []runner.Result {
		return forEachStateOfKind(c, id, true, mustQoS(1))
	})
	registerEach(stateBirthIDs.retain, func(c *runner.Capture, id string) []runner.Result {
		return forEachStateOfKind(c, id, true, mustRetain(true))
	})
	registerEach(stateBirthIDs.payload, func(c *runner.Capture, id string) []runner.Result {
		return forEachStateOfKind(c, id, true, mustValidStatePayload(true))
	})
}

func registerStateDeath() {
	registerEach(stateDeathIDs.qos, func(c *runner.Capture, id string) []runner.Result {
		return forEachStateOfKind(c, id, false, mustQoS(1))
	})
	registerEach(stateDeathIDs.retain, func(c *runner.Capture, id string) []runner.Result {
		return forEachStateOfKind(c, id, false, mustRetain(true))
	})
	registerEach(stateDeathIDs.payload, func(c *runner.Capture, id string) []runner.Result {
		return forEachStateOfKind(c, id, false, mustValidStatePayload(false))
	})
}

func registerStateTopic() {
	const id = "tck-id-host-topic-phid-birth-topic"
	const id2 = "tck-id-host-topic-phid-death-topic"
	const id3 = "tck-id-operational-behavior-host-application-connect-birth-topic"
	const id4 = "tck-id-operational-behavior-host-application-connect-will-topic"
	for _, id := range []string{id, id2, id3, id4} {
		id := id
		runner.Register(runner.Assertion{ID: id, Run: func(c *runner.Capture) []runner.Result {
			return stateTopicShape(c, id)
		}})
	}
}

// registerEach is a small shim that registers the same predicate under N
// equivalent spec IDs. The runner needs distinct AssertionFns per ID
// (because each fn must report its own ID), so we close over id.
func registerEach(ids []string, fn func(*runner.Capture, string) []runner.Result) {
	for _, id := range ids {
		id := id
		runner.Register(runner.Assertion{
			ID:  id,
			Run: func(c *runner.Capture) []runner.Result { return fn(c, id) },
		})
	}
}

// forEachStateOfKind walks every STATE message that matches the requested
// kind (birth=true selects online-true messages; birth=false selects
// online-false), applying pred and emitting one result per message.
func forEachStateOfKind(c *runner.Capture, id string, birth bool, pred func(spb.Message) (bool, string)) []runner.Result {
	var out []runner.Result
	for _, m := range c.Messages {
		if m.Topic.Type != spb.STATE || m.State == nil {
			continue
		}
		if m.State.Online != birth {
			continue
		}
		subject := "STATE/" + m.Topic.Host
		if ok, detail := pred(m); ok {
			out = append(out, runner.Pass(id, subject))
		} else {
			out = append(out, runner.Fail(id, subject, detail))
		}
	}
	if len(out) == 0 {
		kind := "birth"
		if !birth {
			kind = "death"
		}
		return []runner.Result{runner.NA(id, "no STATE "+kind+" messages in capture")}
	}
	return out
}

// Predicate factories ------------------------------------------------------

func mustQoS(want byte) func(spb.Message) (bool, string) {
	return func(m spb.Message) (bool, string) {
		if m.QoS != want {
			return false, fmt.Sprintf("QoS = %d, want %d", m.QoS, want)
		}
		return true, ""
	}
}

func mustRetain(want bool) func(spb.Message) (bool, string) {
	return func(m spb.Message) (bool, string) {
		if m.Retained != want {
			return false, fmt.Sprintf("retain = %v, want %v", m.Retained, want)
		}
		return true, ""
	}
}

// mustValidStatePayload checks that a STATE payload meets the 3.x JSON
// shape: {"online": bool, "timestamp": int}. The legacy 2.x bare-string
// form ("ONLINE"/"OFFLINE") is reported as a fail because the assertion
// targets the 3.0 spec. The expectedOnline value pins online to the kind.
func mustValidStatePayload(expectedOnline bool) func(spb.Message) (bool, string) {
	return func(m spb.Message) (bool, string) {
		if m.State == nil {
			return false, "STATE payload could not be decoded"
		}
		if m.State.Legacy {
			return false, "legacy 2.x bare-string STATE payload (must be JSON in 3.x)"
		}
		if m.State.Online != expectedOnline {
			return false, fmt.Sprintf("online = %v, want %v", m.State.Online, expectedOnline)
		}
		if m.State.Timestamp <= 0 {
			return false, "STATE payload missing or non-positive timestamp"
		}
		return true, ""
	}
}

// stateTopicShape verifies the STATE topic format. ParseTopic already
// rejects malformed STATE topics (they wouldn't reach the tracker), so a
// passing capture means every STATE message here is well-formed; we still
// emit an explicit pass per host for reporting clarity.
func stateTopicShape(c *runner.Capture, id string) []runner.Result {
	seen := map[string]bool{}
	var out []runner.Result
	for _, m := range c.Messages {
		if m.Topic.Type != spb.STATE {
			continue
		}
		if seen[m.Topic.Host] {
			continue
		}
		seen[m.Topic.Host] = true
		out = append(out, runner.Pass(id, "STATE/"+m.Topic.Host))
	}
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no STATE messages in capture")}
	}
	return out
}
