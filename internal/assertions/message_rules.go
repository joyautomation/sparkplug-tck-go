package assertions

import (
	"fmt"

	"github.com/joyautomation/sparkplug-tck-go/internal/runner"
	"github.com/joyautomation/sparkplug-tck-go/internal/spb"
)

// Many spec assertions reduce to "for every message of type X, predicate P
// must hold." Modeling them declaratively keeps the registration table
// readable and makes adding new ones a one-line change.

type messagePredicate func(spb.Message) (ok bool, detail string)

type messageRule struct {
	id   string          // spec [tck-id-*]
	mt   spb.MessageType // which message type this rule filters to
	pred messagePredicate
}

// mustQoS0 implements every "<msg> MUST be published with QoS 0" assertion.
func mustQoS0(m spb.Message) (bool, string) {
	if m.QoS != 0 {
		return false, fmt.Sprintf("QoS = %d, want 0", m.QoS)
	}
	return true, ""
}

// mustRetainFalse implements every "<msg> MUST have retain = false".
func mustRetainFalse(m spb.Message) (bool, string) {
	if m.Retained {
		return false, "retain flag set, must be false"
	}
	return true, ""
}

// mustHaveSeq implements every "<msg> MUST include a sequence number".
func mustHaveSeq(m spb.Message) (bool, string) {
	if m.Payload == nil || m.Payload.Seq == nil {
		return false, "payload missing seq"
	}
	return true, ""
}

// mustHaveTimestamp implements every "<msg> MUST include a payload timestamp".
func mustHaveTimestamp(m spb.Message) (bool, string) {
	if m.Payload == nil || m.Payload.Timestamp == nil {
		return false, "payload missing timestamp"
	}
	return true, ""
}

// runMessageRule is the generic body for a messageRule-shaped assertion.
func runMessageRule(c *runner.Capture, r messageRule) []runner.Result {
	var out []runner.Result
	for _, m := range c.Messages {
		if m.Topic.Type != r.mt {
			continue
		}
		subject := subjectFor(m)
		if ok, detail := r.pred(m); ok {
			out = append(out, runner.Pass(r.id, subject))
		} else {
			out = append(out, runner.Fail(r.id, subject, detail))
		}
	}
	if len(out) == 0 {
		return []runner.Result{runner.NA(r.id, fmt.Sprintf("no %s messages in capture", r.mt))}
	}
	return out
}

// subjectFor builds a human-readable subject string for a message.
// Edge-level messages: "G/N"; device-level: "G/N/D"; STATE: "STATE/host".
func subjectFor(m spb.Message) string {
	if m.Topic.Type == spb.STATE {
		return "STATE/" + m.Topic.Host
	}
	if m.Topic.Device != "" {
		return m.Topic.EdgeNodeID.String() + "/" + m.Topic.Device
	}
	return m.Topic.EdgeNodeID.String()
}

// messageRules enumerates every rule-shaped assertion. Add new ones here.
var messageRules = []messageRule{
	// DBIRTH: chapter 6 says QoS=0, retain=false, seq present, timestamp present.
	{id: "tck-id-payloads-dbirth-qos", mt: spb.DBIRTH, pred: mustQoS0},
	{id: "tck-id-payloads-dbirth-retain", mt: spb.DBIRTH, pred: mustRetainFalse},
	{id: "tck-id-payloads-dbirth-seq", mt: spb.DBIRTH, pred: mustHaveSeq},
	{id: "tck-id-payloads-dbirth-timestamp", mt: spb.DBIRTH, pred: mustHaveTimestamp},

	// NDATA: same envelope rules.
	{id: "tck-id-payloads-ndata-qos", mt: spb.NDATA, pred: mustQoS0},
	{id: "tck-id-payloads-ndata-retain", mt: spb.NDATA, pred: mustRetainFalse},
	{id: "tck-id-payloads-ndata-seq", mt: spb.NDATA, pred: mustHaveSeq},
	{id: "tck-id-payloads-ndata-timestamp", mt: spb.NDATA, pred: mustHaveTimestamp},

	// DDATA: same envelope rules.
	{id: "tck-id-payloads-ddata-qos", mt: spb.DDATA, pred: mustQoS0},
	{id: "tck-id-payloads-ddata-retain", mt: spb.DDATA, pred: mustRetainFalse},
	{id: "tck-id-payloads-ddata-seq", mt: spb.DDATA, pred: mustHaveSeq},
	{id: "tck-id-payloads-ddata-timestamp", mt: spb.DDATA, pred: mustHaveTimestamp},

	// DDEATH: must carry seq + timestamp (unlike NDEATH which has neither).
	{id: "tck-id-payloads-ddeath-seq", mt: spb.DDEATH, pred: mustHaveSeq},
	{id: "tck-id-payloads-ddeath-timestamp", mt: spb.DDEATH, pred: mustHaveTimestamp},
}

func init() {
	for _, r := range messageRules {
		r := r // capture
		runner.Register(runner.Assertion{
			ID:  r.id,
			Run: func(c *runner.Capture) []runner.Result { return runMessageRule(c, r) },
		})
	}
}
