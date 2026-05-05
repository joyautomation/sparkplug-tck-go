// Package assertions implements TCK assertion checks. Each check is a
// runner.AssertionFn registered under its [tck-id-*] from the Sparkplug
// specification. Importing this package for side-effects populates the
// global runner registry.
//
// First batch covers the NBIRTH/NDEATH lifecycle and basic topic structure.
// Coverage will grow toward parity with the official TCK; assertions.json
// at repo root is the authoritative target list.
package assertions

import (
	"fmt"
	"strings"

	"github.com/joyautomation/sparkplug-tck-go/internal/runner"
	"github.com/joyautomation/sparkplug-tck-go/internal/spb"
)

func init() {
	runner.Register(runner.Assertion{ID: "tck-id-topic-structure-namespace-a", Run: namespaceA})
	runner.Register(runner.Assertion{ID: "tck-id-topics-nbirth-mqtt", Run: nbirthMQTT})
	runner.Register(runner.Assertion{ID: "tck-id-topics-nbirth-seq-num", Run: nbirthSeqNum})
	runner.Register(runner.Assertion{ID: "tck-id-payloads-nbirth-seq", Run: nbirthSeqPayload})
	runner.Register(runner.Assertion{ID: "tck-id-payloads-nbirth-timestamp", Run: nbirthTimestamp})
	runner.Register(runner.Assertion{ID: "tck-id-payloads-nbirth-bdseq", Run: nbirthBdSeq})
	// chapter 4 alias for the same bdSeq-presence requirement.
	runner.Register(runner.Assertion{ID: "tck-id-topics-nbirth-bdseq-included", Run: nbirthBdSeqAlias("tck-id-topics-nbirth-bdseq-included")})
	runner.Register(runner.Assertion{ID: "tck-id-payloads-ndeath-seq", Run: ndeathNoSeq})
	runner.Register(runner.Assertion{ID: "tck-id-payloads-ndeath-bdseq", Run: ndeathBdSeqMatches})
	// chapter 4 alias for the NDEATH/NBIRTH bdSeq matching requirement.
	runner.Register(runner.Assertion{ID: "tck-id-topics-nbirth-bdseq-matching", Run: ndeathBdSeqMatchesAlias("tck-id-topics-nbirth-bdseq-matching")})
	runner.Register(runner.Assertion{ID: "tck-id-payloads-sequence-num-incrementing", Run: seqIncrementing})
	runner.Register(runner.Assertion{ID: "tck-id-payloads-sequence-num-always-included", Run: seqAlwaysIncluded})
	// chapter alias for the NBIRTH-seq presence/range requirement.
	runner.Register(runner.Assertion{ID: "tck-id-payloads-sequence-num-req-nbirth", Run: nbirthSeqAlias("tck-id-payloads-sequence-num-req-nbirth")})
}

func nbirthSeqAlias(aliasID string) runner.AssertionFn {
	return aliasOf(nbirthSeqPayload, aliasID)
}

// seqAlwaysIncluded: every Sparkplug edge message except NDEATH must carry
// a sequence number. NDEATH is excluded by spec; STATE is host-application,
// not edge.
func seqAlwaysIncluded(c *runner.Capture) []runner.Result {
	const id = "tck-id-payloads-sequence-num-always-included"
	var out []runner.Result
	for _, m := range c.Messages {
		switch m.Topic.Type {
		case spb.NDEATH, spb.STATE:
			continue
		}
		subject := subjectFor(m)
		if m.Payload == nil || m.Payload.Seq == nil {
			out = append(out, runner.Fail(id, subject, fmt.Sprintf("%s missing seq", m.Topic.Type)))
		} else {
			out = append(out, runner.Pass(id, subject))
		}
	}
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no edge messages in capture")}
	}
	return out
}

// tck-id-topic-structure-namespace-a:
// "For the Sparkplug B version of the payload definition, the UTF-8 string
// constant for the namespace element MUST be: spBv1.0"
//
// Topics that fail to parse never reach the tracker, so we walk Messages
// directly and check each topic's namespace field.
func namespaceA(c *runner.Capture) []runner.Result {
	const id = "tck-id-topic-structure-namespace-a"
	if len(c.Messages) == 0 {
		return []runner.Result{runner.NA(id, "capture had no messages")}
	}
	var out []runner.Result
	for _, m := range c.Messages {
		subject := m.Topic.String()
		if m.Topic.Namespace == spb.Namespace {
			out = append(out, runner.Pass(id, subject))
		} else {
			out = append(out, runner.Fail(id, subject,
				fmt.Sprintf("namespace %q != %q", m.Topic.Namespace, spb.Namespace)))
		}
	}
	return out
}

// tck-id-topics-nbirth-mqtt:
// "NBIRTH messages MUST be published with MQTT QoS equal to 0 and retain
// equal to false."
func nbirthMQTT(c *runner.Capture) []runner.Result {
	const id = "tck-id-topics-nbirth-mqtt"
	var out []runner.Result
	for _, m := range c.Messages {
		if m.Topic.Type != spb.NBIRTH {
			continue
		}
		subject := m.Topic.EdgeNodeID.String()
		switch {
		case m.QoS != 0:
			out = append(out, runner.Fail(id, subject, fmt.Sprintf("QoS = %d, want 0", m.QoS)))
		case m.Retained:
			out = append(out, runner.Fail(id, subject, "retain flag set, must be false"))
		default:
			out = append(out, runner.Pass(id, subject))
		}
	}
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no NBIRTH messages in capture")}
	}
	return out
}

// tck-id-topics-nbirth-seq-num and tck-id-payloads-nbirth-seq are nearly
// the same requirement; only the value bound differs (chapter 4 says =0,
// chapter 6 says 0..255). Implement them as separate functions for clarity.
func nbirthSeqNum(c *runner.Capture) []runner.Result {
	const id = "tck-id-topics-nbirth-seq-num"
	var out []runner.Result
	for _, m := range c.Messages {
		if m.Topic.Type != spb.NBIRTH {
			continue
		}
		subject := m.Topic.EdgeNodeID.String()
		switch {
		case m.Payload == nil || m.Payload.Seq == nil:
			out = append(out, runner.Fail(id, subject, "NBIRTH payload missing seq"))
		case *m.Payload.Seq != 0:
			out = append(out, runner.Fail(id, subject, fmt.Sprintf("seq = %d, want 0", *m.Payload.Seq)))
		default:
			out = append(out, runner.Pass(id, subject))
		}
	}
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no NBIRTH messages in capture")}
	}
	return out
}

func nbirthSeqPayload(c *runner.Capture) []runner.Result {
	const id = "tck-id-payloads-nbirth-seq"
	var out []runner.Result
	for _, m := range c.Messages {
		if m.Topic.Type != spb.NBIRTH {
			continue
		}
		subject := m.Topic.EdgeNodeID.String()
		switch {
		case m.Payload == nil || m.Payload.Seq == nil:
			out = append(out, runner.Fail(id, subject, "NBIRTH payload missing seq"))
		case *m.Payload.Seq > 255:
			out = append(out, runner.Fail(id, subject, fmt.Sprintf("seq = %d, must be 0..255", *m.Payload.Seq)))
		default:
			out = append(out, runner.Pass(id, subject))
		}
	}
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no NBIRTH messages in capture")}
	}
	return out
}

// tck-id-payloads-nbirth-timestamp:
// "NBIRTH messages MUST include a payload timestamp that denotes the time
// at which the message was published."
func nbirthTimestamp(c *runner.Capture) []runner.Result {
	const id = "tck-id-payloads-nbirth-timestamp"
	var out []runner.Result
	for _, m := range c.Messages {
		if m.Topic.Type != spb.NBIRTH {
			continue
		}
		subject := m.Topic.EdgeNodeID.String()
		switch {
		case m.Payload == nil || m.Payload.Timestamp == nil:
			out = append(out, runner.Fail(id, subject, "NBIRTH payload missing timestamp"))
		default:
			out = append(out, runner.Pass(id, subject))
		}
	}
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no NBIRTH messages in capture")}
	}
	return out
}

// tck-id-payloads-nbirth-bdseq:
// "Every NBIRTH message MUST include a bdSeq number metric."
//
// The session tracker already extracts bdSeq into BirthBdSeq for the most
// recent NBIRTH per edge — but to flag every individual NBIRTH we walk the
// raw messages.
func nbirthBdSeq(c *runner.Capture) []runner.Result {
	const id = "tck-id-payloads-nbirth-bdseq"
	var out []runner.Result
	for _, m := range c.Messages {
		if m.Topic.Type != spb.NBIRTH {
			continue
		}
		subject := m.Topic.EdgeNodeID.String()
		if hasBdSeq(m) {
			out = append(out, runner.Pass(id, subject))
		} else {
			out = append(out, runner.Fail(id, subject, "no bdSeq metric in NBIRTH"))
		}
	}
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no NBIRTH messages in capture")}
	}
	return out
}

// tck-id-payloads-ndeath-seq:
// "Every NDEATH message MUST NOT include a sequence number."
func ndeathNoSeq(c *runner.Capture) []runner.Result {
	const id = "tck-id-payloads-ndeath-seq"
	var out []runner.Result
	for _, m := range c.Messages {
		if m.Topic.Type != spb.NDEATH {
			continue
		}
		subject := m.Topic.EdgeNodeID.String()
		if m.Payload != nil && m.Payload.Seq != nil {
			out = append(out, runner.Fail(id, subject,
				fmt.Sprintf("NDEATH includes seq=%d, must be omitted", *m.Payload.Seq)))
		} else {
			out = append(out, runner.Pass(id, subject))
		}
	}
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no NDEATH messages in capture")}
	}
	return out
}

// tck-id-payloads-ndeath-bdseq:
// "The NDEATH message MUST include the same bdSeq number value that will
// be used in the associated NBIRTH message."
//
// The NDEATH is the LWT registered at CONNECT, so it actually precedes the
// NBIRTH whose bdSeq it must match. We pair each NDEATH with the next
// NBIRTH for the same edge node.
func ndeathBdSeqMatches(c *runner.Capture) []runner.Result {
	const id = "tck-id-payloads-ndeath-bdseq"
	var out []runner.Result
	// Build per-edge ordered lists of NBIRTH bdSeqs to match against.
	pendingBirths := map[spb.EdgeNodeID][]uint64{}
	for _, m := range c.Messages {
		if m.Topic.Type == spb.NBIRTH {
			if v := bdSeqOf(m); v != nil {
				pendingBirths[m.Topic.EdgeNodeID] = append(pendingBirths[m.Topic.EdgeNodeID], *v)
			}
		}
	}
	for _, m := range c.Messages {
		if m.Topic.Type != spb.NDEATH {
			continue
		}
		subject := m.Topic.EdgeNodeID.String()
		dbd := bdSeqOf(m)
		if dbd == nil {
			out = append(out, runner.Fail(id, subject, "NDEATH missing bdSeq metric"))
			continue
		}
		queue := pendingBirths[m.Topic.EdgeNodeID]
		if len(queue) == 0 {
			out = append(out, runner.Fail(id, subject,
				fmt.Sprintf("NDEATH bdSeq=%d but no matching NBIRTH in capture", *dbd)))
			continue
		}
		want := queue[0]
		pendingBirths[m.Topic.EdgeNodeID] = queue[1:]
		if *dbd != want {
			out = append(out, runner.Fail(id, subject,
				fmt.Sprintf("NDEATH bdSeq=%d, paired NBIRTH bdSeq=%d", *dbd, want)))
		} else {
			out = append(out, runner.Pass(id, subject))
		}
	}
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no NDEATH messages in capture")}
	}
	return out
}

// tck-id-payloads-sequence-num-incrementing:
// "All subsequent messages after an NBIRTH from an Edge Node MUST contain
// a sequence number that is continually increasing by one in each message
// from that Edge Node until a value of 255 is reached. At that point the
// sequence number MUST wrap back to a value of zero..."
//
// We walk per-edge in capture order: NBIRTH resets the expected counter to
// 0 (NBIRTH carries seq=0); each subsequent message must carry exactly
// (last+1) mod 256. NDEATH is skipped because it doesn't carry a seq.
func seqIncrementing(c *runner.Capture) []runner.Result {
	const id = "tck-id-payloads-sequence-num-incrementing"
	type cursor struct {
		started bool
		last    uint64
	}
	cursors := map[spb.EdgeNodeID]*cursor{}
	violations := map[spb.EdgeNodeID][]string{}

	for _, m := range c.Messages {
		if m.Topic.Type == spb.NDEATH || m.Topic.Type == spb.STATE {
			continue
		}
		cur, ok := cursors[m.Topic.EdgeNodeID]
		if !ok {
			cur = &cursor{}
			cursors[m.Topic.EdgeNodeID] = cur
		}
		if m.Payload == nil || m.Payload.Seq == nil {
			violations[m.Topic.EdgeNodeID] = append(violations[m.Topic.EdgeNodeID],
				fmt.Sprintf("%s missing seq", m.Topic.Type))
			continue
		}
		seq := *m.Payload.Seq
		if !cur.started {
			if m.Topic.Type != spb.NBIRTH {
				violations[m.Topic.EdgeNodeID] = append(violations[m.Topic.EdgeNodeID],
					fmt.Sprintf("first edge message was %s, want NBIRTH", m.Topic.Type))
				continue
			}
			if seq != 0 {
				violations[m.Topic.EdgeNodeID] = append(violations[m.Topic.EdgeNodeID],
					fmt.Sprintf("NBIRTH seq=%d, want 0", seq))
				continue
			}
			cur.started = true
			cur.last = 0
			continue
		}
		want := (cur.last + 1) % 256
		if seq != want {
			violations[m.Topic.EdgeNodeID] = append(violations[m.Topic.EdgeNodeID],
				fmt.Sprintf("%s seq=%d, want %d (after %d)", m.Topic.Type, seq, want, cur.last))
		}
		cur.last = seq
	}

	if len(cursors) == 0 {
		return []runner.Result{runner.NA(id, "no edge-node messages in capture")}
	}

	var out []runner.Result
	for edge := range cursors {
		subject := edge.String()
		if vs := violations[edge]; len(vs) > 0 {
			out = append(out, runner.Fail(id, subject, strings.Join(vs, "; ")))
		} else {
			out = append(out, runner.Pass(id, subject))
		}
	}
	return out
}

// aliasOf wraps an existing AssertionFn so its results report under a
// different spec ID. Many spec IDs across chapters 4/5/6 reduce to checks
// already wired under chapter-6 [tck-id-payloads-*] names — instead of
// duplicating logic, register the same function under each alias.
func aliasOf(fn runner.AssertionFn, aliasID string) runner.AssertionFn {
	return func(c *runner.Capture) []runner.Result {
		results := fn(c)
		for i := range results {
			results[i].AssertionID = aliasID
		}
		return results
	}
}

func nbirthBdSeqAlias(aliasID string) runner.AssertionFn {
	return aliasOf(nbirthBdSeq, aliasID)
}

func ndeathBdSeqMatchesAlias(aliasID string) runner.AssertionFn {
	return aliasOf(ndeathBdSeqMatches, aliasID)
}

// hasBdSeq is a thin wrapper used by NBIRTH-bdSeq assertions; bdSeqOf
// returns the actual value (nil if missing) and is used by paired checks.
func hasBdSeq(m spb.Message) bool { return bdSeqOf(m) != nil }

func bdSeqOf(m spb.Message) *uint64 {
	// session.extractBdSeq is unexported; reproduce the lookup here so
	// assertions don't depend on session internals. (Kept tiny on purpose.)
	if m.Payload == nil {
		return nil
	}
	for _, met := range m.Payload.GetMetrics() {
		if met.GetName() != "bdSeq" {
			continue
		}
		v := met.GetLongValue()
		return &v
	}
	return nil
}

