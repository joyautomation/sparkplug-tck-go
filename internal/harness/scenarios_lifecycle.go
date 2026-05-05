package harness

import (
	"fmt"
	"strings"
	"time"

	"github.com/joyautomation/sparkplug-tck-go/internal/runner"
	"github.com/joyautomation/sparkplug-tck-go/internal/spbpb"
	"google.golang.org/protobuf/proto"
)

// Lifecycle scenarios — checks that span the full session and rely on
// what the broker did or didn't do, beyond just packet shape.

// WillNotFiredOnCleanDisconnect: when an edge issues an MQTT DISCONNECT
// packet, the broker MUST NOT fire its Will message. This is the
// inverse of the standard "Will fires on unclean drop" rule and is
// genuinely harness-only — passive captures don't expose the OnWillSent
// hook the broker uses to deliver the Will. Strict form of the spec
// invariant behind tck-id-operational-behavior-edge-node-intentional-disconnect-packet.
func WillNotFiredOnCleanDisconnect(b *Broker) []runner.Result {
	const id = "tck-id-operational-behavior-edge-node-intentional-disconnect-packet"
	type per struct {
		clean     bool // saw EvDisconnect with no error
		willFired bool // saw EvWillSent for this client
		hasWill   bool // CONNECT advertised a Will
	}
	state := map[string]*per{}
	for _, e := range b.Events() {
		s := state[e.ClientID]
		if s == nil {
			s = &per{}
			state[e.ClientID] = s
		}
		switch e.Type {
		case EvConnect:
			if e.Will != nil {
				s.hasWill = true
			}
		case EvDisconnect:
			if e.DiscErr == "" {
				s.clean = true
			}
		case EvWillSent:
			s.willFired = true
		}
	}
	var out []runner.Result
	for client, s := range state {
		if !s.hasWill || !s.clean {
			continue // not a clean-DISCONNECT-with-Will lifecycle
		}
		if s.willFired {
			out = append(out, runner.Fail(id, client,
				"broker fired Will after a clean MQTT DISCONNECT — host should suppress it"))
		} else {
			out = append(out, runner.Pass(id, client))
		}
	}
	if len(out) == 0 {
		return []runner.Result{runner.NA(id,
			"no clean DISCONNECT with prior Will advertisement in scenario")}
	}
	return out
}

// EdgeRespondsToRebirth: when an NCMD message containing a "Node
// Control/Rebirth"=true metric is published to an Edge Node's NCMD
// topic, the edge MUST publish a fresh NBIRTH within rebirthDeadline.
// Strict form of tck-id-operational-behavior-data-commands-rebirth-action-2.
const rebirthDeadline = 5 * time.Second

func EdgeRespondsToRebirth(b *Broker) []runner.Result {
	const id = "tck-id-operational-behavior-data-commands-rebirth-action-2"
	type rebirth struct {
		at   time.Time
		edge string // matching NBIRTH topic for this NCMD
	}
	var rebirths []rebirth
	events := b.Events()
	for _, e := range events {
		if e.Type != EvPublish || !isNCMDTopic(e.Topic) {
			continue
		}
		if !payloadHasRebirthTrue(e.Payload) {
			continue
		}
		// NCMD topic spBv1.0/<group>/NCMD/<edge> → NBIRTH topic
		// spBv1.0/<group>/NBIRTH/<edge>.
		nbirth := strings.Replace(e.Topic, "/NCMD/", "/NBIRTH/", 1)
		rebirths = append(rebirths, rebirth{at: e.At, edge: nbirth})
	}
	var out []runner.Result
	for _, r := range rebirths {
		matched, late := false, false
		for _, e := range events {
			if e.Type != EvPublish || e.Topic != r.edge {
				continue
			}
			if !e.At.After(r.at) {
				continue
			}
			matched = true
			if e.At.Sub(r.at) > rebirthDeadline {
				late = true
			}
			break
		}
		subj := r.edge
		switch {
		case !matched:
			out = append(out, runner.Fail(id, subj,
				fmt.Sprintf("Rebirth Request received but no NBIRTH followed within %s",
					rebirthDeadline)))
		case late:
			out = append(out, runner.Fail(id, subj,
				fmt.Sprintf("NBIRTH after Rebirth was late (>%s)", rebirthDeadline)))
		default:
			out = append(out, runner.Pass(id, subj))
		}
	}
	if len(out) == 0 {
		return []runner.Result{runner.NA(id,
			"no Rebirth Request published in scenario")}
	}
	return out
}

// payloadHasRebirthTrue returns true if the protobuf payload contains a
// metric named "Node Control/Rebirth" with a boolean value of true.
func payloadHasRebirthTrue(raw []byte) bool {
	var p spbpb.Payload
	if err := proto.Unmarshal(raw, &p); err != nil {
		return false
	}
	for _, m := range p.GetMetrics() {
		if m.GetName() != "Node Control/Rebirth" {
			continue
		}
		if v, ok := m.Value.(*spbpb.Payload_Metric_BooleanValue); ok {
			return v.BooleanValue
		}
	}
	return false
}
