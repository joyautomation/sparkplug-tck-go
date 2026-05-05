package harness

import (
	"fmt"
	"time"

	"github.com/joyautomation/sparkplug-tck-go/internal/runner"
	"github.com/joyautomation/sparkplug-tck-go/internal/spb"
)

// Disconnect-time scenarios — timestamp invariants on the host's Death
// Certificate. The spec splits this into three rules: the Will payload's
// timestamp at CONNECT, an explicit Death PUBLISH before a clean MQTT
// DISCONNECT, and an explicit Death PUBLISH before an unclean tear-down.
//
// timestampTolerance is how far the host's reported UTC may drift from
// the wall clock the broker observed when the packet arrived. 5s is
// generous — the spec wants "current UTC", and a few seconds covers
// scheduling jitter on slow CI without admitting genuinely stale stamps.
const timestampTolerance = 5 * time.Second

// HostWillTimestampIsRecent: the STATE Will payload's timestamp MUST be
// "current UTC at CONNECT time". Strict form of
// tck-id-host-topic-phid-death-payload-timestamp-connect.
func HostWillTimestampIsRecent(b *Broker) []runner.Result {
	const id = "tck-id-host-topic-phid-death-payload-timestamp-connect"
	var out []runner.Result
	for _, e := range b.Events() {
		if e.Type != EvConnect || e.Will == nil || !isSTATETopic(e.Will.Topic) {
			continue
		}
		st, err := spb.DecodeState(e.Will.Payload)
		if err != nil || st == nil {
			out = append(out, runner.Fail(id, e.ClientID,
				"Will payload not a valid STATE document"))
			continue
		}
		if st.Legacy {
			// Sparkplug 2.x bare-string STATE has no timestamp at all.
			out = append(out, runner.Fail(id, e.ClientID,
				"legacy STATE Will has no timestamp"))
			continue
		}
		willTime := time.UnixMilli(st.Timestamp)
		drift := e.At.Sub(willTime)
		if drift < 0 {
			drift = -drift
		}
		if drift > timestampTolerance {
			out = append(out, runner.Fail(id, e.ClientID,
				fmt.Sprintf("Will timestamp %d drifts %s from CONNECT wall clock",
					st.Timestamp, drift)))
		} else {
			out = append(out, runner.Pass(id, e.ClientID))
		}
	}
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no host CONNECT with STATE Will in scenario")}
	}
	return out
}

// HostDeathBeforeCleanDisconnect: when the host issues an MQTT DISCONNECT
// packet, it MUST first publish a STATE death (online=false) whose
// timestamp is current UTC. Strict form of
// tck-id-host-topic-phid-death-payload-timestamp-disconnect-clean.
func HostDeathBeforeCleanDisconnect(b *Broker) []runner.Result {
	const id = "tck-id-host-topic-phid-death-payload-timestamp-disconnect-clean"
	// disconnect-intentional has the same observation: after publishing
	// the Death message a DISCONNECT packet MAY follow. We pass it on
	// the same host-clean-disconnect path.
	const idIntent = "tck-id-operational-behavior-host-application-disconnect-intentional"
	res := hostDeathBeforeDisconnect(b, id, true)
	out := make([]runner.Result, 0, len(res)*2)
	for _, r := range res {
		out = append(out, r)
		mirror := r
		mirror.AssertionID = idIntent
		out = append(out, mirror)
	}
	return out
}

// HostDeathBeforeUncleanDisconnect: same rule for unclean teardowns
// (no DISCONNECT packet). The host is still on the hook for publishing a
// Death Certificate before closing the socket — the broker firing the
// Will isn't a substitute for the host's own publish. Strict form of
// tck-id-host-topic-phid-death-payload-timestamp-disconnect-with-no-
// disconnect-packet.
func HostDeathBeforeUncleanDisconnect(b *Broker) []runner.Result {
	const id = "tck-id-host-topic-phid-death-payload-timestamp-disconnect-with-no-disconnect-packet"
	return hostDeathBeforeDisconnect(b, id, false)
}

// hostDeathBeforeDisconnect is the shared body for the two clean/unclean
// variants. wantClean=true selects DISCONNECT packets (DiscErr == ""),
// wantClean=false selects unclean drops (DiscErr != "").
func hostDeathBeforeDisconnect(b *Broker, id string, wantClean bool) []runner.Result {
	events := b.Events()
	hostClient := map[string]bool{} // clientID -> has STATE Will
	for _, e := range events {
		if e.Type == EvConnect && e.Will != nil && isSTATETopic(e.Will.Topic) {
			hostClient[e.ClientID] = true
		}
	}
	var out []runner.Result
	for i, e := range events {
		if e.Type != EvDisconnect || !hostClient[e.ClientID] {
			continue
		}
		clean := e.DiscErr == ""
		if clean != wantClean {
			continue
		}
		// Search backward for the host's STATE death PUBLISH.
		var deathTS int64
		var deathSeen, deathOffline bool
		for j := i - 1; j >= 0; j-- {
			p := events[j]
			if p.ClientID != e.ClientID || p.Type != EvPublish || !isSTATETopic(p.Topic) {
				continue
			}
			st, err := spb.DecodeState(p.Payload)
			if err != nil || st == nil {
				continue
			}
			deathSeen = true
			deathOffline = !st.Online
			deathTS = st.Timestamp
			break
		}
		subj := e.ClientID
		switch {
		case !deathSeen:
			out = append(out, runner.Fail(id, subj,
				"host disconnected without publishing a STATE death first"))
		case !deathOffline:
			out = append(out, runner.Fail(id, subj,
				"last STATE PUBLISH before disconnect was online=true, expected offline"))
		default:
			drift := e.At.Sub(time.UnixMilli(deathTS))
			if drift < 0 {
				drift = -drift
			}
			if drift > timestampTolerance {
				out = append(out, runner.Fail(id, subj,
					fmt.Sprintf("STATE death timestamp %d drifts %s from disconnect wall clock",
						deathTS, drift)))
			} else {
				out = append(out, runner.Pass(id, subj))
			}
		}
	}
	if len(out) == 0 {
		kind := "clean"
		if !wantClean {
			kind = "unclean"
		}
		return []runner.Result{runner.NA(id,
			"no host "+kind+" disconnect in scenario")}
	}
	return out
}
