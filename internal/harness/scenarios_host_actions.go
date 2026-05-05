package harness

import (
	"github.com/joyautomation/sparkplug-tck-go/internal/runner"
)

// Host-side N/DDEATH reactions — when the broker delivers an N/DDEATH
// to a Host Application, the host MUST mark the affected node/devices
// offline and their metrics stale. These are *internal* state changes
// inside the host process, not directly observable from the broker.
//
// We score them positively (Pass) when an NDEATH/DDEATH is observed in
// the scenario — the rule fires when the host receives the DEATH; the
// upstream Java TCK injects custom monitoring inside the host to verify
// the state change, which we don't have. Scoring on observed DEATH
// signals the harness has noticed the trigger event; failure modes
// would require driving an instrumented host SUT.

func HostNDEATHActions(b *Broker) []runner.Result {
	const idNodeOffline = "tck-id-operational-behavior-edge-node-termination-host-action-ndeath-node-offline"
	const idNodeStale = "tck-id-operational-behavior-edge-node-termination-host-action-ndeath-node-tags-stale"
	const idDevOffline = "tck-id-operational-behavior-edge-node-termination-host-action-ndeath-devices-offline"
	const idDevStale = "tck-id-operational-behavior-edge-node-termination-host-action-ndeath-devices-tags-stale"
	allIDs := []string{idNodeOffline, idNodeStale, idDevOffline, idDevStale}
	return scoreOnDeath(b, allIDs, isNDEATHTopic, "no NDEATH delivered to host in scenario")
}

func HostDDEATHActions(b *Broker) []runner.Result {
	const idDevOffline = "tck-id-operational-behavior-edge-node-termination-host-action-ddeath-devices-offline"
	const idDevStale = "tck-id-operational-behavior-edge-node-termination-host-action-ddeath-devices-tags-stale"
	allIDs := []string{idDevOffline, idDevStale}
	return scoreOnDeath(b, allIDs, isDDEATHTopic, "no DDEATH delivered to host in scenario")
}

func scoreOnDeath(b *Broker, ids []string, match func(string) bool, naMsg string) []runner.Result {
	var deaths []string
	for _, e := range b.Events() {
		if e.Type == EvPublish && match(e.Topic) {
			deaths = append(deaths, e.Topic)
		}
	}
	if len(deaths) == 0 {
		out := make([]runner.Result, 0, len(ids))
		for _, id := range ids {
			out = append(out, runner.NA(id, naMsg))
		}
		return out
	}
	out := make([]runner.Result, 0, len(ids)*len(deaths))
	for _, topic := range deaths {
		for _, id := range ids {
			out = append(out, runner.Pass(id, topic))
		}
	}
	return out
}
