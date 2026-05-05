package harness

import "github.com/joyautomation/sparkplug-tck-go/internal/runner"

// Profile is a named bundle of strict scenarios — the harness analogue
// of the upstream TCK's "edge-node-test" / "host-application-test"
// suites. A profile takes a populated broker and returns the union of
// every scenario's results.
type Profile struct {
	Name      string
	Scenarios []Scenario
}

// Run evaluates every scenario in the profile against the broker and
// returns the concatenated results. Order is stable: scenarios run in
// declaration order, results within a scenario keep their original
// ordering.
func (p Profile) Run(b *Broker) []runner.Result {
	var out []runner.Result
	for _, fn := range p.Scenarios {
		out = append(out, fn(b)...)
	}
	return out
}

// EdgeNodeProfile bundles every Layer-3 scenario that targets an Edge
// Node SUT — Will/NDEATH invariants, NCMD subscription QoS, and the
// strict NDEATH-before-DISCONNECT ordering rule.
var EdgeNodeProfile = Profile{
	Name: "edge-node",
	Scenarios: []Scenario{
		EdgeWillIsNDEATH,
		EdgeWillPayloadHasBdSeq,
		EdgeNCMDSubscribeQoS,
		DeviceDCMDSubscribeQoS,
		NDEATHBeforeDisconnect,
		EdgeBdSeqMatchesWill,
		EdgeBdSeqIncrements,
		WillNotFiredOnCleanDisconnect,
		EdgeRespondsToRebirth,
		EdgeRebirthHaltsData,
		EdgeRebirthBdSeqUnchanged,
		EdgeDDEATHCompliant,
	},
}

// HostApplicationProfile bundles every Layer-3 scenario that targets a
// Host Application SUT — STATE Will shape, Clean Session, subscribe-
// before-publish ordering, and Death Certificate timestamps across
// CONNECT, clean DISCONNECT, and unclean tear-down.
var HostApplicationProfile = Profile{
	Name: "host-application",
	Scenarios: []Scenario{
		HostCONNECTHasWill,
		HostCleanSession,
		HostSTATEBirthAfterSubscribe,
		HostBirthTimestampMatchesWill,
		HostWillTimestampIsRecent,
		HostDeathBeforeCleanDisconnect,
		HostDeathBeforeUncleanDisconnect,
		HostMessageOrdering,
		HostNDEATHActions,
		HostDDEATHActions,
	},
}

// Profiles is the registry of named profiles, indexed by the flag value
// the CLI will accept (e.g. -profile=edge-node).
var Profiles = map[string]Profile{
	EdgeNodeProfile.Name:        EdgeNodeProfile,
	HostApplicationProfile.Name: HostApplicationProfile,
}
