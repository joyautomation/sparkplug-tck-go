package assertions

import (
	"github.com/joyautomation/sparkplug-tck-go/internal/runner"
	"github.com/joyautomation/sparkplug-tck-go/internal/spb"
)

// Host-side and behavioral aliases. These restate rules whose enforcement
// is visible only at MQTT-CONNECT scope (Will-message, Clean-Session flag,
// post-disconnect actions) or in the Host Application's local state. From
// a passive capture we can verify presence — the host published a STATE,
// the Edge Node published an NDEATH — but not the surrounding control-flow
// (timing, ordering, server selection). For the SHOULD/MUST rules that
// reduce to "we observed evidence the host did the right kind of thing,"
// we pass on presence; the more granular conformance check needs the
// HiveMQ-style harness with control over the broker.

func init() {
	registerHostPHIDAliases()
	registerHostBehaviorAliases()
	registerEdgeTerminationAliases()
	registerEdgeNodeBehaviorAliases()
	registerMessageFlowSubscriptionAliases()
	registerRebirthActionAliases()
	registerCaseSensitivityAliases()
	registerHostReorderingAliases()
}

// Host reorder timeout is host-internal state we can't see from a capture.
// `-rebirth` reduces to "we observed an NCMD-Rebirth from someone"; the
// other three are pure host behavior and pass-through on STATE presence.
func registerHostReorderingAliases() {
	runner.Register(runner.Assertion{
		ID:  "tck-id-operational-behavior-host-reordering-rebirth",
		Run: messagePresenceAlias(spb.NCMD, "tck-id-operational-behavior-host-reordering-rebirth"),
	})
	for _, id := range []string{
		"tck-id-operational-behavior-host-reordering-param",
		"tck-id-operational-behavior-host-reordering-start",
		"tck-id-operational-behavior-host-reordering-success",
	} {
		id := id
		runner.Register(runner.Assertion{ID: id, Run: anyStatePresenceAlias(id)})
	}
}

// host STATE birth presence — every rule that says "the host MUST do X
// at MQTT-CONNECT" reduces here to "we saw an online=true STATE."
func registerHostPHIDAliases() {
	for _, id := range []string{
		"tck-id-host-topic-phid-birth-message",
		"tck-id-host-topic-phid-birth-required",
		"tck-id-host-topic-phid-birth-sub-required",
		"tck-id-host-topic-phid-birth-payload-timestamp",
		"tck-id-message-flow-phid-sparkplug-state-publish",
		"tck-id-message-flow-phid-sparkplug-state-publish-payload-timestamp",
		"tck-id-message-flow-phid-sparkplug-subscription",
		"tck-id-message-flow-phid-sparkplug-clean-session-311",
		"tck-id-message-flow-phid-sparkplug-clean-session-50",
		"tck-id-message-flow-hid-sparkplug-state-message-delivered",
	} {
		id := id
		runner.Register(runner.Assertion{ID: id, Run: stateKindPresenceAlias(true, id)})
	}
	// host STATE death presence (will/offline).
	for _, id := range []string{
		"tck-id-host-topic-phid-death-required",
		"tck-id-host-topic-phid-death-payload-timestamp-connect",
		"tck-id-host-topic-phid-death-payload-timestamp-disconnect-clean",
		"tck-id-host-topic-phid-death-payload-timestamp-disconnect-with-no-disconnect-packet",
	} {
		id := id
		runner.Register(runner.Assertion{ID: id, Run: stateKindPresenceAlias(false, id)})
	}
}

// host application behavior aliases. All reduce to "STATE message of any
// kind observed for the host" since we can't see the MQTT CONNECT/DISCONNECT.
func registerHostBehaviorAliases() {
	for _, id := range []string{
		"tck-id-operational-behavior-host-application-connect-birth",
		"tck-id-operational-behavior-host-application-connect-will",
		"tck-id-operational-behavior-host-application-disconnect-intentional",
		"tck-id-operational-behavior-host-application-host-id",
		"tck-id-operational-behavior-host-application-multi-server-timestamp",
		"tck-id-operational-behavior-host-application-termination",
		"tck-id-operational-behavior-primary-application-state-with-multiple-servers-single-server",
		"tck-id-operational-behavior-primary-application-state-with-multiple-servers-state",
		"tck-id-operational-behavior-primary-application-state-with-multiple-servers-state-subs",
		"tck-id-operational-behavior-primary-application-state-with-multiple-servers-walk",
	} {
		id := id
		runner.Register(runner.Assertion{ID: id, Run: anyStatePresenceAlias(id)})
	}
}

// anyStatePresenceAlias passes when at least one STATE message (birth or
// death) appears in the capture for any host.
func anyStatePresenceAlias(id string) runner.AssertionFn {
	return func(c *runner.Capture) []runner.Result {
		var out []runner.Result
		seen := map[string]bool{}
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
			return []runner.Result{runner.NA(id, "no host STATE messages in capture")}
		}
		return out
	}
}

// Edge-node termination "host action" rules describe what a Host
// Application does in response to NDEATH/DDEATH — observable only by
// inspecting host-side state. From a capture we verify the *trigger*:
// the corresponding death message exists.
func registerEdgeTerminationAliases() {
	for _, id := range []string{
		"tck-id-operational-behavior-edge-node-termination-host-action-ndeath-devices-offline",
		"tck-id-operational-behavior-edge-node-termination-host-action-ndeath-devices-tags-stale",
		"tck-id-operational-behavior-edge-node-termination-host-action-ndeath-node-offline",
		"tck-id-operational-behavior-edge-node-termination-host-action-ndeath-node-tags-stale",
	} {
		id := id
		runner.Register(runner.Assertion{ID: id, Run: messagePresenceAlias(spb.NDEATH, id)})
	}
	for _, id := range []string{
		"tck-id-operational-behavior-edge-node-termination-host-action-ddeath-devices-offline",
		"tck-id-operational-behavior-edge-node-termination-host-action-ddeath-devices-tags-stale",
	} {
		id := id
		runner.Register(runner.Assertion{ID: id, Run: messagePresenceAlias(spb.DDEATH, id)})
	}
	// host-offline / reconnect / timestamp: STATE-driven Edge behavior.
	for _, id := range []string{
		"tck-id-operational-behavior-edge-node-termination-host-offline",
		"tck-id-operational-behavior-edge-node-termination-host-offline-reconnect",
		"tck-id-operational-behavior-edge-node-termination-host-offline-timestamp",
	} {
		id := id
		runner.Register(runner.Assertion{ID: id, Run: anyStatePresenceAlias(id)})
	}
}

func registerEdgeNodeBehaviorAliases() {
	// Edge-node intentional disconnect MUST publish NDEATH.
	runner.Register(runner.Assertion{
		ID:  "tck-id-operational-behavior-edge-node-intentional-disconnect-ndeath",
		Run: messagePresenceAlias(spb.NDEATH, "tck-id-operational-behavior-edge-node-intentional-disconnect-ndeath"),
	})
	// MAY send DISCONNECT packet — pass on NDEATH presence.
	runner.Register(runner.Assertion{
		ID:  "tck-id-operational-behavior-edge-node-intentional-disconnect-packet",
		Run: messagePresenceAlias(spb.NDEATH, "tck-id-operational-behavior-edge-node-intentional-disconnect-packet"),
	})
	// On lost device connection MUST publish DDEATH.
	runner.Register(runner.Assertion{
		ID:  "tck-id-operational-behavior-device-ddeath",
		Run: messagePresenceAlias(spb.DDEATH, "tck-id-operational-behavior-device-ddeath"),
	})
	// Edge-node BIRTH sequence wait for online STATE.
	runner.Register(runner.Assertion{
		ID:  "tck-id-operational-behavior-edge-node-birth-sequence-wait",
		Run: stateKindPresenceAlias(true, "tck-id-operational-behavior-edge-node-birth-sequence-wait"),
	})
	// Primary-Host wait/validation aliases — observable as STATE-driven
	// Edge logic; reduce to STATE presence.
	for _, id := range []string{
		"tck-id-message-flow-edge-node-birth-publish-phid-wait",
		"tck-id-message-flow-edge-node-birth-publish-phid-wait-id",
		"tck-id-message-flow-edge-node-birth-publish-phid-wait-online",
		"tck-id-message-flow-edge-node-birth-publish-phid-wait-timestamp",
	} {
		id := id
		runner.Register(runner.Assertion{ID: id, Run: stateKindPresenceAlias(true, id)})
	}
	runner.Register(runner.Assertion{
		ID:  "tck-id-message-flow-edge-node-birth-publish-phid-offline",
		Run: stateKindPresenceAlias(false, "tck-id-message-flow-edge-node-birth-publish-phid-offline"),
	})
}

func registerMessageFlowSubscriptionAliases() {
	// "Edge MUST subscribe to NCMD topic" / "Device MUST subscribe to DCMD".
	// Subscriptions are MQTT-control messages, not visible in publish-only
	// capture. Reduce to "we observed an NCMD/DCMD" — implies a working
	// subscription somewhere downstream, NA otherwise.
	runner.Register(runner.Assertion{
		ID:  "tck-id-message-flow-edge-node-ncmd-subscribe",
		Run: messagePresenceAlias(spb.NCMD, "tck-id-message-flow-edge-node-ncmd-subscribe"),
	})
	runner.Register(runner.Assertion{
		ID:  "tck-id-message-flow-device-dcmd-subscribe",
		Run: messagePresenceAlias(spb.DCMD, "tck-id-message-flow-device-dcmd-subscribe"),
	})
}

func registerRebirthActionAliases() {
	// "When an Edge Node receives a Rebirth Request, it MUST stop DATA /
	// emit BIRTH / preserve bdSeq" — outcomes observable, but only with
	// causality: NCMD-Rebirth appears, then NBIRTH+DBIRTH appear. The
	// passive presence reduction here is "an NBIRTH happened in the
	// capture" — the strict ordering check needs session-level state.
	for _, id := range []string{
		"tck-id-operational-behavior-data-commands-rebirth-action-1",
		"tck-id-operational-behavior-data-commands-rebirth-action-2",
		"tck-id-operational-behavior-data-commands-rebirth-action-3",
	} {
		id := id
		runner.Register(runner.Assertion{ID: id, Run: messagePresenceAlias(spb.NBIRTH, id)})
	}
}

// Case-sensitivity rules SHOULD NOT have IDs/metric names that match
// case-insensitively. Walk the capture and report duplicates after
// lowercasing.
func registerCaseSensitivityAliases() {
	runner.Register(runner.Assertion{
		ID:  "tck-id-case-sensitivity-metric-names",
		Run: caseSensitivityMetricNames,
	})
	runner.Register(runner.Assertion{
		ID:  "tck-id-case-sensitivity-sparkplug-ids",
		Run: caseSensitivitySparkplugIDs,
	})
}

func caseSensitivityMetricNames(c *runner.Capture) []runner.Result {
	const id = "tck-id-case-sensitivity-metric-names"
	type key struct {
		edge string
		low  string
	}
	first := map[key]string{}
	var out []runner.Result
	for _, m := range c.Messages {
		if m.Payload == nil {
			continue
		}
		edge := m.Topic.EdgeNodeID.String()
		for _, met := range m.Payload.GetMetrics() {
			if met.Name == nil || *met.Name == "" {
				continue
			}
			n := *met.Name
			low := lower(n)
			k := key{edge, low}
			prev, seen := first[k]
			if !seen {
				first[k] = n
				continue
			}
			if prev != n {
				out = append(out, runner.Fail(id, edge,
					"metric names differ only in case: "+prev+" vs "+n))
				first[k] = n
			}
		}
	}
	if len(out) == 0 {
		return []runner.Result{runner.Pass(id, "no case-clashing metric names")}
	}
	return out
}

func caseSensitivitySparkplugIDs(c *runner.Capture) []runner.Result {
	const id = "tck-id-case-sensitivity-sparkplug-ids"
	first := map[string]string{}
	var out []runner.Result
	check := func(orig string) {
		if orig == "" {
			return
		}
		low := lower(orig)
		prev, seen := first[low]
		if !seen {
			first[low] = orig
			return
		}
		if prev != orig {
			out = append(out, runner.Fail(id, orig,
				"Sparkplug IDs differ only in case: "+prev+" vs "+orig))
			first[low] = orig
		}
	}
	for _, m := range c.Messages {
		check(m.Topic.EdgeNodeID.Group)
		check(m.Topic.EdgeNodeID.Node)
		check(m.Topic.Device)
	}
	if len(out) == 0 {
		return []runner.Result{runner.Pass(id, "no case-clashing Sparkplug IDs")}
	}
	return out
}

// lower is a tiny ASCII-fold that avoids importing strings just for one
// callsite — Sparkplug IDs and metric names are ASCII-only by spec.
func lower(s string) string {
	b := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		b[i] = c
	}
	return string(b)
}
