package harness

import (
	"fmt"
	"strings"

	"github.com/joyautomation/sparkplug-tck-go/internal/runner"
)

// Scenario is a Layer-3 check evaluated against a populated broker. The
// caller drives the SUT (real edge node, paho stub, etc.) to produce the
// connection lifecycle, then hands the broker to the scenario which
// returns runner.Result entries — interchangeable with passive results.
type Scenario func(b *Broker) []runner.Result

// NDEATHBeforeDisconnect implements the strict form of
// tck-id-operational-behavior-edge-node-intentional-disconnect-ndeath:
// the Edge Node MUST publish an NDEATH before the MQTT DISCONNECT
// packet. Passive captures pass on NDEATH presence alone — the harness
// can verify causal ordering directly from the packet stream.
func NDEATHBeforeDisconnect(b *Broker) []runner.Result {
	// Three spec IDs phrase the same observation: the Edge Node MUST
	// publish an NDEATH before terminating the MQTT connection. The
	// mqtt311/mqtt50 variants narrow it by protocol version; we emit
	// both because we don't track which the client used (the rule is
	// version-independent in practice).
	const idBase = "tck-id-operational-behavior-edge-node-intentional-disconnect-ndeath"
	const idPacket = "tck-id-operational-behavior-edge-node-intentional-disconnect-packet"
	const idShould = "tck-id-payloads-ndeath-will-message-publisher"
	const idMqtt311 = "tck-id-payloads-ndeath-will-message-publisher-disconnect-mqtt311"
	const idMqtt50 = "tck-id-payloads-ndeath-will-message-publisher-disconnect-mqtt50"
	allIDs := []string{idBase, idPacket, idShould, idMqtt311, idMqtt50}

	type lifecycle struct {
		ndeath, disconnect int  // event indices; -1 if not seen
		mqtt5              bool // CONNECT was MQTT 5
	}
	byClient := map[string]*lifecycle{}
	for i, e := range b.Events() {
		lc := byClient[e.ClientID]
		if lc == nil {
			lc = &lifecycle{ndeath: -1, disconnect: -1}
			byClient[e.ClientID] = lc
		}
		switch {
		case e.Type == EvConnect:
			lc.mqtt5 = e.ProtocolVersion == 5
		case e.Type == EvPublish && isNDEATHTopic(e.Topic):
			if lc.ndeath == -1 {
				lc.ndeath = i
			}
		case e.Type == EvDisconnect:
			lc.disconnect = i
		}
	}

	var out []runner.Result
	for client, lc := range byClient {
		if lc.disconnect == -1 {
			// SUT never disconnected — nothing to assert against.
			continue
		}
		// Java grades the mqtt311/mqtt50 IDs only for the protocol the SUT
		// actually used; the other variant stays NOT_EXECUTED. Mirror that.
		coreIDs := []string{idBase, idPacket, idShould}
		var versionID, otherID string
		if lc.mqtt5 {
			versionID, otherID = idMqtt50, idMqtt311
		} else {
			versionID, otherID = idMqtt311, idMqtt50
		}
		ids := append(append([]string{}, coreIDs...), versionID)
		switch {
		case lc.ndeath == -1:
			for _, id := range ids {
				out = append(out, runner.Fail(id, client,
					"DISCONNECT observed but no NDEATH was published first"))
			}
		case lc.ndeath > lc.disconnect:
			detail := fmt.Sprintf("NDEATH at event #%d came after DISCONNECT at event #%d",
				lc.ndeath, lc.disconnect)
			for _, id := range ids {
				out = append(out, runner.Fail(id, client, detail))
			}
		default:
			for _, id := range ids {
				out = append(out, runner.Pass(id, client))
			}
		}
		out = append(out, runner.NA(otherID,
			"client used the other MQTT version; this protocol-specific check NA"))
	}
	if len(out) == 0 {
		na := "no DISCONNECT observed in scenario"
		res := make([]runner.Result, 0, len(allIDs))
		for _, id := range allIDs {
			res = append(res, runner.NA(id, na))
		}
		return res
	}
	return out
}

func isNDEATHTopic(t string) bool {
	// spBv1.0/<group>/NDEATH/<edge>
	parts := strings.Split(t, "/")
	return len(parts) == 4 && parts[0] == "spBv1.0" && parts[2] == "NDEATH"
}
