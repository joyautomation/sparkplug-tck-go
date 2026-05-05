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
	const id = "tck-id-operational-behavior-edge-node-intentional-disconnect-ndeath"
	type lifecycle struct {
		ndeath, disconnect int // event indices; -1 if not seen
	}
	byClient := map[string]*lifecycle{}
	for i, e := range b.Events() {
		lc := byClient[e.ClientID]
		if lc == nil {
			lc = &lifecycle{ndeath: -1, disconnect: -1}
			byClient[e.ClientID] = lc
		}
		switch {
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
		switch {
		case lc.disconnect == -1:
			// SUT never disconnected — nothing to assert against.
			continue
		case lc.ndeath == -1:
			out = append(out, runner.Fail(id, client,
				"DISCONNECT observed but no NDEATH was published first"))
		case lc.ndeath > lc.disconnect:
			out = append(out, runner.Fail(id, client,
				fmt.Sprintf("NDEATH at event #%d came after DISCONNECT at event #%d",
					lc.ndeath, lc.disconnect)))
		default:
			out = append(out, runner.Pass(id, client))
		}
	}
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no DISCONNECT observed in scenario")}
	}
	return out
}

func isNDEATHTopic(t string) bool {
	// spBv1.0/<group>/NDEATH/<edge>
	parts := strings.Split(t, "/")
	return len(parts) == 4 && parts[0] == "spBv1.0" && parts[2] == "NDEATH"
}
