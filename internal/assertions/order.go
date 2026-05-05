package assertions

import (
	"fmt"
	"strings"

	"github.com/joyautomation/sparkplug-tck-go/internal/runner"
	"github.com/joyautomation/sparkplug-tck-go/internal/spb"
)

// Per-edge ordering rules from chapter 6:
//
//   tck-id-payloads-dbirth-order:
//     "All DBIRTH messages sent by an Edge Node MUST be sent immediately
//      after the NBIRTH and before any NDATA or DDATA messages are
//      published by the Edge Node."
//
//   tck-id-payloads-ndata-order:
//     "All NDATA messages sent by an Edge Node MUST NOT be sent until all
//      the NBIRTH and all DBIRTH messages have been published by the
//      Edge Node."
//
//   tck-id-payloads-ddata-order:
//     "All DDATA messages sent by an Edge Node MUST NOT be sent until all
//      the NBIRTH and all DBIRTH messages have been published by the
//      Edge Node."
//
// Reduce all three to a single pass over per-edge message sequences,
// tracking a small phase machine: preBirth → inDBirth → inData.

type orderPhase int

const (
	phasePreBirth orderPhase = iota
	phaseInDBirth
	phaseInData
)

func init() {
	runner.Register(runner.Assertion{ID: "tck-id-payloads-dbirth-order", Run: dbirthOrder})
	runner.Register(runner.Assertion{ID: "tck-id-payloads-ndata-order", Run: ndataOrder})
	runner.Register(runner.Assertion{ID: "tck-id-payloads-ddata-order", Run: ddataOrder})
}

// scanOrderPerEdge runs a phase machine over capture-order messages and
// records every ordering violation it sees per edge. The three assertion
// functions then filter those violations by category.
type orderViolations struct {
	dbirthAfterData []string // DBIRTH after data started
	ndataBeforeBirth []string // NDATA before NBIRTH
	ddataBeforeBirth []string // DDATA before NBIRTH
	sawAny          bool
}

func scanOrderPerEdge(c *runner.Capture) map[spb.EdgeNodeID]*orderViolations {
	type edgeState struct {
		phase orderPhase
		v     *orderViolations
	}
	states := map[spb.EdgeNodeID]*edgeState{}

	for _, m := range c.Messages {
		if m.Topic.Type == spb.STATE {
			continue
		}
		st, ok := states[m.Topic.EdgeNodeID]
		if !ok {
			st = &edgeState{phase: phasePreBirth, v: &orderViolations{sawAny: true}}
			states[m.Topic.EdgeNodeID] = st
		}

		switch m.Topic.Type {
		case spb.NBIRTH:
			// Restart the per-edge phase. (Rebirth resets ordering checks.)
			st.phase = phaseInDBirth
		case spb.NDEATH:
			// NDEATH ends the session for ordering purposes; subsequent
			// messages without a fresh NBIRTH will be flagged again.
			st.phase = phasePreBirth
		case spb.DBIRTH:
			if st.phase == phaseInData {
				st.v.dbirthAfterData = append(st.v.dbirthAfterData,
					fmt.Sprintf("DBIRTH/%s after data flow started", m.Topic.Device))
			}
			// Whether OK or violating, stay in birth phase: more DBIRTHs may follow.
			if st.phase == phasePreBirth {
				// DBIRTH before any NBIRTH — also a dbirth-order violation per spec.
				st.v.dbirthAfterData = append(st.v.dbirthAfterData,
					fmt.Sprintf("DBIRTH/%s before NBIRTH", m.Topic.Device))
			}
		case spb.NDATA:
			if st.phase == phasePreBirth {
				st.v.ndataBeforeBirth = append(st.v.ndataBeforeBirth,
					"NDATA before NBIRTH")
			}
			if st.phase == phaseInDBirth {
				st.phase = phaseInData
			}
		case spb.DDATA:
			if st.phase == phasePreBirth {
				st.v.ddataBeforeBirth = append(st.v.ddataBeforeBirth,
					fmt.Sprintf("DDATA/%s before NBIRTH", m.Topic.Device))
			}
			if st.phase == phaseInDBirth {
				st.phase = phaseInData
			}
		case spb.DDEATH, spb.NCMD, spb.DCMD:
			// Not part of ordering check.
		}
	}

	out := map[spb.EdgeNodeID]*orderViolations{}
	for k, st := range states {
		out[k] = st.v
	}
	return out
}

func dbirthOrder(c *runner.Capture) []runner.Result {
	const id = "tck-id-payloads-dbirth-order"
	return reportOrder(c, id, func(v *orderViolations) []string { return v.dbirthAfterData })
}

func ndataOrder(c *runner.Capture) []runner.Result {
	const id = "tck-id-payloads-ndata-order"
	return reportOrder(c, id, func(v *orderViolations) []string { return v.ndataBeforeBirth })
}

func ddataOrder(c *runner.Capture) []runner.Result {
	const id = "tck-id-payloads-ddata-order"
	return reportOrder(c, id, func(v *orderViolations) []string { return v.ddataBeforeBirth })
}

func reportOrder(c *runner.Capture, id string, pick func(*orderViolations) []string) []runner.Result {
	per := scanOrderPerEdge(c)
	if len(per) == 0 {
		return []runner.Result{runner.NA(id, "no edge-node messages in capture")}
	}
	var out []runner.Result
	for edge, v := range per {
		picked := pick(v)
		subject := edge.String()
		if len(picked) == 0 {
			out = append(out, runner.Pass(id, subject))
		} else {
			out = append(out, runner.Fail(id, subject, strings.Join(picked, "; ")))
		}
	}
	return out
}
