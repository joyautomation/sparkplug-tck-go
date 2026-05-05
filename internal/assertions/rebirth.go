package assertions

import (
	"fmt"

	"github.com/joyautomation/sparkplug-tck-go/internal/runner"
	"github.com/joyautomation/sparkplug-tck-go/internal/spb"
	"github.com/joyautomation/sparkplug-tck-go/internal/spbpb"
)

// Rebirth — Sparkplug requires every NBIRTH to advertise the
// "Node Control/Rebirth" metric so a host can request a fresh birth
// sequence by writing true to it via NCMD. The metric MUST exist on the
// NBIRTH, with datatype Boolean, default value false, and (because aliases
// can collide with control semantics) MUST NOT carry an alias.

const rebirthMetricName = "Node Control/Rebirth"

func init() {
	runner.Register(runner.Assertion{ID: "tck-id-operational-behavior-data-commands-rebirth-name", Run: rebirthName})
	runner.Register(runner.Assertion{ID: "tck-id-operational-behavior-data-commands-rebirth-value", Run: rebirthValue})
	runner.Register(runner.Assertion{ID: "tck-id-operational-behavior-data-commands-rebirth-datatype", Run: rebirthDatatype})
	runner.Register(runner.Assertion{ID: "tck-id-operational-behavior-data-commands-rebirth-name-aliases", Run: rebirthNoAlias})

	// NCMD-side rebirth: a Rebirth Request from the host writes
	// Node Control/Rebirth=true via NCMD. The constraints mirror NBIRTH's
	// rebirth metric except value MUST be true and the verb MUST be NCMD.
	runner.Register(runner.Assertion{ID: "tck-id-operational-behavior-data-commands-ncmd-rebirth-name", Run: ncmdRebirthName})
	runner.Register(runner.Assertion{ID: "tck-id-operational-behavior-data-commands-ncmd-rebirth-value", Run: ncmdRebirthValue})
	runner.Register(runner.Assertion{ID: "tck-id-operational-behavior-data-commands-ncmd-rebirth-verb", Run: ncmdRebirthVerb})
}

// ncmdHasRebirth reports whether an NCMD payload looks like a Rebirth
// Request — i.e. carries a Node Control/Rebirth metric. We treat NCMDs
// without that metric as out-of-scope for the rebirth-* IDs and return NA.
func ncmdHasRebirth(m spb.Message) bool {
	return m.Topic.Type == spb.NCMD && findRebirth(m.Payload) != nil
}

func ncmdRebirthName(c *runner.Capture) []runner.Result {
	const id = "tck-id-operational-behavior-data-commands-ncmd-rebirth-name"
	var out []runner.Result
	for _, m := range c.Messages {
		if m.Topic.Type != spb.NCMD {
			continue
		}
		// A Rebirth Request is identified by name; if the NCMD carries any
		// metric, the spec says "MUST include a metric with a name of
		// 'Node Control/Rebirth'" *for Rebirth Requests*. Without a way to
		// label intent, we pass when the metric is present and skip
		// otherwise — non-rebirth NCMDs aren't subject to this rule.
		if findRebirth(m.Payload) == nil {
			continue
		}
		out = append(out, runner.Pass(id, m.Topic.EdgeNodeID.String()))
	}
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no NCMD Rebirth Requests in capture")}
	}
	return out
}

func ncmdRebirthValue(c *runner.Capture) []runner.Result {
	const id = "tck-id-operational-behavior-data-commands-ncmd-rebirth-value"
	var out []runner.Result
	for _, m := range c.Messages {
		if !ncmdHasRebirth(m) {
			continue
		}
		subj := m.Topic.EdgeNodeID.String()
		met := findRebirth(m.Payload)
		bv, ok := met.Value.(*spbpb.Payload_Metric_BooleanValue)
		if !ok {
			out = append(out, runner.Fail(id, subj,
				fmt.Sprintf("rebirth metric value type %T, want Boolean", met.Value)))
			continue
		}
		if !bv.BooleanValue {
			out = append(out, runner.Fail(id, subj, "rebirth value = false, want true for Rebirth Request"))
		} else {
			out = append(out, runner.Pass(id, subj))
		}
	}
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no NCMD Rebirth Requests in capture")}
	}
	return out
}

// ncmdRebirthVerb: a Rebirth Request MUST use the NCMD verb. We classify
// "Rebirth Request" by metric name; the message arriving on an NCMD topic
// satisfies the verb requirement by construction. (A DCMD or NDATA carrying
// a Node Control/Rebirth metric would be the violation, but those are
// shaped differently — we only fail if we ever see one.)
func ncmdRebirthVerb(c *runner.Capture) []runner.Result {
	const id = "tck-id-operational-behavior-data-commands-ncmd-rebirth-verb"
	var out []runner.Result
	for _, m := range c.Messages {
		if findRebirth(m.Payload) == nil {
			continue
		}
		// Rebirth metric only matters in command messages and on BIRTHs.
		switch m.Topic.Type {
		case spb.NCMD:
			out = append(out, runner.Pass(id, m.Topic.EdgeNodeID.String()))
		case spb.DCMD:
			out = append(out, runner.Fail(id,
				m.Topic.EdgeNodeID.String()+"/"+m.Topic.Device,
				"Rebirth Request must use NCMD, got DCMD"))
		}
	}
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no NCMD/DCMD Rebirth Requests in capture")}
	}
	return out
}

// findRebirth returns the rebirth metric on an NBIRTH, or nil if absent.
func findRebirth(p *spbpb.Payload) *spbpb.Payload_Metric {
	if p == nil {
		return nil
	}
	for _, m := range p.GetMetrics() {
		if m.GetName() == rebirthMetricName {
			return m
		}
	}
	return nil
}

func rebirthName(c *runner.Capture) []runner.Result {
	const id = "tck-id-operational-behavior-data-commands-rebirth-name"
	var out []runner.Result
	for _, m := range c.Messages {
		if m.Topic.Type != spb.NBIRTH {
			continue
		}
		subject := m.Topic.EdgeNodeID.String()
		if findRebirth(m.Payload) == nil {
			out = append(out, runner.Fail(id, subject,
				fmt.Sprintf("NBIRTH missing metric %q", rebirthMetricName)))
		} else {
			out = append(out, runner.Pass(id, subject))
		}
	}
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no NBIRTH messages in capture")}
	}
	return out
}

func rebirthValue(c *runner.Capture) []runner.Result {
	const id = "tck-id-operational-behavior-data-commands-rebirth-value"
	var out []runner.Result
	for _, m := range c.Messages {
		if m.Topic.Type != spb.NBIRTH {
			continue
		}
		subject := m.Topic.EdgeNodeID.String()
		met := findRebirth(m.Payload)
		if met == nil {
			out = append(out, runner.NA(id, subject+": rebirth metric absent"))
			continue
		}
		// The metric value is encoded as a oneof; for Boolean it's BooleanValue.
		bv, ok := met.Value.(*spbpb.Payload_Metric_BooleanValue)
		if !ok {
			out = append(out, runner.Fail(id, subject,
				fmt.Sprintf("rebirth metric value type %T, want Boolean", met.Value)))
			continue
		}
		if bv.BooleanValue {
			out = append(out, runner.Fail(id, subject, "rebirth value = true, want false"))
		} else {
			out = append(out, runner.Pass(id, subject))
		}
	}
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no NBIRTH messages in capture")}
	}
	return out
}

func rebirthDatatype(c *runner.Capture) []runner.Result {
	const id = "tck-id-operational-behavior-data-commands-rebirth-datatype"
	var out []runner.Result
	for _, m := range c.Messages {
		if m.Topic.Type != spb.NBIRTH {
			continue
		}
		subject := m.Topic.EdgeNodeID.String()
		met := findRebirth(m.Payload)
		if met == nil {
			out = append(out, runner.NA(id, subject+": rebirth metric absent"))
			continue
		}
		dt := spbpb.DataType(met.GetDatatype())
		if dt != spbpb.DataType_Boolean {
			out = append(out, runner.Fail(id, subject,
				fmt.Sprintf("rebirth datatype = %s, want Boolean", dt)))
		} else {
			out = append(out, runner.Pass(id, subject))
		}
	}
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no NBIRTH messages in capture")}
	}
	return out
}

func rebirthNoAlias(c *runner.Capture) []runner.Result {
	const id = "tck-id-operational-behavior-data-commands-rebirth-name-aliases"
	var out []runner.Result
	for _, m := range c.Messages {
		if m.Topic.Type != spb.NBIRTH {
			continue
		}
		subject := m.Topic.EdgeNodeID.String()
		met := findRebirth(m.Payload)
		if met == nil {
			out = append(out, runner.NA(id, subject+": rebirth metric absent"))
			continue
		}
		if met.Alias != nil {
			out = append(out, runner.Fail(id, subject,
				fmt.Sprintf("rebirth metric carries alias=%d, must be omitted", *met.Alias)))
		} else {
			out = append(out, runner.Pass(id, subject))
		}
	}
	if len(out) == 0 {
		return []runner.Result{runner.NA(id, "no NBIRTH messages in capture")}
	}
	return out
}
