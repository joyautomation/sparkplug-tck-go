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
