package assertions

import (
	"testing"

	"github.com/joyautomation/sparkplug-tck-go/internal/runner"
	"github.com/joyautomation/sparkplug-tck-go/internal/spb"
	"github.com/joyautomation/sparkplug-tck-go/internal/spbpb"
)

func TestHostAliases_AllRegistered(t *testing.T) {
	want := []string{
		"tck-id-host-topic-phid-birth-message",
		"tck-id-host-topic-phid-birth-required",
		"tck-id-host-topic-phid-death-required",
		"tck-id-message-flow-phid-sparkplug-state-publish",
		"tck-id-message-flow-phid-sparkplug-clean-session-311",
		"tck-id-operational-behavior-host-application-connect-birth",
		"tck-id-operational-behavior-host-application-host-id",
		"tck-id-operational-behavior-edge-node-termination-host-action-ndeath-node-offline",
		"tck-id-operational-behavior-edge-node-termination-host-action-ddeath-devices-offline",
		"tck-id-operational-behavior-edge-node-intentional-disconnect-ndeath",
		"tck-id-operational-behavior-device-ddeath",
		"tck-id-message-flow-edge-node-ncmd-subscribe",
		"tck-id-message-flow-device-dcmd-subscribe",
		"tck-id-operational-behavior-data-commands-rebirth-action-1",
		"tck-id-operational-behavior-data-commands-rebirth-action-2",
		"tck-id-operational-behavior-data-commands-rebirth-action-3",
		"tck-id-case-sensitivity-metric-names",
		"tck-id-case-sensitivity-sparkplug-ids",
		"tck-id-operational-behavior-host-reordering-rebirth",
		"tck-id-operational-behavior-host-reordering-start",
	}
	got := map[string]bool{}
	for _, a := range runner.All() {
		got[a.ID] = true
	}
	for _, id := range want {
		if !got[id] {
			t.Errorf("alias %q missing from registry", id)
		}
	}
}

func TestHostBehavior_PassesOnSTATE(t *testing.T) {
	res := runner.RunAll(runner.NewCapture([]spb.Message{stateBirth("h1", 1000, 1, true)}))
	for _, id := range []string{
		"tck-id-host-topic-phid-birth-required",
		"tck-id-message-flow-phid-sparkplug-state-publish",
		"tck-id-operational-behavior-host-application-connect-birth",
	} {
		r := resultByID(t, res, id)
		if r.Status != runner.StatusPass {
			t.Errorf("%s: expected pass, got %+v", id, r)
		}
	}
}

func TestEdgeTermination_NDEATHTriggersHostActions(t *testing.T) {
	res := runner.RunAll(runner.NewCapture([]spb.Message{ndeath("G", "N", u64(1))}))
	r := resultByID(t, res, "tck-id-operational-behavior-edge-node-termination-host-action-ndeath-node-offline")
	if r.Status != runner.StatusPass {
		t.Errorf("expected pass with NDEATH, got %+v", r)
	}
}

func TestCaseSensitivity_MetricNames_Clash(t *testing.T) {
	mk := func(name string) *spbpb.Payload_Metric {
		return &spbpb.Payload_Metric{
			Name:      str(name),
			Timestamp: u64(1),
			Datatype:  u32(uint32(spbpb.DataType_Int32)),
			Value:     &spbpb.Payload_Metric_IntValue{IntValue: 1},
		}
	}
	m1 := nbirth("G", "N", 0, 1, 1, 0, false)
	m1.Payload.Metrics = append(m1.Payload.Metrics, mk("MyTag"))
	m2 := nbirth("G", "N", 0, 2, 1, 0, false)
	m2.Payload.Metrics = append(m2.Payload.Metrics, mk("mytag"))
	res := runner.RunAll(runner.NewCapture([]spb.Message{m1, m2}))
	rs := resultsByID(res, "tck-id-case-sensitivity-metric-names")
	hasFail := false
	for _, r := range rs {
		if r.Status == runner.StatusFail {
			hasFail = true
		}
	}
	if !hasFail {
		t.Errorf("expected fail for case-clashing metric names, got %+v", rs)
	}
}

func TestCaseSensitivity_NoClash_Pass(t *testing.T) {
	res := runner.RunAll(runner.NewCapture([]spb.Message{nbirth("G", "N", 0, 1, 1, 0, false)}))
	r := resultByID(t, res, "tck-id-case-sensitivity-metric-names")
	if r.Status != runner.StatusPass {
		t.Errorf("expected pass without clashes, got %+v", r)
	}
}
