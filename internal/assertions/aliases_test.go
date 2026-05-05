package assertions

import (
	"testing"

	"github.com/joyautomation/sparkplug-tck-go/internal/runner"
	"github.com/joyautomation/sparkplug-tck-go/internal/spb"
)

func TestAliases_AllRegistered(t *testing.T) {
	want := []string{
		// topic-structure-namespace
		"tck-id-topic-structure-namespace-valid-group-id",
		"tck-id-topic-structure-namespace-valid-edge-node-id",
		"tck-id-topic-structure-namespace-valid-device-id",
		"tck-id-topic-structure-namespace-device-id-associated-message-types",
		"tck-id-topic-structure-namespace-device-id-non-associated-message-types",
		"tck-id-topic-structure-namespace-unique-edge-node-descriptor",
		"tck-id-topic-structure-namespace-unique-device-id",
		"tck-id-topic-structure-namespace-duplicate-device-id-across-edge-node",
		// message-flow-edge
		"tck-id-message-flow-edge-node-birth-publish-nbirth-payload",
		"tck-id-message-flow-edge-node-birth-publish-nbirth-payload-bdSeq",
		"tck-id-message-flow-edge-node-birth-publish-nbirth-payload-seq",
		"tck-id-message-flow-edge-node-birth-publish-nbirth-qos",
		"tck-id-message-flow-edge-node-birth-publish-nbirth-retained",
		"tck-id-message-flow-edge-node-birth-publish-nbirth-topic",
		"tck-id-message-flow-edge-node-birth-publish-connect",
		"tck-id-message-flow-edge-node-birth-publish-will-message",
		"tck-id-message-flow-edge-node-birth-publish-will-message-payload",
		"tck-id-message-flow-edge-node-birth-publish-will-message-payload-bdSeq",
		"tck-id-message-flow-edge-node-birth-publish-will-message-qos",
		"tck-id-message-flow-edge-node-birth-publish-will-message-topic",
		"tck-id-message-flow-edge-node-birth-publish-will-message-will-retained",
		// message-flow-device
		"tck-id-message-flow-device-birth-publish-dbirth-payload",
		"tck-id-message-flow-device-birth-publish-dbirth-payload-seq",
		"tck-id-message-flow-device-birth-publish-dbirth-qos",
		"tck-id-message-flow-device-birth-publish-dbirth-retained",
		"tck-id-message-flow-device-birth-publish-dbirth-topic",
		"tck-id-message-flow-device-birth-publish-dbirth-match-edge-node-topic",
		"tck-id-message-flow-device-birth-publish-nbirth-wait",
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

func TestAliases_HappyPath_PassThroughs(t *testing.T) {
	msgs := []spb.Message{
		nbirth("G", "N", 0, 1, 1, 0, false),
		dbirth("G", "N", "D", 1, 1, 0, false),
		ndeath("G", "N", u64(1)),
	}
	res := runner.RunAll(runner.NewCapture(msgs))

	// Sample a representative subset; full coverage is verified in the
	// happy-path test in assertions_test.go (which now includes aliases).
	checks := []string{
		"tck-id-topic-structure-namespace-valid-group-id",
		"tck-id-message-flow-edge-node-birth-publish-nbirth-qos",
		"tck-id-message-flow-edge-node-birth-publish-will-message-qos",
		"tck-id-message-flow-device-birth-publish-dbirth-qos",
		"tck-id-message-flow-device-birth-publish-nbirth-wait",
	}
	for _, id := range checks {
		r := resultByID(t, res, id)
		if r.Status != runner.StatusPass {
			t.Errorf("%s: expected pass, got %+v", id, r)
		}
	}
}

func TestAliases_DBIRTH_QoSFails(t *testing.T) {
	bad := dbirth("G", "N", "D", 1, 1, 1, false) // QoS=1 not allowed
	res := runner.RunAll(runner.NewCapture([]spb.Message{bad}))
	r := resultByID(t, res, "tck-id-message-flow-device-birth-publish-dbirth-qos")
	if r.Status != runner.StatusFail {
		t.Errorf("expected fail on alias, got %+v", r)
	}
}

func TestAliases_NDEATHWill_RetainFails(t *testing.T) {
	bad := ndeath("G", "N", u64(1))
	bad.Retained = true
	res := runner.RunAll(runner.NewCapture([]spb.Message{bad}))
	r := resultByID(t, res, "tck-id-message-flow-edge-node-birth-publish-will-message-will-retained")
	if r.Status != runner.StatusFail {
		t.Errorf("expected fail on alias, got %+v", r)
	}
}
