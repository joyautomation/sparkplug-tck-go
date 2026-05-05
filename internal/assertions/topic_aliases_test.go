package assertions

import (
	"testing"

	"github.com/joyautomation/sparkplug-tck-go/internal/runner"
	"github.com/joyautomation/sparkplug-tck-go/internal/spb"
)

func TestTopicAliases_AllRegistered(t *testing.T) {
	want := []string{
		// MQTT (QoS+retain) aliases for non-NBIRTH births/data/death.
		"tck-id-topics-dbirth-mqtt",
		"tck-id-topics-ndata-mqtt",
		"tck-id-topics-ddata-mqtt",
		"tck-id-topics-ddeath-mqtt",
		// Topic-presence aliases.
		"tck-id-topics-nbirth-topic",
		"tck-id-topics-dbirth-topic",
		"tck-id-topics-ndata-topic",
		"tck-id-topics-ddata-topic",
		"tck-id-topics-ndeath-topic",
		"tck-id-topics-ddeath-topic",
		// Payload presence.
		"tck-id-topics-ndata-payload",
		"tck-id-topics-ddata-payload",
		"tck-id-topics-ndeath-payload",
		// Timestamp.
		"tck-id-topics-nbirth-timestamp",
		"tck-id-topics-dbirth-timestamp",
		"tck-id-topics-ndata-timestamp",
		"tck-id-topics-ddata-timestamp",
		// Seq.
		"tck-id-topics-dbirth-seq",
		"tck-id-topics-ndata-seq-num",
		"tck-id-topics-ddata-seq-num",
		"tck-id-topics-ddeath-seq-num",
		"tck-id-topics-ndeath-seq",
		// Payload-rule aliases.
		"tck-id-payloads-nbirth-qos",
		"tck-id-payloads-nbirth-retain",
		"tck-id-payloads-ddeath-seq-number",
		// Per-edge ordering aliases.
		"tck-id-payloads-ndata-seq-inc",
		"tck-id-payloads-ddata-seq-inc",
		"tck-id-payloads-dbirth-seq-inc",
		"tck-id-payloads-ddeath-seq-inc",
		// State presence.
		"tck-id-payloads-state-birth",
		"tck-id-payloads-state-subscribe",
		"tck-id-payloads-state-will-message",
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

func TestTopicAliases_HappyPath(t *testing.T) {
	msgs := []spb.Message{
		nbirth("G", "N", 0, 1, 1, 0, false),
		dbirth("G", "N", "D", 1, 1, 0, false),
		ndata("G", "N", 2),
		ndeath("G", "N", u64(1)),
	}
	res := runner.RunAll(runner.NewCapture(msgs))
	for _, id := range []string{
		"tck-id-topics-dbirth-mqtt",
		"tck-id-topics-ndata-mqtt",
		"tck-id-topics-ndata-timestamp",
		"tck-id-topics-ndata-seq-num",
		"tck-id-topics-ndeath-seq",
		"tck-id-payloads-nbirth-qos",
		"tck-id-payloads-nbirth-retain",
	} {
		r := resultByID(t, res, id)
		if r.Status != runner.StatusPass {
			t.Errorf("%s: expected pass, got %+v", id, r)
		}
	}
}

func TestTopicAliases_NDEATH_SeqPresent_Fails(t *testing.T) {
	bad := ndeath("G", "N", u64(1))
	bad.Payload.Seq = u64(7)
	res := runner.RunAll(runner.NewCapture([]spb.Message{bad}))
	r := resultByID(t, res, "tck-id-topics-ndeath-seq")
	if r.Status != runner.StatusFail {
		t.Errorf("expected fail (NDEATH must not have seq), got %+v", r)
	}
}
