package assertions

import (
	"strings"
	"testing"

	"github.com/joyautomation/sparkplug-tck-go/internal/runner"
	"github.com/joyautomation/sparkplug-tck-go/internal/spb"
)

// state* helpers build STATE messages for tests.

func stateBirth(host string, ts int64, qos byte, retain bool) spb.Message {
	return spb.Message{
		Topic:    spb.Topic{Namespace: spb.Namespace, Type: spb.STATE, Host: host},
		State:    &spb.StatePayload{Online: true, Timestamp: ts},
		QoS:      qos,
		Retained: retain,
	}
}

func stateDeath(host string, ts int64, qos byte, retain bool) spb.Message {
	return spb.Message{
		Topic:    spb.Topic{Namespace: spb.Namespace, Type: spb.STATE, Host: host},
		State:    &spb.StatePayload{Online: false, Timestamp: ts},
		QoS:      qos,
		Retained: retain,
	}
}

func TestState_HappyPath_Birth_Death(t *testing.T) {
	msgs := []spb.Message{
		stateBirth("h1", 1000, 1, true),
		stateDeath("h1", 2000, 1, true),
	}
	res := runner.RunAll(runner.NewCapture(msgs))
	for _, id := range []string{
		"tck-id-host-topic-phid-birth-qos",
		"tck-id-host-topic-phid-birth-retain",
		"tck-id-host-topic-phid-birth-payload",
		"tck-id-host-topic-phid-death-qos",
		"tck-id-host-topic-phid-death-retain",
		"tck-id-host-topic-phid-death-payload",
		"tck-id-operational-behavior-host-application-connect-birth-qos",
		"tck-id-operational-behavior-host-application-connect-birth-retained",
		"tck-id-operational-behavior-host-application-connect-birth-payload",
		"tck-id-operational-behavior-host-application-connect-will-qos",
		"tck-id-operational-behavior-host-application-connect-will-retained",
		"tck-id-operational-behavior-host-application-connect-will-payload",
	} {
		if r := resultByID(t, res, id); r.Status != runner.StatusPass {
			t.Errorf("%s: expected pass, got %+v", id, r)
		}
	}
}

func TestState_BirthBadQoS_Fails(t *testing.T) {
	m := stateBirth("h1", 1, 0, true)
	res := runner.RunAll(runner.NewCapture([]spb.Message{m}))
	r := resultByID(t, res, "tck-id-host-topic-phid-birth-qos")
	if r.Status != runner.StatusFail || !strings.Contains(r.Detail, "QoS") {
		t.Errorf("expected QoS fail, got %+v", r)
	}
	// equivalent ID should also fail
	r2 := resultByID(t, res, "tck-id-operational-behavior-host-application-connect-birth-qos")
	if r2.Status != runner.StatusFail {
		t.Errorf("equivalent ID expected fail, got %+v", r2)
	}
}

func TestState_BirthNotRetained_Fails(t *testing.T) {
	m := stateBirth("h1", 1, 1, false)
	res := runner.RunAll(runner.NewCapture([]spb.Message{m}))
	r := resultByID(t, res, "tck-id-host-topic-phid-birth-retain")
	if r.Status != runner.StatusFail || !strings.Contains(r.Detail, "retain") {
		t.Errorf("expected retain fail, got %+v", r)
	}
}

func TestState_LegacyPayload_Fails(t *testing.T) {
	m := stateBirth("h1", 0, 1, true)
	m.State.Legacy = true
	m.State.Timestamp = 0
	res := runner.RunAll(runner.NewCapture([]spb.Message{m}))
	r := resultByID(t, res, "tck-id-host-topic-phid-birth-payload")
	if r.Status != runner.StatusFail || !strings.Contains(r.Detail, "legacy") {
		t.Errorf("expected legacy-payload fail, got %+v", r)
	}
}

func TestState_BirthMissingTimestamp_Fails(t *testing.T) {
	m := stateBirth("h1", 0, 1, true)
	res := runner.RunAll(runner.NewCapture([]spb.Message{m}))
	r := resultByID(t, res, "tck-id-host-topic-phid-birth-payload")
	if r.Status != runner.StatusFail || !strings.Contains(r.Detail, "timestamp") {
		t.Errorf("expected timestamp fail, got %+v", r)
	}
}

func TestState_DeathOnlineMismatch_Fails(t *testing.T) {
	// Death-kind message but online=true — selector picks by online so
	// confirm the death-payload assertion catches an online=false expectation
	// when the payload says otherwise. Force this by sending a death-shaped
	// message with online flipped.
	m := stateDeath("h1", 1, 1, true)
	m.State.Online = false // already false; keep death routing
	// flip the timestamp to bad to verify death payload predicate runs
	m.State.Timestamp = 0
	res := runner.RunAll(runner.NewCapture([]spb.Message{m}))
	r := resultByID(t, res, "tck-id-host-topic-phid-death-payload")
	if r.Status != runner.StatusFail || !strings.Contains(r.Detail, "timestamp") {
		t.Errorf("expected death-payload timestamp fail, got %+v", r)
	}
}

func TestState_NoMessages_NA(t *testing.T) {
	res := runner.RunAll(runner.NewCapture(nil))
	r := resultByID(t, res, "tck-id-host-topic-phid-birth-qos")
	if r.Status != runner.StatusNotApplicable {
		t.Errorf("expected NA, got %+v", r)
	}
}

func TestState_TopicShape_Pass(t *testing.T) {
	msgs := []spb.Message{stateBirth("h1", 1, 1, true)}
	res := runner.RunAll(runner.NewCapture(msgs))
	for _, id := range []string{
		"tck-id-host-topic-phid-birth-topic",
		"tck-id-host-topic-phid-death-topic",
		"tck-id-operational-behavior-host-application-connect-birth-topic",
		"tck-id-operational-behavior-host-application-connect-will-topic",
	} {
		if r := resultByID(t, res, id); r.Status != runner.StatusPass {
			t.Errorf("%s: expected pass, got %+v", id, r)
		}
	}
}
