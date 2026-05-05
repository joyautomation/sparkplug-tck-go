package harness

import (
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/joyautomation/sparkplug-tck-go/internal/runner"
)

// connect returns a paho client connected to the harness broker with a
// Will message advertising the NDEATH topic. The Will is what the broker
// fires if the client disappears uncleanly; here it just lets us see the
// CONNECT-time advertisement.
func connect(t *testing.T, b *Broker, clientID, willTopic string) mqtt.Client {
	t.Helper()
	opts := mqtt.NewClientOptions().
		AddBroker(b.URL()).
		SetClientID(clientID).
		SetCleanSession(true).
		SetConnectTimeout(2 * time.Second).
		SetBinaryWill(willTopic, []byte{0x01}, 1, false)
	c := mqtt.NewClient(opts)
	tok := c.Connect()
	if !tok.WaitTimeout(3 * time.Second) {
		t.Fatalf("connect timeout")
	}
	if err := tok.Error(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	return c
}

func TestNDEATHBeforeDisconnect_Compliant(t *testing.T) {
	b, err := NewBroker()
	if err != nil {
		t.Fatalf("broker: %v", err)
	}
	defer b.Close()

	c := connect(t, b, "edge-good", "spBv1.0/G/NDEATH/N")
	// Publish NDEATH then disconnect — the spec-compliant path.
	tok := c.Publish("spBv1.0/G/NDEATH/N", 0, false, []byte("ndeath-payload"))
	tok.WaitTimeout(time.Second)
	c.Disconnect(250)

	// Allow the broker recorder a moment to drain.
	time.Sleep(100 * time.Millisecond)

	res := NDEATHBeforeDisconnect(b)
	if len(res) == 0 {
		t.Fatalf("expected at least one result, got none")
	}
	for _, r := range res {
		if r.Status != runner.StatusPass {
			t.Fatalf("expected all pass, got %+v", res)
		}
	}
}

func TestNDEATHBeforeDisconnect_NonCompliant(t *testing.T) {
	b, err := NewBroker()
	if err != nil {
		t.Fatalf("broker: %v", err)
	}
	defer b.Close()

	c := connect(t, b, "edge-bad", "spBv1.0/G/NDEATH/N")
	// Disconnect without ever publishing NDEATH — the violation we want
	// to catch and that passive mode currently passes silently.
	c.Disconnect(250)

	time.Sleep(100 * time.Millisecond)

	res := NDEATHBeforeDisconnect(b)
	if len(res) == 0 {
		t.Fatalf("expected at least one result, got none")
	}
	for _, r := range res {
		if r.Status != runner.StatusFail {
			t.Fatalf("expected all fail, got %+v", res)
		}
	}
}

func TestNDEATHBeforeDisconnect_NoLifecycle_NA(t *testing.T) {
	b, err := NewBroker()
	if err != nil {
		t.Fatalf("broker: %v", err)
	}
	defer b.Close()

	// Connect and stay connected — no DISCONNECT, scenario is NA.
	c := connect(t, b, "edge-idle", "spBv1.0/G/NDEATH/N")
	defer c.Disconnect(0) // tear down after assertion runs

	time.Sleep(50 * time.Millisecond)
	res := NDEATHBeforeDisconnect(b)
	if len(res) == 0 {
		t.Fatalf("expected at least one result, got none")
	}
	for _, r := range res {
		if r.Status != runner.StatusNotApplicable {
			t.Fatalf("expected all NA, got %+v", res)
		}
	}
}

func TestBroker_RecordsCONNECTAndPublish(t *testing.T) {
	b, err := NewBroker()
	if err != nil {
		t.Fatalf("broker: %v", err)
	}
	defer b.Close()

	c := connect(t, b, "edge-records", "spBv1.0/G/NDEATH/N")
	c.Publish("spBv1.0/G/NBIRTH/N", 0, false, []byte{0x08, 0x00}).WaitTimeout(time.Second)
	c.Disconnect(250)
	time.Sleep(100 * time.Millisecond)

	evs := b.Events()
	saw := map[EventType]bool{}
	for _, e := range evs {
		saw[e.Type] = true
		if e.Type == EvConnect {
			if e.Will == nil || e.Will.Topic != "spBv1.0/G/NDEATH/N" {
				t.Errorf("CONNECT did not record Will: %+v", e)
			}
			if !e.CleanStart {
				t.Errorf("CONNECT Clean flag not propagated: %+v", e)
			}
		}
	}
	for _, want := range []EventType{EvConnect, EvPublish, EvDisconnect} {
		if !saw[want] {
			t.Errorf("missing event type %s in: %+v", want, evs)
		}
	}
}
