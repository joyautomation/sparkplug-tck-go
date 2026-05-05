package harness

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/joyautomation/sparkplug-tck-go/internal/runner"
)

// stateBody marshals a STATE JSON body. Mirrors the host clients' format.
func stateBody(online bool, ts int64) []byte {
	body, _ := json.Marshal(struct {
		Online    bool  `json:"online"`
		Timestamp int64 `json:"timestamp"`
	}{Online: online, Timestamp: ts})
	return body
}

// killBrokerSide forces an unclean tear-down by reaching into the
// broker's client map and Stop-ing the connection — this closes the
// socket without first receiving a DISCONNECT packet, which is exactly
// what we need to test the unclean-disconnect path.
func killBrokerSide(t *testing.T, b *Broker, clientID string) {
	t.Helper()
	cl, ok := b.server.Clients.Get(clientID)
	if !ok {
		t.Fatalf("no broker-side client %q", clientID)
	}
	cl.Stop(errors.New("test: forced unclean close"))
}

func TestHostWillTimestampIsRecent_Pass(t *testing.T) {
	b, _ := NewBroker()
	defer b.Close()
	now := time.Now().UnixMilli()
	c := connectHost(t, b, "host-1", "factory", now, true)
	defer c.Disconnect(200)
	time.Sleep(50 * time.Millisecond)

	res := HostWillTimestampIsRecent(b)
	if len(res) != 1 || res[0].Status != runner.StatusPass {
		t.Errorf("expected pass, got %+v", res)
	}
}

func TestHostWillTimestampIsRecent_Stale(t *testing.T) {
	b, _ := NewBroker()
	defer b.Close()
	stale := time.Now().Add(-1 * time.Hour).UnixMilli()
	c := connectHost(t, b, "host-stale", "factory", stale, true)
	defer c.Disconnect(200)
	time.Sleep(50 * time.Millisecond)

	res := HostWillTimestampIsRecent(b)
	if len(res) != 1 || res[0].Status != runner.StatusFail {
		t.Errorf("expected fail for hour-old timestamp, got %+v", res)
	}
}

func TestHostDeathBeforeCleanDisconnect_Pass(t *testing.T) {
	b, _ := NewBroker()
	defer b.Close()
	willTS := time.Now().UnixMilli()
	c := connectHost(t, b, "host-1", "factory", willTS, true)

	deathTS := time.Now().UnixMilli()
	c.Publish("spBv1.0/STATE/factory", 1, true, stateBody(false, deathTS)).
		WaitTimeout(2 * time.Second)
	c.Disconnect(200) // clean DISCONNECT packet
	time.Sleep(100 * time.Millisecond)

	res := HostDeathBeforeCleanDisconnect(b)
	if len(res) == 0 {
		t.Errorf("expected at least one result, got none")
	}
	for _, r := range res {
		if r.Status != runner.StatusPass {
			t.Errorf("expected all pass, got %+v", res)
			break
		}
	}
}

func TestHostDeathBeforeCleanDisconnect_NoDeath(t *testing.T) {
	b, _ := NewBroker()
	defer b.Close()
	willTS := time.Now().UnixMilli()
	c := connectHost(t, b, "host-bad", "factory", willTS, true)
	// No explicit STATE death — straight to DISCONNECT.
	c.Disconnect(200)
	time.Sleep(100 * time.Millisecond)

	res := HostDeathBeforeCleanDisconnect(b)
	if len(res) == 0 {
		t.Errorf("expected at least one result, got none")
	}
	for _, r := range res {
		if r.Status != runner.StatusFail {
			t.Errorf("expected all fail (no death publish), got %+v", res)
			break
		}
	}
}

func TestHostDeathBeforeCleanDisconnect_OnlineLast(t *testing.T) {
	b, _ := NewBroker()
	defer b.Close()
	willTS := time.Now().UnixMilli()
	c := connectHost(t, b, "host-bad2", "factory", willTS, true)
	// Publish a birth (online=true) but never a death — last STATE seen
	// is online, so the rule should still fail.
	c.Publish("spBv1.0/STATE/factory", 1, true, stateBody(true, willTS)).
		WaitTimeout(2 * time.Second)
	c.Disconnect(200)
	time.Sleep(100 * time.Millisecond)

	res := HostDeathBeforeCleanDisconnect(b)
	if len(res) == 0 {
		t.Errorf("expected at least one result, got none")
	}
	for _, r := range res {
		if r.Status != runner.StatusFail {
			t.Errorf("expected all fail (last STATE was online), got %+v", res)
			break
		}
	}
}

func TestHostDeathBeforeUncleanDisconnect_Pass(t *testing.T) {
	b, _ := NewBroker()
	defer b.Close()
	willTS := time.Now().UnixMilli()
	c := connectHost(t, b, "host-uncl", "factory", willTS, true)

	deathTS := time.Now().UnixMilli()
	c.Publish("spBv1.0/STATE/factory", 1, true, stateBody(false, deathTS)).
		WaitTimeout(2 * time.Second)
	time.Sleep(50 * time.Millisecond)

	// Force unclean tear-down from the broker side: socket close with no
	// DISCONNECT packet.
	killBrokerSide(t, b, "host-uncl")
	time.Sleep(100 * time.Millisecond)
	_ = c // paho will see the conn drop

	res := HostDeathBeforeUncleanDisconnect(b)
	if len(res) != 1 || res[0].Status != runner.StatusPass {
		t.Errorf("expected pass, got %+v", res)
	}
}

func TestHostDeathBeforeUncleanDisconnect_NoDeath(t *testing.T) {
	b, _ := NewBroker()
	defer b.Close()
	willTS := time.Now().UnixMilli()
	c := connectHost(t, b, "host-uncl-bad", "factory", willTS, true)
	time.Sleep(50 * time.Millisecond)
	// Tear down without ever publishing STATE death.
	killBrokerSide(t, b, "host-uncl-bad")
	time.Sleep(100 * time.Millisecond)
	_ = c

	res := HostDeathBeforeUncleanDisconnect(b)
	if len(res) != 1 || res[0].Status != runner.StatusFail {
		t.Errorf("expected fail (no death publish), got %+v", res)
	}
}
