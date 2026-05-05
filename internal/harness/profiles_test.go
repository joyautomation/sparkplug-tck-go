package harness

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/joyautomation/sparkplug-tck-go/internal/runner"
)

// TestEdgeNodeProfile_Pass drives a compliant edge-node lifecycle and
// verifies the profile bundle returns no Fails.
func TestEdgeNodeProfile_Pass(t *testing.T) {
	b, _ := NewBroker()
	defer b.Close()

	c := connectEdge(t, b, "edge-good", "spBv1.0/G/NDEATH/N")
	c.Subscribe("spBv1.0/G/NCMD/N", 1, nil).WaitTimeout(2 * time.Second)
	c.Subscribe("spBv1.0/G/DCMD/N/D", 1, nil).WaitTimeout(2 * time.Second)
	c.Publish("spBv1.0/G/NDEATH/N", 0, false, edgeWill(0)).
		WaitTimeout(2 * time.Second)
	c.Disconnect(200)
	time.Sleep(100 * time.Millisecond)

	res := EdgeNodeProfile.Run(b)
	for _, r := range res {
		if r.Status == runner.StatusFail {
			t.Errorf("edge profile fail: %+v", r)
		}
	}
	if len(res) == 0 {
		t.Fatalf("edge profile produced no results")
	}
}

// TestHostApplicationProfile_Pass drives a compliant host lifecycle
// (CONNECT with Will + clean=true, subscribe to STATE, publish birth,
// publish death, clean DISCONNECT) and verifies the bundle returns no
// Fails.
func TestHostApplicationProfile_Pass(t *testing.T) {
	b, _ := NewBroker()
	defer b.Close()

	willTS := time.Now().UnixMilli()
	c := connectHost(t, b, "host-good", "factory", willTS, true)
	c.Subscribe("spBv1.0/STATE/factory", 1, nil).WaitTimeout(2 * time.Second)

	birth, _ := json.Marshal(struct {
		Online    bool  `json:"online"`
		Timestamp int64 `json:"timestamp"`
	}{Online: true, Timestamp: willTS})
	c.Publish("spBv1.0/STATE/factory", 1, true, birth).
		WaitTimeout(2 * time.Second)

	deathTS := time.Now().UnixMilli()
	c.Publish("spBv1.0/STATE/factory", 1, true, stateBody(false, deathTS)).
		WaitTimeout(2 * time.Second)
	c.Disconnect(200)
	time.Sleep(100 * time.Millisecond)

	res := HostApplicationProfile.Run(b)
	for _, r := range res {
		if r.Status == runner.StatusFail {
			t.Errorf("host profile fail: %+v", r)
		}
	}
	if len(res) == 0 {
		t.Fatalf("host profile produced no results")
	}
}

func TestProfilesRegistry(t *testing.T) {
	for _, want := range []string{"edge-node", "host-application"} {
		if _, ok := Profiles[want]; !ok {
			t.Errorf("registry missing profile %q", want)
		}
	}
}
