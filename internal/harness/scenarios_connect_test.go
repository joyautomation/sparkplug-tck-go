package harness

import (
	"encoding/json"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"google.golang.org/protobuf/proto"

	"github.com/joyautomation/sparkplug-tck-go/internal/runner"
	"github.com/joyautomation/sparkplug-tck-go/internal/spbpb"
)

// edgeWill builds a minimal compliant NDEATH protobuf payload for
// CONNECT Will tests — bdSeq metric, no seq, no timestamp.
func edgeWill(bdSeq uint64) []byte {
	dt := uint32(spbpb.DataType_UInt64)
	bd := uint64(bdSeq)
	name := "bdSeq"
	p := &spbpb.Payload{
		Metrics: []*spbpb.Payload_Metric{{
			Name:     &name,
			Datatype: &dt,
			Value:    &spbpb.Payload_Metric_LongValue{LongValue: bd},
		}},
	}
	raw, _ := proto.Marshal(p)
	return raw
}

// connectEdge connects a paho client posing as an Edge Node, with the
// Will pointing at NDEATH and carrying a compliant bdSeq payload.
func connectEdge(t *testing.T, b *Broker, clientID, willTopic string, opt ...func(*mqtt.ClientOptions)) mqtt.Client {
	t.Helper()
	opts := mqtt.NewClientOptions().
		AddBroker(b.URL()).
		SetClientID(clientID).
		SetCleanSession(true).
		SetConnectTimeout(2 * time.Second).
		SetBinaryWill(willTopic, edgeWill(0), 1, false)
	for _, fn := range opt {
		fn(opts)
	}
	c := mqtt.NewClient(opts)
	if tok := c.Connect(); !tok.WaitTimeout(3*time.Second) || tok.Error() != nil {
		t.Fatalf("connect: %v", tok.Error())
	}
	return c
}

// connectHost connects a paho client posing as a host application:
// Will on STATE, payload is JSON with online=false + a timestamp.
func connectHost(t *testing.T, b *Broker, clientID, hostID string, willTS int64, clean bool) mqtt.Client {
	t.Helper()
	willTopic := "spBv1.0/STATE/" + hostID
	willBody, _ := json.Marshal(struct {
		Online    bool  `json:"online"`
		Timestamp int64 `json:"timestamp"`
	}{Online: false, Timestamp: willTS})
	opts := mqtt.NewClientOptions().
		AddBroker(b.URL()).
		SetClientID(clientID).
		SetCleanSession(clean).
		SetConnectTimeout(2 * time.Second).
		SetBinaryWill(willTopic, willBody, 1, true)
	c := mqtt.NewClient(opts)
	if tok := c.Connect(); !tok.WaitTimeout(3*time.Second) || tok.Error() != nil {
		t.Fatalf("connect: %v", tok.Error())
	}
	return c
}

func TestEdgeWillIsNDEATH_Pass(t *testing.T) {
	b, _ := NewBroker()
	defer b.Close()
	c := connectEdge(t, b, "edge-good", "spBv1.0/G/NDEATH/N")
	defer c.Disconnect(200)
	time.Sleep(50 * time.Millisecond)

	for _, fn := range []Scenario{EdgeWillIsNDEATH, EdgeWillPayloadHasBdSeq} {
		res := fn(b)
		if len(res) != 1 || res[0].Status != runner.StatusPass {
			t.Errorf("expected pass, got %+v", res)
		}
	}
}

func TestEdgeWillPayloadHasBdSeq_BadPayload(t *testing.T) {
	b, _ := NewBroker()
	defer b.Close()
	// Will payload is gibberish — proto unmarshal fails.
	opts := mqtt.NewClientOptions().
		AddBroker(b.URL()).
		SetClientID("edge-bad").
		SetCleanSession(true).
		SetConnectTimeout(2 * time.Second).
		SetBinaryWill("spBv1.0/G/NDEATH/N", []byte("not protobuf"), 1, false)
	c := mqtt.NewClient(opts)
	c.Connect().WaitTimeout(3 * time.Second)
	defer c.Disconnect(200)
	time.Sleep(50 * time.Millisecond)

	res := EdgeWillPayloadHasBdSeq(b)
	if len(res) != 1 || res[0].Status != runner.StatusFail {
		t.Errorf("expected fail for non-proto Will, got %+v", res)
	}
}

func TestHostCONNECTHasWill_Pass(t *testing.T) {
	b, _ := NewBroker()
	defer b.Close()
	c := connectHost(t, b, "host-1", "factory", 1000, true)
	defer c.Disconnect(200)
	time.Sleep(50 * time.Millisecond)

	res := HostCONNECTHasWill(b)
	if len(res) != 1 || res[0].Status != runner.StatusPass {
		t.Errorf("expected pass, got %+v", res)
	}
}

func TestHostCleanSession_Pass(t *testing.T) {
	b, _ := NewBroker()
	defer b.Close()
	c := connectHost(t, b, "host-1", "factory", 1000, true)
	defer c.Disconnect(200)
	time.Sleep(50 * time.Millisecond)

	res := HostCleanSession(b)
	hasFail := false
	for _, r := range res {
		if r.Status == runner.StatusFail {
			hasFail = true
		}
	}
	if hasFail {
		t.Errorf("expected all passes, got %+v", res)
	}
}

func TestHostCleanSession_Fail(t *testing.T) {
	b, _ := NewBroker()
	defer b.Close()
	c := connectHost(t, b, "host-bad", "factory", 1000, false /* clean=false */)
	defer c.Disconnect(200)
	time.Sleep(50 * time.Millisecond)

	res := HostCleanSession(b)
	hasFail := false
	for _, r := range res {
		if r.Status == runner.StatusFail {
			hasFail = true
		}
	}
	if !hasFail {
		t.Errorf("expected fail when Clean=false, got %+v", res)
	}
}

func TestEdgeNCMDSubscribeQoS_Pass(t *testing.T) {
	b, _ := NewBroker()
	defer b.Close()
	c := connectEdge(t, b, "edge-sub", "spBv1.0/G/NDEATH/N")
	defer c.Disconnect(200)
	if tok := c.Subscribe("spBv1.0/G/NCMD/N", 1, nil); !tok.WaitTimeout(2*time.Second) {
		t.Fatalf("subscribe timeout")
	}
	time.Sleep(50 * time.Millisecond)

	res := EdgeNCMDSubscribeQoS(b)
	if len(res) != 1 || res[0].Status != runner.StatusPass {
		t.Errorf("expected pass, got %+v", res)
	}
}

func TestEdgeNCMDSubscribeQoS_BadQoS(t *testing.T) {
	b, _ := NewBroker()
	defer b.Close()
	c := connectEdge(t, b, "edge-sub-bad", "spBv1.0/G/NDEATH/N")
	defer c.Disconnect(200)
	c.Subscribe("spBv1.0/G/NCMD/N", 0, nil).WaitTimeout(2 * time.Second)
	time.Sleep(50 * time.Millisecond)

	res := EdgeNCMDSubscribeQoS(b)
	if len(res) != 1 || res[0].Status != runner.StatusFail {
		t.Errorf("expected fail for QoS=0, got %+v", res)
	}
}

func TestHostSTATEBirthAfterSubscribe_Pass(t *testing.T) {
	b, _ := NewBroker()
	defer b.Close()
	c := connectHost(t, b, "host-1", "factory", 1000, true)
	defer c.Disconnect(200)
	c.Subscribe("spBv1.0/STATE/factory", 1, nil).WaitTimeout(2 * time.Second)
	birth, _ := json.Marshal(struct {
		Online    bool  `json:"online"`
		Timestamp int64 `json:"timestamp"`
	}{Online: true, Timestamp: 1000})
	c.Publish("spBv1.0/STATE/factory", 1, true, birth).WaitTimeout(2 * time.Second)
	time.Sleep(100 * time.Millisecond)

	res := HostSTATEBirthAfterSubscribe(b)
	if len(res) != 1 || res[0].Status != runner.StatusPass {
		t.Errorf("expected pass, got %+v", res)
	}
}

func TestHostSTATEBirthAfterSubscribe_OutOfOrder(t *testing.T) {
	b, _ := NewBroker()
	defer b.Close()
	c := connectHost(t, b, "host-bad", "factory", 1000, true)
	defer c.Disconnect(200)
	birth, _ := json.Marshal(struct {
		Online    bool  `json:"online"`
		Timestamp int64 `json:"timestamp"`
	}{Online: true, Timestamp: 1000})
	c.Publish("spBv1.0/STATE/factory", 1, true, birth).WaitTimeout(2 * time.Second)
	c.Subscribe("spBv1.0/STATE/factory", 1, nil).WaitTimeout(2 * time.Second)
	time.Sleep(100 * time.Millisecond)

	res := HostSTATEBirthAfterSubscribe(b)
	if len(res) != 1 || res[0].Status != runner.StatusFail {
		t.Errorf("expected fail (publish before subscribe), got %+v", res)
	}
}

func TestHostBirthTimestampMatchesWill_Pass(t *testing.T) {
	b, _ := NewBroker()
	defer b.Close()
	c := connectHost(t, b, "host-1", "factory", 9999, true)
	defer c.Disconnect(200)
	c.Subscribe("spBv1.0/STATE/factory", 1, nil).WaitTimeout(2 * time.Second)
	birth, _ := json.Marshal(struct {
		Online    bool  `json:"online"`
		Timestamp int64 `json:"timestamp"`
	}{Online: true, Timestamp: 9999})
	c.Publish("spBv1.0/STATE/factory", 1, true, birth).WaitTimeout(2 * time.Second)
	time.Sleep(100 * time.Millisecond)

	res := HostBirthTimestampMatchesWill(b)
	if len(res) != 1 || res[0].Status != runner.StatusPass {
		t.Errorf("expected pass, got %+v", res)
	}
}

func TestHostBirthTimestampMatchesWill_Mismatch(t *testing.T) {
	b, _ := NewBroker()
	defer b.Close()
	c := connectHost(t, b, "host-bad", "factory", 9999, true)
	defer c.Disconnect(200)
	birth, _ := json.Marshal(struct {
		Online    bool  `json:"online"`
		Timestamp int64 `json:"timestamp"`
	}{Online: true, Timestamp: 7777}) // != Will timestamp
	c.Publish("spBv1.0/STATE/factory", 1, true, birth).WaitTimeout(2 * time.Second)
	time.Sleep(100 * time.Millisecond)

	res := HostBirthTimestampMatchesWill(b)
	if len(res) != 1 || res[0].Status != runner.StatusFail {
		t.Errorf("expected fail for timestamp mismatch, got %+v", res)
	}
}
