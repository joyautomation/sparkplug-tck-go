package main

import (
	"encoding/json"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"google.golang.org/protobuf/proto"

	"github.com/joyautomation/sparkplug-tck-go/internal/harness"
	"github.com/joyautomation/sparkplug-tck-go/internal/spbpb"
)

// driveEdge replays a compliant Edge Node lifecycle: CONNECT with
// NDEATH Will + bdSeq, subscribe NCMD/DCMD at QoS 1, publish NDEATH,
// clean DISCONNECT. Used by the parity bench to populate the broker
// without a real SUT.
func driveEdge(b *harness.Broker) {
	c := mqtt.NewClient(mqtt.NewClientOptions().
		AddBroker(b.URL()).
		SetClientID("bench-edge").
		SetCleanSession(true).
		SetConnectTimeout(2 * time.Second).
		SetBinaryWill("spBv1.0/G/NDEATH/N", bdSeqPayload(0), 1, false))
	if tok := c.Connect(); !tok.WaitTimeout(3*time.Second) || tok.Error() != nil {
		return
	}
	c.Subscribe("spBv1.0/G/NCMD/N", 1, nil).WaitTimeout(2 * time.Second)
	c.Subscribe("spBv1.0/G/DCMD/N/D", 1, nil).WaitTimeout(2 * time.Second)
	c.Publish("spBv1.0/G/NDEATH/N", 0, false, bdSeqPayload(0)).WaitTimeout(2 * time.Second)
	c.Disconnect(200)
	time.Sleep(100 * time.Millisecond)
}

// driveHost replays a compliant Host Application lifecycle: CONNECT
// with STATE Will + clean=true, subscribe STATE, publish birth, publish
// death, clean DISCONNECT.
func driveHost(b *harness.Broker) {
	now := time.Now().UnixMilli()
	willBody, _ := json.Marshal(stateBody{Online: false, Timestamp: now})
	c := mqtt.NewClient(mqtt.NewClientOptions().
		AddBroker(b.URL()).
		SetClientID("bench-host").
		SetCleanSession(true).
		SetConnectTimeout(2 * time.Second).
		SetBinaryWill("spBv1.0/STATE/factory", willBody, 1, true))
	if tok := c.Connect(); !tok.WaitTimeout(3*time.Second) || tok.Error() != nil {
		return
	}
	c.Subscribe("spBv1.0/STATE/factory", 1, nil).WaitTimeout(2 * time.Second)
	birth, _ := json.Marshal(stateBody{Online: true, Timestamp: now})
	c.Publish("spBv1.0/STATE/factory", 1, true, birth).WaitTimeout(2 * time.Second)
	deathTS := time.Now().UnixMilli()
	death, _ := json.Marshal(stateBody{Online: false, Timestamp: deathTS})
	c.Publish("spBv1.0/STATE/factory", 1, true, death).WaitTimeout(2 * time.Second)
	c.Disconnect(200)
	time.Sleep(100 * time.Millisecond)
}

type stateBody struct {
	Online    bool  `json:"online"`
	Timestamp int64 `json:"timestamp"`
}

func bdSeqPayload(seq uint64) []byte {
	dt := uint32(spbpb.DataType_UInt64)
	name := "bdSeq"
	v := seq
	p := &spbpb.Payload{Metrics: []*spbpb.Payload_Metric{{
		Name:     &name,
		Datatype: &dt,
		Value:    &spbpb.Payload_Metric_LongValue{LongValue: v},
	}}}
	raw, _ := proto.Marshal(p)
	return raw
}
