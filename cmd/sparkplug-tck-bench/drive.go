package main

import (
	"encoding/json"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"google.golang.org/protobuf/proto"

	"github.com/joyautomation/sparkplug-tck-go/internal/harness"
	"github.com/joyautomation/sparkplug-tck-go/internal/spbpb"
)

// driveEdge replays a spec-compliant Edge Node lifecycle: CONNECT with
// NDEATH Will + bdSeq, subscribe NCMD/DCMD at QoS 1, publish NBIRTH
// (bdSeq + Node Control/Rebirth + a sample metric) and per-device
// DBIRTH, then NDATA + DDATA, then DDEATH and NDEATH before clean
// DISCONNECT. Mirrors driveCompliantEdge in sparkplug-tck-correctness so
// the per-ID verdicts in the bench JSON line up with what the upstream
// Java TCK observes.
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

	now := time.Now().UnixMilli()
	seq := uint64(0)

	c.Publish("spBv1.0/G/NBIRTH/N", 0, false, nbirthPayload(now, seq, 0)).WaitTimeout(2 * time.Second)
	seq++
	c.Publish("spBv1.0/G/DBIRTH/N/D", 0, false, dbirthPayload(now, seq)).WaitTimeout(2 * time.Second)
	seq++
	c.Publish("spBv1.0/G/NDATA/N", 0, false, ndataPayload(time.Now().UnixMilli(), seq)).WaitTimeout(2 * time.Second)
	seq++
	c.Publish("spBv1.0/G/DDATA/N/D", 0, false, ddataPayload(time.Now().UnixMilli(), seq)).WaitTimeout(2 * time.Second)
	seq++

	// Inject an NCMD/Rebirth + spec-compliant response (NBIRTH + DBIRTH
	// with bdSeq UNCHANGED, fresh seq=0/1) so EdgeRespondsToRebirth /
	// EdgeRebirthHaltsData / EdgeRebirthBdSeqUnchanged score against a
	// real observation pair. Mirrors the orchestrator's NCMD rebirth
	// handler — both engines need to see this dance to PASS rebirth-action-*.
	c.Publish("spBv1.0/G/NCMD/N", 0, false, ncmdRebirthPayload(time.Now().UnixMilli())).WaitTimeout(2 * time.Second)
	time.Sleep(50 * time.Millisecond) // ensure NBIRTH timestamp is strictly after NCMD
	rebirthNow := time.Now().UnixMilli()
	c.Publish("spBv1.0/G/NBIRTH/N", 0, false, nbirthPayload(rebirthNow, 0, 0)).WaitTimeout(2 * time.Second)
	c.Publish("spBv1.0/G/DBIRTH/N/D", 0, false, dbirthPayload(rebirthNow, 1)).WaitTimeout(2 * time.Second)
	seq = 2

	c.Publish("spBv1.0/G/DDEATH/N/D", 0, false, ddeathPayload(time.Now().UnixMilli(), seq)).WaitTimeout(2 * time.Second)
	c.Publish("spBv1.0/G/NDEATH/N", 0, false, bdSeqPayload(0)).WaitTimeout(2 * time.Second)
	c.Disconnect(200)
	time.Sleep(100 * time.Millisecond)
}

func nbirthPayload(ts int64, seq, bdSeq uint64) []byte {
	tsU := uint64(ts)
	bdSeqDT := uint32(spbpb.DataType_Int64)
	boolDT := uint32(spbpb.DataType_Boolean)
	intDT := uint32(spbpb.DataType_Int32)
	tmplDT := uint32(spbpb.DataType_Template)
	bdSeqName := "bdSeq"
	rebirthName := "Node Control/Rebirth"
	hbName := "Heartbeat"
	tempName := "Temperature"
	tmplName := "MotorDef"
	rebirthVal := false
	hbVal := uint32(0)
	bd := bdSeq
	p := &spbpb.Payload{
		Timestamp: &tsU,
		Seq:       &seq,
		Metrics: []*spbpb.Payload_Metric{
			{Name: &bdSeqName, Datatype: &bdSeqDT, Timestamp: &tsU,
				Value: &spbpb.Payload_Metric_LongValue{LongValue: bd}},
			{Name: &rebirthName, Datatype: &boolDT, Timestamp: &tsU,
				Value: &spbpb.Payload_Metric_BooleanValue{BooleanValue: rebirthVal}},
			{Name: &hbName, Datatype: &intDT, Timestamp: &tsU,
				Value: &spbpb.Payload_Metric_IntValue{IntValue: hbVal}},
			// Metric carrying a PropertySet so EdgePropertySetCompliant
			// has something to score (keys/values sizes, PropertyValue
			// shape, the Quality property type rule).
			{Name: &tempName, Datatype: &intDT, Timestamp: &tsU,
				Value:      &spbpb.Payload_Metric_IntValue{IntValue: uint32(72)},
				Properties: sampleMetricProperties()},
			// Template Definition so EdgeTemplateCompliant scores
			// definition rules + matches the DBIRTH instance below.
			{Name: &tmplName, Datatype: &tmplDT, Timestamp: &tsU,
				Value: &spbpb.Payload_Metric_TemplateValue{
					TemplateValue: motorTemplate(true /*definition*/),
				}},
		},
	}
	raw, _ := proto.Marshal(p)
	return raw
}

func sampleMetricProperties() *spbpb.Payload_PropertySet {
	int32Type := uint32(spbpb.DataType_Int32)
	strType := uint32(spbpb.DataType_String)
	qualityVal := uint32(192) // good
	engUnit := "degF"
	return &spbpb.Payload_PropertySet{
		Keys: []string{"quality", "engUnit"},
		Values: []*spbpb.Payload_PropertyValue{
			{Type: &int32Type, Value: &spbpb.Payload_PropertyValue_IntValue{IntValue: qualityVal}},
			{Type: &strType, Value: &spbpb.Payload_PropertyValue_StringValue{StringValue: engUnit}},
		},
	}
}

// motorTemplate returns a Template payload — either the definition (no
// template_ref, has Members) or an instance (template_ref="MotorDef",
// has Members + Parameters). EdgeTemplateCompliant scores both shapes.
func motorTemplate(isDefinition bool) *spbpb.Payload_Template {
	intDT := uint32(spbpb.DataType_Int32)
	strDT := uint32(spbpb.DataType_String)
	rpmName := "rpm"
	statusName := "status"
	rpmVal := uint32(0)
	statusVal := "stopped"
	version := "1.0.0"
	isDef := isDefinition
	tmpl := &spbpb.Payload_Template{
		Version:      &version,
		IsDefinition: &isDef,
		Metrics: []*spbpb.Payload_Metric{
			{Name: &rpmName, Datatype: &intDT,
				Value: &spbpb.Payload_Metric_IntValue{IntValue: rpmVal}},
			{Name: &statusName, Datatype: &strDT,
				Value: &spbpb.Payload_Metric_StringValue{StringValue: statusVal}},
		},
		Parameters: []*spbpb.Payload_Template_Parameter{
			{Name: stringPtr("model"), Type: &strDT,
				Value: &spbpb.Payload_Template_Parameter_StringValue{StringValue: "ACME-1000"}},
			{Name: stringPtr("rated_rpm"), Type: &intDT,
				Value: &spbpb.Payload_Template_Parameter_IntValue{IntValue: 1750}},
		},
	}
	if !isDefinition {
		ref := "MotorDef"
		tmpl.TemplateRef = &ref
	}
	return tmpl
}

func stringPtr(s string) *string { return &s }

func dbirthPayload(ts int64, seq uint64) []byte {
	tsU := uint64(ts)
	intDT := uint32(spbpb.DataType_Int32)
	tmplDT := uint32(spbpb.DataType_Template)
	name := "Counter"
	tmplName := "Motor1"
	v := uint32(0)
	p := &spbpb.Payload{
		Timestamp: &tsU,
		Seq:       &seq,
		Metrics: []*spbpb.Payload_Metric{
			{Name: &name, Datatype: &intDT, Timestamp: &tsU,
				Value: &spbpb.Payload_Metric_IntValue{IntValue: v}},
			// Template Instance referencing the MotorDef definition
			// published in NBIRTH so EdgeTemplateCompliant can score
			// instance/ref/members rules against a known definition.
			{Name: &tmplName, Datatype: &tmplDT, Timestamp: &tsU,
				Value: &spbpb.Payload_Metric_TemplateValue{
					TemplateValue: motorTemplate(false /*instance*/),
				}},
		},
	}
	raw, _ := proto.Marshal(p)
	return raw
}

func ndataPayload(ts int64, seq uint64) []byte {
	tsU := uint64(ts)
	intDT := uint32(spbpb.DataType_Int32)
	name := "Heartbeat"
	v := uint32(1)
	p := &spbpb.Payload{
		Timestamp: &tsU,
		Seq:       &seq,
		Metrics: []*spbpb.Payload_Metric{
			{Name: &name, Datatype: &intDT, Timestamp: &tsU,
				Value: &spbpb.Payload_Metric_IntValue{IntValue: v}},
		},
	}
	raw, _ := proto.Marshal(p)
	return raw
}

func ddataPayload(ts int64, seq uint64) []byte {
	tsU := uint64(ts)
	intDT := uint32(spbpb.DataType_Int32)
	tmplDT := uint32(spbpb.DataType_Template)
	name := "Counter"
	tmplName := "Motor1"
	v := uint32(1)
	p := &spbpb.Payload{
		Timestamp: &tsU,
		Seq:       &seq,
		Metrics: []*spbpb.Payload_Metric{
			{Name: &name, Datatype: &intDT, Timestamp: &tsU,
				Value: &spbpb.Payload_Metric_IntValue{IntValue: v}},
			// Template Instance update so template-instance-members-data
			// can score (the assertion only fires on NDATA/DDATA).
			{Name: &tmplName, Datatype: &tmplDT, Timestamp: &tsU,
				Value: &spbpb.Payload_Metric_TemplateValue{
					TemplateValue: motorTemplate(false /*instance*/),
				}},
		},
	}
	raw, _ := proto.Marshal(p)
	return raw
}

func ddeathPayload(ts int64, seq uint64) []byte {
	tsU := uint64(ts)
	p := &spbpb.Payload{
		Timestamp: &tsU,
		Seq:       &seq,
	}
	raw, _ := proto.Marshal(p)
	return raw
}

// driveHost replays a compliant Host Application lifecycle: CONNECT
// with STATE Will + clean=true, subscribe STATE, publish birth, publish
// NCMD (rebirth + named-metric write) + DCMD (named-metric write) so
// the HostNCMDCompliant / HostDCMDCompliant scenarios actually score
// instead of falling through to NOT_EXECUTED, then publish death and
// clean DISCONNECT.
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

	c.Publish("spBv1.0/G/NCMD/N", 0, false, ncmdRebirthPayload(time.Now().UnixMilli())).WaitTimeout(2 * time.Second)
	c.Publish("spBv1.0/G/NCMD/N", 0, false, ncmdMetricPayload(time.Now().UnixMilli(), "Heartbeat", 7)).WaitTimeout(2 * time.Second)
	c.Publish("spBv1.0/G/DCMD/N/D", 0, false, dcmdMetricPayload(time.Now().UnixMilli(), "Counter", 42)).WaitTimeout(2 * time.Second)

	// Inject NDEATH + DDEATH on the broker so HostNDEATHActions /
	// HostDDEATHActions can score the host's edge-termination rules.
	// In a real deployment these come from the edge — the bench
	// publishes them as a phantom edge so the broker events include
	// them in the host profile run.
	c.Publish("spBv1.0/G/DDEATH/N/D", 0, false, ddeathPayload(time.Now().UnixMilli(), 5)).WaitTimeout(2 * time.Second)
	c.Publish("spBv1.0/G/NDEATH/N", 0, false, bdSeqPayload(0)).WaitTimeout(2 * time.Second)

	deathTS := time.Now().UnixMilli()
	death, _ := json.Marshal(stateBody{Online: false, Timestamp: deathTS})
	c.Publish("spBv1.0/STATE/factory", 1, true, death).WaitTimeout(2 * time.Second)
	c.Disconnect(200)
	time.Sleep(100 * time.Millisecond)
}

func ncmdRebirthPayload(ts int64) []byte {
	tsU := uint64(ts)
	boolDT := uint32(spbpb.DataType_Boolean)
	name := "Node Control/Rebirth"
	p := &spbpb.Payload{
		Timestamp: &tsU,
		Metrics: []*spbpb.Payload_Metric{{
			Name: &name, Datatype: &boolDT, Timestamp: &tsU,
			Value: &spbpb.Payload_Metric_BooleanValue{BooleanValue: true},
		}},
	}
	raw, _ := proto.Marshal(p)
	return raw
}

func ncmdMetricPayload(ts int64, name string, v int32) []byte {
	tsU := uint64(ts)
	intDT := uint32(spbpb.DataType_Int32)
	val := uint32(v)
	p := &spbpb.Payload{
		Timestamp: &tsU,
		Metrics: []*spbpb.Payload_Metric{{
			Name: &name, Datatype: &intDT, Timestamp: &tsU,
			Value: &spbpb.Payload_Metric_IntValue{IntValue: val},
		}},
	}
	raw, _ := proto.Marshal(p)
	return raw
}

func dcmdMetricPayload(ts int64, name string, v int32) []byte {
	tsU := uint64(ts)
	intDT := uint32(spbpb.DataType_Int32)
	val := uint32(v)
	p := &spbpb.Payload{
		Timestamp: &tsU,
		Metrics: []*spbpb.Payload_Metric{{
			Name: &name, Datatype: &intDT, Timestamp: &tsU,
			Value: &spbpb.Payload_Metric_IntValue{IntValue: val},
		}},
	}
	raw, _ := proto.Marshal(p)
	return raw
}

type stateBody struct {
	Online    bool  `json:"online"`
	Timestamp int64 `json:"timestamp"`
}

func bdSeqPayload(seq uint64) []byte {
	// Java TCK Monitor strictly checks bdSeq datatype == Int64 per
	// spec text. Match that to keep orchestrator/bench in lockstep.
	dt := uint32(spbpb.DataType_Int64)
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
