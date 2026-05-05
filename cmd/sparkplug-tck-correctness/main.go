// Command sparkplug-tck-correctness drives synthetic SUTs through the
// upstream Java TCK (running as a HiveMQ extension on localhost:1883)
// and captures per-ID verdicts for each named test class. Pair with the
// Go bench's harness output to diff per-ID agreement — see
// scripts/upstream-tck/README.md.
//
// Prereqs: HiveMQ + Sparkplug TCK extension running on the broker URL,
// staged by gradle :tck:prepareHivemqHome and booted by
// scripts/upstream-tck/start-hivemq.sh.
//
// Output is JSON (one report per test) on stdout plus a one-line
// summary per test on stderr. The Java TCK additionally writes
// SparkplugTCKresults.log in the HiveMQ working directory; we capture
// results live off MQTT and don't depend on that file.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"google.golang.org/protobuf/proto"

	"github.com/joyautomation/sparkplug-tck-go/internal/spbpb"
)

// driverKind picks which synthetic SUT lifecycle the orchestrator drives
// for a given test. Edge tests need a Sparkplug edge node; host tests
// need a Sparkplug Host Application publishing STATE.
type driverKind string

const (
	driverEdge driverKind = "edge"
	driverHost driverKind = "host"
)

const (
	topicTestControl   = "SPARKPLUG_TCK/TEST_CONTROL"
	topicResults       = "SPARKPLUG_TCK/RESULT"
	topicLog           = "SPARKPLUG_TCK/LOG"
	topicResultsConfig = "SPARKPLUG_TCK/RESULT_CONFIG"
)

type verdict struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

type report struct {
	Test     string    `json:"test"`
	Verdicts []verdict `json:"verdicts"`
	Overall  string    `json:"overall,omitempty"`
	Counts   counts    `json:"counts"`
}

type counts struct {
	Pass        int `json:"pass"`
	Fail        int `json:"fail"`
	NotExecuted int `json:"not_executed"`
	Other       int `json:"other"`
	Total       int `json:"total"`
}

// defaultTestSet is the small spread the bench uses out-of-the-box —
// covers both SUT profiles and an "edge data" path so we sample more
// than the session-handshake corner of the spec.
const defaultTestSet = "edge SessionEstablishmentTest,edge SendDataTest,host SessionEstablishmentTest"

func main() {
	broker := flag.String("broker", "tcp://localhost:1883", "MQTT broker URL (HiveMQ + TCK extension)")
	tests := flag.String("tests", defaultTestSet, "comma-separated TCK tests as 'profile TestClass' (e.g. 'edge SessionEstablishmentTest')")
	hostID := flag.String("host", "TCKHost", "Primary Host Application ID for tests that take one")
	groupID := flag.String("group", "TCKGroup", "Sparkplug Group ID for edge tests")
	edgeID := flag.String("edge", "TCKEdge", "Sparkplug Edge Node ID prefix for edge tests")
	deviceID := flag.String("device", "TCKDevice", "Sparkplug Device ID for edge tests")
	timeout := flag.Duration("timeout", 60*time.Second, "max wall-clock for one test")
	flag.Parse()

	specs := splitTests(*tests)
	if len(specs) == 0 {
		fail("no tests requested")
	}

	ctrl := newCollector()
	if err := ctrl.connect(*broker); err != nil {
		fail("connect: %v", err)
	}
	defer ctrl.close()

	reports := make([]report, 0, len(specs))
	for i, spec := range specs {
		profile, testName, err := parseSpec(spec)
		if err != nil {
			fail("test %d: %v", i, err)
		}

		// Each test gets fresh collector state so verdicts don't bleed.
		ctrl.reset()

		// Use unique edge/host IDs per run so the TCK extension never
		// thinks "this SUT was already seen this session" — the second
		// run after that diagnosis would otherwise time out.
		runEdge := fmt.Sprintf("%s%d", *edgeID, i)
		runHost := fmt.Sprintf("%s%d", *hostID, i)

		var args []string
		var driver func()
		switch driverKindFor(profile, testName) {
		case driverEdge:
			args = []string{runHost, *groupID, runEdge, *deviceID}
			driver = func() { driveCompliantEdge(*broker, runHost, *groupID, runEdge, *deviceID) }
		case driverHost:
			args = []string{runHost}
			driver = func() { driveCompliantHost(*broker, runHost) }
		}

		if err := ctrl.startTest(profile, testName, args); err != nil {
			fail("start %s: %v", spec, err)
		}
		go driver()

		if err := ctrl.waitForOverall(*timeout); err != nil {
			fmt.Fprintf(os.Stderr, "%s — TIMEOUT after %s, partial results captured\n", spec, *timeout)
		}
		_ = ctrl.endTest()

		rep := ctrl.report(profile + "/" + testName)
		reports = append(reports, rep)
		fmt.Fprintf(os.Stderr, "%s — pass:%d fail:%d not_executed:%d other:%d (overall %s)\n",
			rep.Test, rep.Counts.Pass, rep.Counts.Fail, rep.Counts.NotExecuted, rep.Counts.Other, rep.Overall)
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(struct {
		Tests []report `json:"tests"`
	}{reports})
}

func splitTests(s string) []string {
	var out []string
	for _, t := range strings.Split(s, ",") {
		t = strings.TrimSpace(t)
		if t != "" {
			out = append(out, t)
		}
	}
	return out
}

func parseSpec(s string) (profile, testName string, err error) {
	parts := strings.Fields(s)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("expected 'profile TestClass', got %q", s)
	}
	return parts[0], parts[1], nil
}

// driverKindFor maps a test spec to which SUT lifecycle to run. Tests
// in the host/* tree need a host SUT; everything else (edge/*) needs
// an edge node.
func driverKindFor(profile, _ string) driverKind {
	if profile == "host" {
		return driverHost
	}
	return driverEdge
}

// collector subscribes to TCK result topics, parses lines, and waits
// for the OVERALL marker that signals end-of-test.
type collector struct {
	c        mqtt.Client
	mu       sync.Mutex
	verdicts []verdict
	overall  string
	overallC chan struct{}
}

func newCollector() *collector {
	return &collector{overallC: make(chan struct{})}
}

func (c *collector) connect(url string) error {
	// Route via DefaultPublishHandler — paho's per-subscription callbacks
	// get bypassed in a way we couldn't pin down on this broker, so the
	// safe path is one global handler that dispatches by topic.
	opts := mqtt.NewClientOptions().
		AddBroker(url).
		SetClientID(fmt.Sprintf("sparkplug-tck-correctness-%d", time.Now().UnixNano())).
		SetCleanSession(true).
		SetConnectTimeout(5 * time.Second).
		SetDefaultPublishHandler(func(cli mqtt.Client, msg mqtt.Message) {
			if msg.Topic() == topicResults {
				c.onResult(cli, msg)
			}
		})
	c.c = mqtt.NewClient(opts)
	tok := c.c.Connect()
	if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		return fmt.Errorf("connect: %v", tok.Error())
	}
	if tok := c.c.Subscribe(topicResults, 1, nil); !tok.WaitTimeout(2*time.Second) || tok.Error() != nil {
		return fmt.Errorf("sub results: %v", tok.Error())
	}
	if tok := c.c.Subscribe(topicLog, 1, nil); !tok.WaitTimeout(2*time.Second) || tok.Error() != nil {
		return fmt.Errorf("sub log: %v", tok.Error())
	}
	return nil
}

// startTest publishes a NEW_TEST command to the TCK extension and gives
// it a moment to wire interceptors before the synthetic SUT connects.
// `args` is the test-specific positional list (e.g. "host group edge
// device" for edge tests, "host" for host tests).
func (c *collector) startTest(profile, name string, args []string) error {
	parts := append([]string{"NEW_TEST", profile, name}, args...)
	payload := strings.Join(parts, " ")
	tok := c.c.Publish(topicTestControl, 1, false, payload)
	if !tok.WaitTimeout(2*time.Second) || tok.Error() != nil {
		return fmt.Errorf("publish NEW_TEST: %v", tok.Error())
	}
	time.Sleep(500 * time.Millisecond)
	return nil
}

// reset clears collector state between test runs so verdicts from
// run N don't leak into run N+1.
func (c *collector) reset() {
	c.mu.Lock()
	c.verdicts = nil
	c.overall = ""
	c.overallC = make(chan struct{})
	c.mu.Unlock()
}

func (c *collector) endTest() error {
	tok := c.c.Publish(topicTestControl, 1, false, "END_TEST")
	if !tok.WaitTimeout(2*time.Second) || tok.Error() != nil {
		return fmt.Errorf("publish END_TEST: %v", tok.Error())
	}
	return nil
}

// onResult parses lines emitted by the TCK extension. Each line takes
// the shape "<assertion-id>: <PASS|FAIL|NOT_EXECUTED>;" with an
// "OVERALL: <status>;" line as the terminator. Some lines (timestamp
// header, "Monitor: <subID>: <status>;" indirection) are skipped or
// flattened so each verdict has a single ID.
func (c *collector) onResult(_ mqtt.Client, msg mqtt.Message) {
	body := strings.TrimSpace(string(msg.Payload()))
	for _, ln := range strings.Split(body, "\n") {
		ln = strings.TrimSpace(strings.TrimSuffix(ln, ";"))
		if ln == "" {
			continue
		}
		// "Monitor: <subId>: <status>" → flatten to "<subId>: <status>".
		ln = strings.TrimPrefix(ln, "Monitor: ")
		// Status comes after the LAST ": " — IDs themselves never embed it.
		sep := strings.LastIndex(ln, ": ")
		if sep <= 0 {
			continue
		}
		id := strings.TrimSpace(ln[:sep])
		status := strings.TrimSpace(ln[sep+2:])
		// Filter to recognized verdict statuses; everything else is preamble
		// (timestamp lines, "Summary Test Results...", etc).
		if !isVerdictStatus(status) {
			continue
		}
		c.mu.Lock()
		if id == "OVERALL" {
			c.overall = status
			c.mu.Unlock()
			select {
			case <-c.overallC:
			default:
				close(c.overallC)
			}
			continue
		}
		c.verdicts = append(c.verdicts, verdict{ID: id, Status: status})
		c.mu.Unlock()
	}
}

func isVerdictStatus(s string) bool {
	switch {
	case strings.HasPrefix(s, "PASS"),
		strings.HasPrefix(s, "FAIL"),
		strings.HasPrefix(s, "NOT_EXECUTED"),
		strings.HasPrefix(s, "NOT EXECUTED"),
		strings.HasPrefix(s, "EMPTY"):
		return true
	}
	return false
}

func (c *collector) waitForOverall(d time.Duration) error {
	select {
	case <-c.overallC:
		return nil
	case <-time.After(d):
		return fmt.Errorf("timed out after %s waiting for OVERALL", d)
	}
}

func (c *collector) report(test string) report {
	c.mu.Lock()
	defer c.mu.Unlock()
	rep := report{Test: test, Verdicts: append([]verdict(nil), c.verdicts...), Overall: c.overall}
	for _, v := range c.verdicts {
		rep.Counts.Total++
		switch {
		case strings.HasPrefix(v.Status, "PASS"):
			rep.Counts.Pass++
		case strings.HasPrefix(v.Status, "FAIL"):
			rep.Counts.Fail++
		case strings.HasPrefix(v.Status, "NOT_EXECUTED"), strings.HasPrefix(v.Status, "NOT EXECUTED"):
			rep.Counts.NotExecuted++
		default:
			rep.Counts.Other++
		}
	}
	return rep
}

func (c *collector) close() {
	if c.c != nil {
		c.c.Disconnect(200)
	}
}

// driveCompliantEdge runs a minimal-but-spec-compliant edge node
// lifecycle: NDEATH Will + bdSeq, NBIRTH (bdSeq + Node Control/Rebirth +
// timestamp + seq=0), per-device DBIRTH (seq=1+), NDATA, DDATA, DDEATH,
// NDEATH on disconnect. Enough for SessionEstablishment / SendData to
// reach a verdict on every assertion ID they track.
func driveCompliantEdge(broker, _, group, edge, device string) {
	clientID := "tck-correctness-edge-" + edge
	willTopic := fmt.Sprintf("spBv1.0/%s/NDEATH/%s", group, edge)

	bdSeq := uint64(0)
	willPayload := bdSeqPayload(bdSeq)

	opts := mqtt.NewClientOptions().
		AddBroker(broker).
		SetClientID(clientID).
		SetCleanSession(true).
		SetConnectTimeout(5 * time.Second).
		SetBinaryWill(willTopic, willPayload, 1, false)
	c := mqtt.NewClient(opts)
	if tok := c.Connect(); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		return
	}
	defer c.Disconnect(200)

	// Subscribe NCMD/DCMD before publishing births.
	c.Subscribe(fmt.Sprintf("spBv1.0/%s/NCMD/%s", group, edge), 1, nil).WaitTimeout(2 * time.Second)
	c.Subscribe(fmt.Sprintf("spBv1.0/%s/DCMD/%s/%s", group, edge, device), 1, nil).WaitTimeout(2 * time.Second)

	now := time.Now().UnixMilli()
	seq := uint64(0)

	// NBIRTH: bdSeq + Node Control/Rebirth (Boolean=false) + a generic metric.
	c.Publish(fmt.Sprintf("spBv1.0/%s/NBIRTH/%s", group, edge), 0, false,
		nbirthPayload(now, seq, bdSeq)).WaitTimeout(2 * time.Second)
	seq++

	// DBIRTH for the device.
	c.Publish(fmt.Sprintf("spBv1.0/%s/DBIRTH/%s/%s", group, edge, device), 0, false,
		dbirthPayload(now, seq)).WaitTimeout(2 * time.Second)
	seq++

	// One NDATA + DDATA so SendData-class IDs get a verdict.
	c.Publish(fmt.Sprintf("spBv1.0/%s/NDATA/%s", group, edge), 0, false,
		ndataPayload(time.Now().UnixMilli(), seq)).WaitTimeout(2 * time.Second)
	seq++
	c.Publish(fmt.Sprintf("spBv1.0/%s/DDATA/%s/%s", group, edge, device), 0, false,
		ddataPayload(time.Now().UnixMilli(), seq)).WaitTimeout(2 * time.Second)
	seq++

	// Allow the extension to process before clean tear-down.
	time.Sleep(1 * time.Second)

	// DDEATH then NDEATH before clean DISCONNECT.
	c.Publish(fmt.Sprintf("spBv1.0/%s/DDEATH/%s/%s", group, edge, device), 0, false,
		ddeathPayload(time.Now().UnixMilli(), seq)).WaitTimeout(2 * time.Second)
	seq++
	c.Publish(fmt.Sprintf("spBv1.0/%s/NDEATH/%s", group, edge), 0, false,
		bdSeqPayload(bdSeq)).WaitTimeout(2 * time.Second)
}

// driveCompliantHost runs a Sparkplug Host Application lifecycle.
// host/SessionEstablishmentTest after receiving the host's BIRTH publishes
// a fake `online:false` STATE provocation and waits for the host to
// republish its BIRTH ("resend good"); only then does it emit OVERALL.
// We mimic that: subscribe namespace+STATE, publish BIRTH (timestamp
// MUST match the will payload), watch the STATE topic, and republish
// BIRTH with a fresh timestamp when the TCK's offline provocation arrives.
func driveCompliantHost(broker, host string) {
	clientID := "tck-correctness-host-" + host
	stateTopic := fmt.Sprintf("spBv1.0/STATE/%s", host)
	// The spec requires the BIRTH timestamp to equal the WILL/Death timestamp,
	// so reuse the same value for both.
	birthTS := time.Now().UnixMilli()
	willBody, _ := json.Marshal(map[string]any{"online": false, "timestamp": birthTS})
	birthBody, _ := json.Marshal(map[string]any{"online": true, "timestamp": birthTS})

	var (
		mu        sync.Mutex
		responded bool
		client    mqtt.Client
	)

	onState := func(_ mqtt.Client, msg mqtt.Message) {
		var s struct {
			Online    bool  `json:"online"`
			Timestamp int64 `json:"timestamp"`
		}
		if err := json.Unmarshal(msg.Payload(), &s); err != nil {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		if responded || client == nil || s.Online {
			return
		}
		// Respond to the TCK's offline provocation with a fresh BIRTH.
		responded = true
		ts := time.Now().UnixMilli()
		body, _ := json.Marshal(map[string]any{"online": true, "timestamp": ts})
		client.Publish(stateTopic, 1, true, body)
	}

	opts := mqtt.NewClientOptions().
		AddBroker(broker).
		SetClientID(clientID).
		SetCleanSession(true).
		SetConnectTimeout(5 * time.Second).
		SetBinaryWill(stateTopic, willBody, 1, true).
		SetDefaultPublishHandler(func(cli mqtt.Client, msg mqtt.Message) {
			if msg.Topic() == stateTopic {
				onState(cli, msg)
			}
		})
	c := mqtt.NewClient(opts)
	if tok := c.Connect(); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		return
	}
	defer c.Disconnect(200)
	mu.Lock()
	client = c
	mu.Unlock()

	// Spec requires the host to subscribe to BOTH the Sparkplug namespace
	// (so it sees edge nodes) AND the STATE topic for its own birth/death
	// before publishing — host SessionEstablishmentTest stays in
	// CONNECTED state until both filters appear.
	c.Subscribe("spBv1.0/#", 1, nil).WaitTimeout(2 * time.Second)
	c.Subscribe(stateTopic, 1, nil).WaitTimeout(2 * time.Second)

	c.Publish(stateTopic, 1, true, birthBody).WaitTimeout(2 * time.Second)

	// Hold the session open long enough for the TCK to send its offline
	// provocation and observe our BIRTH-resend before we DISCONNECT.
	time.Sleep(5 * time.Second)
}

// --- payload builders ---

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

func nbirthPayload(ts int64, seq, bdSeq uint64) []byte {
	tsU := uint64(ts)
	bdSeqDT := uint32(spbpb.DataType_UInt64)
	boolDT := uint32(spbpb.DataType_Boolean)
	intDT := uint32(spbpb.DataType_Int32)
	bdSeqName := "bdSeq"
	rebirthName := "Node Control/Rebirth"
	hbName := "Heartbeat"
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
		},
	}
	raw, _ := proto.Marshal(p)
	return raw
}

func dbirthPayload(ts int64, seq uint64) []byte {
	tsU := uint64(ts)
	intDT := uint32(spbpb.DataType_Int32)
	name := "Counter"
	v := uint32(0)
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
	name := "Counter"
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

func ddeathPayload(ts int64, seq uint64) []byte {
	tsU := uint64(ts)
	p := &spbpb.Payload{
		Timestamp: &tsU,
		Seq:       &seq,
	}
	raw, _ := proto.Marshal(p)
	return raw
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "sparkplug-tck-correctness: "+format+"\n", args...)
	os.Exit(1)
}
