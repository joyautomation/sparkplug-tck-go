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
// need a Sparkplug Host Application publishing STATE. driverHostOnline
// is a host that must already be online BEFORE NEW_TEST — those tests
// gate on checkHostApplicationIsOnline and bail immediately if the
// retained STATE topic doesn't show online:true.
type driverKind string

const (
	driverEdge        driverKind = "edge"
	driverHost        driverKind = "host"
	driverHostOnline  driverKind = "host-online"
	driverEdgePrimary driverKind = "edge-primary"
)

const (
	topicTestControl   = "SPARKPLUG_TCK/TEST_CONTROL"
	topicResults       = "SPARKPLUG_TCK/RESULT"
	topicLog           = "SPARKPLUG_TCK/LOG"
	topicResultsConfig = "SPARKPLUG_TCK/RESULT_CONFIG"
	topicConsolePrompt = "SPARKPLUG_TCK/CONSOLE_PROMPT"
	topicConsoleReply  = "SPARKPLUG_TCK/CONSOLE_REPLY"
)

type verdict struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

type report struct {
	Test       string    `json:"test"`
	Verdicts   []verdict `json:"verdicts"`
	Overall    string    `json:"overall,omitempty"`
	Counts     counts    `json:"counts"`
	WallclockUS int64    `json:"wallclock_us"`
}

type counts struct {
	Pass        int `json:"pass"`
	Fail        int `json:"fail"`
	NotExecuted int `json:"not_executed"`
	Other       int `json:"other"`
	Total       int `json:"total"`
}

// defaultTestSet covers every in-scope upstream test (broker/multi-broker
// are out of scope per project policy). Some require richer SUT behavior
// than the basic edge/host lifecycles — those will time out until their
// driver is upgraded, but the orchestrator still captures partial
// verdicts the upstream emitted before timeout.
const defaultTestSet = "edge SessionEstablishmentTest," +
	"edge SendDataTest," +
	"edge SendComplexDataTest," +
	"edge SessionTerminationTest," +
	"edge ReceiveCommandTest," +
	"edge PrimaryHostTest," +
	"host SessionEstablishmentTest," +
	"host SessionTerminationTest," +
	"host EdgeSessionTerminationTest," +
	"host MessageOrderingTest," +
	"host SendCommandTest"

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
		var preStartedHost *onlineHost
		switch driverKindFor(profile, testName) {
		case driverEdge:
			args = []string{runHost, *groupID, runEdge, *deviceID}
			driver = func() { driveCompliantEdge(*broker, runHost, *groupID, runEdge, *deviceID) }
		case driverEdgePrimary:
			args = []string{runHost, *groupID, runEdge, *deviceID}
			driver = func() { drivePrimaryHostEdge(*broker, runHost, *groupID, runEdge, *deviceID) }
		case driverHost:
			args = []string{runHost}
			driver = func() { driveCompliantHost(*broker, runHost) }
		case driverHostOnline:
			// Test gates on host being online at NEW_TEST time, plus needs
			// the host to publish NCMD/DCMD or reply to console prompts
			// during the test. Pre-start the host so its STATE birth is
			// retained before the TCK queries checkHostApplicationIsOnline.
			args = []string{runHost, *groupID, runEdge, *deviceID}
			// MessageOrderingTest takes a 5th parameter — reorderTimeout
			// in milliseconds. Without it the test errors out at NEW_TEST
			// time before publishing any verdicts.
			if testName == "MessageOrderingTest" {
				args = append(args, "5000")
			}
			oh, err := startOnlineHost(*broker, runHost, *groupID, runEdge, *deviceID)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s — pre-start failed: %v\n", spec, err)
			}
			preStartedHost = oh
			driver = func() {} // host already running; nothing more to spawn
		}

		testStart := time.Now()
		if err := ctrl.startTest(profile, testName, args); err != nil {
			fail("start %s: %v", spec, err)
		}
		go driver()

		if err := ctrl.waitForOverall(*timeout); err != nil {
			fmt.Fprintf(os.Stderr, "%s — TIMEOUT after %s, partial results captured\n", spec, *timeout)
		}
		_ = ctrl.endTest()
		if preStartedHost != nil {
			preStartedHost.stop()
		}
		// END_TEST triggers the TCK extension to publish a SECOND, empty
		// OVERALL summary. If we let it leak into the next test slot, it
		// closes that test's channel and waitForOverall returns instantly
		// before the new SUT can connect. Sleep briefly so the spurious
		// OVERALL lands here, then reset() at top of the next iteration
		// discards it.
		time.Sleep(500 * time.Millisecond)

		rep := ctrl.report(profile + "/" + testName)
		rep.WallclockUS = time.Since(testStart).Microseconds()
		reports = append(reports, rep)
		fmt.Fprintf(os.Stderr, "%s — pass:%d fail:%d not_executed:%d other:%d (overall %s) [%dms]\n",
			rep.Test, rep.Counts.Pass, rep.Counts.Fail, rep.Counts.NotExecuted, rep.Counts.Other, rep.Overall, rep.WallclockUS/1000)
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
// an edge node. Three host tests (EdgeSessionTermination, MessageOrdering,
// SendCommand) call checkHostApplicationIsOnline at NEW_TEST time and
// bail unless the host's retained STATE shows online:true — those need
// driverHostOnline (pre-start the host before NEW_TEST).
func driverKindFor(profile, testName string) driverKind {
	if profile == "host" {
		switch testName {
		case "EdgeSessionTerminationTest", "MessageOrderingTest", "SendCommandTest":
			return driverHostOnline
		}
		return driverHost
	}
	if profile == "edge" && testName == "PrimaryHostTest" {
		return driverEdgePrimary
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
	if !tok.WaitTimeout(10 * time.Second) {
		return fmt.Errorf("publish NEW_TEST: timed out waiting for QoS1 ack after 10s")
	}
	if err := tok.Error(); err != nil {
		return fmt.Errorf("publish NEW_TEST: %v", err)
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
	if !tok.WaitTimeout(10 * time.Second) {
		return fmt.Errorf("publish END_TEST: timed out waiting for QoS1 ack after 10s")
	}
	if err := tok.Error(); err != nil {
		return fmt.Errorf("publish END_TEST: %v", err)
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
		// Ignore everything after the first OVERALL — END_TEST triggers the
		// TCK extension to publish a SECOND empty summary that would
		// otherwise corrupt our verdicts and OVERALL status.
		if c.overall != "" {
			c.mu.Unlock()
			continue
		}
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

// drivePrimaryHostEdge runs an edge node configured with a primary host
// id. The TCK's PrimaryHostTest provokes the edge by toggling the host's
// retained STATE — wrong-host-online (ignore), correct-host-offline
// (don't BIRTH), correct-host-online (BIRTH), then offline / older-ts /
// online-again. The edge MUST:
//   - Wait for STATE online for the configured host before publishing BIRTH
//   - Compare timestamps and ignore older STATE updates
//   - Publish NDEATH + DDEATH and disconnect when the host goes offline
//   - Re-BIRTH when the host comes back
func drivePrimaryHostEdge(broker, hostID, group, edge, device string) {
	stateTopic := fmt.Sprintf("spBv1.0/STATE/%s", hostID)
	willTopic := fmt.Sprintf("spBv1.0/%s/NDEATH/%s", group, edge)
	ncmdSub := fmt.Sprintf("spBv1.0/%s/NCMD/%s", group, edge)
	dcmdSub := fmt.Sprintf("spBv1.0/%s/DCMD/%s/%s", group, edge, device)

	var (
		mu          sync.Mutex
		client      mqtt.Client
		bdSeq       uint64
		seq         uint64
		online      bool
		lastTS      int64
		birthSent   bool
		quitCh      = make(chan struct{})
	)

	connect := func() (mqtt.Client, error) {
		bdSeqVal := bdSeq
		mu.Lock()
		// Reset per-connect bookkeeping; bdSeq increments per CONNECT.
		seq = 0
		mu.Unlock()
		opts := mqtt.NewClientOptions().
			AddBroker(broker).
			SetClientID("tck-correctness-edge-"+edge).
			SetCleanSession(true).
			SetConnectTimeout(5 * time.Second).
			SetBinaryWill(willTopic, bdSeqPayload(bdSeqVal), 1, false).
			SetDefaultPublishHandler(func(_ mqtt.Client, msg mqtt.Message) {
				if msg.Topic() != stateTopic {
					return
				}
				var s struct {
					Online    bool  `json:"online"`
					Timestamp int64 `json:"timestamp"`
				}
				if err := json.Unmarshal(msg.Payload(), &s); err != nil {
					return
				}
				mu.Lock()
				if s.Timestamp <= lastTS {
					mu.Unlock()
					return
				}
				lastTS = s.Timestamp
				wasOnline := online
				online = s.Online
				cli := client
				mu.Unlock()
				if cli == nil {
					return
				}
				if online && !birthSent {
					mu.Lock()
					birthSent = true
					mu.Unlock()
					publishBirths(cli, group, edge, device, &seq, bdSeqVal)
				} else if !online && wasOnline {
					publishDeaths(cli, group, edge, device, &seq, bdSeqVal)
					cli.Disconnect(200)
					mu.Lock()
					client = nil
					birthSent = false
					mu.Unlock()
				}
			})
		c := mqtt.NewClient(opts)
		if tok := c.Connect(); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
			return nil, fmt.Errorf("connect: %v", tok.Error())
		}
		c.Subscribe(stateTopic, 1, nil).WaitTimeout(2 * time.Second)
		c.Subscribe(ncmdSub, 1, nil).WaitTimeout(2 * time.Second)
		c.Subscribe(dcmdSub, 1, nil).WaitTimeout(2 * time.Second)
		return c, nil
	}

	c, err := connect()
	if err != nil {
		return
	}
	mu.Lock()
	client = c
	mu.Unlock()

	// Reconnect loop — when the edge disconnects after host goes offline,
	// reconnect and wait for STATE online again to BIRTH.
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-quitCh:
				return
			case <-ticker.C:
				mu.Lock()
				cli := client
				mu.Unlock()
				if cli == nil {
					nc, err := connect()
					if err != nil {
						continue
					}
					mu.Lock()
					client = nc
					bdSeq++
					mu.Unlock()
				}
			}
		}
	}()

	// Hold open long enough to walk the test's full state machine
	// (~18 s of host-state toggles plus settle time).
	time.Sleep(30 * time.Second)
	close(quitCh)

	mu.Lock()
	cli := client
	mu.Unlock()
	if cli != nil {
		// Clean shutdown: publish DDEATH+NDEATH, disconnect cleanly.
		publishDeaths(cli, group, edge, device, &seq, bdSeq)
		cli.Disconnect(200)
	}
}

func publishBirths(c mqtt.Client, group, edge, device string, seq *uint64, bdSeq uint64) {
	now := time.Now().UnixMilli()
	c.Publish(fmt.Sprintf("spBv1.0/%s/NBIRTH/%s", group, edge), 0, false,
		nbirthPayload(now, *seq, bdSeq)).WaitTimeout(2 * time.Second)
	*seq++
	c.Publish(fmt.Sprintf("spBv1.0/%s/DBIRTH/%s/%s", group, edge, device), 0, false,
		dbirthPayload(now, *seq)).WaitTimeout(2 * time.Second)
	*seq++
}

func publishDeaths(c mqtt.Client, group, edge, device string, seq *uint64, bdSeq uint64) {
	c.Publish(fmt.Sprintf("spBv1.0/%s/DDEATH/%s/%s", group, edge, device), 0, false,
		ddeathPayload(time.Now().UnixMilli(), *seq)).WaitTimeout(2 * time.Second)
	*seq++
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
		bornFlag  bool
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
		// Ignore retained messages delivered before we've published our birth,
		// and ignore offline-state messages whose timestamp doesn't match our
		// will (those are stale retained from a previous test). The TCK's
		// provocation always echoes our will timestamp.
		if !bornFlag || responded || client == nil || s.Online || s.Timestamp != birthTS {
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
	mu.Lock()
	bornFlag = true
	mu.Unlock()

	// Hold the session open long enough for the TCK to send its offline
	// provocation and observe our BIRTH-resend before we DISCONNECT.
	time.Sleep(5 * time.Second)

	// Spec: a Host Application MUST publish a death (online:false) on
	// the STATE topic before intentionally disconnecting — host/
	// SessionTerminationTest fails the SUT when the death isn't seen.
	deathTS := time.Now().UnixMilli()
	deathBody, _ := json.Marshal(map[string]any{"online": false, "timestamp": deathTS})
	c.Publish(stateTopic, 1, true, deathBody).WaitTimeout(2 * time.Second)
	c.Disconnect(200)
}

// onlineHost is a long-lived Sparkplug Host Application used by the
// host/* tests that gate on checkHostApplicationIsOnline at NEW_TEST
// time (EdgeSessionTermination, MessageOrdering, SendCommand). It
// connects, subscribes to the namespace + STATE topic + CONSOLE_PROMPT,
// publishes its BIRTH retained (so the gate passes), and replies to
// every prompt — either with a Sparkplug NCMD/DCMD (SendCommand) or
// "PASS" on CONSOLE_REPLY (EdgeSessionTermination, MessageOrdering).
type onlineHost struct {
	c     mqtt.Client
	group string
	edge  string
	dev   string

	mu                sync.Mutex
	nbirthMetricNames []string
	dbirthMetricNames []string
}

// startOnlineHost connects the host, publishes the retained BIRTH, and
// returns once it's safe to start the test (broker has the BIRTH).
func startOnlineHost(broker, host, group, edge, dev string) (*onlineHost, error) {
	clientID := "tck-correctness-host-" + host
	stateTopic := fmt.Sprintf("spBv1.0/STATE/%s", host)
	birthTS := time.Now().UnixMilli()
	willBody, _ := json.Marshal(map[string]any{"online": false, "timestamp": birthTS})
	birthBody, _ := json.Marshal(map[string]any{"online": true, "timestamp": birthTS})

	oh := &onlineHost{group: group, edge: edge, dev: dev}

	opts := mqtt.NewClientOptions().
		AddBroker(broker).
		SetClientID(clientID).
		SetCleanSession(true).
		SetConnectTimeout(5 * time.Second).
		SetBinaryWill(stateTopic, willBody, 1, true).
		SetDefaultPublishHandler(func(_ mqtt.Client, msg mqtt.Message) {
			oh.onMessage(msg)
		})
	c := mqtt.NewClient(opts)
	if tok := c.Connect(); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		return nil, fmt.Errorf("connect: %v", tok.Error())
	}
	oh.c = c

	c.Subscribe("spBv1.0/#", 1, nil).WaitTimeout(2 * time.Second)
	c.Subscribe(stateTopic, 1, nil).WaitTimeout(2 * time.Second)
	c.Subscribe(topicConsolePrompt, 1, nil).WaitTimeout(2 * time.Second)

	tok := c.Publish(stateTopic, 1, true, birthBody)
	tok.WaitTimeout(2 * time.Second)
	// Tiny pause so the broker stores the retained STATE before
	// checkHostApplicationIsOnline runs at NEW_TEST time.
	time.Sleep(200 * time.Millisecond)
	return oh, nil
}

func (h *onlineHost) stop() {
	if h == nil || h.c == nil {
		return
	}
	h.c.Disconnect(200)
}

func (h *onlineHost) onMessage(msg mqtt.Message) {
	topic := msg.Topic()
	switch {
	case topic == topicConsolePrompt:
		h.handlePrompt(string(msg.Payload()))
	case strings.Contains(topic, "/NBIRTH/"):
		names := metricNamesFrom(msg.Payload())
		h.mu.Lock()
		h.nbirthMetricNames = names
		h.mu.Unlock()
	case strings.Contains(topic, "/DBIRTH/"):
		names := metricNamesFrom(msg.Payload())
		h.mu.Lock()
		h.dbirthMetricNames = names
		h.mu.Unlock()
	}
}

func metricNamesFrom(raw []byte) []string {
	var p spbpb.Payload
	if err := proto.Unmarshal(raw, &p); err != nil {
		return nil
	}
	out := make([]string, 0, len(p.Metrics))
	for _, m := range p.Metrics {
		if m.Name != nil {
			out = append(out, *m.Name)
		}
	}
	return out
}

// handlePrompt dispatches by keyword match on the prompt text. The
// SendCommand test asks the host to publish specific NCMD/DCMD; the
// other prompt-based tests just want a "PASS" reply.
func (h *onlineHost) handlePrompt(text string) {
	switch {
	case strings.Contains(text, "edge rebirth"):
		h.publishNCMD("Node Control/Rebirth", boolValue(true))
	case strings.Contains(text, "edge command") && strings.Contains(text, "update"):
		name := h.firstNonRebirthMetric()
		if name != "" {
			h.publishNCMD(name, int32Value(1))
		}
	case strings.Contains(text, "device rebirth"):
		name := h.firstDeviceMetric()
		if name != "" {
			h.publishDCMD(name, int32Value(0))
		}
	case strings.Contains(text, "device command"):
		name := h.firstDeviceMetric()
		if name != "" {
			h.publishDCMD(name, int32Value(2))
		}
	default:
		// EdgeSessionTermination / MessageOrdering style yes/no prompt.
		h.c.Publish(topicConsoleReply, 1, false, []byte("PASS"))
	}
}

func (h *onlineHost) firstNonRebirthMetric() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, n := range h.nbirthMetricNames {
		if n != "Node Control/Rebirth" && n != "bdSeq" {
			return n
		}
	}
	return ""
}

func (h *onlineHost) firstDeviceMetric() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.dbirthMetricNames) > 0 {
		return h.dbirthMetricNames[0]
	}
	return ""
}

func (h *onlineHost) publishNCMD(metric string, val metricValue) {
	topic := fmt.Sprintf("spBv1.0/%s/NCMD/%s", h.group, h.edge)
	h.c.Publish(topic, 0, false, cmdPayload(metric, val))
}

func (h *onlineHost) publishDCMD(metric string, val metricValue) {
	topic := fmt.Sprintf("spBv1.0/%s/DCMD/%s/%s", h.group, h.edge, h.dev)
	h.c.Publish(topic, 0, false, cmdPayload(metric, val))
}

// cmdPayload builds an NCMD/DCMD payload: timestamp, no seq, single metric.
func cmdPayload(name string, val metricValue) []byte {
	ts := uint64(time.Now().UnixMilli())
	dt := uint32(val.dataType)
	m := &spbpb.Payload_Metric{
		Name:      &name,
		Timestamp: &ts,
		Datatype:  &dt,
	}
	val.applyTo(m)
	p := &spbpb.Payload{
		Timestamp: &ts,
		Metrics:   []*spbpb.Payload_Metric{m},
	}
	raw, _ := proto.Marshal(p)
	return raw
}

type metricValue struct {
	dataType spbpb.DataType
	applyTo  func(*spbpb.Payload_Metric)
}

func boolValue(v bool) metricValue {
	return metricValue{
		dataType: spbpb.DataType_Boolean,
		applyTo: func(m *spbpb.Payload_Metric) {
			m.Value = &spbpb.Payload_Metric_BooleanValue{BooleanValue: v}
		},
	}
}

func int32Value(v int32) metricValue {
	return metricValue{
		dataType: spbpb.DataType_Int32,
		applyTo: func(m *spbpb.Payload_Metric) {
			m.Value = &spbpb.Payload_Metric_IntValue{IntValue: uint32(v)}
		},
	}
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
			// PropertySet on a metric so propertyvalue/propertyset
			// assertions have something to score in both engines.
			{Name: &tempName, Datatype: &intDT, Timestamp: &tsU,
				Value:      &spbpb.Payload_Metric_IntValue{IntValue: uint32(72)},
				Properties: sampleMetricProperties()},
			// Template Definition so template-* assertions score; the
			// instance gets published in DBIRTH.
			{Name: &tmplName, Datatype: &tmplDT, Timestamp: &tsU,
				Value: &spbpb.Payload_Metric_TemplateValue{
					TemplateValue: motorTemplate(true /*definition*/),
				}},
		},
	}
	raw, _ := proto.Marshal(p)
	return raw
}

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
			{Name: &tmplName, Datatype: &tmplDT, Timestamp: &tsU,
				Value: &spbpb.Payload_Metric_TemplateValue{
					TemplateValue: motorTemplate(false /*instance*/),
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
			{Name: stringPtrCorrectness("model"), Type: &strDT,
				Value: &spbpb.Payload_Template_Parameter_StringValue{StringValue: "ACME-1000"}},
			{Name: stringPtrCorrectness("rated_rpm"), Type: &intDT,
				Value: &spbpb.Payload_Template_Parameter_IntValue{IntValue: 1750}},
		},
	}
	if !isDefinition {
		ref := "MotorDef"
		tmpl.TemplateRef = &ref
	}
	return tmpl
}

func stringPtrCorrectness(s string) *string { return &s }

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
