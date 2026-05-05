// Command sparkplug-tck runs the registered TCK assertions against a
// captured sequence of Sparkplug messages.
//
// Three input modes:
//   -fixture <path|->                JSON fixture (offline, deterministic)
//   -broker <url>                    Passive: tap a live broker and assert
//   -harness -profile <edge|host>    Active: run an in-process broker, the
//                                    SUT connects to it, then evaluate a
//                                    profile of strict (Layer-3) scenarios
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	_ "github.com/joyautomation/sparkplug-tck-go/internal/assertions" // registry side-effects
	"github.com/joyautomation/sparkplug-tck-go/internal/capture"
	"github.com/joyautomation/sparkplug-tck-go/internal/harness"
	"github.com/joyautomation/sparkplug-tck-go/internal/runner"
	"github.com/joyautomation/sparkplug-tck-go/internal/spb"
	"github.com/joyautomation/sparkplug-tck-go/internal/spbpb"
	"google.golang.org/protobuf/proto"
)

// fixtureFile is the on-disk shape:
//
//	{
//	  "messages": [
//	    {"topic":"spBv1.0/G/NBIRTH/N","qos":0,"retain":false,
//	     "payload_b64":"...","at":"2026-05-04T18:00:00Z"}
//	  ]
//	}
//
// payload_b64 holds raw Sparkplug B protobuf bytes (or, for STATE topics,
// the JSON/text payload — same wire bytes as MQTT delivers).
type fixtureFile struct {
	Messages []fixtureMsg `json:"messages"`
}

type fixtureMsg struct {
	Topic      string    `json:"topic"`
	QoS        byte      `json:"qos"`
	Retain     bool      `json:"retain"`
	PayloadB64 string    `json:"payload_b64"`
	At         time.Time `json:"at"`
}

func main() {
	fixture := flag.String("fixture", "", "path to JSON fixture file (use - for stdin)")
	broker := flag.String("broker", "", "MQTT broker URL (e.g. tcp://localhost:1883)")
	username := flag.String("username", "", "MQTT username (with -broker)")
	password := flag.String("password", "", "MQTT password (with -broker)")
	duration := flag.Duration("duration", 30*time.Second, "capture/harness duration")
	jsonOut := flag.Bool("json", false, "emit results as JSON instead of human-readable")
	harnessMode := flag.Bool("harness", false, "run an in-process broker; SUT connects to it")
	harnessBind := flag.String("listen", "127.0.0.1:1883", "harness broker bind address")
	profile := flag.String("profile", "", "harness profile: "+strings.Join(profileNames(), ", "))
	rebirthEdge := flag.String("rebirth", "", "harness stimulus: publish NCMD/Rebirth to <group>/<edge> (e.g. TestGroup/TestNode)")
	rebirthAfter := flag.Duration("rebirth-after", 5*time.Second, "delay before publishing the rebirth stimulus")
	flag.Parse()

	modes := 0
	if *fixture != "" {
		modes++
	}
	if *broker != "" {
		modes++
	}
	if *harnessMode {
		modes++
	}
	if modes != 1 {
		fmt.Fprintln(os.Stderr, "usage: sparkplug-tck (-fixture <path|->) | (-broker <url>) | (-harness -profile <name>)")
		os.Exit(2)
	}

	if *harnessMode {
		if err := runHarness(*harnessBind, *profile, *duration, *jsonOut, *rebirthEdge, *rebirthAfter); err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			os.Exit(1)
		}
		return
	}

	var msgs []spb.Message
	var err error
	if *fixture != "" {
		msgs, err = loadFixture(*fixture)
	} else {
		msgs, err = liveCapture(*broker, *username, *password, *duration)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	results := runner.RunAll(runner.NewCapture(msgs))

	if *jsonOut {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(results)
	} else {
		printHuman(os.Stdout, results)
	}

	if hasFail(results) {
		os.Exit(1)
	}
}

// liveCapture connects to a broker and runs until duration elapses or the
// process receives SIGINT/SIGTERM. Stats (capture/drop counts) are printed
// to stderr so the JSON or human result table on stdout stays clean.
func liveCapture(brokerURL, username, password string, duration time.Duration) ([]spb.Message, error) {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	fmt.Fprintf(os.Stderr, "capturing from %s for %s (Ctrl-C to stop early)\n", brokerURL, duration)
	msgs, stats, err := capture.Run(ctx, capture.Options{
		BrokerURL: brokerURL,
		Username:  username,
		Password:  password,
		Duration:  duration,
	})
	if err != nil {
		return nil, fmt.Errorf("capture: %w", err)
	}
	fmt.Fprintf(os.Stderr, "captured %d messages (%d dropped)\n", stats.Captured, stats.Dropped)
	for _, e := range stats.SampleErrors {
		fmt.Fprintf(os.Stderr, "  drop: %v\n", e)
	}
	return msgs, nil
}

func loadFixture(path string) ([]spb.Message, error) {
	var raw []byte
	var err error
	if path == "-" {
		raw, err = io.ReadAll(os.Stdin)
	} else {
		raw, err = os.ReadFile(path)
	}
	if err != nil {
		return nil, err
	}

	var f fixtureFile
	if err := json.Unmarshal(raw, &f); err != nil {
		return nil, fmt.Errorf("parse fixture json: %w", err)
	}

	out := make([]spb.Message, 0, len(f.Messages))
	for i, fm := range f.Messages {
		topic, err := spb.ParseTopic(fm.Topic)
		if err != nil {
			return nil, fmt.Errorf("message %d: %w", i, err)
		}
		payload, err := base64.StdEncoding.DecodeString(fm.PayloadB64)
		if err != nil {
			return nil, fmt.Errorf("message %d: payload_b64: %w", i, err)
		}
		m := spb.Message{
			Topic:    topic,
			Raw:      payload,
			QoS:      fm.QoS,
			Retained: fm.Retain,
			At:       fm.At,
		}
		switch topic.Type {
		case spb.STATE:
			st, err := spb.DecodeState(payload)
			if err != nil {
				return nil, fmt.Errorf("message %d: STATE: %w", i, err)
			}
			m.State = st
		default:
			p, err := spb.DecodePayload(payload)
			if err != nil {
				return nil, fmt.Errorf("message %d: %w", i, err)
			}
			m.Payload = p
		}
		out = append(out, m)
	}
	return out, nil
}

func printHuman(w io.Writer, results []runner.Result) {
	var pass, fail, na int
	for _, r := range results {
		switch r.Status {
		case runner.StatusPass:
			pass++
		case runner.StatusFail:
			fail++
		case runner.StatusNotApplicable:
			na++
		}
	}
	for _, r := range results {
		marker := "·"
		switch r.Status {
		case runner.StatusPass:
			marker = "✓"
		case runner.StatusFail:
			marker = "✗"
		}
		if r.Subject != "" {
			fmt.Fprintf(w, "%s %s [%s]", marker, r.AssertionID, r.Subject)
		} else {
			fmt.Fprintf(w, "%s %s", marker, r.AssertionID)
		}
		if r.Detail != "" {
			fmt.Fprintf(w, " — %s", r.Detail)
		}
		fmt.Fprintln(w)
	}
	fmt.Fprintf(w, "\n%d pass, %d fail, %d n/a, %d total\n", pass, fail, na, len(results))
}

// runHarness boots the in-process broker, blocks until the duration
// elapses or the operator hits Ctrl-C, then evaluates the named profile
// against everything the broker recorded. The same printHuman / JSON
// emitters are reused so harness output is interchangeable with passive
// output.
func runHarness(bind, profileName string, duration time.Duration, jsonOut bool,
	rebirthEdge string, rebirthAfter time.Duration,
) error {
	prof, ok := harness.Profiles[profileName]
	if !ok {
		return fmt.Errorf("unknown profile %q (have: %s)", profileName,
			strings.Join(profileNames(), ", "))
	}

	b, err := harness.NewBrokerAt(bind)
	if err != nil {
		return fmt.Errorf("start harness broker: %w", err)
	}
	defer b.Close()

	fmt.Fprintf(os.Stderr, "harness broker listening on %s — point your SUT here\n", b.URL())
	fmt.Fprintf(os.Stderr, "running profile %q for %s (Ctrl-C to evaluate early)\n",
		prof.Name, duration)

	ctx, cancel := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if rebirthEdge != "" {
		go scheduleRebirth(ctx, b, rebirthEdge, rebirthAfter)
	}

	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}

	results := prof.Run(b)
	if jsonOut {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(results)
	} else {
		printHuman(os.Stdout, results)
	}
	if hasFail(results) {
		return fmt.Errorf("harness: %d failure(s)", countFails(results))
	}
	return nil
}

func countFails(results []runner.Result) int {
	n := 0
	for _, r := range results {
		if r.Status == runner.StatusFail {
			n++
		}
	}
	return n
}

func profileNames() []string {
	names := make([]string, 0, len(harness.Profiles))
	for n := range harness.Profiles {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// scheduleRebirth waits for `after` (or until ctx is cancelled) then
// publishes a Sparkplug NCMD/Rebirth ("Node Control/Rebirth"=true) to
// the edge's NCMD topic via the harness broker's inline client.
// `edge` is "<group>/<node>"; the published topic is
// spBv1.0/<group>/NCMD/<node>.
func scheduleRebirth(ctx context.Context, b *harness.Broker, edge string, after time.Duration) {
	parts := strings.SplitN(edge, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		fmt.Fprintf(os.Stderr, "rebirth: invalid edge %q (want <group>/<node>)\n", edge)
		return
	}
	select {
	case <-ctx.Done():
		return
	case <-time.After(after):
	}
	topic := fmt.Sprintf("spBv1.0/%s/NCMD/%s", parts[0], parts[1])
	dt := uint32(spbpb.DataType_Boolean)
	name := "Node Control/Rebirth"
	tru := true
	p := &spbpb.Payload{Metrics: []*spbpb.Payload_Metric{{
		Name:     &name,
		Datatype: &dt,
		Value:    &spbpb.Payload_Metric_BooleanValue{BooleanValue: tru},
	}}}
	raw, err := proto.Marshal(p)
	if err != nil {
		fmt.Fprintf(os.Stderr, "rebirth: marshal payload: %v\n", err)
		return
	}
	if err := b.Publish(topic, raw, false, 1); err != nil {
		fmt.Fprintf(os.Stderr, "rebirth: publish %s: %v\n", topic, err)
		return
	}
	fmt.Fprintf(os.Stderr, "rebirth: published NCMD/Rebirth to %s\n", topic)
}

func hasFail(results []runner.Result) bool {
	for _, r := range results {
		if r.Status == runner.StatusFail {
			return true
		}
	}
	return false
}
