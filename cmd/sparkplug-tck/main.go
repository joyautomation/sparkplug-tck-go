// Command sparkplug-tck runs the registered TCK assertions against a
// captured sequence of Sparkplug messages.
//
// Today the only input format is a JSON fixture (see -fixture). Once the
// MQTT harness lands, an alternative -broker mode will subscribe to a live
// broker and assemble the capture on the fly.
package main

import (
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	_ "github.com/joyautomation/sparkplug-tck-go/internal/assertions" // registry side-effects
	"github.com/joyautomation/sparkplug-tck-go/internal/runner"
	"github.com/joyautomation/sparkplug-tck-go/internal/spb"
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
	jsonOut := flag.Bool("json", false, "emit results as JSON instead of human-readable")
	flag.Parse()

	if *fixture == "" {
		fmt.Fprintln(os.Stderr, "usage: sparkplug-tck -fixture <path|->")
		os.Exit(2)
	}

	msgs, err := loadFixture(*fixture)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load fixture: %v\n", err)
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

func hasFail(results []runner.Result) bool {
	for _, r := range results {
		if r.Status == runner.StatusFail {
			return true
		}
	}
	return false
}
