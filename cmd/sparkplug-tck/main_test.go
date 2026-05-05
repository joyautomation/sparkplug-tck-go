package main

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/joyautomation/sparkplug-tck-go/internal/runner"
	"github.com/joyautomation/sparkplug-tck-go/internal/spb"
	"github.com/joyautomation/sparkplug-tck-go/internal/spbpb"
	"google.golang.org/protobuf/proto"
)

// TestEndToEnd_GoldenCompliant proves the whole pipeline: build a known-compliant
// capture, marshal it to the on-disk fixture format, parse it back via
// loadFixture, run every registered assertion, and verify zero failures.
func TestEndToEnd_GoldenCompliant(t *testing.T) {
	u64 := func(v uint64) *uint64 { return &v }
	str := func(v string) *string { return &v }
	u32 := func(v uint32) *uint32 { return &v }
	bdSeq := func(v uint64) *spbpb.Payload_Metric {
		return &spbpb.Payload_Metric{
			Name:  str("bdSeq"),
			Value: &spbpb.Payload_Metric_LongValue{LongValue: v},
		}
	}
	rebirth := func() *spbpb.Payload_Metric {
		return &spbpb.Payload_Metric{
			Name:     str("Node Control/Rebirth"),
			Datatype: u32(uint32(spbpb.DataType_Boolean)),
			Value:    &spbpb.Payload_Metric_BooleanValue{BooleanValue: false},
		}
	}

	ts := uint64(time.Now().UnixMilli())
	payloads := []struct {
		topic   string
		qos     byte
		retain  bool
		payload *spbpb.Payload
	}{
		{
			topic: "spBv1.0/G/NBIRTH/N",
			payload: &spbpb.Payload{
				Seq:       u64(0),
				Timestamp: u64(ts),
				Metrics:   []*spbpb.Payload_Metric{bdSeq(1), rebirth()},
			},
		},
		{
			topic:   "spBv1.0/G/NDATA/N",
			payload: &spbpb.Payload{Seq: u64(1), Timestamp: u64(ts + 100)},
		},
		{
			topic:   "spBv1.0/G/NDATA/N",
			payload: &spbpb.Payload{Seq: u64(2), Timestamp: u64(ts + 200)},
		},
		{
			topic: "spBv1.0/G/NDEATH/N",
			// no Seq for NDEATH
			payload: &spbpb.Payload{Metrics: []*spbpb.Payload_Metric{bdSeq(1)}},
		},
	}

	var fx fixtureFile
	for _, p := range payloads {
		raw, err := proto.Marshal(p.payload)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		fx.Messages = append(fx.Messages, fixtureMsg{
			Topic:      p.topic,
			QoS:        p.qos,
			Retain:     p.retain,
			PayloadB64: base64.StdEncoding.EncodeToString(raw),
			At:         time.Now(),
		})
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "golden.json")
	body, err := json.Marshal(fx)
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}
	if err := os.WriteFile(path, body, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	msgs, err := loadFixture(path)
	if err != nil {
		t.Fatalf("loadFixture: %v", err)
	}
	if len(msgs) != len(payloads) {
		t.Fatalf("loaded %d messages, want %d", len(msgs), len(payloads))
	}

	results := runner.RunAll(runner.NewCapture(msgs))
	if hasFail(results) {
		for _, r := range results {
			if r.Status == runner.StatusFail {
				t.Errorf("unexpected fail: %s [%s]: %s", r.AssertionID, r.Subject, r.Detail)
			}
		}
	}

	// Sanity check we actually exercised assertions, not just produced N/As.
	var passed int
	for _, r := range results {
		if r.Status == runner.StatusPass {
			passed++
		}
	}
	if passed == 0 {
		t.Fatal("no passing assertions — fixture may not have triggered any checks")
	}

	// Avoid unused-import grumbles from spb when this test stands alone.
	_ = spb.Namespace
}
