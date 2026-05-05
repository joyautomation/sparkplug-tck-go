package capture

import (
	"testing"
	"time"

	"github.com/joyautomation/sparkplug-tck-go/internal/spb"
	"github.com/joyautomation/sparkplug-tck-go/internal/spbpb"
	"google.golang.org/protobuf/proto"
)

func TestBuilder_AddSparkplugBPayload(t *testing.T) {
	b := NewBuilder(0)
	seq := uint64(0)
	ts := uint64(time.Now().UnixMilli())
	raw, err := proto.Marshal(&spbpb.Payload{Seq: &seq, Timestamp: &ts})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	b.Add("spBv1.0/G/NBIRTH/N", raw, 0, false, time.Now())

	msgs := b.Messages()
	if len(msgs) != 1 {
		t.Fatalf("got %d messages, want 1", len(msgs))
	}
	if msgs[0].Topic.Type != spb.NBIRTH {
		t.Errorf("topic type = %s, want NBIRTH", msgs[0].Topic.Type)
	}
	if msgs[0].Payload == nil || msgs[0].Payload.GetSeq() != 0 {
		t.Errorf("payload not decoded: %+v", msgs[0].Payload)
	}
}

func TestBuilder_AddState_3x(t *testing.T) {
	b := NewBuilder(0)
	b.Add("spBv1.0/STATE/host1", []byte(`{"online":true,"timestamp":1234}`), 1, true, time.Now())
	msgs := b.Messages()
	if len(msgs) != 1 {
		t.Fatalf("got %d messages, want 1", len(msgs))
	}
	if msgs[0].State == nil || !msgs[0].State.Online || msgs[0].State.Timestamp != 1234 {
		t.Errorf("state not decoded: %+v", msgs[0].State)
	}
}

func TestBuilder_AddState_legacy(t *testing.T) {
	b := NewBuilder(0)
	b.Add("spBv1.0/STATE/host1", []byte("OFFLINE"), 1, true, time.Now())
	msgs := b.Messages()
	if len(msgs) != 1 || !msgs[0].State.Legacy || msgs[0].State.Online {
		t.Errorf("legacy state not decoded: %+v", msgs[0].State)
	}
}

func TestBuilder_DropsBadTopic(t *testing.T) {
	b := NewBuilder(0)
	b.Add("not-sparkplug/foo", []byte{0x00}, 0, false, time.Now())
	if got := b.Stats().Dropped; got != 1 {
		t.Errorf("dropped = %d, want 1", got)
	}
	if got := len(b.Messages()); got != 0 {
		t.Errorf("messages = %d, want 0", got)
	}
}

func TestBuilder_DropsBadPayload(t *testing.T) {
	b := NewBuilder(0)
	// Topic is valid, payload is unparseable proto bytes.
	b.Add("spBv1.0/G/NDATA/N", []byte{0xff, 0xff, 0xff, 0xff}, 0, false, time.Now())
	if got := b.Stats().Dropped; got != 1 {
		t.Errorf("dropped = %d, want 1 (sample errs: %v)", got, b.Stats().SampleErrors)
	}
}

func TestBuilder_RawCopied(t *testing.T) {
	b := NewBuilder(0)
	seq := uint64(0)
	raw, _ := proto.Marshal(&spbpb.Payload{Seq: &seq})
	buf := make([]byte, len(raw))
	copy(buf, raw)
	b.Add("spBv1.0/G/NBIRTH/N", buf, 0, false, time.Now())
	// Mutate the caller's buffer; Builder must hold its own copy.
	for i := range buf {
		buf[i] = 0
	}
	msgs := b.Messages()
	if len(msgs[0].Raw) != len(raw) || msgs[0].Raw[0] != raw[0] {
		t.Errorf("Raw not copied; observed mutation in stored Message")
	}
}
