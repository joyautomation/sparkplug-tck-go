// Package capture turns raw MQTT receive events into the spb.Message stream
// the assertion runner consumes. The Builder is the decoder + accumulator;
// the Run function (in mqtt.go) wires it to a paho client.
//
// Builder is split out so it can be unit-tested without spinning up a real
// broker — the MQTT bit is just glue.
package capture

import (
	"sync"
	"time"

	"github.com/joyautomation/sparkplug-tck-go/internal/spb"
)

// Builder accumulates decoded Sparkplug messages, safe for concurrent use
// from multiple paho callbacks.
type Builder struct {
	mu       sync.Mutex
	msgs     []spb.Message
	dropped  int     // count of messages we couldn't decode
	dropLog  []error // first few decode errors, capped to keep memory bounded
	maxDrops int
}

// NewBuilder returns an empty Builder. maxDrops bounds the number of
// retained decode-error samples (set 0 for unlimited).
func NewBuilder(maxDrops int) *Builder {
	if maxDrops == 0 {
		maxDrops = 64
	}
	return &Builder{maxDrops: maxDrops}
}

// Add decodes one MQTT receive into a spb.Message and appends it.
// Topics that fail to parse are counted as drops, never silently lost —
// the dropped count and a sample of errors are surfaced via Stats.
func (b *Builder) Add(topic string, raw []byte, qos byte, retain bool, at time.Time) {
	t, err := spb.ParseTopic(topic)
	if err != nil {
		b.recordDrop(err)
		return
	}
	m := spb.Message{
		Topic:    t,
		Raw:      append([]byte(nil), raw...), // copy: paho reuses its buffer
		QoS:      qos,
		Retained: retain,
		At:       at,
	}
	switch t.Type {
	case spb.STATE:
		st, err := spb.DecodeState(raw)
		if err != nil {
			b.recordDrop(err)
			return
		}
		m.State = st
	default:
		p, err := spb.DecodePayload(raw)
		if err != nil {
			b.recordDrop(err)
			return
		}
		m.Payload = p
	}
	b.mu.Lock()
	b.msgs = append(b.msgs, m)
	b.mu.Unlock()
}

func (b *Builder) recordDrop(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.dropped++
	if len(b.dropLog) < b.maxDrops {
		b.dropLog = append(b.dropLog, err)
	}
}

// Messages returns a snapshot of every message captured so far.
func (b *Builder) Messages() []spb.Message {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]spb.Message, len(b.msgs))
	copy(out, b.msgs)
	return out
}

// Stats reports counters useful for diagnostics.
type Stats struct {
	Captured     int
	Dropped      int
	SampleErrors []error
}

func (b *Builder) Stats() Stats {
	b.mu.Lock()
	defer b.mu.Unlock()
	errs := make([]error, len(b.dropLog))
	copy(errs, b.dropLog)
	return Stats{Captured: len(b.msgs), Dropped: b.dropped, SampleErrors: errs}
}
