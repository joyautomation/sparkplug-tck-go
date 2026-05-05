// Package harness drives strict ("Layer 3") TCK assertions by running
// an in-process MQTT broker (mochi) that the SUT connects to. The broker
// records every CONNECT, PUBLISH, SUBSCRIBE, DISCONNECT and Will-sent
// packet in arrival order, so scenarios can verify rules the passive
// runner can't observe — e.g. "NDEATH MUST be published before
// DISCONNECT" or "host CONNECT MUST set Clean Session = true".
package harness

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/mochi-mqtt/server/v2/packets"
)

// EventType labels a recorded packet. We don't record every Mochi hook —
// only the ones a Sparkplug scenario can branch on.
type EventType string

const (
	EvConnect    EventType = "CONNECT"
	EvPublish    EventType = "PUBLISH"
	EvSubscribe  EventType = "SUBSCRIBE"
	EvDisconnect EventType = "DISCONNECT"
	EvWillSent   EventType = "WILL_SENT"
)

// Event is one packet observed by the broker. Scenarios consume an
// ordered slice of these to verify causality across the connection.
type Event struct {
	At         time.Time
	Type       EventType
	ClientID   string
	Topic      string  // PUBLISH, WILL_SENT, SUBSCRIBE
	QoS        byte    // PUBLISH, SUBSCRIBE, WILL
	Retained   bool    // PUBLISH, WILL
	CleanStart bool    // CONNECT (3.1.1: Clean Session, 5.0: Clean Start)
	Will       *Will   // CONNECT
	Payload    []byte  // PUBLISH, WILL_SENT
	DiscErr    string  // DISCONNECT — non-empty if the disconnect was unclean
}

// Will captures the Will-message fields from a CONNECT packet so a
// scenario can verify the host/edge advertised the right Death Cert.
type Will struct {
	Topic   string
	QoS     byte
	Retain  bool
	Payload []byte
}

// Broker is a Mochi server bound to a localhost TCP port (chosen at
// construction so multiple harness instances run in parallel). Use
// NewBroker in tests; call Close when done.
type Broker struct {
	server *mqtt.Server
	addr   string

	mu     sync.Mutex
	events []Event
}

// NewBroker spins up a fresh mochi server on a free localhost TCP port,
// installs an allow-all auth hook, and registers the recorder. Used by
// tests; CLI/harness mode wants NewBrokerAt to bind a known address.
func NewBroker() (*Broker, error) {
	return NewBrokerAt("127.0.0.1:0")
}

// NewBrokerAt is NewBroker but binds the listener to the supplied
// address (host:port). Pass "host:0" to let the OS pick the port.
func NewBrokerAt(bind string) (*Broker, error) {
	// Reserve up front so we can echo the resolved address back to the
	// caller (and so :0 yields a usable port instead of a race).
	ln, err := net.Listen("tcp", bind)
	if err != nil {
		return nil, fmt.Errorf("reserve port: %w", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	srv := mqtt.New(&mqtt.Options{
		// Silence server logs; tests can re-enable by replacing srv.Log.
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		// InlineClient lets the harness publish stimuli (e.g. NCMD/Rebirth)
		// through srv.Publish so scenarios can score the SUT's response.
		InlineClient: true,
	})
	if err := srv.AddHook(new(auth.AllowHook), nil); err != nil {
		return nil, fmt.Errorf("install auth: %w", err)
	}

	b := &Broker{server: srv, addr: addr}
	if err := srv.AddHook(&recorder{b: b}, nil); err != nil {
		return nil, fmt.Errorf("install recorder: %w", err)
	}

	tcp := listeners.NewTCP(listeners.Config{ID: "tck-tcp", Address: addr})
	if err := srv.AddListener(tcp); err != nil {
		return nil, fmt.Errorf("add listener: %w", err)
	}

	go func() { _ = srv.Serve() }()
	if err := waitListening(addr, 2*time.Second); err != nil {
		_ = srv.Close()
		return nil, err
	}
	return b, nil
}

func (b *Broker) Addr() string { return b.addr }
func (b *Broker) URL() string  { return "tcp://" + b.addr }

// Events returns a snapshot of every packet seen, in arrival order.
func (b *Broker) Events() []Event {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]Event, len(b.events))
	copy(out, b.events)
	return out
}

func (b *Broker) Close() error {
	return b.server.Close()
}

// Publish injects a message into the broker as if a connected client
// had sent it. The harness CLI uses this for stimuli (e.g. publishing
// NCMD/Rebirth at a known instant so the scenario can score the
// edge's response).
func (b *Broker) Publish(topic string, payload []byte, retain bool, qos byte) error {
	return b.server.Publish(topic, payload, retain, qos)
}

func (b *Broker) record(e Event) {
	if e.At.IsZero() {
		e.At = time.Now()
	}
	b.mu.Lock()
	b.events = append(b.events, e)
	b.mu.Unlock()
}

// recorder bridges Mochi's hook interface into the broker's event log.
// We only override the hooks we actually use; everything else falls
// through to HookBase no-ops.
type recorder struct {
	mqtt.HookBase
	b *Broker
}

func (r *recorder) ID() string { return "tck-recorder" }

func (r *recorder) Provides(b byte) bool {
	return bytesContain(b,
		mqtt.OnConnect, mqtt.OnDisconnect,
		mqtt.OnPublish, mqtt.OnSubscribe,
		mqtt.OnWillSent,
	)
}

func (r *recorder) Init(_ any) error { return nil }

func (r *recorder) OnConnect(cl *mqtt.Client, pk packets.Packet) error {
	ev := Event{
		Type:       EvConnect,
		ClientID:   cl.ID,
		CleanStart: pk.Connect.Clean,
	}
	if pk.Connect.WillFlag {
		ev.Will = &Will{
			Topic:   pk.Connect.WillTopic,
			QoS:     pk.Connect.WillQos,
			Retain:  pk.Connect.WillRetain,
			Payload: append([]byte(nil), pk.Connect.WillPayload...),
		}
	}
	r.b.record(ev)
	return nil
}

func (r *recorder) OnDisconnect(cl *mqtt.Client, err error, expire bool) {
	ev := Event{Type: EvDisconnect, ClientID: cl.ID}
	if err != nil {
		ev.DiscErr = err.Error()
	}
	r.b.record(ev)
}

func (r *recorder) OnPublish(cl *mqtt.Client, pk packets.Packet) (packets.Packet, error) {
	r.b.record(Event{
		Type:     EvPublish,
		ClientID: cl.ID,
		Topic:    pk.TopicName,
		QoS:      pk.FixedHeader.Qos,
		Retained: pk.FixedHeader.Retain,
		Payload:  append([]byte(nil), pk.Payload...),
	})
	return pk, nil
}

func (r *recorder) OnSubscribe(cl *mqtt.Client, pk packets.Packet) packets.Packet {
	for _, sub := range pk.Filters {
		r.b.record(Event{
			Type:     EvSubscribe,
			ClientID: cl.ID,
			Topic:    sub.Filter,
			QoS:      sub.Qos,
		})
	}
	return pk
}

func (r *recorder) OnWillSent(cl *mqtt.Client, pk packets.Packet) {
	r.b.record(Event{
		Type:     EvWillSent,
		ClientID: cl.ID,
		Topic:    pk.TopicName,
		QoS:      pk.FixedHeader.Qos,
		Retained: pk.FixedHeader.Retain,
		Payload:  append([]byte(nil), pk.Payload...),
	})
}

func bytesContain(needle byte, hay ...byte) bool {
	for _, b := range hay {
		if b == needle {
			return true
		}
	}
	return false
}

// waitListening polls until the broker accepts a TCP connection on its
// chosen port. Mochi's Serve goroutine binds asynchronously, so without
// this the first client connect can race the listener.
func waitListening(addr string, max time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), max)
	defer cancel()
	d := &net.Dialer{}
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("broker not listening on %s within %s", addr, max)
		default:
		}
		conn, err := d.DialContext(ctx, "tcp", addr)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
}
