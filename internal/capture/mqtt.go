package capture

import (
	"context"
	"fmt"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/joyautomation/sparkplug-tck-go/internal/spb"
)

// Options configures a live MQTT capture.
type Options struct {
	BrokerURL  string        // e.g. "tcp://localhost:1883" or "tls://broker:8883"
	ClientID   string        // defaults to "sparkplug-tck-<unix-nanos>"
	Username   string        // optional
	Password   string        // optional
	Duration   time.Duration // 0 means "until ctx is cancelled"
	OnConnect  func()        // optional hook fired once subscribed
	OnDropped  func(error)   // optional per-drop callback (sampling, logging)
}

// Run connects to a Sparkplug broker, subscribes to spBv1.0/# (which covers
// the STATE topic too in 3.x), captures every received message until either
// the context is cancelled or opts.Duration elapses, and returns the
// accumulated messages plus capture stats.
func Run(ctx context.Context, opts Options) ([]spb.Message, Stats, error) {
	if opts.BrokerURL == "" {
		return nil, Stats{}, fmt.Errorf("capture: BrokerURL required")
	}
	if opts.ClientID == "" {
		opts.ClientID = fmt.Sprintf("sparkplug-tck-%d", time.Now().UnixNano())
	}

	b := NewBuilder(0)

	co := mqtt.NewClientOptions().
		AddBroker(opts.BrokerURL).
		SetClientID(opts.ClientID).
		SetCleanSession(true).
		SetAutoReconnect(true).
		SetConnectTimeout(10 * time.Second)
	if opts.Username != "" {
		co.SetUsername(opts.Username).SetPassword(opts.Password)
	}

	client := mqtt.NewClient(co)
	tok := client.Connect()
	if !tok.WaitTimeout(15 * time.Second) {
		return nil, Stats{}, fmt.Errorf("capture: connect timeout to %s", opts.BrokerURL)
	}
	if err := tok.Error(); err != nil {
		return nil, Stats{}, fmt.Errorf("capture: connect: %w", err)
	}
	defer client.Disconnect(250)

	handler := func(_ mqtt.Client, msg mqtt.Message) {
		// paho hands us a Message whose Payload buffer it owns; Builder.Add
		// makes its own copy to keep concurrent reads safe.
		b.Add(msg.Topic(), msg.Payload(), msg.Qos(), msg.Retained(), time.Now())
	}

	// spBv1.0/# captures every Sparkplug B edge/device message. Sparkplug 3.x
	// places host STATE under the same prefix (spBv1.0/STATE/<host_id>),
	// so a single subscription covers both. QoS 1 to avoid losing messages
	// the assertion runner needs to reason over.
	if tok := client.Subscribe("spBv1.0/#", 1, handler); !tok.WaitTimeout(10 * time.Second) {
		return nil, b.Stats(), fmt.Errorf("capture: subscribe timeout")
	} else if err := tok.Error(); err != nil {
		return nil, b.Stats(), fmt.Errorf("capture: subscribe: %w", err)
	}

	if opts.OnConnect != nil {
		opts.OnConnect()
	}

	// Wait for whichever stop condition fires first.
	if opts.Duration > 0 {
		timer := time.NewTimer(opts.Duration)
		defer timer.Stop()
		select {
		case <-ctx.Done():
		case <-timer.C:
		}
	} else {
		<-ctx.Done()
	}

	return b.Messages(), b.Stats(), nil
}
