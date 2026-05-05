package spb

import (
	"fmt"
	"time"

	"github.com/joyautomation/sparkplug-tck-go/internal/spbpb"
	"google.golang.org/protobuf/proto"
)

// Message is one captured Sparkplug message: a parsed topic, the decoded
// payload (nil for STATE which is plain UTF-8), the raw bytes, the MQTT QoS
// and retain flags, and the time of capture.
//
// Captures flow through the system as a stream of Messages. The session
// tracker consumes them; the assertion runner inspects them.
type Message struct {
	Topic    Topic
	Payload  *spbpb.Payload // nil if Topic.Type == STATE
	State    *StatePayload  // populated only when Topic.Type == STATE
	Raw      []byte
	QoS      byte
	Retained bool
	At       time.Time
}

// StatePayload mirrors the Sparkplug 3.0 host STATE payload (JSON):
//
//	{"online":true,"timestamp":1234}
//
// Older 2.x hosts published the bare UTF-8 strings "ONLINE"/"OFFLINE";
// the parser falls back to that form when the bytes aren't valid JSON.
type StatePayload struct {
	Online    bool  `json:"online"`
	Timestamp int64 `json:"timestamp"`
	Legacy    bool  `json:"-"` // true if parsed from plain "ONLINE"/"OFFLINE"
}

// DecodePayload parses raw Sparkplug B protobuf bytes.
func DecodePayload(raw []byte) (*spbpb.Payload, error) {
	p := &spbpb.Payload{}
	if err := proto.Unmarshal(raw, p); err != nil {
		return nil, fmt.Errorf("unmarshal sparkplug payload: %w", err)
	}
	return p, nil
}
