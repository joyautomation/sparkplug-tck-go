package spb

import (
	"encoding/json"
	"fmt"
	"strings"
)

// DecodeState parses the payload of a STATE topic. Sparkplug 3.0 mandates a
// JSON object with "online" and "timestamp" fields; the 2.x flavor used the
// bare strings "ONLINE" and "OFFLINE". DecodeState handles both, marking the
// legacy form so assertions can flag it as a 2.x compatibility leak.
func DecodeState(raw []byte) (*StatePayload, error) {
	trimmed := strings.TrimSpace(string(raw))

	// Legacy 2.x form first because it would not parse as JSON.
	switch strings.ToUpper(trimmed) {
	case "ONLINE":
		return &StatePayload{Online: true, Legacy: true}, nil
	case "OFFLINE":
		return &StatePayload{Online: false, Legacy: true}, nil
	}

	var s StatePayload
	if err := json.Unmarshal(raw, &s); err != nil {
		return nil, fmt.Errorf("decode STATE payload: %w", err)
	}
	return &s, nil
}
