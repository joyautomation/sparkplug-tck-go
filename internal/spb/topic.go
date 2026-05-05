// Package spb defines core Sparkplug B types: topic parsing, message envelopes,
// and the lightweight identifiers used throughout the TCK pipeline.
package spb

import (
	"fmt"
	"strings"
)

// Namespace is the Sparkplug B namespace constant. Sparkplug 3.0 spec
// asserts this MUST equal "spBv1.0" (tck-id-topic-structure-namespace-a,
// tck-id-topic-structure-namespace-b).
const Namespace = "spBv1.0"

// MessageType enumerates the eight Sparkplug message types plus STATE.
type MessageType string

const (
	NBIRTH MessageType = "NBIRTH"
	NDEATH MessageType = "NDEATH"
	NDATA  MessageType = "NDATA"
	NCMD   MessageType = "NCMD"
	DBIRTH MessageType = "DBIRTH"
	DDEATH MessageType = "DDEATH"
	DDATA  MessageType = "DDATA"
	DCMD   MessageType = "DCMD"
	STATE  MessageType = "STATE"
)

// IsNode reports whether t is a node-level message (no device_id).
func (t MessageType) IsNode() bool {
	switch t {
	case NBIRTH, NDEATH, NDATA, NCMD, STATE:
		return true
	}
	return false
}

// IsDevice reports whether t is a device-level message (requires device_id).
func (t MessageType) IsDevice() bool {
	switch t {
	case DBIRTH, DDEATH, DDATA, DCMD:
		return true
	}
	return false
}

// IsBirth reports whether t is a birth certificate (NBIRTH or DBIRTH).
func (t MessageType) IsBirth() bool {
	return t == NBIRTH || t == DBIRTH
}

// IsDeath reports whether t is a death certificate (NDEATH or DDEATH).
func (t MessageType) IsDeath() bool {
	return t == NDEATH || t == DDEATH
}

// EdgeNodeID identifies an edge node within the Sparkplug topic namespace.
type EdgeNodeID struct {
	Group string
	Node  string
}

func (e EdgeNodeID) String() string { return e.Group + "/" + e.Node }

// Topic is a parsed Sparkplug B topic.
type Topic struct {
	Namespace string
	EdgeNodeID
	Type   MessageType
	Device string // empty for node-level messages
	Host   string // populated only for STATE topics
}

// String reassembles the topic in its wire form. Useful for logging and tests.
func (t Topic) String() string {
	if t.Type == STATE {
		// Sparkplug 3.x form: spBv1.0/STATE/<host_id>
		return fmt.Sprintf("%s/STATE/%s", t.Namespace, t.Host)
	}
	if t.Device != "" {
		return fmt.Sprintf("%s/%s/%s/%s/%s", t.Namespace, t.Group, t.Type, t.Node, t.Device)
	}
	return fmt.Sprintf("%s/%s/%s/%s", t.Namespace, t.Group, t.Type, t.Node)
}

// ParseTopic splits a Sparkplug B topic string into its components and
// validates structural rules from chapter 4 of the spec.
//
// Two layouts are accepted:
//   - Edge: spBv1.0/<group>/<msg_type>/<node>[/<device>]
//   - Host: spBv1.0/STATE/<host_id>
func ParseTopic(s string) (Topic, error) {
	parts := strings.Split(s, "/")
	if len(parts) < 3 {
		return Topic{}, fmt.Errorf("topic %q: expected at least 3 segments, got %d", s, len(parts))
	}
	ns := parts[0]
	if ns != Namespace {
		return Topic{}, fmt.Errorf("topic %q: namespace %q != %q", s, ns, Namespace)
	}

	// Host STATE: spBv1.0/STATE/<host_id>
	if parts[1] == "STATE" {
		if len(parts) != 3 {
			return Topic{}, fmt.Errorf("topic %q: STATE topic must be %s/STATE/<host_id>", s, Namespace)
		}
		if parts[2] == "" {
			return Topic{}, fmt.Errorf("topic %q: empty host_id", s)
		}
		return Topic{Namespace: ns, Type: STATE, Host: parts[2]}, nil
	}

	// Edge form requires at least 4 segments.
	if len(parts) < 4 {
		return Topic{}, fmt.Errorf("topic %q: edge topic needs >= 4 segments", s)
	}
	group, mt, node := parts[1], MessageType(parts[2]), parts[3]
	if group == "" || node == "" {
		return Topic{}, fmt.Errorf("topic %q: empty group_id or edge_node_id", s)
	}

	t := Topic{Namespace: ns, EdgeNodeID: EdgeNodeID{Group: group, Node: node}, Type: mt}

	switch {
	case mt.IsNode():
		// tck-id-topic-structure-namespace-device-id-non-associated-message-types:
		// NBIRTH/NDEATH/NDATA/NCMD MUST NOT include a device_id segment.
		if len(parts) > 4 {
			return Topic{}, fmt.Errorf("topic %q: %s must not include a device_id", s, mt)
		}
	case mt.IsDevice():
		// tck-id-topic-structure-namespace-device-id-associated-message-types:
		// DBIRTH/DDEATH/DDATA/DCMD MUST include a device_id segment.
		if len(parts) != 5 {
			return Topic{}, fmt.Errorf("topic %q: %s must include a device_id", s, mt)
		}
		if parts[4] == "" {
			return Topic{}, fmt.Errorf("topic %q: empty device_id", s)
		}
		t.Device = parts[4]
	default:
		return Topic{}, fmt.Errorf("topic %q: unknown message type %q", s, mt)
	}

	// tck-id-topic-structure-namespace-valid-group-id (and node/device variants):
	// the reserved characters +, /, # MUST NOT appear. The slash is already
	// implicit in the split, but + and # could survive inside a segment.
	for _, seg := range []string{group, node, t.Device} {
		if seg == "" {
			continue
		}
		if strings.ContainsAny(seg, "+#") {
			return Topic{}, fmt.Errorf("topic %q: segment %q contains reserved character", s, seg)
		}
	}

	return t, nil
}
