// Package session reduces a stream of captured Sparkplug messages into the
// derived state assertions need: per-edge-node lifecycle, sequence numbers,
// metric alias maps, device births, host STATE history.
//
// State is built up incrementally by Apply; assertion functions then read
// from EdgeNodes / Hosts after a capture completes (or mid-stream for
// live-running profiles).
package session

import (
	"github.com/joyautomation/sparkplug-tck-go/internal/spb"
)

// SeqUnseen is the sentinel for "no sequence number observed yet".
const SeqUnseen = -1

// AliasMap maps the alias number declared in an NBIRTH/DBIRTH to its
// metric name. tck-id-payloads-name-aliases-* assertions use this to
// validate that subsequent NDATA/DDATA references resolve.
type AliasMap map[uint64]string

// EdgeState captures everything we know about a single edge node.
type EdgeState struct {
	ID           spb.EdgeNodeID
	Online       bool      // last NBIRTH set true; NDEATH set false
	BirthBdSeq   *uint64   // bdSeq value from the most recent NBIRTH
	LastSeq      int       // last seq observed (-1 if none yet)
	NodeAliases  AliasMap  // alias -> metric name from the NBIRTH
	Devices      map[string]*DeviceState
	BirthCount   int // how many NBIRTHs we've seen (>1 implies a rebirth)
	DeathCount   int
	DataCount    int
}

// DeviceState captures everything we know about a single device.
type DeviceState struct {
	ID         string
	Online     bool
	Aliases    AliasMap
	BirthCount int
	DeathCount int
	DataCount  int
}

// HostState captures STATE history for one host application.
type HostState struct {
	ID      string
	Online  bool   // latest STATE.online
	Legacy  bool   // most recent STATE was the bare-string 2.x form
	History []HostStateEntry
}

type HostStateEntry struct {
	Online    bool
	Timestamp int64
	Retained  bool
	Legacy    bool
}

// Tracker is the aggregate state of all edges and hosts seen in a capture.
type Tracker struct {
	Edges map[spb.EdgeNodeID]*EdgeState
	Hosts map[string]*HostState
}

// New returns an empty Tracker.
func New() *Tracker {
	return &Tracker{
		Edges: map[spb.EdgeNodeID]*EdgeState{},
		Hosts: map[string]*HostState{},
	}
}

// Apply folds a single message into the tracker state.
//
// Apply is intentionally permissive: it never rejects malformed input.
// Validation lives in the assertion runner, which inspects the resulting
// state to decide pass/fail per assertion. That separation lets us record
// even spec-violating sequences and report on them.
func (t *Tracker) Apply(m spb.Message) {
	switch m.Topic.Type {
	case spb.STATE:
		t.applyState(m)
	default:
		t.applyEdge(m)
	}
}

func (t *Tracker) applyState(m spb.Message) {
	if m.State == nil {
		return
	}
	h, ok := t.Hosts[m.Topic.Host]
	if !ok {
		h = &HostState{ID: m.Topic.Host}
		t.Hosts[m.Topic.Host] = h
	}
	h.Online = m.State.Online
	h.Legacy = m.State.Legacy
	h.History = append(h.History, HostStateEntry{
		Online:    m.State.Online,
		Timestamp: m.State.Timestamp,
		Retained:  m.Retained,
		Legacy:    m.State.Legacy,
	})
}

func (t *Tracker) applyEdge(m spb.Message) {
	e, ok := t.Edges[m.Topic.EdgeNodeID]
	if !ok {
		e = &EdgeState{
			ID:          m.Topic.EdgeNodeID,
			LastSeq:     SeqUnseen,
			NodeAliases: AliasMap{},
			Devices:     map[string]*DeviceState{},
		}
		t.Edges[m.Topic.EdgeNodeID] = e
	}

	if m.Payload != nil && m.Payload.Seq != nil {
		e.LastSeq = int(*m.Payload.Seq)
	}

	switch m.Topic.Type {
	case spb.NBIRTH:
		e.Online = true
		e.BirthCount++
		e.BirthBdSeq = extractBdSeq(m.Payload)
		e.NodeAliases = aliasesFrom(m.Payload)
	case spb.NDEATH:
		e.Online = false
		e.DeathCount++
	case spb.NDATA:
		e.DataCount++
	case spb.DBIRTH:
		d := e.device(m.Topic.Device)
		d.Online = true
		d.BirthCount++
		d.Aliases = aliasesFrom(m.Payload)
	case spb.DDEATH:
		d := e.device(m.Topic.Device)
		d.Online = false
		d.DeathCount++
	case spb.DDATA:
		d := e.device(m.Topic.Device)
		d.DataCount++
	}
}

func (e *EdgeState) device(id string) *DeviceState {
	d, ok := e.Devices[id]
	if !ok {
		d = &DeviceState{ID: id, Aliases: AliasMap{}}
		e.Devices[id] = d
	}
	return d
}
