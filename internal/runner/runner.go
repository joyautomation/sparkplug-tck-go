// Package runner is the assertion engine: a registry of named checks plus the
// Capture context they read from. Each assertion is a small function that
// returns zero or more Results; the runner aggregates them across the
// registry and reports pass/fail.
package runner

import (
	"sort"
	"sync"

	"github.com/joyautomation/sparkplug-tck-go/internal/session"
	"github.com/joyautomation/sparkplug-tck-go/internal/spb"
)

// Status is the outcome of a single assertion against a single subject.
type Status string

const (
	StatusPass          Status = "pass"
	StatusFail          Status = "fail"
	StatusNotApplicable Status = "n/a"
)

// Result records the outcome of one assertion against one subject.
//
// Many assertions evaluate against multiple subjects (e.g. one Result per
// edge node), so Run returns a slice. Subject is a free-form label used
// for reporting; AssertionID matches the [tck-id-*] from the spec.
type Result struct {
	AssertionID string `json:"assertion_id"`
	Subject     string `json:"subject,omitempty"`
	Status      Status `json:"status"`
	Detail      string `json:"detail,omitempty"`
}

// Capture is the input each assertion reads from. Build with NewCapture.
type Capture struct {
	Messages []spb.Message
	Tracker  *session.Tracker
}

// NewCapture builds a Capture by replaying messages through a fresh Tracker.
func NewCapture(msgs []spb.Message) *Capture {
	tr := session.New()
	for _, m := range msgs {
		tr.Apply(m)
	}
	return &Capture{Messages: msgs, Tracker: tr}
}

// AssertionFn evaluates a Capture and returns Results.
type AssertionFn func(*Capture) []Result

// Assertion is one registry entry.
type Assertion struct {
	ID  string
	Run AssertionFn
}

var (
	regMu sync.RWMutex
	reg   = map[string]Assertion{}
)

// Register adds an assertion to the global registry. Intended to be called
// from package init() in internal/assertions/*.
func Register(a Assertion) {
	if a.ID == "" || a.Run == nil {
		panic("runner.Register: empty ID or nil Run")
	}
	regMu.Lock()
	defer regMu.Unlock()
	if _, dup := reg[a.ID]; dup {
		panic("runner.Register: duplicate assertion " + a.ID)
	}
	reg[a.ID] = a
}

// All returns every registered assertion, sorted by ID.
func All() []Assertion {
	regMu.RLock()
	defer regMu.RUnlock()
	out := make([]Assertion, 0, len(reg))
	for _, a := range reg {
		out = append(out, a)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// RunAll executes every registered assertion against c and returns the
// flattened result list, sorted by assertion ID then subject.
func RunAll(c *Capture) []Result {
	var out []Result
	for _, a := range All() {
		out = append(out, a.Run(c)...)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].AssertionID != out[j].AssertionID {
			return out[i].AssertionID < out[j].AssertionID
		}
		return out[i].Subject < out[j].Subject
	})
	return out
}

// Pass is a tiny helper for assertions returning a single passing result.
func Pass(id, subject string) Result {
	return Result{AssertionID: id, Subject: subject, Status: StatusPass}
}

// Fail is a tiny helper for assertions returning a single failing result.
func Fail(id, subject, detail string) Result {
	return Result{AssertionID: id, Subject: subject, Status: StatusFail, Detail: detail}
}

// NA is a tiny helper for "not applicable" — used when there's no subject
// in the capture that the assertion would even apply to.
func NA(id, detail string) Result {
	return Result{AssertionID: id, Status: StatusNotApplicable, Detail: detail}
}
