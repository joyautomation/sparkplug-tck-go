// Command sparkplug-tck-bench reports parity between this Go TCK and the
// upstream Eclipse Sparkplug spec. The spec catalog (assertions.json) is
// the parity contract: every [tck-id-*] in the spec is one row, and the
// bench reports which mode of this TCK produces a result for it
// (passive, harness, both, or neither).
//
// Output is a Markdown report on stdout (paste into README) plus a
// summary line on stderr. Use -json for machine-readable output.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	_ "github.com/joyautomation/sparkplug-tck-go/internal/assertions"
	"github.com/joyautomation/sparkplug-tck-go/internal/harness"
	"github.com/joyautomation/sparkplug-tck-go/internal/runner"
)

type catalogEntry struct {
	ID         string `json:"id"`
	Text       string `json:"text"`
	Category   string `json:"category"`
	SourceFile string `json:"source_file"`
	SourceLine int    `json:"source_line"`
}

type report struct {
	CatalogTotal int                       `json:"catalog_total"`
	OutOfScope   []string                  `json:"out_of_scope_ids"`
	PassiveIDs   int                       `json:"passive_ids"`
	HarnessIDs   int                       `json:"harness_ids"`
	UnionIDs     int                       `json:"union_ids"`
	UncoveredIDs []string                  `json:"uncovered_ids"`
	HarnessOnly  []string                  `json:"harness_only_ids"`
	ProfileTimes map[string]int            `json:"profile_times_ms"` // legacy, kept for compat
	Profiles     map[string]profilePerf    `json:"profile_perf"`
	Wallclock    int64                     `json:"wallclock_us"`
	Upstream     []upstreamTestParity      `json:"upstream_tests,omitempty"`
	// HarnessVerdicts is the per-ID verdict the harness produced for the
	// compliant synthetic SUT — fold of every scenario's results down to
	// one status per ID (FAIL beats PASS beats N/A). Used by the
	// correctness comparator to diff against the upstream Java TCK.
	HarnessVerdicts map[string]string `json:"harness_verdicts,omitempty"`
}

// profilePerf is the per-profile perf breakdown the bench emits.
type profilePerf struct {
	BrokerBootUS int `json:"broker_boot_us"`
	DriveUS      int `json:"drive_us"`
	EvalUS       int `json:"eval_us"`
	TotalUS      int `json:"total_us"`
	Events       int `json:"events_captured"`
	Results      int `json:"results_emitted"`
	Scenarios    int `json:"scenarios"`
}

// upstreamTest is the on-disk shape of one upstream-tck inventory entry
// (see upstream_tests.json — produced by scripts/update-upstream-inventory.sh).
type upstreamTest struct {
	File string   `json:"file"` // e.g. "edge/SessionEstablishmentTest.java"
	IDs  []string `json:"ids"`  // tck-id-* the test class asserts on
}

// upstreamTestParity is one row in the per-upstream-test parity table.
// "Harness covered" means at least one of our harness scenarios emits a
// result for that ID — passive coverage is reported in aggregate.
type upstreamTestParity struct {
	File         string   `json:"file"`
	IDs          int      `json:"ids"`
	HarnessHit   int      `json:"harness_hit"`
	HarnessMiss  []string `json:"harness_miss,omitempty"`
}

func main() {
	catalog := flag.String("catalog", "assertions.json", "path to spec assertion catalog")
	upstreamPath := flag.String("upstream", "upstream_tests.json", "path to upstream TCK inventory (produced by scripts/update-upstream-inventory.sh)")
	jsonOut := flag.Bool("json", false, "emit JSON report instead of Markdown")
	flag.Parse()

	rawCat, err := loadCatalog(*catalog)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load catalog: %v\n", err)
		os.Exit(1)
	}
	// Filter the catalog down to client-conformance IDs. Broker
	// conformance (`tck-id-conformance-mqtt-*`) is intentionally out of
	// scope — the upstream Java TCK is the right tool for those, and
	// counting them in our denominator would understate client parity.
	cat := make([]catalogEntry, 0, len(rawCat))
	outOfScope := []string{}
	for _, a := range rawCat {
		if isOutOfScope(a.ID) {
			outOfScope = append(outOfScope, a.ID)
			continue
		}
		cat = append(cat, a)
	}
	sort.Strings(outOfScope)
	catIDs := map[string]bool{}
	for _, a := range cat {
		catIDs[a.ID] = true
	}

	passiveIDs := map[string]bool{}
	for _, a := range runner.All() {
		passiveIDs[a.ID] = true
	}

	wallStart := time.Now()
	harnessIDs, profileTimes, profilePerfs, harnessVerdicts, err := harnessCoverage()
	if err != nil {
		fmt.Fprintf(os.Stderr, "harness coverage: %v\n", err)
		os.Exit(1)
	}
	wallclock := time.Since(wallStart)

	union := map[string]bool{}
	for id := range passiveIDs {
		union[id] = true
	}
	for id := range harnessIDs {
		union[id] = true
	}

	uncovered := diffSorted(catIDs, union)
	harnessOnly := diffSorted(harnessIDs, passiveIDs)

	upstream, err := loadUpstream(*upstreamPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load upstream: %v (continuing without per-test parity)\n", err)
	}
	upstreamRows := upstreamParity(upstream, harnessIDs)

	rep := report{
		CatalogTotal: len(cat),
		OutOfScope:   outOfScope,
		PassiveIDs:   countCovered(catIDs, passiveIDs),
		HarnessIDs:   countCovered(catIDs, harnessIDs),
		UnionIDs:     countCovered(catIDs, union),
		UncoveredIDs: uncovered,
		HarnessOnly:  harnessOnly,
		ProfileTimes: profileTimes,
		Profiles:     profilePerfs,
		Wallclock:    wallclock.Microseconds(),
		Upstream:     upstreamRows,
		HarnessVerdicts: harnessVerdicts,
	}

	if *jsonOut {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(rep)
		return
	}
	printMarkdown(os.Stdout, rep)
}

// harnessCoverage runs each profile against a compliant synthetic SUT
// driven through the in-process broker, collects the set of assertion
// IDs the profile emits, and breaks the wallclock down by phase.
//
// verdicts is the per-ID rollup matching the upstream TCK's PASS/FAIL/
// NOT_EXECUTED grammar — across all scenarios for one ID, FAIL beats
// PASS beats NA. Used by the correctness comparator.
func harnessCoverage() (ids map[string]bool, times map[string]int, perfs map[string]profilePerf, verdicts map[string]string, err error) {
	ids = map[string]bool{}
	times = map[string]int{}
	perfs = map[string]profilePerf{}
	verdicts = map[string]string{}
	for name, p := range harness.Profiles {
		bootStart := time.Now()
		var b *harness.Broker
		b, err = harness.NewBroker()
		if err != nil {
			return nil, nil, nil, nil, err
		}
		bootUS := time.Since(bootStart).Microseconds()

		driveStart := time.Now()
		drive(name, b)
		driveUS := time.Since(driveStart).Microseconds()

		evalStart := time.Now()
		results := p.Run(b)
		evalUS := time.Since(evalStart).Microseconds()

		for _, r := range results {
			ids[r.AssertionID] = true
			cur := verdicts[r.AssertionID]
			next := mapStatus(string(r.Status))
			verdicts[r.AssertionID] = foldVerdict(cur, next)
		}
		times[name] = int(evalUS / 1000) // legacy ms field
		perfs[name] = profilePerf{
			BrokerBootUS: int(bootUS),
			DriveUS:      int(driveUS),
			EvalUS:       int(evalUS),
			TotalUS:      int(bootUS + driveUS + evalUS),
			Events:       len(b.Events()),
			Results:      len(results),
			Scenarios:    len(p.Scenarios),
		}
		_ = b.Close()
	}
	return ids, times, perfs, verdicts, nil
}

// mapStatus translates internal runner.Status values to the upstream
// TCK's PASS/FAIL/NOT_EXECUTED grammar so verdicts diff cleanly.
func mapStatus(s string) string {
	switch s {
	case "pass":
		return "PASS"
	case "fail":
		return "FAIL"
	case "n/a":
		return "NOT_EXECUTED"
	}
	return s
}

// foldVerdict combines two per-ID statuses for the same assertion. FAIL
// dominates PASS dominates NOT_EXECUTED — same precedence the Java TCK
// applies in Results.getSummary().
func foldVerdict(a, b string) string {
	rank := func(s string) int {
		switch s {
		case "FAIL":
			return 3
		case "PASS":
			return 2
		case "NOT_EXECUTED":
			return 1
		}
		return 0
	}
	if rank(a) >= rank(b) {
		return a
	}
	return b
}

// drive replays a known-good lifecycle for the named profile through the
// in-process broker. Synthetic drivers live alongside the bench so the
// coverage report doesn't need a real SUT.
func drive(profile string, b *harness.Broker) {
	switch profile {
	case "edge-node":
		driveEdge(b)
	case "host-application":
		driveHost(b)
	}
}

func loadUpstream(path string) ([]upstreamTest, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var ts []upstreamTest
	if err := json.Unmarshal(raw, &ts); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	return ts, nil
}

func upstreamParity(tests []upstreamTest, harness map[string]bool) []upstreamTestParity {
	out := make([]upstreamTestParity, 0, len(tests))
	for _, t := range tests {
		// Skip upstream broker test classes — broker conformance is out
		// of scope (see isOutOfScope) so reporting their misses against
		// our parity number would be dishonest.
		if strings.HasPrefix(t.File, "broker/") {
			continue
		}
		hit := 0
		var miss []string
		for _, id := range t.IDs {
			if harness[id] {
				hit++
			} else {
				miss = append(miss, id)
			}
		}
		out = append(out, upstreamTestParity{
			File: t.File, IDs: len(t.IDs), HarnessHit: hit, HarnessMiss: miss,
		})
	}
	return out
}

// isOutOfScope marks IDs that this project intentionally doesn't grade.
// Broker conformance (`tck-id-conformance-mqtt-*`) — both basic and
// "Sparkplug-aware" — measures broker behaviour, not client behaviour.
// The upstream Java TCK is the right tool for that. Excluding these
// from the catalog total keeps the parity numbers honest about what we
// actually claim to cover.
func isOutOfScope(id string) bool {
	return strings.HasPrefix(id, "tck-id-conformance-mqtt-")
}

func loadCatalog(path string) ([]catalogEntry, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cat []catalogEntry
	if err := json.Unmarshal(raw, &cat); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	return cat, nil
}

func countCovered(catalog, mode map[string]bool) int {
	n := 0
	for id := range mode {
		if catalog[id] {
			n++
		}
	}
	return n
}

func diffSorted(a, b map[string]bool) []string {
	var out []string
	for id := range a {
		if !b[id] {
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out
}

func printMarkdown(w *os.File, r report) {
	pct := func(n int) string {
		if r.CatalogTotal == 0 {
			return "—"
		}
		return fmt.Sprintf("%.1f%%", 100*float64(n)/float64(r.CatalogTotal))
	}
	fmt.Fprintln(w, "# Sparkplug TCK parity report")
	fmt.Fprintln(w)
	fmt.Fprintf(w, "Catalog total: **%d** assertion IDs (from spec)\n\n", r.CatalogTotal)
	fmt.Fprintln(w, "| Mode | IDs covered | Coverage |")
	fmt.Fprintln(w, "|------|-------------|----------|")
	fmt.Fprintf(w, "| Passive only | %d | %s |\n", r.PassiveIDs, pct(r.PassiveIDs))
	fmt.Fprintf(w, "| Harness only | %d | %s |\n", r.HarnessIDs, pct(r.HarnessIDs))
	fmt.Fprintf(w, "| Union (passive ∪ harness) | %d | %s |\n", r.UnionIDs, pct(r.UnionIDs))
	fmt.Fprintln(w)
	if len(r.Profiles) > 0 {
		fmt.Fprintln(w, "## Harness profile perf")
		fmt.Fprintln(w)
		fmt.Fprintln(w, "Per-profile breakdown of broker boot, synthetic SUT drive, and scenario evaluation. All times in microseconds (μs).")
		fmt.Fprintln(w)
		fmt.Fprintln(w, "```")
		fmt.Fprintln(w, "Profile           Boot     Drive     Eval     Total   Events   Results   Scenarios")
		fmt.Fprintln(w, "----------------  -------  --------  -------  ------  -------  --------  ---------")
		names := make([]string, 0, len(r.Profiles))
		for n := range r.Profiles {
			names = append(names, n)
		}
		sort.Strings(names)
		for _, n := range names {
			p := r.Profiles[n]
			fmt.Fprintf(w, "%-16s  %7d  %8d  %7d  %6d  %7d  %8d  %9d\n",
				n, p.BrokerBootUS, p.DriveUS, p.EvalUS, p.TotalUS,
				p.Events, p.Results, p.Scenarios)
		}
		fmt.Fprintln(w, "```")
		fmt.Fprintln(w)
		fmt.Fprintf(w, "Total wallclock (broker boot + drive + eval, both profiles): **%d μs (%.1f ms)**\n\n",
			r.Wallclock, float64(r.Wallclock)/1000.0)
		fmt.Fprintln(w, "_The upstream Eclipse Sparkplug TCK runs as a HiveMQ extension and waits for a real SUT to connect before assertions fire — see scripts/upstream-tck for the side-by-side correctness diff. This Go bench drives a synthetic SUT through an in-process broker and scores every scenario in a single binary launch._")
		fmt.Fprintln(w)
	}
	if len(r.Upstream) > 0 {
		fmt.Fprintln(w, "## Per-upstream-test parity")
		fmt.Fprintln(w)
		fmt.Fprintln(w, "Each row is one upstream Eclipse-TCK test class. \"Harness\" counts how many of its assertion IDs our harness scenarios emit a result for.")
		fmt.Fprintln(w)
		fmt.Fprintln(w, "| Upstream test | IDs | Harness covered |")
		fmt.Fprintln(w, "|---------------|-----|-----------------|")
		var totalIDs, totalHit int
		for _, u := range r.Upstream {
			fmt.Fprintf(w, "| %s | %d | %d (%.0f%%) |\n",
				u.File, u.IDs, u.HarnessHit, percent(u.HarnessHit, u.IDs))
			totalIDs += u.IDs
			totalHit += u.HarnessHit
		}
		fmt.Fprintf(w, "| **Total (unique IDs may be lower)** | **%d** | **%d (%.0f%%)** |\n",
			totalIDs, totalHit, percent(totalHit, totalIDs))
		fmt.Fprintln(w)
	}
	if len(r.HarnessOnly) > 0 {
		fmt.Fprintln(w, "## Harness-only IDs")
		fmt.Fprintln(w)
		fmt.Fprintln(w, "These are scored only when running the harness — passive captures can't observe them.")
		fmt.Fprintln(w)
		for _, id := range r.HarnessOnly {
			fmt.Fprintf(w, "- %s\n", id)
		}
		fmt.Fprintln(w)
	}
	if len(r.UncoveredIDs) > 0 {
		fmt.Fprintln(w, "## Uncovered IDs")
		fmt.Fprintln(w)
		fmt.Fprintf(w, "%d spec IDs are not yet scored in either mode.\n\n", len(r.UncoveredIDs))
		for _, id := range r.UncoveredIDs {
			fmt.Fprintf(w, "- %s\n", id)
		}
	}
	fmt.Fprintf(os.Stderr, "parity: %s union (%d/%d), %d harness-only, %d uncovered\n",
		pct(r.UnionIDs), r.UnionIDs, r.CatalogTotal,
		len(r.HarnessOnly), len(r.UncoveredIDs))
	if len(r.Upstream) > 0 {
		var ids, hit int
		for _, u := range r.Upstream {
			ids += u.IDs
			hit += u.HarnessHit
		}
		fmt.Fprintf(os.Stderr, "upstream-test parity: %.1f%% (%d/%d test-asserted IDs covered by harness)\n",
			percent(hit, ids), hit, ids)
	}
}

func percent(n, d int) float64 {
	if d == 0 {
		return 0
	}
	return 100 * float64(n) / float64(d)
}
