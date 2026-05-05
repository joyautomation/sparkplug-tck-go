// Command sparkplug-tck-diff compares per-ID verdicts produced by the
// upstream Java Sparkplug TCK against this repo's Go harness. It reads
// two JSON files:
//
//   -java   verdicts captured by sparkplug-tck-correctness (one test
//           run, e.g. edge SessionEstablishmentTest)
//   -go     full report from sparkplug-tck-bench -json (the
//           harness_verdicts field is required)
//
// Output is a Markdown table on stdout plus an agreement summary on
// stderr. Verdicts are normalized to PASS/FAIL/NOT_EXECUTED so the
// "agreement" cell is a direct equality check.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
)

type javaReport struct {
	Test     string         `json:"test"`
	Verdicts []javaVerdict  `json:"verdicts"`
	Overall  string         `json:"overall"`
}

// javaMulti is the multi-test JSON shape sparkplug-tck-correctness emits
// when run with -tests; the single-test shape (one javaReport) is
// recognized for backwards compat.
type javaMulti struct {
	Tests []javaReport `json:"tests"`
}

type javaVerdict struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

type goReport struct {
	HarnessVerdicts map[string]string `json:"harness_verdicts"`
}

type row struct {
	ID    string
	Java  string
	Go    string
	Agree bool
}

func main() {
	javaPath := flag.String("java", "", "Path to upstream Java verdicts JSON (from sparkplug-tck-correctness)")
	goPath := flag.String("go", "", "Path to Go bench JSON (from sparkplug-tck-bench -json)")
	jsonOut := flag.Bool("json", false, "emit JSON diff instead of Markdown")
	flag.Parse()

	if *javaPath == "" || *goPath == "" {
		fmt.Fprintln(os.Stderr, "usage: sparkplug-tck-diff -java <path> -go <path>")
		os.Exit(2)
	}

	javaTests, err := loadJava(*javaPath)
	if err != nil {
		die("load java: %v", err)
	}
	gr, err := loadGo(*goPath)
	if err != nil {
		die("load go: %v", err)
	}

	if *jsonOut {
		emitJSONMulti(javaTests, gr)
	} else {
		emitMarkdownMulti(javaTests, gr)
	}

	for _, jr := range javaTests {
		rows := buildRows(jr, gr)
		t := count(rows)
		fmt.Fprintf(os.Stderr,
			"%s — agree:%d disagree:%d (java only:%d, go only:%d) — agreement %.1f%%\n",
			jr.Test, t.agree, t.disagree, t.javaOnly, t.goOnly,
			percent(t.agree, t.agree+t.disagree))
	}
}

// loadJava accepts either the multi-test shape `{tests: [...]}` or the
// single-test shape (one javaReport) and normalizes to a slice.
func loadJava(path string) ([]javaReport, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var multi javaMulti
	if err := json.Unmarshal(raw, &multi); err == nil && len(multi.Tests) > 0 {
		return multi.Tests, nil
	}
	var single javaReport
	if err := json.Unmarshal(raw, &single); err != nil {
		return nil, err
	}
	if single.Test == "" {
		return nil, fmt.Errorf("%s has neither tests[] nor a single test report", path)
	}
	return []javaReport{single}, nil
}

func loadGo(path string) (goReport, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return goReport{}, err
	}
	var gr goReport
	if err := json.Unmarshal(raw, &gr); err != nil {
		return goReport{}, err
	}
	if gr.HarnessVerdicts == nil {
		return gr, fmt.Errorf("%s missing harness_verdicts (rerun sparkplug-tck-bench -json)", path)
	}
	return gr, nil
}

// buildRows produces one row per ID seen in either engine. Java IDs use
// hyphenated form ("payloads-nbirth-bdseq"); Go uses spec-prefixed
// "tck-id-payloads-nbirth-bdseq". We normalize both to the hyphenated
// short form for matching.
func buildRows(jr javaReport, gr goReport) []row {
	java := map[string]string{}
	for _, v := range jr.Verdicts {
		java[normalize(v.ID)] = normalizeStatus(v.Status)
	}
	goVerdicts := map[string]string{}
	for id, status := range gr.HarnessVerdicts {
		goVerdicts[normalize(id)] = normalizeStatus(status)
	}

	all := map[string]struct{}{}
	for id := range java {
		all[id] = struct{}{}
	}
	for id := range goVerdicts {
		all[id] = struct{}{}
	}

	rows := make([]row, 0, len(all))
	for id := range all {
		j := java[id]
		g := goVerdicts[id]
		rows = append(rows, row{ID: id, Java: j, Go: g, Agree: j != "" && g != "" && j == g})
	}
	return rows
}

// normalize strips the "tck-id-" prefix our internal IDs carry so they
// match the Java TCK's hyphenated short form.
func normalize(id string) string {
	return strings.TrimPrefix(id, "tck-id-")
}

// normalizeStatus collapses the upstream TCK's verbose FAIL/EMPTY tail
// strings down to a single token for direct equality comparison.
func normalizeStatus(s string) string {
	s = strings.TrimSpace(s)
	switch {
	case strings.HasPrefix(s, "PASS"):
		return "PASS"
	case strings.HasPrefix(s, "FAIL"):
		return "FAIL"
	case strings.HasPrefix(s, "NOT_EXECUTED"), strings.HasPrefix(s, "NOT EXECUTED"):
		return "NOT_EXECUTED"
	case strings.HasPrefix(s, "EMPTY"):
		return "NOT_EXECUTED"
	}
	return s
}

type totals struct {
	agree, disagree, javaOnly, goOnly, bothEmpty int
}

func count(rows []row) totals {
	var t totals
	for _, r := range rows {
		switch {
		case r.Java == "" && r.Go == "":
			t.bothEmpty++
		case r.Java != "" && r.Go == "":
			t.javaOnly++
		case r.Java == "" && r.Go != "":
			t.goOnly++
		case r.Agree:
			t.agree++
		default:
			t.disagree++
		}
	}
	return t
}

func percent(num, denom int) float64 {
	if denom == 0 {
		return 0
	}
	return float64(num) * 100.0 / float64(denom)
}

func emitMarkdownMulti(javaTests []javaReport, gr goReport) {
	fmt.Println("# Sparkplug TCK correctness diff")
	fmt.Println()
	fmt.Println("Per-ID verdicts where both engines emitted a result for the same upstream test class. \"Agree\" counts only IDs both sides scored.")
	fmt.Println()
	fmt.Println("| Test | Both | Agree | Disagree | Agreement | Java-only | Go-only |")
	fmt.Println("| --- | --- | --- | --- | --- | --- | --- |")
	var totalAgree, totalDisagree int
	for _, jr := range javaTests {
		rows := buildRows(jr, gr)
		t := count(rows)
		totalAgree += t.agree
		totalDisagree += t.disagree
		fmt.Printf("| %s | %d | %d | %d | %.1f%% | %d | %d |\n",
			jr.Test, t.agree+t.disagree, t.agree, t.disagree,
			percent(t.agree, t.agree+t.disagree), t.javaOnly, t.goOnly)
	}
	fmt.Printf("| **all** | **%d** | **%d** | **%d** | **%.1f%%** | — | — |\n",
		totalAgree+totalDisagree, totalAgree, totalDisagree,
		percent(totalAgree, totalAgree+totalDisagree))
	fmt.Println()
	for _, jr := range javaTests {
		rows := buildRows(jr, gr)
		sort.Slice(rows, func(i, j int) bool { return rows[i].ID < rows[j].ID })
		emitTestSection(jr, rows)
	}
}

func emitTestSection(jr javaReport, rows []row) {
	fmt.Printf("## %s\n\n", jr.Test)
	fmt.Printf("Java overall: `%s`\n\n", jr.Overall)
	// Only render disagreements + Java-only — they're the actionable rows.
	var actionable []row
	for _, r := range rows {
		if r.Agree {
			continue
		}
		if r.Java == "" {
			continue // Go-only: scenario covers something Java doesn't track here, fine.
		}
		actionable = append(actionable, r)
	}
	if len(actionable) == 0 {
		fmt.Println("_No disagreements or Java-only IDs._")
		fmt.Println()
		return
	}
	fmt.Println("| ID | Java | Go | Note |")
	fmt.Println("| --- | --- | --- | --- |")
	for _, r := range actionable {
		j := r.Java
		g := r.Go
		if g == "" {
			g = "—"
		}
		note := "disagree"
		if r.Go == "" {
			note = "java-only (Go harness has no scenario)"
		}
		fmt.Printf("| %s | %s | %s | %s |\n", r.ID, j, g, note)
	}
	fmt.Println()
}

func emitJSONMulti(javaTests []javaReport, gr goReport) {
	type perTest struct {
		Test      string `json:"test"`
		Overall   string `json:"overall"`
		Agree     int    `json:"agree"`
		Disagree  int    `json:"disagree"`
		JavaOnly  int    `json:"java_only"`
		GoOnly    int    `json:"go_only"`
		Agreement string `json:"agreement_pct"`
		Rows      []row  `json:"rows"`
	}
	type combined struct {
		Tests   []perTest `json:"tests"`
		Overall struct {
			Agree     int    `json:"agree"`
			Disagree  int    `json:"disagree"`
			Agreement string `json:"agreement_pct"`
		} `json:"overall"`
	}
	var c combined
	for _, jr := range javaTests {
		rows := buildRows(jr, gr)
		sort.Slice(rows, func(i, j int) bool { return rows[i].ID < rows[j].ID })
		t := count(rows)
		c.Tests = append(c.Tests, perTest{
			Test: jr.Test, Overall: jr.Overall,
			Agree: t.agree, Disagree: t.disagree,
			JavaOnly: t.javaOnly, GoOnly: t.goOnly,
			Agreement: fmt.Sprintf("%.1f", percent(t.agree, t.agree+t.disagree)),
			Rows:      rows,
		})
		c.Overall.Agree += t.agree
		c.Overall.Disagree += t.disagree
	}
	c.Overall.Agreement = fmt.Sprintf("%.1f", percent(c.Overall.Agree, c.Overall.Agree+c.Overall.Disagree))
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(c)
}

// MarshalJSON ensures the row's `Agree` field is named for stable JSON
// output rather than the default lowercase Go-struct serialization.
func (r row) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		ID    string `json:"id"`
		Java  string `json:"java"`
		Go    string `json:"go"`
		Agree bool   `json:"agree"`
	}{r.ID, r.Java, r.Go, r.Agree})
}

func die(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "sparkplug-tck-diff: "+format+"\n", args...)
	os.Exit(1)
}
