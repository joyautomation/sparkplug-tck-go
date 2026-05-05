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

	jr, err := loadJava(*javaPath)
	if err != nil {
		die("load java: %v", err)
	}
	gr, err := loadGo(*goPath)
	if err != nil {
		die("load go: %v", err)
	}

	rows := buildRows(jr, gr)
	sort.Slice(rows, func(i, j int) bool { return rows[i].ID < rows[j].ID })

	totals := count(rows)

	if *jsonOut {
		emitJSON(rows, totals, jr.Test, jr.Overall)
	} else {
		emitMarkdown(rows, totals, jr.Test, jr.Overall)
	}

	fmt.Fprintf(os.Stderr,
		"%s — agree:%d disagree:%d (java only:%d, go only:%d, both empty:%d) — agreement %.1f%%\n",
		jr.Test, totals.agree, totals.disagree, totals.javaOnly, totals.goOnly, totals.bothEmpty,
		percent(totals.agree, totals.agree+totals.disagree))
}

func loadJava(path string) (javaReport, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return javaReport{}, err
	}
	var jr javaReport
	if err := json.Unmarshal(raw, &jr); err != nil {
		return javaReport{}, err
	}
	return jr, nil
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

func emitMarkdown(rows []row, t totals, test, overall string) {
	fmt.Printf("# Sparkplug TCK correctness diff — %s\n\n", test)
	fmt.Printf("Java overall: `%s`\n\n", overall)
	fmt.Printf("Both engines emit a verdict on %d IDs; %d agree, %d disagree (%.1f%%).\n",
		t.agree+t.disagree, t.agree, t.disagree, percent(t.agree, t.agree+t.disagree))
	fmt.Printf("Java-only IDs (Go has no scenario): %d. Go-only IDs (Java didn't track in this test): %d.\n\n",
		t.javaOnly, t.goOnly)
	fmt.Println("| ID | Java | Go | Agree |")
	fmt.Println("| --- | --- | --- | --- |")
	for _, r := range rows {
		mark := "✗"
		if r.Agree {
			mark = "✓"
		} else if r.Java == "" || r.Go == "" {
			mark = "·"
		}
		j := r.Java
		if j == "" {
			j = "—"
		}
		g := r.Go
		if g == "" {
			g = "—"
		}
		fmt.Printf("| %s | %s | %s | %s |\n", r.ID, j, g, mark)
	}
}

func emitJSON(rows []row, t totals, test, overall string) {
	type out struct {
		Test       string `json:"test"`
		Overall    string `json:"overall"`
		Agree      int    `json:"agree"`
		Disagree   int    `json:"disagree"`
		JavaOnly   int    `json:"java_only"`
		GoOnly     int    `json:"go_only"`
		Agreement  string `json:"agreement_pct"`
		Rows       []row  `json:"rows"`
	}
	o := out{
		Test: test, Overall: overall,
		Agree: t.agree, Disagree: t.disagree,
		JavaOnly: t.javaOnly, GoOnly: t.goOnly,
		Agreement: fmt.Sprintf("%.1f", percent(t.agree, t.agree+t.disagree)),
		Rows:      rows,
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(o)
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
