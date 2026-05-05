// Command extract-assertions parses Sparkplug specification asciidoc chapters
// and emits a structured JSON catalog of every [tck-id-*] testable assertion.
//
// The extracted catalog is the parity contract between this TCK and the
// official Eclipse Sparkplug spec: each assertion ID maps to its normative
// requirement text. CI re-runs this against upstream master to detect drift.
//
// Input: asciidoc chapter files (paths passed as args)
// Output: assertions.json on stdout
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

type Assertion struct {
	ID         string `json:"id"`
	Text       string `json:"text"`
	Category   string `json:"category"`
	SourceFile string `json:"source_file"`
	SourceLine int    `json:"source_line"`
}

// Format in the spec asciidoc:
//   [tck-testable tck-id-FOO]#[yellow-background]*[tck-id-FOO] <text spanning
//   one or more lines>*#
//
// The opening marker, the inner ID echo, and the closing *# can all wrap
// across lines. We collapse the file to a single string and use a regex with
// dotall semantics.
var assertionRE = regexp.MustCompile(
	`(?s)\[tck-testable\s+tck-id-([a-zA-Z0-9_-]+)\]#\[yellow-background\]\*\[tck-id-[a-zA-Z0-9_-]+\]\s*(.*?)\*#`,
)

func extract(path string) ([]Assertion, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	content := string(raw)
	base := filepath.Base(path)

	// Pre-compute line offsets so we can report the source line of each match.
	var lineStarts []int
	lineStarts = append(lineStarts, 0)
	for i, r := range content {
		if r == '\n' {
			lineStarts = append(lineStarts, i+1)
		}
	}
	lineOf := func(off int) int {
		i := sort.SearchInts(lineStarts, off+1) - 1
		return i + 1
	}

	var out []Assertion
	for _, m := range assertionRE.FindAllStringSubmatchIndex(content, -1) {
		id := content[m[2]:m[3]]
		text := normalize(content[m[4]:m[5]])
		out = append(out, Assertion{
			ID:         "tck-id-" + id,
			Text:       text,
			Category:   strings.SplitN(id, "-", 2)[0],
			SourceFile: base,
			SourceLine: lineOf(m[0]),
		})
	}
	return out, nil
}

// normalize collapses asciidoc artifacts (line wraps, attribute markers,
// stray whitespace) into a single clean sentence.
func normalize(s string) string {
	// Strip asciidoc inline formatting that survives inside the assertion span.
	s = strings.ReplaceAll(s, "_", "")
	// Collapse whitespace runs (including newlines) to single spaces.
	s = regexp.MustCompile(`\s+`).ReplaceAllString(s, " ")
	return strings.TrimSpace(s)
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: extract-assertions <chapter.adoc> [chapter.adoc ...]")
		os.Exit(2)
	}

	var all []Assertion
	seen := map[string]string{}
	for _, path := range os.Args[1:] {
		got, err := extract(path)
		if err != nil {
			fmt.Fprintf(os.Stderr, "extract %s: %v\n", path, err)
			os.Exit(1)
		}
		for _, a := range got {
			if prev, ok := seen[a.ID]; ok {
				fmt.Fprintf(os.Stderr, "warn: duplicate assertion %s (also in %s)\n", a.ID, prev)
				continue
			}
			seen[a.ID] = a.SourceFile
			all = append(all, a)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(all); err != nil {
		fmt.Fprintf(os.Stderr, "encode: %v\n", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "extracted %d assertions\n", len(all))
}
