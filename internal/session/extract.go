package session

import "github.com/joyautomation/sparkplug-tck-go/internal/spbpb"

// extractBdSeq finds the bdSeq metric on a birth/death payload, returning
// nil if absent. Both NBIRTH and NDEATH carry it as a uint64 metric named
// "bdSeq" (per spec). Defensive: also accepts long-valued metrics.
func extractBdSeq(p *spbpb.Payload) *uint64 {
	if p == nil {
		return nil
	}
	for _, m := range p.GetMetrics() {
		if m.GetName() != "bdSeq" {
			continue
		}
		v := m.GetLongValue()
		return &v
	}
	return nil
}

// aliasesFrom builds an alias->name map from a birth payload's metrics.
// Metrics without an alias are skipped.
func aliasesFrom(p *spbpb.Payload) AliasMap {
	out := AliasMap{}
	if p == nil {
		return out
	}
	for _, m := range p.GetMetrics() {
		if m.Alias == nil {
			continue
		}
		out[m.GetAlias()] = m.GetName()
	}
	return out
}
