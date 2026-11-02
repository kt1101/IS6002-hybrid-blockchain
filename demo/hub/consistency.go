package hub

import "sort"

// Consistency is the hub's verdict on whether the up replicas agree on the
// ledger root.
type Consistency struct {
	Verdict   string   `json:"verdict"` // CONSISTENT | DIVERGENT | UNKNOWN
	Height    int64    `json:"height"`  // height the roots were compared at; 0 when UNKNOWN
	Divergent []string `json:"divergent"`
	Down      []string `json:"down"`
	Agree     int      `json:"agree"` // up nodes in the majority group (0 when there is none)
	Up        int      `json:"up"`
}

// Judge compares the up nodes' ledger roots. roots maps node → height → hex
// root. Replicas apply a block a few ms apart, so roots are only compared at
// the highest height every up node has; with no such height the verdict is
// UNKNOWN. A strictly largest group of equal roots is the majority and every
// other up node is divergent; with no strict majority (a tie) every up node is.
// Nodes in roots that are not in up are reported as Down, never as divergent.
func Judge(roots map[string]map[int64]string, up []string) Consistency {
	c := Consistency{Verdict: "UNKNOWN", Divergent: []string{}, Down: []string{}, Up: len(up)}
	isUp := make(map[string]bool, len(up))
	for _, n := range up {
		isUp[n] = true
	}
	for n := range roots {
		if !isUp[n] {
			c.Down = append(c.Down, n)
		}
	}
	sort.Strings(c.Down)

	h, ok := commonHeight(roots, up)
	if !ok {
		return c
	}
	c.Height = h

	groups := map[string][]string{} // root → nodes
	for _, n := range up {
		r := roots[n][h]
		groups[r] = append(groups[r], n)
	}
	if len(groups) == 1 {
		c.Verdict = "CONSISTENT"
		c.Agree = len(up)
		return c
	}

	c.Verdict = "DIVERGENT"
	var best []string
	tie := false
	for _, g := range groups {
		switch {
		case len(g) > len(best):
			best, tie = g, false
		case len(g) == len(best):
			tie = true
		}
	}
	if !tie {
		c.Agree = len(best)
	}
	inMajority := make(map[string]bool, len(best))
	if !tie {
		for _, n := range best {
			inMajority[n] = true
		}
	}
	for _, n := range up {
		if !inMajority[n] {
			c.Divergent = append(c.Divergent, n)
		}
	}
	sort.Strings(c.Divergent)
	return c
}

// commonHeight is the highest height at which every node in up has a root.
func commonHeight(roots map[string]map[int64]string, up []string) (int64, bool) {
	if len(up) == 0 {
		return 0, false
	}
	var best int64
	found := false
	for h := range roots[up[0]] {
		everyone := true
		for _, n := range up[1:] {
			if _, ok := roots[n][h]; !ok {
				everyone = false
				break
			}
		}
		if everyone && (!found || h > best) {
			best, found = h, true
		}
	}
	return best, found
}

// groupLetters names the groups of equal roots at height h: "A" is the largest
// group, ties broken by lexicographic root. Up nodes without a root at h get
// no entry.
func groupLetters(roots map[string]map[int64]string, up []string, h int64) map[string]string {
	size := map[string]int{}
	for _, n := range up {
		if r, ok := roots[n][h]; ok {
			size[r]++
		}
	}
	order := make([]string, 0, len(size))
	for r := range size {
		order = append(order, r)
	}
	sort.Slice(order, func(i, j int) bool {
		if size[order[i]] != size[order[j]] {
			return size[order[i]] > size[order[j]]
		}
		return order[i] < order[j]
	})
	letter := make(map[string]string, len(order))
	for i, r := range order {
		letter[r] = string(rune('A' + i))
	}
	out := map[string]string{}
	for _, n := range up {
		if r, ok := roots[n][h]; ok {
			out[n] = letter[r]
		}
	}
	return out
}
