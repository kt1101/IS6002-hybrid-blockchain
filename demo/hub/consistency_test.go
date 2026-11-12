package hub

import (
	"reflect"
	"testing"
)

func TestJudge(t *testing.T) {
	same := func(r string, hs ...int64) map[int64]string {
		m := map[int64]string{}
		for _, h := range hs {
			m[h] = r
		}
		return m
	}
	tests := []struct {
		name  string
		roots map[string]map[int64]string
		up    []string
		want  Consistency
	}{
		{
			name: "all equal",
			roots: map[string]map[int64]string{
				"node1": same("X", 5), "node2": same("X", 5), "node3": same("X", 5), "node4": same("X", 5),
			},
			up:   []string{"node1", "node2", "node3", "node4"},
			want: Consistency{Verdict: "CONSISTENT", Height: 5, Divergent: []string{}, Down: []string{}, Agree: 4, Up: 4},
		},
		{
			name: "one differs",
			roots: map[string]map[int64]string{
				"node1": same("X", 5), "node2": same("X", 5), "node3": same("Y", 5), "node4": same("X", 5),
			},
			up:   []string{"node1", "node2", "node3", "node4"},
			want: Consistency{Verdict: "DIVERGENT", Height: 5, Divergent: []string{"node3"}, Down: []string{}, Agree: 3, Up: 4},
		},
		{
			name: "two against two has no majority",
			roots: map[string]map[int64]string{
				"node1": same("X", 5), "node2": same("X", 5), "node3": same("Y", 5), "node4": same("Y", 5),
			},
			up:   []string{"node1", "node2", "node3", "node4"},
			want: Consistency{Verdict: "DIVERGENT", Height: 5, Divergent: []string{"node1", "node2", "node3", "node4"}, Down: []string{}, Agree: 0, Up: 4},
		},
		{
			name: "four nodes split 2-1-1 has no majority",
			roots: map[string]map[int64]string{
				"node1": same("X", 5), "node2": same("X", 5), "node3": same("Y", 5), "node4": same("Z", 5),
			},
			up:   []string{"node1", "node2", "node3", "node4"},
			want: Consistency{Verdict: "DIVERGENT", Height: 5, Divergent: []string{"node1", "node2", "node3", "node4"}, Down: []string{}, Agree: 0, Up: 4},
		},
		{
			name: "five nodes split 2-2-1 has no majority",
			roots: map[string]map[int64]string{
				"node1": same("X", 5), "node2": same("X", 5), "node3": same("Y", 5), "node4": same("Y", 5), "node5": same("Z", 5),
			},
			up:   []string{"node1", "node2", "node3", "node4", "node5"},
			want: Consistency{Verdict: "DIVERGENT", Height: 5, Divergent: []string{"node1", "node2", "node3", "node4", "node5"}, Down: []string{}, Agree: 0, Up: 5},
		},
		{
			name: "five nodes split 3-1-1 has a majority",
			roots: map[string]map[int64]string{
				"node1": same("X", 5), "node2": same("X", 5), "node3": same("Y", 5), "node4": same("X", 5), "node5": same("Z", 5),
			},
			up:   []string{"node1", "node2", "node3", "node4", "node5"},
			want: Consistency{Verdict: "DIVERGENT", Height: 5, Divergent: []string{"node3", "node5"}, Down: []string{}, Agree: 3, Up: 5},
		},
		{
			name: "down node is never divergent",
			roots: map[string]map[int64]string{
				"node1": same("X", 5), "node2": same("X", 5), "node3": same("X", 5), "node4": same("Z", 2),
			},
			up:   []string{"node1", "node2", "node3"},
			want: Consistency{Verdict: "CONSISTENT", Height: 5, Divergent: []string{}, Down: []string{"node4"}, Agree: 3, Up: 3},
		},
		{
			name: "down node with divergent stale root stays in Down only",
			roots: map[string]map[int64]string{
				"node1": same("X", 5), "node2": same("X", 5), "node3": same("Y", 5), "node4": same("X", 5),
			},
			up:   []string{"node1", "node2", "node4"},
			want: Consistency{Verdict: "CONSISTENT", Height: 5, Divergent: []string{}, Down: []string{"node3"}, Agree: 3, Up: 3},
		},
		{
			name: "no common height",
			roots: map[string]map[int64]string{
				"node1": same("X", 10), "node2": same("X", 9),
			},
			up:   []string{"node1", "node2"},
			want: Consistency{Verdict: "UNKNOWN", Divergent: []string{}, Down: []string{}, Up: 2},
		},
		{
			name:  "no up nodes",
			roots: map[string]map[int64]string{"node1": same("X", 3)},
			up:    nil,
			want:  Consistency{Verdict: "UNKNOWN", Divergent: []string{}, Down: []string{"node1"}, Up: 0},
		},
		{
			name:  "up node without roots",
			roots: map[string]map[int64]string{"node1": same("X", 3)},
			up:    []string{"node1", "node2"},
			want:  Consistency{Verdict: "UNKNOWN", Divergent: []string{}, Down: []string{}, Up: 2},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := Judge(tc.roots, tc.up)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("Judge = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestConsistencyComparesCommonHeight(t *testing.T) {
	roots := map[string]map[int64]string{
		"node1": {9: "A", 10: "X"}, // node1 is one block ahead
		"node2": {9: "A"},
	}
	got := Judge(roots, []string{"node1", "node2"})
	if got.Verdict != "CONSISTENT" || got.Height != 9 {
		t.Fatalf("Judge = %+v, want CONSISTENT at height 9", got)
	}
}

func TestGroupLetters(t *testing.T) {
	roots := map[string]map[int64]string{
		"node1": {4: "bb"}, "node2": {4: "aa"}, "node3": {4: "bb"}, "node4": {4: "cc"},
	}
	got := groupLetters(roots, []string{"node1", "node2", "node3", "node4"}, 4)
	want := map[string]string{"node1": "A", "node3": "A", "node2": "B", "node4": "C"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("groupLetters = %v, want %v", got, want)
	}
}
