package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
)

func fixed(roots map[string][]byte, err error) collectFunc {
	return func(ctx context.Context, addrs []string) (map[string][]byte, error) {
		return roots, err
	}
}

func TestRun(t *testing.T) {
	same := map[string][]byte{"node1": []byte("a"), "node2": []byte("a"), "node3": []byte("a")}
	minority := map[string][]byte{"node1": []byte("a"), "node2": []byte("a"), "node3": []byte("b"), "node4": []byte("a")}
	two := map[string][]byte{"node1": []byte("a"), "node2": []byte("b"), "node3": []byte("a"), "node4": []byte("b")}
	cases := []struct {
		name    string
		args    []string
		collect collectFunc
		code    int
		stdout  string
		stderr  string // substring
	}{
		{"consistent", []string{"--addrs", "a:1,b:2,c:3", "--once"}, fixed(same, nil), 0, "CONSISTENT\n", ""},
		{"divergent", []string{"--addrs", "a:1,b:2,c:3,d:4", "--once"}, fixed(minority, nil), 1, "DIVERGENT node3\n", ""},
		{"divergent no majority", []string{"--addrs", "a:1,b:2,c:3,d:4", "--once"}, fixed(two, nil), 1, "DIVERGENT node1 node2 node3 node4\n", "no strict majority"},
		{"collect error", []string{"--addrs", "a:1,b:2", "--once"}, fixed(nil, errors.New("audit: node2 down")), 2, "", "node2 down"},
		{"missing addrs", []string{"--once"}, fixed(same, nil), 2, "", "--addrs"},
		{"bad addrs", []string{"--addrs", "a:1,,b:2", "--once"}, fixed(same, nil), 2, "", "--addrs"},
		{"missing once", []string{"--addrs", "a:1"}, fixed(same, nil), 2, "", "--once"},
		{"once and interval", []string{"--addrs", "a:1", "--once", "--interval", "1s"}, fixed(same, nil), 2, "", "mutually exclusive"},
		{"out needs interval", []string{"--addrs", "a:1", "--once", "--out", "x"}, fixed(same, nil), 2, "", "--interval"},
		{"negative interval", []string{"--addrs", "a:1", "--interval", "-1s"}, fixed(same, nil), 2, "", "--interval"},
		{"zero node timeout", []string{"--addrs", "a:1", "--interval", "1s", "--node-timeout", "0s"}, fixed(same, nil), 2, "", "--node-timeout"},
		{"unwritable out", []string{"--addrs", "a:1", "--interval", "1s", "--out", "/nonexistent-dir/x.audit"}, fixed(same, nil), 2, "", "--out"},
		{"unknown flag", []string{"--bogus"}, fixed(same, nil), 2, "", ""},
	}
	for _, c := range cases {
		var out, errb bytes.Buffer
		code := run(c.args, &out, &errb, c.collect)
		if code != c.code {
			t.Errorf("%s: exit %d, want %d (stderr %q)", c.name, code, c.code, errb.String())
		}
		if out.String() != c.stdout {
			t.Errorf("%s: stdout %q, want %q", c.name, out.String(), c.stdout)
		}
		if !strings.Contains(errb.String(), c.stderr) {
			t.Errorf("%s: stderr %q should contain %q", c.name, errb.String(), c.stderr)
		}
	}
}

func TestRunPassesAddrsInOrder(t *testing.T) {
	var got []string
	collect := func(ctx context.Context, addrs []string) (map[string][]byte, error) {
		got = addrs
		return map[string][]byte{"node1": []byte("a")}, nil
	}
	var out, errb bytes.Buffer
	if code := run([]string{"--addrs", "x:1, y:2,z:3", "--once"}, &out, &errb, collect); code != 0 {
		t.Fatalf("exit %d, stderr %q", code, errb.String())
	}
	if strings.Join(got, "|") != "x:1|y:2|z:3" {
		t.Errorf("collector got %v", got)
	}
}
