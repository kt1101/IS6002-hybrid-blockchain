package main

import (
	"bytes"
	"context"
	"errors"
	"io/ioutil"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"hybrid/veritas/audit"
)

type wrapped struct{ err error }

func (w wrapped) Error() string { return "wrapped: " + w.err.Error() }
func (w wrapped) Unwrap() error { return w.err }

func TestFormatTick(t *testing.T) {
	ts := time.Unix(1730000000, 123456789)
	a, b := []byte("a"), []byte("b")
	partial := func(roots map[string][]byte, failed ...string) error {
		return &audit.CollectError{Roots: roots, Failed: failed}
	}
	cases := []struct {
		name  string
		roots map[string][]byte
		err   error
		n     int
		want  string
	}{
		{"consistent", map[string][]byte{"node1": a, "node2": a, "node3": a, "node4": a}, nil, 4, "1730000000.123,"},
		{"one divergent", map[string][]byte{"node1": a, "node2": a, "node3": a, "node4": b}, nil, 4, "1730000000.123,node4"},
		{"two divergent keep one field", map[string][]byte{"node1": a, "node2": a, "node3": a, "node4": b, "node5": b, "node6": a, "node7": a}, nil, 7, "1730000000.123,node4+node5"},
		{"unreachable is not divergent", nil, partial(map[string][]byte{"node1": a, "node2": a, "node3": a}, "node4"), 4, "1730000000.123,down:node4"},
		{"divergent and unreachable", nil, partial(map[string][]byte{"node1": a, "node2": a, "node3": b}, "node4"), 4, "1730000000.123,node3+down:node4"},
		{"two unreachable", nil, partial(map[string][]byte{"node1": a, "node2": a}, "node3", "node4"), 4, "1730000000.123,down:node3+down:node4"},
		{"nobody answered", nil, partial(map[string][]byte{}, "node1", "node2"), 2, "1730000000.123,down:node1+down:node2"},
		{"other error", nil, errors.New("boom"), 3, "1730000000.123,down:node1+down:node2+down:node3"},
		{"wrapped partial", nil, wrapped{partial(map[string][]byte{"node1": a, "node2": a}, "node3")}, 3, "1730000000.123,down:node3"},
	}
	for _, c := range cases {
		got := formatTick(ts, c.roots, c.err, c.n)
		if got != c.want {
			t.Errorf("%s: %q, want %q", c.name, got, c.want)
		}
		if strings.Count(got, ",") != 1 {
			t.Errorf("%s: %q is not a 2-field CSV line", c.name, got)
		}
	}
	// The millisecond field is zero padded.
	if got := formatTick(time.Unix(5, 7000000), nil, nil, 1); got != "5.007," {
		t.Errorf("padding: %q", got)
	}
}

var tickLine = regexp.MustCompile(`^[0-9]+\.[0-9]{3},[A-Za-z0-9:+]*$`)

func TestPeriodicWritesOneLinePerTick(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.audit")
	same := map[string][]byte{"node1": []byte("a"), "node2": []byte("a")}
	var out, errb bytes.Buffer
	start := time.Now()
	code := run([]string{"--addrs", "a:1,b:2", "--interval", "20ms", "--count", "3", "--out", path},
		&out, &errb, fixed(same, nil))
	if code != 0 {
		t.Fatalf("exit %d, stderr %q", code, errb.String())
	}
	if d := time.Since(start); d < 35*time.Millisecond {
		t.Errorf("3 ticks of 20ms took only %v: the interval is not honoured", d)
	}
	if out.Len() != 0 {
		t.Errorf("stdout %q, want empty with --out", out.String())
	}
	data, err := ioutil.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
	if len(lines) != 3 {
		t.Fatalf("%d lines %q, want 3", len(lines), lines)
	}
	for _, l := range lines {
		if !tickLine.MatchString(l) || !strings.HasSuffix(l, ",") {
			t.Errorf("line %q: want unix_ts with an empty node field", l)
		}
	}
}

func TestPeriodicAppendsAndStdoutDefault(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.audit")
	if err := ioutil.WriteFile(path, []byte("1.000,\n"), 0644); err != nil {
		t.Fatal(err)
	}
	same := map[string][]byte{"node1": []byte("a")}
	var out, errb bytes.Buffer
	if code := run([]string{"--addrs", "a:1", "--interval", "5ms", "--count", "1", "--out", path}, &out, &errb, fixed(same, nil)); code != 0 {
		t.Fatalf("exit %d %q", code, errb.String())
	}
	data, _ := ioutil.ReadFile(path)
	if !strings.HasPrefix(string(data), "1.000,\n") || strings.Count(string(data), "\n") != 2 {
		t.Errorf("file %q: want the old line kept and one appended", data)
	}
	out.Reset()
	if code := run([]string{"--addrs", "a:1", "--interval", "5ms", "--count", "2"}, &out, &errb, fixed(same, nil)); code != 0 {
		t.Fatalf("exit %d %q", code, errb.String())
	}
	if strings.Count(out.String(), "\n") != 2 {
		t.Errorf("stdout %q, want 2 lines", out.String())
	}
}

// A node that does not answer must neither stop the auditor nor be written as
// divergent: the line says down:node4 and the ticking continues.
func TestPeriodicSurvivesUnreachableNode(t *testing.T) {
	a := []byte("a")
	collect := func(ctx context.Context, addrs []string) (map[string][]byte, error) {
		return nil, &audit.CollectError{
			Roots:  map[string][]byte{"node1": a, "node2": a, "node3": a},
			Failed: []string{"node4"},
		}
	}
	var out, errb bytes.Buffer
	if code := run([]string{"--addrs", "a:1,b:2,c:3,d:4", "--interval", "5ms", "--count", "3"}, &out, &errb, collect); code != 0 {
		t.Fatalf("exit %d, stderr %q", code, errb.String())
	}
	lines := strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n")
	if len(lines) != 3 {
		t.Fatalf("%d lines %q, want 3", len(lines), lines)
	}
	for _, l := range lines {
		if !strings.HasSuffix(l, ",down:node4") || !tickLine.MatchString(l) {
			t.Errorf("line %q: want ...,down:node4", l)
		}
	}
}

// SIGTERM ends the periodic mode with exit 0 and complete lines only.
func TestPeriodicStopsOnSIGTERM(t *testing.T) {
	var once sync.Once
	collect := func(ctx context.Context, addrs []string) (map[string][]byte, error) {
		once.Do(func() { syscall.Kill(syscall.Getpid(), syscall.SIGTERM) })
		return map[string][]byte{"node1": []byte("a")}, nil
	}
	var out, errb bytes.Buffer
	done := make(chan int, 1)
	go func() {
		done <- run([]string{"--addrs", "a:1", "--interval", "10ms"}, &out, &errb, collect)
	}()
	select {
	case code := <-done:
		if code != 0 {
			t.Errorf("exit %d, stderr %q", code, errb.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run did not return after SIGTERM")
	}
	for _, l := range strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n") {
		if l != "" && !tickLine.MatchString(l) {
			t.Errorf("incomplete line %q", l)
		}
	}
}
