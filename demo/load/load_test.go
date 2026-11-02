package load

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"hybrid/demo/cluster"
)

func TestConfigValidate(t *testing.T) {
	ok := Config{Clients: 8, Workload: "a", Duration: 10 * time.Second}
	cases := []struct {
		name string
		mod  func(*Config)
		bad  bool
	}{
		{"valid", func(*Config) {}, false},
		{"min bounds", func(c *Config) { c.Clients = 1; c.Duration = 5 * time.Second }, false},
		{"max bounds", func(c *Config) { c.Clients = 256; c.Duration = 300 * time.Second }, false},
		{"zero clients", func(c *Config) { c.Clients = 0 }, true},
		{"too many clients", func(c *Config) { c.Clients = 257 }, true},
		{"workload b", func(c *Config) { c.Workload = "b" }, false},
		{"workload c", func(c *Config) { c.Workload = "c" }, false},
		{"workload d", func(c *Config) { c.Workload = "d" }, true},
		{"empty workload", func(c *Config) { c.Workload = "" }, true},
		{"short duration", func(c *Config) { c.Duration = 4 * time.Second }, true},
		{"long duration", func(c *Config) { c.Duration = 301 * time.Second }, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := ok
			tc.mod(&c)
			if err := c.Validate(); (err != nil) != tc.bad {
				t.Fatalf("Validate() = %v, want error=%v", err, tc.bad)
			}
		})
	}
}

func newSim(t *testing.T) *cluster.Sim {
	t.Helper()
	c, err := cluster.NewSim(cluster.SimConfig{N: 4})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

type counter struct{ ops, errs int64 }

func (c *counter) rec(_ time.Duration, err error) {
	atomic.AddInt64(&c.ops, 1)
	if err != nil {
		atomic.AddInt64(&c.errs, 1)
	}
}
func (c *counter) Ops() int64  { return atomic.LoadInt64(&c.ops) }
func (c *counter) Errs() int64 { return atomic.LoadInt64(&c.errs) }

func TestLoadRunsAndStops(t *testing.T) {
	var cnt counter
	r := NewRunner(newSim(t), cnt.rec)
	var calls, endCalls int32
	err := r.Start(Config{Clients: 8, Workload: "a", Duration: 5 * time.Second, Keys: 20}, func(running bool, rem time.Duration) {
		atomic.AddInt32(&calls, 1)
		if !running {
			atomic.AddInt32(&endCalls, 1)
			if rem != 0 {
				t.Errorf("remaining on end = %v, want 0", rem)
			}
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if !r.Running() {
		t.Fatal("Running() false after Start")
	}
	time.Sleep(time.Second)
	r.Stop()
	if r.Running() {
		t.Fatal("Running() true after Stop")
	}
	if cnt.Ops() == 0 {
		t.Fatal("no ops recorded")
	}
	if e := cnt.Errs(); e != 0 {
		t.Fatalf("%d errors on a healthy cluster", e)
	}
	if n := atomic.LoadInt32(&endCalls); n != 1 {
		t.Fatalf("onState(false) called %d times, want 1", n)
	}
	if atomic.LoadInt32(&calls) < 2 {
		t.Fatal("expected a running state callback before the end")
	}
	r.Stop() // idempotent
}

func TestLoadCountsErrorsAndContinues(t *testing.T) {
	sim := newSim(t)
	var cnt counter
	r := NewRunner(sim, cnt.rec)
	if err := r.Start(Config{Clients: 8, Workload: "a", Duration: 5 * time.Second, Keys: 20}, nil); err != nil {
		t.Fatal(err)
	}
	defer r.Stop()
	deadline := time.Now().Add(5 * time.Second) // preload runs first
	for cnt.Ops() == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if cnt.Ops() == 0 {
		t.Fatal("no ops before crash")
	}
	if err := sim.Inject(context.Background(), "node2", cluster.FaultCrash); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	before := cnt.Ops()
	time.Sleep(900 * time.Millisecond)
	if cnt.Errs() == 0 {
		t.Fatal("no errors counted after crashing node2")
	}
	if cnt.Ops() <= before {
		t.Fatal("ops stopped being recorded after the crash")
	}
}

func TestStartWhileRunning(t *testing.T) {
	r := NewRunner(newSim(t), func(time.Duration, error) {})
	cfg := Config{Clients: 2, Workload: "c", Duration: 5 * time.Second, Keys: 10}
	if err := r.Start(cfg, nil); err != nil {
		t.Fatal(err)
	}
	defer r.Stop()
	if err := r.Start(cfg, nil); !errors.Is(err, ErrRunning) {
		t.Fatalf("second Start = %v, want ErrRunning", err)
	}
}

func TestStartRejectsInvalidConfig(t *testing.T) {
	r := NewRunner(newSim(t), func(time.Duration, error) {})
	if err := r.Start(Config{Clients: 0, Workload: "a", Duration: 5 * time.Second}, nil); err == nil {
		t.Fatal("expected validation error")
	}
	if r.Running() {
		t.Fatal("Running() true after rejected Start")
	}
}

func TestPreloadOncePerGeneration(t *testing.T) {
	sim := newSim(t)
	r := NewRunner(sim, func(time.Duration, error) {})
	cfg := Config{Clients: 1, Workload: "c", Duration: 5 * time.Second, Keys: 5}
	if err := r.Start(cfg, nil); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	r.Stop()
	for i := 0; i < cfg.Keys; i++ {
		if _, ok, err := sim.Get(context.Background(), "node1", keyName(i)); err != nil || !ok {
			t.Fatalf("key %d not preloaded (ok=%v err=%v)", i, ok, err)
		}
	}
	if !r.preloaded {
		t.Fatal("preloaded flag not set")
	}
	r.ResetPreload()
	if r.preloaded {
		t.Fatal("ResetPreload did not clear the flag")
	}
}
