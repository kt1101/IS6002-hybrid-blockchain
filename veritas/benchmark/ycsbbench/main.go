package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"gopkg.in/alecthomas/kingpin.v2"

	"hybrid/veritas/benchmark"
	"hybrid/veritas/driver"
)

var (
	dataLoad          = kingpin.Flag("load-path", "Path of YCSB initial data").Required().String()
	dataRun           = kingpin.Flag("run-path", "Path of YCSB operation data").Required().String()
	driverNum         = kingpin.Flag("ndrivers", "Number of drivers for sending requests").Default("4").Int()
	driverConcurrency = kingpin.Flag("nthreads", "Number of threads for each driver").Default("10").Int()
	veritasAddrs      = kingpin.Flag("veritas-addrs", "Address of veritas nodes").Required().String()
	tsoAddr           = kingpin.Flag("tso-addr", "TSO server address (empty: no TSO, writes use version 0)").Default("").String()
	warmup            = kingpin.Flag("warmup", "Warmup period excluded from the results").Default("10s").Duration()
	duration          = kingpin.Flag("duration", "Measurement period").Default("60s").Duration()
	outPath           = kingpin.Flag("out", "CSV file to append one result row to").Default("").String()
	tagsFlag          = kingpin.Flag("tags", "Extra CSV columns as k=v,k=v").Default("").String()
	timelinePath      = kingpin.Flag("timeline", "File to write per-second successful request counts to").Default("").String()
	wtimelinePath     = kingpin.Flag("wtimeline", "File to write per-second successful Set counts to (same t=0 as --timeline)").Default("").String()
	opsPath           = kingpin.Flag("ops", "File to write the per-op-kind (read, write) summary CSV to: count, tps, lat_mean_ms, lat_p50_ms, lat_p99_ms, errors (not aborted), aborted (gRPC Aborted)").Default("").String()
	errBackoffMin     = kingpin.Flag("error-backoff", "Pause of a run-phase worker after a failed request, doubled on each further failure up to --error-backoff-max, reset by a success (0: no pause)").Default("50ms").Duration()
	errBackoffMax     = kingpin.Flag("error-backoff-max", "Longest pause of a run-phase worker after consecutive failed requests").Default("1s").Duration()
	mode              = kingpin.Flag("mode", "Backend mode: kafka (uses TSO if given) or tm (never uses TSO)").Default("kafka").Enum("kafka", "tm")
	loadThreads       = kingpin.Flag("load-threads", "Concurrent Sets during the data-load phase (all through the first driver)").Default("10").Int()
	loadTimeout       = kingpin.Flag("load-timeout", "Deadline of the data-load phase; exceeding it or any failed load Set aborts the run").Default("20m").Duration()
	loadOpTimeout     = kingpin.Flag("load-op-timeout", "Deadline of one load Set attempt (bounded by --load-timeout)").Default("30s").Duration()
	loadAttempts      = kingpin.Flag("load-attempts", "Attempts per record in the data-load phase; a record failing all of them is a failed load Set").Default("3").Int()
	rate              = kingpin.Flag("rate", "Open-loop load: total requests per second issued at fixed intervals regardless of completions, latency measured from the scheduled time (0: closed loop, every worker waits for its reply)").Default("0").Float64()
	maxInflight       = kingpin.Flag("max-inflight", "Open-loop load: at most this many outstanding requests; an operation due while the cap is reached is dropped and counted, not queued").Default("4096").Int()
	settleTimeout     = kingpin.Flag("settle-timeout", "Deadline for every node to serve Get for a sample of loaded keys before the run phase").Default("10m").Duration()
)

func encodeVal(val string) string {
	runes := []rune(val)
	for i := 0; i < len(runes); i++ {
		if (runes[i] >= 'a' && runes[i] <= 'z') ||
			(runes[i] >= 'A' && runes[i] <= 'Z') ||
			(runes[i] >= '0' && runes[i] <= '9') {
			continue
		}
		runes[i] = '0'
	}
	return string(runes)
}

func parseTags(s string) (map[string]string, error) {
	tags := map[string]string{}
	if s == "" {
		return tags, nil
	}
	for _, kv := range strings.Split(s, ",") {
		parts := strings.SplitN(kv, "=", 2)
		if len(parts) != 2 || parts[0] == "" {
			return nil, fmt.Errorf("invalid tag %q, want k=v", kv)
		}
		tags[parts[0]] = parts[1]
	}
	return tags, nil
}

func writeTimeline(path string, tl []int64) error {
	var sb strings.Builder
	for _, n := range tl {
		sb.WriteString(strconv.FormatInt(n, 10))
		sb.WriteByte('\n')
	}
	return os.WriteFile(path, []byte(sb.String()), 0o644)
}

func main() {
	kingpin.Parse()

	// Validate output options up front so a long run is not wasted.
	tags, err := parseTags(*tagsFlag)
	if err != nil {
		kingpin.Fatalf("%v", err)
	}
	if *loadThreads < 1 {
		kingpin.Fatalf("--load-threads must be >= 1")
	}
	if *loadAttempts < 1 {
		kingpin.Fatalf("--load-attempts must be >= 1")
	}
	if *loadOpTimeout <= 0 {
		kingpin.Fatalf("--load-op-timeout must be > 0")
	}
	if *errBackoffMin < 0 || *errBackoffMax < *errBackoffMin {
		kingpin.Fatalf("want 0 <= --error-backoff <= --error-backoff-max, got %v and %v", *errBackoffMin, *errBackoffMax)
	}
	if *rate < 0 {
		kingpin.Fatalf("--rate must be >= 0")
	}
	if *maxInflight < 1 {
		kingpin.Fatalf("--max-inflight must be >= 1")
	}
	if *mode == "tm" {
		*tsoAddr = ""
	}

	addrs := strings.Split(*veritasAddrs, ",")
	clis := make([]*driver.Driver, 0)
	defer func() {
		for _, cli := range clis {
			cli.Close()
		}
	}()
	for i := 0; i < *driverNum; i++ {
		cli, err := driver.Open(addrs[i%len(addrs)], *tsoAddr, benchmark.GenRandString(16))
		if err != nil {
			panic(err)
		}
		clis = append(clis, cli)
	}

	// Data-load phase.
	loadFile, err := os.Open(*dataLoad)
	if err != nil {
		panic(err)
	}
	defer loadFile.Close()
	loadStart := time.Now()
	loadCtx, loadCancel := context.WithTimeout(context.Background(), *loadTimeout)
	defer loadCancel()
	loadBuf := make(chan [2]string, 10)
	var loadedKeys []string // in load-file order; read by the settle phase
	var (
		loadMu       sync.Mutex
		loadErrs     int64
		loadRetries  int64
		loadFirstErr error
	)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(loadBuf)
		err := benchmark.LineByLine(loadFile, func(line string) error {
			operands := strings.SplitN(line, " ", 4)
			select {
			case loadBuf <- [2]string{operands[2], encodeVal(operands[3])}:
				loadedKeys = append(loadedKeys, operands[2])
				return nil
			case <-loadCtx.Done():
				return loadCtx.Err()
			}
		})
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			panic(err)
		}
	}()
	for i := 0; i < *loadThreads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for kv := range loadBuf {
				// Rewriting the same key/value is idempotent, so a Set that
				// timed out or failed (e.g. RPC EOF under backlog) is retried.
				retries, err := benchmark.RetryWithTimeout(loadCtx, *loadAttempts, *loadOpTimeout, loadRetryWait, func(ctx context.Context) error {
					return clis[0].Set(ctx, kv[0], kv[1])
				})
				loadMu.Lock()
				loadRetries += int64(retries)
				if err != nil {
					loadErrs++
					if loadFirstErr == nil {
						loadFirstErr = fmt.Errorf("set %s: %w", kv[0], err)
					}
				}
				loadMu.Unlock()
			}
		}()
	}
	if err := benchmark.WaitWithGrace(loadCtx, wg, phaseGrace); err != nil {
		fatalf("load phase did not end within --load-timeout=%v: %v", *loadTimeout, err)
	}
	if loadCtx.Err() != nil {
		fatalf("load phase did not finish within --load-timeout=%v (%d records sent, %d Sets failed, %d retries, first error: %v)",
			*loadTimeout, len(loadedKeys), loadErrs, loadRetries, loadFirstErr)
	}
	if loadErrs > 0 {
		fatalf("load phase: %d of %d Sets failed after %d attempts each (%d retries in total), first error: %v",
			loadErrs, len(loadedKeys), *loadAttempts, loadRetries, loadFirstErr)
	}
	fmt.Printf("ycsbbench: loaded %d records in %v\n", len(loadedKeys), time.Since(loadStart).Round(time.Millisecond))
	fmt.Printf("ycsbbench: load retries: %d (--load-attempts=%d, --load-op-timeout=%v)\n", loadRetries, *loadAttempts, *loadOpTimeout)

	// Settle phase: asynchronous backends apply loaded Sets later; wait until
	// every node serves a sample of the loaded keys before measuring.
	settleStart := time.Now()
	sample := sampleKeys(loadedKeys, settleSampleSize)
	if err := settle(addrs, sample, *settleTimeout); err != nil {
		fatalf("settle phase: %v", err)
	}
	fmt.Printf("ycsbbench: settled %d sample keys on %d nodes in %v\n",
		len(sample), len(uniqueStrings(addrs)), time.Since(settleStart).Round(time.Millisecond))
	// experiments/run_one.sh starts its docker-stats sampling on this line.
	fmt.Printf("ycsbbench: run phase started (warmup %v, duration %v)\n", *warmup, *duration)

	// Time-based run phase: the run file is replayed in a loop until
	// warmup+duration has elapsed.
	start := time.Now()
	rec := benchmark.NewRecorder(*warmup, *duration, start)
	runCtx, cancel := context.WithDeadline(context.Background(), start.Add(*warmup+*duration))
	defer cancel()

	runBuf := make(chan *benchmark.Request, 20*(*driverNum)*(*driverConcurrency))
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(runBuf)
		for runCtx.Err() == nil {
			n, err := replayRunFile(runCtx, *dataRun, runBuf)
			if err != nil {
				panic(err)
			}
			if n == 0 && runCtx.Err() == nil {
				panic(errors.New("run file has no operations"))
			}
		}
	}()

	if *rate > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runOpenLoop(runCtx, rec, clis, runBuf)
		}()
	}
	for i := 0; *rate == 0 && i < *driverNum; i++ {
		for j := 0; j < *driverConcurrency; j++ {
			wg.Add(1)
			go func(seq int) {
				defer wg.Done()
				// A worker whose node is stopped or unreachable (F1, F2)
				// would otherwise fail fast in a loop; it pauses instead.
				bo := benchmark.Backoff{Min: *errBackoffMin, Max: *errBackoffMax}
				for {
					select {
					case <-runCtx.Done():
						return
					case op, ok := <-runBuf:
						if !ok {
							return
						}
						t0 := time.Now()
						var err error
						switch op.ReqType {
						case benchmark.GetOp:
							var val string
							val, err = clis[seq].Get(runCtx, op.Key)
							rec.ObserveGet(time.Now(), time.Since(t0), val, err)
						case benchmark.SetOp:
							err = clis[seq].Set(runCtx, op.Key, op.Val)
							rec.ObserveSet(time.Now(), time.Since(t0), err)
						default:
							panic(fmt.Sprintf("invalid operation: %v", op.ReqType))
						}
						if err == nil {
							bo.Success()
						} else if *errBackoffMin > 0 {
							benchmark.SleepCtx(runCtx, bo.Failure())
						}
					}
				}
			}(i)
		}
	}
	if err := benchmark.WaitWithGrace(runCtx, wg, phaseGrace); err != nil {
		fatalf("run phase did not end at warmup+duration=%v: %v", *warmup+*duration, err)
	}

	s := rec.Summary()
	fmt.Printf("Throughput of %v drivers with %v concurrency: %.2f req/s\n",
		*driverNum, *driverConcurrency, s.TPS)
	fmt.Printf("Latency mean/p50/p99: %.3f / %.3f / %.3f ms; errors: %d; wrong gets: %d\n",
		s.LatMeanMs, s.LatP50Ms, s.LatP99Ms, s.Errors, s.WrongGets)
	if *rate > 0 {
		fmt.Printf("Open loop: offered %.2f req/s, max in flight %d, dropped %d\n", *rate, *maxInflight, s.Dropped)
	}
	for _, o := range rec.OpSummaries() {
		fmt.Printf("  %-5s %d ok (%.2f req/s), latency mean/p50/p99: %.3f / %.3f / %.3f ms; errors: %d; aborted: %d\n",
			o.Op, o.Count, o.TPS, o.LatMeanMs, o.LatP50Ms, o.LatP99Ms, o.Errors, o.Aborted)
	}
	if kinds := rec.ErrorKinds(); len(kinds) > 0 {
		fmt.Printf("Errors by message (measurement window, at most %d distinct):\n", benchmark.MaxErrorKinds)
		for _, k := range kinds {
			fmt.Printf("  %8d  %s\n", k.Count, k.Msg)
		}
	}

	if *outPath != "" {
		if err := benchmark.AppendCSVRow(*outPath, tags, s); err != nil {
			panic(err)
		}
	}
	if *timelinePath != "" {
		if err := writeTimeline(*timelinePath, rec.Timeline()); err != nil {
			panic(err)
		}
	}
	if *wtimelinePath != "" {
		if err := writeTimeline(*wtimelinePath, rec.SetTimeline()); err != nil {
			panic(err)
		}
	}
	if *opsPath != "" {
		if err := benchmark.WriteOpsCSV(*opsPath, rec.OpSummaries(), s.Dropped); err != nil {
			panic(err)
		}
	}
}

// runOpenLoop issues the operations of runBuf at --rate (benchmark.OpenLoop);
// operation i goes to driver i mod len(clis). Latency is measured from the
// scheduled time; dropped operations are recorded with ObserveDropped.
func runOpenLoop(ctx context.Context, rec *benchmark.Recorder, clis []*driver.Driver, runBuf <-chan *benchmark.Request) {
	var idx uint64
	var mu sync.Mutex
	next := func() *benchmark.Request {
		select {
		case op, ok := <-runBuf:
			if !ok {
				return nil
			}
			return op
		case <-ctx.Done():
			return nil
		}
	}
	do := func(ctx context.Context, op *benchmark.Request, scheduled time.Time) {
		mu.Lock()
		cli := clis[idx%uint64(len(clis))]
		idx++
		mu.Unlock()
		switch op.ReqType {
		case benchmark.GetOp:
			val, err := cli.Get(ctx, op.Key)
			rec.ObserveGet(time.Now(), time.Since(scheduled), val, err)
		case benchmark.SetOp:
			err := cli.Set(ctx, op.Key, op.Val)
			rec.ObserveSet(time.Now(), time.Since(scheduled), err)
		default:
			panic(fmt.Sprintf("invalid operation: %v", op.ReqType))
		}
	}
	_, dropped := benchmark.OpenLoop{Rate: *rate, MaxInflight: *maxInflight}.Run(ctx, next, do)
	for i := int64(0); i < dropped; i++ {
		rec.ObserveDropped()
	}
}

// replayRunFile sends every operation of the run file once to out, stopping
// early when ctx is done. It returns the number of operations sent.
func replayRunFile(ctx context.Context, path string, out chan<- *benchmark.Request) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	n := 0
	err = benchmark.LineByLine(f, func(line string) error {
		operands := strings.SplitN(line, " ", 4)
		r := &benchmark.Request{
			Key: operands[2],
		}
		if operands[0] == "READ" {
			r.ReqType = benchmark.GetOp
		} else {
			r.ReqType = benchmark.SetOp
			r.Val = encodeVal(operands[3])
		}
		select {
		case out <- r:
			n++
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		err = nil
	}
	return n, err
}

// settleSampleSize is the number of loaded keys checked on every node.
const settleSampleSize = 200

// loadRetryWait is the pause between attempts of a load Set: veritastm builds
// its tx bytes from the current second, so a retry within the same second
// would be a duplicate tx.
const loadRetryWait = time.Second

// phaseGrace is how long the load and run phases may overrun their deadline
// (workers stuck in a call that ignores its context) before ycsbbench exits.
const phaseGrace = 30 * time.Second

func fatalf(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "ycsbbench: "+format+"\n", args...)
	os.Exit(1)
}

// sampleKeys picks up to n keys spread evenly over keys (deterministic).
func sampleKeys(keys []string, n int) []string {
	if len(keys) <= n {
		return append([]string(nil), keys...)
	}
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, keys[i*len(keys)/n])
	}
	return out
}

func uniqueStrings(in []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(in))
	for _, s := range in {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}

// settle waits until every node in addrs answers Get without error for every
// key in keys, using one driver per node. It fails after timeout.
func settle(addrs []string, keys []string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	nodes := uniqueStrings(addrs)
	errs := make([]error, len(nodes))
	var wg sync.WaitGroup
	for i, addr := range nodes {
		wg.Add(1)
		go func(i int, addr string) {
			defer wg.Done()
			errs[i] = settleNode(ctx, addr, keys)
		}(i, addr)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func settleNode(ctx context.Context, addr string, keys []string) error {
	// Get needs no TSO.
	cli, err := driver.Open(addr, "", benchmark.GenRandString(16))
	if err != nil {
		return fmt.Errorf("node %s: %w", addr, err)
	}
	defer cli.Close()
	for n, key := range keys {
		for {
			gctx, gcancel := context.WithTimeout(ctx, 5*time.Second)
			_, err := cli.Get(gctx, key)
			gcancel()
			if err == nil {
				break
			}
			select {
			case <-ctx.Done():
				return fmt.Errorf("node %s: key %s (%d of %d sample keys visible) not readable before the deadline: %v",
					addr, key, n, len(keys), err)
			case <-time.After(200 * time.Millisecond):
			}
		}
	}
	return nil
}
