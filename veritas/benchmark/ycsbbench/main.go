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
	mode              = kingpin.Flag("mode", "Backend mode: kafka (uses TSO if given) or tm (never uses TSO)").Default("kafka").Enum("kafka", "tm")
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
	loadBuf := make(chan [2]string, 10)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(loadBuf)
		if err := benchmark.LineByLine(loadFile, func(line string) error {
			operands := strings.SplitN(line, " ", 4)
			loadBuf <- [2]string{operands[2], encodeVal(operands[3])}
			return nil
		}); err != nil {
			panic(err)
		}
	}()
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for kv := range loadBuf {
				clis[0].Set(context.Background(), kv[0], kv[1])
			}
		}()
	}
	wg.Wait()

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

	for i := 0; i < *driverNum; i++ {
		for j := 0; j < *driverConcurrency; j++ {
			wg.Add(1)
			go func(seq int) {
				defer wg.Done()
				for {
					select {
					case <-runCtx.Done():
						return
					case op, ok := <-runBuf:
						if !ok {
							return
						}
						t0 := time.Now()
						switch op.ReqType {
						case benchmark.GetOp:
							val, err := clis[seq].Get(runCtx, op.Key)
							rec.ObserveGet(time.Now(), time.Since(t0), val, err)
						case benchmark.SetOp:
							err := clis[seq].Set(runCtx, op.Key, op.Val)
							rec.Observe(time.Now(), time.Since(t0), err)
						default:
							panic(fmt.Sprintf("invalid operation: %v", op.ReqType))
						}
					}
				}
			}(i)
		}
	}
	wg.Wait()

	s := rec.Summary()
	fmt.Printf("Throughput of %v drivers with %v concurrency: %.2f req/s\n",
		*driverNum, *driverConcurrency, s.TPS)
	fmt.Printf("Latency mean/p50/p99: %.3f / %.3f / %.3f ms; errors: %d; wrong gets: %d\n",
		s.LatMeanMs, s.LatP50Ms, s.LatP99Ms, s.Errors, s.WrongGets)

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
