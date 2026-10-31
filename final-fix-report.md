# Final-review fix round: report

Branch `worktree-agent-a71b5a1f3f4d4d629`, based on `exp/pluggable-consensus` @ 27ef981.
Brief: `.superpowers/sdd/2026-10-29-pluggable-consensus-experiments/final-fix-brief.md`.

The 22-column CSV schema is unchanged. All new data goes into per-run sidecars in
`results/raw/<EXP>/`. go.mod and go.sum are unchanged, and no new modules were added.

**I could not compile the Go code** (this shell has no Go toolchain). The Go changes are
small and I reviewed them by hand. They must still pass `go vet` / `go test -race` before
any image is rebuilt (commands in the last section).

## Per brief item

### 1. C1: read/write split (done)
- `veritas/benchmark/recorder.go`:
  - New `OpKind` type: `Observe` = OpOther, `ObserveGet` = OpRead, new `ObserveSet` = OpWrite.
  - Latencies and errors are kept per kind.
  - `Summary()` still merges every kind, so the CSV row is the same as before.
  - New `OpSummaries()`: one entry for read and one for write, each with count, tps, mean, p50, p99 and errors.
  - New `WriteOpsCSV`, using `OpsCSVHeader = op,count,tps,lat_mean_ms,lat_p50_ms,lat_p99_ms,errors`.
  - New `SetTimeline()`: successful Sets per second, with the same length and t=0 as `Timeline()`.
- `ycsbbench`:
  - New flags `--ops` and `--wtimeline`.
  - Sets now call `ObserveSet`.
  - The per-kind lines are also printed to the bench log.
- `run_one.sh` passes `--ops=/results/<EXP>/<tag>.ops.csv` and `--wtimeline=/results/<EXP>/<tag>.wtimeline`.
- Tests added:
  - `TestOpSummariesSplitReadsAndWrites`
  - `TestSetTimelineCountsOnlyWrites`
  - `TestWriteOpsCSV`
  - the itest "sidecars" in `test_faults.sh`
- Deferred: `plot.py` latency figures still read the CSV. NOTES.md says the thesis figures must use the `write` line of `.ops.csv`.

### 2. C2: resources and net_bytes_per_block (done)
- **Sampler** (`run_one.sh`):
  - Every `docker stats --no-stream --format '{{.Name}},{{.CPUPerc}},{{.MemUsage}},{{.NetIO}}'` call goes to `<tag>.stats`, one line per container. Format is `HBDB_STATS_HEADER` in lib.sh: epoch_ms, utc, name, cpu_pct, mem_used, mem_limit, net_rx, net_tx, and the same values parsed to bytes (`hbdb_stats_lines`).
  - `host_cpu_max_pct` is computed from the same output.
  - M1: the sampler and the watchdog exit once `kill -0 $$` fails, and an empty or failed `docker stats` call is followed by `sleep 0.5`.
- **Window block count**: `cmd/veritas` has a new SIGUSR2 handler (`watchSnapshotSignal`). The k-th signal writes `breakdown-node<i>.snap<k>.json`.
  - `run_one.sh` signals every core node in parallel at the run-phase marker + WARMUP (snapshot 1).
  - It signals again right after the bench returns (snapshot 2), while the sampler is still running.
  - The epochs and node1's result go to `<tag>.snaps`.
  - The snap files are moved into `<tag>.breakdown/` like the final breakdown.
- **net_bytes_per_block** (`hbdb_net_bytes_per_block` in lib.sh) is defined as:
  - numerator: Σ net_tx_bytes of `nodeK` and `kafka` containers in the stats sample closest to snapshot 2, minus the same sum in the sample closest to snapshot 1;
  - denominator: node1's `blocks` in snap2 minus snap1;
  - result rounded to whole bytes.
- Where it stays empty:
  - always for kafka-orig and tm-orig;
  - for a core backend, when a snapshot or sample is missing or Δblocks ≤ 0 (with a warning).
- Approximations (in NOTES.md and the run_one header):
  - window edges are off by up to the ~1–1.5 s sample spacing;
  - docker NetIO has about 4 significant digits;
  - node tx also includes replies to clients;
  - TSO and client traffic are excluded;
  - the Kafka broker is included because it is the kafka backend's consensus path.
- Tests:
  - unit tests of the stats parsing, `hbdb_json_int` and `hbdb_net_bytes_per_block`;
  - itests: sidecars, failed snapshot, `docker stats` failing (no busy loop), kafka-orig empty column.

### 3. I1: lost clients (done)
- (a) New `benchmark.Backoff` (Min/Max, doubling, reset on success) and `SleepCtx`. Each run-phase worker pauses after any failed request: 50 ms doubling to 1 s (`--error-backoff`, `--error-backoff-max`). Tests: `TestBackoffDoublesToMaxAndResets`, `TestBackoffMaxBelowMin`, `TestSleepCtx`.
- (b) `rq3.py` threshold:
  - The recovery and dip threshold is 0.9 × pre_mean × (N−k)/N.
  - k = the distinct nodes in `docker_stop` / `iptables_drop` rows, so F1, network F2 and F5-F2.
  - Kafka F2 (`silent_on`), F3 and F4 keep a share of 1.
  - N comes from `_n<N>_` in the tag (`main`) or from `load_run(..., n=)`; unknown N gives a share of 1.
  - New output keys: `n`, `faulty_nodes`, `threshold_share`, `recovery_threshold_tps`.
  - The existing tests are unchanged (their tag "r" has no N). 8 new tests.
- Deferred (review suggestions not in the brief):
  - a per-node success timeline;
  - skipping the TSO call while a node connection is not READY. The backoff already limits the TSO hammering to at most ~20 calls/s per lost worker.

### 4. I4: E6 leader faults (done)
- `e6_faults.sh`: with `E6_LEADER=1` (the default), each backend in `E6_LEADER_BACKENDS` (default `raft kafka`) gets two more configs per (N,f): `F1:node1@60` and `F3:node1@60`. That is 8 configs, 24 more runs: **E6 = 144**. `E6_LEADER=0` gives back 120.
- Kafka has no leader node, so its node1 runs are a control; this is documented.
- Leader logging: for raft with a fault, `run_one.sh` writes every `entering leader state` line of every node log (docker timestamps) to `<tag>.leaders`. This is hashicorp/raft's INFO line, and raft already logs to stderr at INFO.
- Tests: dry-run counts, backend selection, validation, an itest for the leaders file, and the E6 run count / fault column.

### 5. I7: validity gate (done; one threshold deliberately less strict than the brief)
For FAULT=none, `check_validity` runs after the existing checks and before the row is written. Any failure is exit 3 (no row; the driver retries). The checks:
- write errors / (ok+errors) of `.ops.csv` > `VALID_MAX_WRITE_ERR_PCT`=1%. A missing ops sidecar also fails (an old image).
- **≥ `VALID_ZERO_RUN_SECS`=3 consecutive zero seconds** in the measurement window of `.timeline`. Missing trailing seconds count as zero.
  - The brief asked for "any second with 0 ops", but the existing P0 data shows that rule would reject valid runs: every tm-orig c4 rep (`results/raw/p0/tm-orig_n4_f0_c4_*.timeline`) has exactly one isolated zero second.
  - `VALID_ZERO_RUN_SECS=1` gives the literal rule.
- any node log containing `FATAL apply`, `buffer is full`, `replica failed, exiting` or `can no longer commit` (the last two are bft2-node's give-up and exit-3 messages).
- core backends only, after the checks above pass: `auditor --once` in the client container, run `AUDIT_WAIT_SECS`=5 s after the bench. It is retried twice while it says DIVERGENT; still DIVERGENT means exit 3. No verdict is only a warning. The output is kept in `<tag>.final_audit`.

Fault runs are not gated, which matches the brief. Tests: 17 itests (write-error boundary 3/203 vs 2/202, missing ops, zero runs including the warmup and short-timeline cases, the three log patterns, DIVERGENT 3× / 1×, no verdict, the fault-run exemption, kafka-orig).

### 6. I8: caffeinate (done)
- In `exp_lib.sh`, `exp_keep_awake` runs at the start of `exp_begin`. When the run is not a dry run, `HBDB_CAFFEINATED` is unset, `NO_CAFFEINATE` is not 1 and `caffeinate` exists, it does `export HBDB_CAFFEINATED=1; exec caffeinate -dims "$0" "$@"`. Elsewhere it is a no-op.
- Tests use a stub `caffeinate` on PATH that logs and execs the command. They cover: one re-exec, no loop, no re-exec in dry-run, `NO_CAFFEINATE`, and exit-code passthrough through the stub.
- **Unverified**: that the real macOS `caffeinate` returns the child's exit status (smoke step 5 checks it).

### 7. I9: rep outermost (done)
- `exp_main` makes these passes:
  - a validate pass;
  - one pass per rep (`EXP_PASS=1..REPS`, `exp_cfg` runs only that rep);
  - one pass per extra rep (`EXP_PASS=x1..xEXTRA_REPS`, new `exp_extra_rep`, the same CV rule as before).
- The resume key is unchanged.
- `exp_resolve_clients` logs and records skipped backends only in pass 1, so there are no duplicate lines.
- Tests: the updated E1 order, the cross-backend order, the dry-run order, and the noise-rule tests unchanged (20 rows).

### 8. M3/M4: params and disk guards (done)
- Params guard:
  - `results/raw/<EXP>.params` holds DURATION, WARMUP, CPUS, MEM, LOAD_THREADS ("default" when unset), BATCH_FLUSH (unset = 1s) and `commit` (the same `git describe --dirty` as run_one).
  - It is written just before the first real run of the EXP, so a refused plan or a dry run writes nothing.
  - Every later invocation, including `--dry-run`, compares and dies on any difference.
  - `EXP_ALLOW_COMMIT_CHANGE=1` waives only the commit.
- Disk guard: `exp_check_disk` runs before every run_one attempt. It runs `df -Pk results/raw` and requires at least `EXP_MIN_FREE_GB`=5 GiB free; otherwise it prints the summary and exits 1.
- Tests (with stub `df`):
  - params guard: a changed DURATION, CPUS (in a dry run), LOAD_THREADS or commit is refused; `EXP_ALLOW_COMMIT_CHANGE` goes on; a new EXP may use other settings;
  - disk guard: low disk stops the batch; `EXP_MIN_FREE_GB` lowers the limit; a dry run is not checked.

### 9. I5: RTT on the broker and the TSO (done; needs an image rebuild)
- `net.sh` `net_set_rtt` also delays `kafka` (kafka-orig, kafka) and `tso` (kafka-orig, kafka, raft) by RTT/2, and sets `NET_RTT_EXTRA`. The `.faults` netem_rtt detail now reads e.g. "on every node and on kafka tso".
- `gen_compose.py` gives `cap_add: [NET_ADMIN]` to the kafka and tso services. `Dockerfile.kafka` installs iproute2 in a separate layer, so the cached Kafka download is reused.
- With an old hbdb-kafka image, `tc` is missing: net_set_rtt fails and run_one exits 1, which aborts the batch (loud, not silent).
- Tests: net.sh unit tests (raft 5 tc calls, kafka 6), `test_gen_compose.py::test_broker_and_tso_can_take_netem`, and the itest rtt (tso tc call).

### Documentation only (NOTES.md, Vietnamese)
- New section "Sửa sau rà soát cuối" describes every change above.
- Notes added for I2, I3, I6, M2 (rebuild after every code change; this round changes ycsbbench, veritas and hbdb-kafka), and M7/M8.
- M5 corrected: 20 configs per (N,f), 40 × 3 = 120, now 48 × 3 = 144.
- M6 corrected: `net_bytes_per_block` is empty only for the original backends.

### Not in this round
C3 (SMT growth): the memory pilot is the controller's. `<tag>.stats` now records per-container memory for it.

## Files
- Go: `veritas/benchmark/{recorder.go,recorder_test.go,backoff.go,backoff_test.go}`, `veritas/benchmark/ycsbbench/main.go`, `cmd/veritas/{main.go,main_test.go}`
- Shell: `experiments/{run_one.sh,lib.sh,net.sh,test_faults.sh}`, `experiments/exp/{exp_lib.sh,e6_faults.sh,test_exp.sh}`
- Python: `experiments/plot/{rq3.py,test_rq3.py}`, `experiments/cluster/{gen_compose.py,test_gen_compose.py}`
- Docker/doc: `experiments/docker/Dockerfile.kafka`, `experiments/NOTES.md`

## Tests run here
- `bash experiments/test_faults.sh`: **387 passed, 0 failed**, under both `bash` (Homebrew) and `/bin/bash` 3.2; run twice with no flakes.
- `bash experiments/exp/test_exp.sh`: **198 passed, 0 failed**, under both bashes.
- Python (anaconda pytest, host) `pytest experiments/plot experiments/cluster experiments/exp experiments/test_faults.py`: **185 passed**.
- `bash -n` and `/bin/bash -n` on every touched shell file: ok. `py_compile` of rq3.py and gen_compose.py: ok.
- Dry runs (stdout `DRY_RUN:` lines):

  | driver | runs |
  |---|---|
  | e1 | 48 |
  | e2 | 24 |
  | e3 | 36 |
  | e4 | 36 |
  | e5 | 36 |
  | **e6** | **144** (was 120) |

  `E6_LEADER=0` gives 120; `F5_KINDS=F5-F4` gives 132. p0_baseline is unaffected.
- Go: **not compiled or run** (no toolchain).

## Controller commands and expected results

Unit:
```
gofmt -l veritas/benchmark cmd/veritas            # expect no output
go vet ./veritas/benchmark/... ./veritas/core ./cmd/veritas ./cmd/auditor
go test -race ./veritas/benchmark/... ./veritas/core ./cmd/veritas ./cmd/auditor
#   expect ok; new tests: TestOpSummariesSplitReadsAndWrites, TestSetTimelineCountsOnlyWrites,
#   TestWriteOpsCSV, TestBackoffDoublesToMaxAndResets, TestBackoffMaxBelowMin, TestSleepCtx,
#   TestSnapshotPath, TestSnapshotSignalWritesNumberedSnapshots
docker run --rm -v "$PWD":/w -w /w python:3 sh -c 'pip -q install pytest && python -m pytest -q experiments/plot experiments/cluster experiments/exp experiments/test_faults.py'
#   expect 185 passed (test_plot needs pandas/matplotlib as before)
bash experiments/test_faults.sh      # 387 passed, 0 failed
bash experiments/exp/test_exp.sh     # 198 passed, 0 failed
```

Docker smoke (after `experiments/build.sh`; about 15 min in total):
```
# 1. sidecars, net_bytes_per_block, validity + final audit (raft)
EXP=ffsmoke BACKEND=raft N=4 CLIENTS=16 DURATION=20s experiments/run_one.sh; echo rc=$?
T=results/raw/ffsmoke/raft_n4_f0_c16_b100_wa_rtt0_none_sync_r1
cat $T.ops.csv          # header + read,... + write,... (write lat_p50 >> read lat_p50)
wc -l $T.timeline $T.wtimeline   # both 30
head -3 $T.stats; wc -l $T.stats # header + ~5 containers x ~20-30 samples
cat $T.snaps            # 1,<ms>,ok / 2,<ms>,ok
ls $T.breakdown/        # breakdown-node{1..4}.json + .snap1.json + .snap2.json
cat $T.final_audit      # CONSISTENT
tail -1 results/raw/ffsmoke.csv | cut -d, -f20-22   # cpu,saturated,<positive integer>
#   log lines: "auditor --once: CONSISTENT", "net_bytes_per_block=<n> (node1 applied <k> blocks ...)"
# 2. kafka + RTT on broker/TSO (needs the rebuilt hbdb-kafka)
EXP=ffsmoke BACKEND=kafka N=4 CLIENTS=16 DURATION=20s RTT_MS=20 experiments/run_one.sh; echo rc=$?
grep netem_rtt results/raw/ffsmoke/kafka_n4_*rtt20*.faults   # "... on every node and on kafka tso (RTT_MS=20)"
# 3. raft leader crash: leaders file, backoff, rq3 threshold
EXP=ffsmoke BACKEND=raft N=4 CLIENTS=16 DURATION=40s FAULT=F1:node1@15 experiments/run_one.sh; echo rc=$?
L=results/raw/ffsmoke/raft_n4_f0_c16_b100_wa_rtt0_F1_node1@15_sync_r1
cat $L.leaders          # node1 ... entering leader state, then a later line from node2/3/4
grep -A3 'Errors by message' $L.bench.log   # errors ~ hundreds, not ~100k (backoff)
python3 experiments/plot/rq3.py $L | grep -E '^(n|faulty_nodes|threshold_share|recovery_threshold_tps|recovery_status)='
#   n=4 faulty_nodes=1 threshold_share=0.75
# 4. bft2 / tendermint pass the validity gate
EXP=ffsmoke BACKEND=tendermint N=4 CLIENTS=16 DURATION=20s experiments/run_one.sh; echo rc=$?   # 0, CONSISTENT
EXP=ffsmoke BACKEND=bft2 N=4 CLIENTS=16 DURATION=20s experiments/run_one.sh; echo rc=$?         # 0, CONSISTENT
# 5. caffeinate
caffeinate -dims sh -c 'exit 4'; echo $?    # expect 4 (exit status passthrough)
EXP=ffsmoke-e1 BACKENDS=raft REPS=1 EXTRA_REPS=0 DURATION=20s experiments/exp/e1_clients.sh &
sleep 5; pmset -g assertions | grep -i caffeinate   # an assertion owned by caffeinate
wait; cat results/raw/ffsmoke-e1.params             # DURATION=20s ... commit=<sha>
#   driver log starts with "re-executing under caffeinate -dims"; its 4 runs are rep 1 of c4/16/64/256
```

## Concerns
- The Go code is uncompiled.
  - Highest risk: alignment and `gofmt` only. The recorder refactor, `Backoff` and `watchSnapshotSignal` are plain code.
  - The SIGUSR2 test signals its own test process. Go has no default action for SIGUSR2, so an unhandled one is harmless.
- Every core run now does more work, about +6–8 s per fault-free run:
  - N parallel `docker compose exec` calls at the start of the window and at its end;
  - for fault-free runs, `docker compose logs` per node and `auditor --once` after 5 s.
  - The window-start execs coincide with the first measured second.
- Old images: ycsbbench rejects `--ops` (every run then fails rc 3 twice and lands in `.failures`), and an old hbdb-kafka breaks E5 kafka (exit 1). **Rebuild before the campaign.**
- `VALID_ZERO_RUN_SECS=3` differs from the brief's "any zero second" (see item 5).
- "buffer is full" also matches tendermint CheckTx submit errors, which also make the gate fail. The CLIENTS ≤ 256 guard should prevent them.
- `net_bytes_per_block` includes client replies in node tx. For read-heavy workloads (E4 B/C) it is dominated by Get values. Use it for cross-backend comparison at the same workload only. The raw `.stats` file allows other definitions offline.
- Backoff applies to every error, including kafka `Aborted`. That is a small throughput cost at the rare abort rate, but it changes the `errors` count against the P0/P1 runs, which had no backoff.
- E5 numbers measured before this round are not comparable with later ones (the broker and TSO are now delayed).
- The params guard compares raw env strings: e.g. `LOAD_THREADS=256` vs unset (default 256) counts as a difference.
