# Hybrid Blockchain Database Systems

This repository contains the code and instructions to reproduce the experiments presented in the paper "Hybrid Blockchain Database Systems: Design and Performance" (Ge et al., PVLDB 15(5), 2022).

It also contains a master's thesis extension (IS6002): Veritas rebuilt so that **only the consensus layer changes**, with four interchangeable backends, fault injection and an auditor. The extension is described in [Thesis extension](#thesis-extension-veritas-with-pluggable-consensus) and run with the E1–E6 drivers in [Running E1–E6](#running-e1e6). The original artifact and its scripts are unchanged and documented in [Reproduce the paper (original artifact)](#reproduce-the-paper-original-artifact).

## Thesis extension: Veritas with pluggable consensus

The paper concludes that consensus is the bottleneck of hybrid blockchain databases, but it only tests Kafka (CFT) and Tendermint (BFT), and only measures performance. The extension keeps the Veritas data path fixed, swaps the consensus protocol, and measures both performance and the cost of faults.

Research questions:

- **RQ1:** How does a BFT protocol with linear message complexity (HotStuff) compare with Tendermint in throughput, latency and scalability?
- **RQ2:** How does Raft (leader-based CFT) compare with Kafka approve-all (pub-sub CFT)?
- **RQ3:** With f faulty or Byzantine nodes, how much performance does each backend lose, and how fast does the ledger layer (Sparse Merkle Tree root digest) detect divergent state?

Design spec: [`docs/superpowers/specs/2026-10-29-pluggable-consensus-experiments-design.md`](docs/superpowers/specs/2026-10-29-pluggable-consensus-experiments-design.md). Implementation plan: [`docs/superpowers/plans/2026-10-29-pluggable-consensus-experiments.md`](docs/superpowers/plans/2026-10-29-pluggable-consensus-experiments.md). Every deviation, ruling and result is logged in [`experiments/NOTES.md`](experiments/NOTES.md); read it before quoting numbers.

### Architecture

```
Client (ycsbbench, YCSB)
      | gRPC Get / Set / BatchSet / Verify (proto/veritas, unchanged)
+-----v----------------------------+
| veritas/core  (same for all)     |
|  Batcher   -> blocks of Sets     |
|  Executor  -> version check,     |
|               Redis + SMT ledger |
|  Verify    -> Merkle proof       |
|  FaultPolicy (silent / corrupt)  |
+-----^---------------+------------+
      | Deliver()     | Submit(block)
+-----+---------------v------------+
| consensus.Consensus              |
+--------+--------+-----------+----+
| kafka  | raft   | tendermint| bft2 (HotStuff)
+--------+--------+-----------+-----
```

| Path | Content |
|---|---|
| `consensus/consensus.go` | `Consensus` interface (total-order broadcast: `Submit`, `Deliver`, `Close`) |
| `consensus/kafka` | Paper's approve-all protocol, made deterministic (a block is delivered once all N approvals are read back from the topic) |
| `consensus/raft` | hashicorp/raft v1.3.2, static cluster bootstrapped by node1; followers forward blocks to the leader over HTTP |
| `consensus/tendermint` | Tendermint v0.35.0 over ABCI; one ABCI tx = one Veritas block |
| `consensus/bft2`, `bft2node/` | Chained HotStuff (`relab/hotstuff` v0.5.0) in a sidecar process (`bft2-node`, separate Go module, Go 1.23) |
| `consensus/memnet` | In-memory backend for tests |
| `veritas/core` | Batcher, deterministic executor, gRPC server, `sync`/`async` commit, time breakdown, `FaultPolicy` |
| `cmd/veritas` | Node binary; `--consensus=kafka\|raft\|tendermint\|bft2` selects the backend (empty = the original Veritas server) |
| `cmd/auditor`, `veritas/audit` | Compares the ledger root digest of every node (`--once`, or periodic `--interval 1s` for RQ3) |
| `veritas/benchmark/ycsbbench` | Time-based YCSB client: CSV row, per-second timeline, p50/p99, read/write split, wrong-read counter |
| `experiments/` | Docker cluster harness, fault injection, E1–E6 drivers, plotting |
| `results/final`, `results/figs` | Final data set and figures |

Main design decisions (spec §2):

- **Same block for every backend.** The batcher lives in the core, so the block size is identical across backends.
- **Version source.** Kafka and Raft take versions from the TSO, as in the paper (stale writes abort). Tendermint and HotStuff use `(height, index)` from the consensus order (`--version-mode=ordered`). This is a known confounder.
- **Commit mode.** The original Kafka server answers `Set` when the tx is queued (`async`); the original Tendermint server waits for the block (`sync`). All E1–E6 comparisons use `--commit-mode=sync`.
- **Backend names.** `kafka-orig` and `tm-orig` are the paper's servers (P0 baseline). `kafka`, `raft`, `tendermint` and `bft2` run on the new core.

### Fault scenarios (RQ3)

N = 3f+1 for every backend. Client, Kafka broker and TSO are trusted.

| Code | Fault | Injection |
|---|---|---|
| F1 | Crash of f nodes | `docker stop` |
| F2 | Silent: alive but sends no votes | Kafka: `veritas --fault=silent` (votes withheld). Raft, Tendermint, HotStuff: consensus ports dropped with iptables |
| F3 | Slow: +200 ms | `tc netem` on the node's egress |
| F4 | Wrong state: corrupted values in Redis and the SMT | `veritas --fault=corrupt` |
| F5 | f+1 nodes with F2 (`F5-F2`, liveness) or F4 (`F5-F4`, safety); Tendermint and HotStuff only | as F2 / F4 |

### Differences from the paper's setup

All nodes run in Docker on one MacBook M3 Pro (arm64, 18 GB; Docker VM 11 CPU / 7.65 GiB), with 0.5 CPU and 768 MB per node. Consequences, detailed in `experiments/NOTES.md`:

- **Cluster size.** At most N = 8 on this machine (paper: up to 64), so E2 runs N = 4 and 7.
- **Data set.** 10k YCSB records of 1 KB instead of 100k.
- **Run length.** Runs are time-based (10 s warm-up, 60 s measurement) instead of a fixed number of operations, so only trends can be compared with the paper, not absolute numbers.
- **Toolchain.** `confluent-kafka-go/v2` (arm64), pruned SMT, memory-lean Badger options, Redis without persistence, and Tendermint limits raised for the ABCI queue.
- **Systems not re-run.** BlockchainDB, BigchainDB, record size, key distribution, processing time, bandwidth and underlying-database experiments are out of scope.

## Running E1–E6

All commands run from the repository root. The harness is written for macOS (it uses `caffeinate` when available) but only needs Docker and bash.

### 1. Prerequisites

- Docker with enough memory for N × 768 MB plus Kafka/ZooKeeper (about 7.5 GiB for N = 8).
- Python 3 with `pandas`, `matplotlib` and `pytest` (plots and tests).
- YCSB 0.17.0 unpacked in `scripts/temp/ycsb-0.17.0` (used offline by the data generator):

```bash
mkdir -p scripts/temp && curl -L https://github.com/brianfrankcooper/YCSB/releases/download/0.17.0/ycsb-0.17.0.tar.gz | tar xz -C scripts/temp
```

### 2. Build images and data

Build the binaries and the `hbdb-build`, `hbdb-bft2-build`, `hbdb-node` and `hbdb-kafka` images. **Re-run this after every code change**: the CSV `commit` column records the checkout, not the image.

```bash
experiments/build.sh
```

Generate the YCSB files `results/ycsb/{a,b,c}.{load,run}.dat` (10k records, 100k operations, 1 KB values):

```bash
experiments/ycsb/gen.sh
```

### 3. Smoke test

One run on a fresh cluster, appending one row to `results/raw/smoke.csv`:

```bash
EXP=smoke BACKEND=raft N=4 CLIENTS=16 DURATION=20s experiments/run_one.sh
```

Same-root-digest check of a backend (N = 4, 30 s of YCSB-A, then `auditor --once`; prints `CONSISTENT` or `DIVERGENT nodeX`):

```bash
experiments/check_consistency.sh bft2
```

### 4. Run the experiments

Run **E1 first**: E2–E6 use each backend's best `CLIENTS` from `results/raw/e1.csv`, and skip a backend whose E1 rows are incomplete. Every driver accepts `--dry-run`, which prints the planned runs and touches nothing.

| Exp. | RQ | Varies | Fixed | Runs | Command |
|---|---|---|---|---|---|
| E1 | 1, 2 | clients {4, 16, 64, 256} | N = 4, block 100, YCSB-A | 48 | `experiments/exp/e1_clients.sh` |
| E2 | 1, 2 | N {4, 7} (`NS`) | best clients | 24 | `experiments/exp/e2_nodes.sh` |
| E3 | 1, 2 | block size {10, 100, 1000} | N = 4 | 36 | `experiments/exp/e3_block.sh` |
| E4 | 1, 2 | YCSB workload {a, b, c} | N = 4 | 36 | `experiments/exp/e4_workload.sh` |
| E5 | 1, 2 | RTT {0, 20, 60} ms | N = 4 | 36 | `experiments/exp/e5_rtt.sh` |
| E6 | 3 | F1–F5 × (N, f) ∈ {(4, 1), (7, 2)} | 180 s, fault at second 60 | 144 | `experiments/exp/e6_faults.sh` |
| E7 | 1, 2 (phase 2) | open-loop rate (`RATES`, tendermint `TM_RATES`) | N = 4, block 100, YCSB-A, 256 drivers, `MEM=1g`, flush 100 ms | 81 | `experiments/exp/e7_rate.sh`, then `experiments/plot/saturation.py` |

Each run takes about 2–3 minutes including cluster setup (E6: about 4 minutes). The four backends are run in the order `kafka raft tendermint bft2`.

Examples:

```bash
experiments/exp/e1_clients.sh --dry-run
```

```bash
experiments/exp/e1_clients.sh
```

```bash
BACKENDS="raft bft2" experiments/exp/e3_block.sh
```

```bash
NS="4 7 10 13 16" experiments/exp/e2_nodes.sh
```

The last command is the plan's full N sweep, for a machine with at least about 12 GiB for Docker.

**Batch flush.** E1–E6 keep the original 1 s batcher flush. With closed-loop clients a block of 100 is rarely full, so at 1 s the throughput mostly reflects the timer (see `BATCH_FLUSH` in `experiments/NOTES.md`). The 100 ms variant of E1 compares the protocols more directly; it needs its own `EXP` name because the CSV has no flush column:

```bash
EXP=e1-bf100ms BATCH_FLUSH=100ms experiments/exp/e1_clients.sh
```

**Common environment variables** (see the header of `experiments/exp/exp_lib.sh` and `experiments/run_one.sh`):

| Variable | Default | Meaning |
|---|---|---|
| `BACKENDS` | `kafka raft tendermint bft2` | Subset or order of backends |
| `REPS` / `EXTRA_REPS` | 3 / 2 | Repetitions; extra reps while CV of tps > 10% |
| `EXP` | `e1` … `e6` | Name of `results/raw/<EXP>.csv` |
| `DURATION` / `WARMUP` | 60s / 10s (E6: 180s) | Measurement window |
| `CPUS` / `MEM` | 0.5 / 768m | Per-node limits |
| `FIXED_CLIENTS`, `BEST_CLIENTS_<BACKEND>` | from E1 | Override the clients of E2–E6 |
| `E6_NF`, `F5_KINDS`, `E6_LEADER` | `4:1 7:2`, `F5-F2 F5-F4`, 1 | E6 matrix; `E6_LEADER=1` adds crash and slow faults on node1 (the Raft leader) for raft and kafka |
| `E6_KINDS` | `F1 F2 F3 F4` | f-node faults of E6 (and, for F1/F3, the leader faults); e.g. `F1 F2 F4` where `sch_netem` is missing |

**Behaviour of the drivers:**

- **Resumable.** A run whose row already exists is skipped. A failed run (exit 2 or 3) is retried once on a fresh cluster, then logged to `results/raw/<EXP>.failures`.
- **Parameter guard.** `results/raw/<EXP>.params` stores DURATION, WARMUP, CPUS, MEM, LOAD_THREADS, BATCH_FLUSH and the commit. A later run of the same `EXP` with other values stops; use a new `EXP` instead.
- **Validity gate** (no-fault runs only). A run is rejected and retried in any of these cases:
  - more than 1% of its writes fail with an error other than `Aborted` (`VALID_MAX_WRITE_ERR_PCT`; tendermint needed 5% at 256 clients, see `experiments/NOTES.md`);
  - 5 consecutive seconds pass without a successful operation (`VALID_ZERO_RUN_SECS`);
  - a node log shows a fatal error;
  - the final `auditor --once` reports `DIVERGENT`.
- **Single run at a time.** A lock under `results/raw/.hbdb.lock` enforces it. Do not run other Docker workloads during a batch, and keep the host idle and plugged in.

### 5. Output

Per run, under `results/raw/`:

| File | Content |
|---|---|
| `<EXP>.csv` | One 22-column row: commit, backend, N, f, clients, block size, workload, RTT, fault, rep, tps, mean/p50/p99 latency, errors, wrong reads, host CPU, `saturated`, network bytes per block |
| `<EXP>/<tag>.timeline`, `.wtimeline` | Successful operations (all / writes) per second |
| `<EXP>/<tag>.ops.csv` | Separate read and write throughput, latency and errors |
| `<EXP>/<tag>.stats` | Every `docker stats` sample (CPU, memory, network) |
| `<EXP>/<tag>.faults`, `.audit`, `.leaders` | E6: injection times, auditor results, Raft leader changes |
| `<EXP>/<tag>.breakdown/` | Per-node time breakdown of the core (batch / consensus / execute / ledger) |
| `e2-kafka-ops.csv` | Kafka read/write offsets per run (the paper's Figure 8) |

### 6. Final data, figures and RQ3 table

```bash
python3 experiments/plot/assemble_final.py results/raw results/final
```

```bash
python3 experiments/plot/plot.py results/final/*.csv --out results/figs
```

```bash
python3 experiments/plot/rq3_table.py results/final/e6 --out results/figs
```

`assemble_final.py` replaces or pools the re-run configurations (decisions in `results/final/assembly.txt`) and copies the per-run files into `results/final/<exp>/` (not committed). `plot.py` also lists in its output the configurations that still need a re-run (CV > 10% or fewer than 3 reps). The committed final data are in `results/final/*.csv`, the figures and `rq3_summary.csv` in `results/figs/`.

The `commit` column of the committed CSVs holds the hash from before the history rewrite (`a0e9aa9`, today `e7baa60`); the mapping is at the end of `experiments/NOTES.md`.

### 7. Tests

Go tests of the main module run in the `hbdb-build` image (Go 1.20), and those of `bft2node/` in the `hbdb-bft2-build` image (Go 1.23):

```bash
docker run --rm -v "$PWD":/src -w /src hbdb-build go test ./consensus/... ./veritas/... ./cmd/auditor ./cmd/veritas
```

Do not use `./cmd/...` or `./...`: some packages of the original artifact (`raftkv`, `cmd/veritastm-mongodb`, `cmd/veritas-redisql`) do not compile as they are.

```bash
docker run --rm -v "$PWD":/src -w /src/bft2node hbdb-bft2-build go test ./...
```

Python and shell tests of the harness:

```bash
python3 -m pytest experiments -q
```

```bash
experiments/exp/test_exp.sh
```

### P0 baseline (paper's servers)

`experiments/exp/p0_baseline.sh` runs the paper's `kafka-orig` and `tm-orig` servers on the same harness (clients {4, 16, 64, 256} at N = 4, then N = {4, 8, 16} via `NS_B`). The results are in `results/p0-baseline.csv`.

### Cloud campaign (E1–E6 at 100 ms flush)

A second, separate data set re-runs E1–E6 with `BATCH_FLUSH=100ms` on one linux/amd64 cloud host (4 CPU, 15.7 GiB), so that the 1 s batcher timer no longer hides the protocol differences. Its data, figures and RQ3 tables are in [`results/cloud-bf100ms/`](results/cloud-bf100ms/). Setup, gaps and results are in the "Đợt chạy cloud" section of [`experiments/NOTES.md`](experiments/NOTES.md). Do not mix this data with the M3 data: the hardware differs.

Gaps of this campaign:

- **No E5 and no F3.** The host kernel has no `sch_netem`.
- **N = 7 saturates the 4 host CPUs.** Do not draw scaling conclusions from it.
- **Some runs ran out of memory at 768 MB.** bft2 F4/F5-F4 runs are therefore in a separate `MEM=1g` set (`e6mem1g`), and raft N = 7 F1-leader and F4 have 2 of 3 reps.

```bash
experiments/cloud/build.sh
```

```bash
experiments/cloud/campaign.sh
```

```bash
python3 experiments/cloud/assemble.py results/cloud-bf100ms results/cloud-bf100ms/final
```

```bash
python3 experiments/plot/plot.py results/cloud-bf100ms/final/*.csv --out results/cloud-bf100ms/figs --rerun results/cloud-bf100ms/rerun.txt
```

```bash
python3 experiments/plot/rq3_table.py results/cloud-bf100ms/final/e6 --out results/cloud-bf100ms/figs
```

`build.sh` is only needed behind a TLS-intercepting proxy without Debian mirrors (a Claude Code cloud session). On a normal Docker host, use `experiments/build.sh`. `campaign.sh` itself runs anywhere.

## Reproduce the paper (original artifact)

The scripts below are the paper's original artifact. They expect the original multi-host setup (SSH, Open vSwitch), not the Docker harness above.

### Pre-requisites

- Install dependecies
- Download and prepare YCSB data
- Build binaries
- Build docker images

```bash
$ cd scripts
$ ./install_dependencies.sh
$ ./gen_ycsb_data.sh 
$ ./build_binaries.sh
$ cd ../docker/veritas
$ ./build_docker.sh
$ cd ../bigchaindb
$ ./build_docker.sh
$ cd ../blockchaindb
$ ./build_docker.sh
```

For each system, we run the following set of experiments:

- Effect of number of clients
- Effect of number of nodes (peers)
- Effect of key access distribution (uniform, latest, zipfian)
- Effect of different YCSB workloads (WorkloadA, WorkloadB, WorkloadC)
- Effect of block size
- Effect of transaction size (record size)
- Effect of transaction processing time
- Effect of Networking

### Veritas + Kafka

Run the following scripts to reproduce the above-mentioned experiments:

```
./run_benchmark_veritas_kafka_clients.sh
./run_benchmark_veritas_kafka_nodes.sh
./run_benchmark_veritas_kafka_distribution.sh
./run_benchmark_veritas_kafka_workload.sh
./run_benchmark_veritas_kafka_blocksize.sh
./run_benchmark_veritas_kafka_recordsize.sh
./run_benchmark_veritas_kafka_proctime.sh
./run_benchmark_veritas_kafka_networking.sh
```

Each script will generate a folder of the form ``logs-...-<timestamp>``. Check the results recorded in the files of these folders.

In addition, we run the following experiments for Veritas + Kafka:

- Effect of Veritas Peer Count on Kafka Operations (Figure 8)
- Effect of the Underlying Database (Figure 11)
- Effect of Zookeeper TSO (not included in the paper)

```
./run_benchmark_veritas_kafka_database.sh
./run_benchmark_veritas_kafka_clients_tso_zk.sh
```

#### Effect of Veritas Peer Count on Kafka Operations

This reuses the logs from running ``./run_benchmark_veritas_kafka_nodes.sh``, which are in the form ``logs-nodes-veritas-<timestamp>``. In particular, we are interested in the following Kafka counters: 

- CURRENT-OFFSET - the current position of a consumer. There are N consumers in total, so we need the sum of these counters to get the number of read operations.
- LOG-END-OFFSET - the offset of the last message written to the topic. This represents the number of write operations.

Please run ``./get_kafka_ops.sh logs-nodes-veritas-...`` to get the read and write operations for a given log folder. Note that Figure 8 represents the average of 3 such logs (runs).


### Veritas + Tendermint

We use the same ``veritas`` docker images. Then, we run the following scripts:

```
./run_benchmark_veritas_tendermint_clients.sh
./run_benchmark_veritas_tendermint_nodes.sh
./run_benchmark_veritas_tendermint_distribution.sh
./run_benchmark_veritas_tendermint_workload.sh
./run_benchmark_veritas_tendermint_recordsize.sh
./run_benchmark_veritas_tendermint_proctime.sh
./run_benchmark_veritas_tendermint_networking.sh
```

### BlockchainDB

We use ``blockchaindb`` docker images. Then, we run the following scripts:

```
./run_benchmark_blockchaindb_clients.sh
./run_benchmark_blockchaindb_nodes.sh
./run_benchmark_blockchaindb_distribution.sh
./run_benchmark_blockchaindb_workload.sh
./run_benchmark_blockchaindb_blocksize.sh
./run_benchmark_blockchaindb_recordsize.sh
./run_benchmark_blockchaindb_proctime.sh
./run_benchmark_blockchaindb_networking.sh
./run_benchmark_blockchaindb_sharding.sh
```

### 

### BigchainDB

We use ``bigchaindb`` docker images. Then, we run the following scripts:

```
./run_benchmark_bigchaindb_clients.sh
./run_benchmark_bigchaindb_nodes.sh
./run_benchmark_bigchaindb_distribution.sh
./run_benchmark_bigchaindb_workload.sh
./run_benchmark_bigchaindb_recordsize.sh
./run_benchmark_bigchaindb_proctime.sh
./run_benchmark_bigchaindb_networking.sh
```

### BigchainDB Parallel Validation

To run BigchainDB with Parallel Validation (PV), modify lines 16 and 17 of [BigchainDB/scripts/start-all.sh](BigchainDB/scripts/start-all.sh), such as:

```
# bigchaindb start > /dev/null 2>&1 &
bigchaindb start --experimental-parallel-validation > /dev/null 2>&1 &
```

Next, re-build the Docker image:

```
cd docker/bigchaindb
rm -r bigchaindb-2.2.2/scripts
./build_docker.sh
```

Then repeat all the steps of BigchainDB:

```
./run_benchmark_bigchaindb_clients.sh
./run_benchmark_bigchaindb_nodes.sh
./run_benchmark_bigchaindb_distribution.sh
./run_benchmark_bigchaindb_workload.sh
./run_benchmark_bigchaindb_recordsize.sh
./run_benchmark_bigchaindb_proctime.sh
./run_benchmark_bigchaindb_networking.sh
```


### Aborted Transaction

The number of aborted transactions is reported by Veritas (Kafka) in its server log, such as:

```
2021/11/13 13:57:20 Abort transaction 1
```

By counting the number of such messages, one can get the total number of aborted transactions:

```
cat logs-distribution-veritas-kafka-.../veritas-latest-logs/veritas-* | grep Abort | wc -l
```

There are no aborted transactions in Veritas (TM) and BlockchainDB.

## License

MIT License

## Authors

Zerui Ge, Dumitrel Loghin, Tianwen Wang, Pingcheng Ruan, Beng Chin Ooi 
