# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repository is

The artifact of *Hybrid Blockchain Database Systems: Design and Performance* (Ge et al., PVLDB 15(5), 2022), extended for a thesis in three phases:

1. **Pluggable consensus and fault injection** (E1–E6): one Veritas core behind a `consensus.Consensus` interface, four backends, faults F1–F5.
2. **Fair comparison and verifiability** (E7–E9): open-loop load, Ed25519-signed Sets, verified reads, root attestations.
3. **Accountability** (E10–E12): eviction of faulty nodes from evidence, signed reads, client failover, quorum threshold.

`README.md` is the user guide. `experiments/NOTES.md` (Vietnamese) is the log of every deviation, ruling and result; read the relevant section before quoting a number or changing an experiment. Specs and plans for each phase are in `docs/superpowers/specs/` and `docs/superpowers/plans/`.

## Two code bases in one module

- **Original artifact, mostly untouched:** `BlockchainDB/`, `BigchainDB/`, `veritastm/`, `kafkarole/`, `raftkv/`, `tso/`, `tso_zookeeper/`, `dbconn/`, `veritas/server.go`, `scripts/`, `docker/`. Several of these packages do not compile as they are, so never build or test with `./...` or `./cmd/...`.
- **Thesis code:** `consensus/`, `veritas/core`, `veritas/sigs`, `veritas/audit`, `veritas/observe`, `veritas/benchmark/ycsbbench`, `cmd/veritas`, `cmd/auditor`, `cmd/demo`, `demo/`, `bft2node/`, `experiments/`.

`cmd/veritas --consensus=kafka|raft|tendermint|bft2` runs the new core; an empty `--consensus` runs the original Veritas server. In the harness, the backends `kafka-orig` and `tm-orig` are the paper's servers, the other four are the new core.

`bft2node/` is a separate Go module (Go 1.23, `relab/hotstuff` needs it); the main module builds with Go 1.20.

## Commands

All commands run from the repository root. Go is built and tested inside Docker images, not on the host.

Build the images (`hbdb-build`, `hbdb-bft2-build`, `hbdb-node`, `hbdb-kafka`) and `./bin`. Re-run after every code change before measuring: the CSV `commit` column records the checkout, not the image.

```bash
experiments/build.sh
```

Go tests of the main module (this package list, not `./...`):

```bash
docker run --rm -v "$PWD":/src -w /src hbdb-build go test -race ./consensus/... ./veritas/... ./cmd/auditor ./cmd/veritas ./cmd/demo ./demo/...
```

One Go test:

```bash
docker run --rm -v "$PWD":/src -w /src hbdb-build go test -race ./veritas/core -run 'TestName' -v
```

Go tests of the HotStuff sidecar:

```bash
docker run --rm -v "$PWD":/src -w /src/bft2node hbdb-bft2-build go test -race ./...
```

`go vet` takes the same package list. `gofmt -l cmd demo veritas consensus` already lists a few original files (`cmd/kafkabench`, `cmd/mongodb`, `cmd/veritastm-mongodb`, `veritas/benchmark/util.go`, `veritas/server.go`); leave them, and keep new code clean.

Harness tests (Python needs `pytest`; the plot tests also need `pandas` and `matplotlib`):

```bash
python3 -m pytest experiments -q
```

```bash
python3 -m pytest experiments/cluster/test_gen_compose.py -q -k publish_ports
```

```bash
experiments/exp/test_exp.sh
```

```bash
experiments/test_faults.sh
```

```bash
bash experiments/test_demo_scripts.sh
```

Browser smoke tests of the demo and the dashboard need Node with Playwright: `node demo/web/smoke.cjs`, `node experiments/plot/smoke_dashboard.cjs`.

Generate the YCSB data once (needs YCSB 0.17.0 in `scripts/temp/`, see README):

```bash
experiments/ycsb/gen.sh
```

One benchmark run on a fresh cluster (appends one row to `results/raw/<EXP>.csv`):

```bash
EXP=smoke BACKEND=raft N=4 CLIENTS=16 DURATION=20s experiments/run_one.sh
```

Same-root check of a backend (prints `CONSISTENT` or `DIVERGENT nodeX`):

```bash
experiments/check_consistency.sh bft2
```

Experiment drivers are `experiments/exp/e*.sh`; every driver accepts `--dry-run`. After a change to `proto/veritas/veritas.proto`, regenerate with `scripts/genproto_docker.sh` (it pins protoc-gen-go v1.3.2 so the diff stays minimal).

Live demo: `go run ./cmd/demo` (simulated cluster, needs Go on the host), or `BACKEND=bft2 N=4 experiments/demo_up.sh` and then the command it prints. Operator guide: `docs/demo.md`.

## Architecture

### Node (`veritas/core`)

Every backend runs the same core; only the consensus adapter differs, and the block size is the same for all of them.

- **Batcher** groups Sets into blocks (size or `--batch-flush` timer) and attaches the node's pending root attestations and evidence to its next block.
- **`consensus.Consensus`** (`consensus/consensus.go`) is total-order broadcast: `Submit`, `Deliver`, `Close`. `Submit` success means accepted, not delivered; a block may be lost or delivered twice and callers must tolerate both.
- **Apply loop** (`server.go`) consumes `Deliver` in order and is the only place where state changes: the Executor checks versions, writes Redis and updates the Sparse Merkle Tree ledger; then the Certifier and Accountant process the attestations and evidence carried by that block.
- **Determinism rule:** everything that decides certification or eviction is derived from the block order inside the apply loop, so all honest nodes certify the same roots and evict the same node at the same height without extra messages. Do not move such decisions outside the apply loop or make them depend on local time.

Backends: `consensus/kafka` (the paper's approve-all, made deterministic), `consensus/raft` (hashicorp/raft, followers forward to the leader over HTTP), `consensus/tendermint` (v0.35 over ABCI, one ABCI tx = one Veritas block), `consensus/bft2` (adapter to the `bft2-node` sidecar: replicas on port 7200, local API 7201, forwarding 7202), `consensus/memnet` (in-memory, for tests and the demo simulation).

Versions come from the TSO for kafka and raft (stale writes abort) and from `(height, index)` of the consensus order for tendermint and bft2 (`--version-mode=ordered`). This is a known confounder when comparing backends.

### Verifiability and accountability (phases 2–3)

- `--attest-every=K --n=N`: after applying height h a node signs `(h, root)`; a height is certified with Q matching signatures (default 2f+1, `--attest-quorum` to change). A node whose root differs from a certified one turns `diverged` and answers reads with `DataLoss`.
- `GetVerified` returns value, version, Merkle proof, root and height from one executor lock hold, plus the attestations; `ycsbbench --read-mode=plain|verified|quorum|certified` selects what the client checks.
- `--sign-reads=off|on-failure|always`: a signed response whose proof does not match the certified root is evidence; the client sends it with `ReportEvidence`, the receiving node puts it in a block, and every node re-checks it in the apply loop.
- The Accountant evicts for `divergent_root` and `bad_read`. Eviction affects reads and attestation counting only; an evicted node still takes part in consensus, and the quorum does not shrink after an eviction.
- `--fault=silent|corrupt|readlie|readlie-evasive` (`fault.go`) are the node-side fault policies. `experiments/faults.sh` documents the `FAULT=` strings (F1–F6E) and how each is injected (`docker stop`, iptables, `tc netem`, or a policy armed at start and activated by signal).

Every flag added in phases 2–3 defaults to the previous behaviour; keep that when adding flags.

### Harness (`experiments/`)

`run_one.sh` is the single entry point of a measurement: it takes its configuration from environment variables (documented in its header), generates the Compose file with `cluster/gen_compose.py`, starts a fresh cluster (`cluster/up.sh`), loads and runs `ycsbbench`, injects the fault, samples `docker stats`, applies validity gates and the host-noise guard, and appends one CSV row plus per-run sidecar files under `results/raw/<EXP>/`. Drivers in `exp/` only loop over configurations and call it. `plot/` turns CSVs into the committed summaries and figures (`results/final`, `results/figs`, `results/e7`, `results/e9`, `results/e9b`, `results/e1x`).

Things that are easy to get wrong:

- `run_one.sh` holds `results/raw/.hbdb.lock`; one run at a time on the Compose project `hbdb`. Do not run other Docker workloads during a measurement (shared daemon, benchmark noise).
- A value that is not a CSV column (for example `BATCH_FLUSH`, `RATE`) is encoded in the run tag; rows with different values must go to different `EXP`s. Drivers treat existing rows as done, so do not reuse an `EXP` with different settings.
- `BACKEND=tendermint` requires `COMMIT_MODE=sync` and `CLIENTS`, `LOAD_THREADS` ≤ 256 (ABCI queue limit).
- E2–E6 read each backend's best `CLIENTS` from E1; E9–E12 run at 80% of the saturation rate found by E7 (`E9_RATE_<BACKEND>` and similar). Run the prerequisite first.
- Drivers E7–E12 fix `MEM=1g` and `BATCH_FLUSH=100ms`; phase-1 runs use 768 MB and the 1 s default.
- `results/raw/`, `results/ycsb/`, `bin/` and `experiments/cluster/generated/` are not committed.

### Demo (`cmd/demo`, `demo/`)

A web UI that drives either an in-process cluster on `memnet` (`demo/cluster/sim.go`) or the Docker cluster (`demo/cluster/docker.go`). Nodes expose applied-block events through `ServerConfig.OnApplied` and the observer HTTP server (`veritas --demo-events-addr`, `veritas/observe`); `PUBLISH_PORTS=1` makes `gen_compose.py` publish node ports on 127.0.0.1. Static pages are in `demo/web/static/`; the pages under `present/` take their numbers from `present/data.js`, generated by `experiments/plot/build_present_data.py` from a tagged commit, so regenerate it instead of editing numbers by hand.

## Conventions

- Record deviations from a spec, rulings and results in `experiments/NOTES.md`, in Vietnamese, with the commit they refer to.
- Milestones are tagged: `p0-baseline` … `p4-faults` (phase 1), `p7-openloop`, `p10-attest` (phase 2), `p11-accountability` (phase 3).
- The `commit` column of the phase-1 CSVs holds pre-rewrite hashes; the mapping is at the end of the phase-1 part of `experiments/NOTES.md`.
