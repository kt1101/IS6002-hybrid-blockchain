# Veritas Pluggable Consensus Experiments — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Biến Veritas thành nền tảng đồng thuận cắm-rút (Kafka, Raft, Tendermint, HotStuff/CometBFT), kèm bộ tiêm lỗi và bộ chạy thí nghiệm E1–E6 trên MacBook M3, để trả lời RQ1–RQ3.

**Architecture:** Tách `veritas/server.go` thành `veritas/core` (Batcher, Executor, gRPC Server, FaultPolicy) dùng chung, nói chuyện với từng backend qua interface `consensus.Consensus`. Mọi node chạy trong Docker (linux/arm64) do một harness sinh docker-compose điều khiển; bộ benchmark chạy trong container client, ghi kết quả ra CSV; script Python vẽ biểu đồ.

**Tech Stack:** Go 1.20 toolchain (module `hybrid`, go.mod giữ `go 1.15`), gRPC + `proto/veritas`, Redis 6, BadgerDB + Sparse Merkle Tree (`veritas/ledger`), Kafka 2.7.0 (`confluent-kafka-go`), hashicorp/raft v1.3.2, Tendermint v0.35.0, Docker Desktop (Apple Silicon), Bash, Python 3 (pandas, matplotlib, pytest).

**Spec:** `docs/superpowers/specs/2026-10-29-pluggable-consensus-experiments-design.md`

## Global Constraints

- Nền tảng chạy: mọi image `linux/arm64`; không dùng binary x86-64 có sẵn (`docker/veritas/tendermint` là x86-64 — build lại từ source).
- Phiên bản cố định: Kafka `2.7.0`, Tendermint `v0.35.0`, hashicorp/raft `v1.3.2`, Redis `6.x`.
- Quy mô: N ∈ {4, 7, 10, 13, 16}; (N, f) ∈ {(4,1), (7,2), (10,3)} cho E6.
- Mặc định: block size `100`, giá trị YCSB `1 KB`, workload `YCSB-A`, phân phối `uniform`.
- Đo: khởi động `10s` (bỏ), đo `60s`; E6 đo `180s`, tiêm lỗi ở giây `60`. 3 lần chạy; độ lệch chuẩn/trung bình > `10%` thì chạy lại; CPU host > `90%` thì gắn cờ `saturated`.
- Chế độ commit: E1–E6 dùng `--commit-mode=sync`; `async` chỉ cho kiểm tra hồi quy P1.
- Kiểm tra hồi quy P1: throughput Kafka mới (async) chênh ≤ ±10% so với P0 ở N=4, block 100, YCSB-A, 64 và 256 client.
- Version: Kafka và Raft dùng TSO (`--version-mode=client`); Tendermint và BFT thứ hai dùng `--version-mode=ordered`, version = `height<<20 | index`.
- Mỗi phase kết thúc bằng git tag: `p0-baseline`, `p1-core`, `p2-backends`, `p3-bft2`, `p4-faults`. Mọi dòng CSV ghi commit hash.
- Cột CSV kết quả (thứ tự cố định): `ts,commit,exp,backend,n,f,clients,block_size,workload,rtt_ms,fault,rep,commit_mode,tps,lat_mean_ms,lat_p50_ms,lat_p99_ms,errors,wrong_gets,host_cpu_max_pct,saturated,net_bytes_per_block`.
- Thư mục: code Go theo package hiện có; mọi thứ về harness/thí nghiệm nằm dưới `experiments/`; kết quả dưới `results/` (gitignore trừ CSV đã chốt).

## Review Focus

1. **Lỗi bị đếm như thành công:** `Set` timeout/aborted mà vẫn cộng vào TPS sẽ thổi phồng số liệu. Kỳ vọng: chỉ request thành công được tính, lỗi ghi ở cột `errors`. → test ở Task 2.
2. **Sync `Set` treo khi block bị abort:** waiter không bao giờ được đánh thức. Kỳ vọng: trả `ErrAborted`. → test ở Task 7.
3. **Cùng key xuất hiện nhiều lần trong một block** (YCSB-A có nhiều update): thứ tự áp dụng phải tất định trên mọi node. Kỳ vọng: lần ghi sau cùng thắng, root digest giống nhau. → test ở Task 6.
4. **Cụm chưa sẵn sàng khi benchmark bắt đầu** (topic Kafka chưa tạo, Tendermint chưa ra block): sinh ra dòng TPS ≈ 0 trông như kết quả thật. Kỳ vọng: `run_one.sh` thoát lỗi, không ghi dòng CSV. → test ở Task 3.
5. **Laptop bão hòa ở N lớn:** số liệu phản ánh laptop chứ không phải giao thức. Kỳ vọng: dòng có `host_cpu_max_pct > 90` được gắn `saturated=1` và vẽ khác biệt. → test ở Task 18.

---

## P0 — Môi trường và baseline (tuần 1–2)

### Task 1: Git, toolchain và image Docker arm64

**Files:**
- Create: `.gitignore`, `experiments/docker/Dockerfile.build`, `experiments/docker/Dockerfile.node`, `experiments/docker/Dockerfile.kafka`, `experiments/build.sh`
- Modify: `go.mod`, `go.sum` (chỉ khi phải nâng `confluent-kafka-go`)

**Interfaces:**
- Produces: image `hbdb-build`, `hbdb-node`, `hbdb-kafka`; thư mục `bin/` (linux/arm64) chứa `veritas-kafka` (từ `cmd/veritas`), `veritas-tm` (từ `cmd/veritastm`), `veritas-tso` (từ `cmd/tso`), `ycsbbench` (từ `veritas/benchmark/ycsbbench`). Image `hbdb-node` có `redis-server`, `tendermint` v0.35.0, `iproute2` (tc), `iptables`, và `bin/*` trong `/usr/local/bin`.

- [ ] **Step 1: Khởi tạo git (người dùng tự chạy nếu sandbox chặn `.git`)**

```bash
git init && git add -A ':!scripts/temp' ':!bin' && git commit -m "chore: import original artifact"
```
`.gitignore` gồm: `scripts/temp/`, `bin/`, `results/raw/`, `experiments/cluster/generated/`, `__pycache__/`.

- [ ] **Step 2: Viết `Dockerfile.build`** — base `golang:1.20-bullseye`, cài `build-essential`, chép repo, `go build -o /out/<tên> <pkg>` cho 4 binary ở trên, và `go install github.com/tendermint/tendermint/cmd/tendermint@v0.35.0` ra `/out/tendermint`.

- [ ] **Step 3: Chạy build**

Run: `experiments/build.sh`
Expected: thư mục `bin/` có 5 file; `file bin/veritas-kafka` in ra `ARM aarch64`.
Nếu lỗi liên quan `librdkafka` (không có bản bundled cho linux/arm64): nâng `gopkg.in/confluentinc/confluent-kafka-go.v1` lên `v1.9.2`, chạy `go mod tidy` trong container build, build lại. Nếu vẫn lỗi: dừng và ghi vào `experiments/NOTES.md` để chuyển sang `segmentio/kafka-go` (rủi ro đã nêu trong spec mục 6).

- [ ] **Step 4: Viết `Dockerfile.node` và `Dockerfile.kafka`** — node: `debian:bullseye-slim` + `redis-server iproute2 iptables ca-certificates` + `COPY bin/ /usr/local/bin/`. kafka: `eclipse-temurin:11-jre` + `kafka_2.12-2.7.0.tgz` từ `archive.apache.org`.

- [ ] **Step 5: Kiểm tra image**

Run: `docker run --rm hbdb-node sh -c 'uname -m && tendermint version && veritas-kafka --help 2>&1 | head -1'`
Expected: `aarch64`, `v0.35.0`, dòng usage của kingpin.

- [ ] **Step 6: Commit**

```bash
git add .gitignore experiments/docker experiments/build.sh go.mod go.sum
git commit -m "build: arm64 toolchain and images"
```

### Task 2: Benchmark chạy theo thời gian, ghi CSV

**Files:**
- Create: `veritas/benchmark/recorder.go`, `veritas/benchmark/recorder_test.go`
- Modify: `veritas/benchmark/ycsbbench/main.go`, `veritas/driver/driver.go`

**Interfaces:**
- Produces:
  - `type Recorder struct` với `NewRecorder(warmup, duration time.Duration, start time.Time) *Recorder`, `Observe(at time.Time, lat time.Duration, err error)`, `Summary() Summary`, `Timeline() []int64` (số request thành công mỗi giây, tính từ `start`, cả giai đoạn khởi động).
  - `type Summary struct { TPS, LatMeanMs, LatP50Ms, LatP99Ms float64; Errors, WrongGets int64 }`
  - `ObserveGet(at time.Time, lat time.Duration, val string, err error)` — tăng `WrongGets` khi `strings.HasSuffix(val, CorruptMarker)`; `const CorruptMarker = "-CORRUPT"` (dùng lại ở Task 15).
  - `AppendCSVRow(path string, tags map[string]string, s Summary) error` — viết header nếu file chưa có, theo đúng thứ tự cột ở Global Constraints; cột không có trong `tags` để trống.
  - Flag mới của `ycsbbench`: `--warmup=10s --duration=60s --out=<csv> --tags=k=v,k=v --timeline=<path> --mode=kafka|tm` ; `--tso-addr` có thể rỗng.
  - `driver.Open(serverAddr, tsoAddr, signature)`: khi `tsoAddr == ""`, `Set` gửi `Version: 0` và không gọi TSO.

- [ ] **Step 1: Viết test cho Recorder**

```go
func TestRecorderCountsOnlySuccessInWindow(t *testing.T) {
    s0 := time.Unix(0, 0)
    r := NewRecorder(10*time.Second, 60*time.Second, s0)
    r.Observe(s0.Add(5*time.Second), time.Millisecond, nil)              // warmup: bỏ
    for i := 0; i < 600; i++ { r.Observe(s0.Add(11*time.Second), 2*time.Millisecond, nil) }
    r.Observe(s0.Add(12*time.Second), time.Second, errors.New("timeout"))  // lỗi
    r.Observe(s0.Add(71*time.Second), time.Millisecond, nil)             // sau cửa sổ: bỏ
    s := r.Summary()
    if s.TPS != 10 { t.Fatalf("TPS=%v want 10", s.TPS) }   // 600 / 60s
    if s.Errors != 1 { t.Fatalf("Errors=%v", s.Errors) }
    if s.LatP99Ms != 2 { t.Fatalf("p99=%v", s.LatP99Ms) }  // lỗi không vào phân bố latency
}
func TestRecorderCountsWrongGets(t *testing.T) { /* ObserveGet với "v-CORRUPT" trong cửa sổ → WrongGets == 1 */ }
func TestTimelineBucketsPerSecond(t *testing.T) { /* 3 obs ở giây 11, 1 obs ở giây 12 → Timeline()[11]==3, [12]==1 */ }
func TestAppendCSVRowWritesHeaderOnce(t *testing.T) { /* gọi 2 lần → file có 3 dòng, dòng 1 đúng header */ }
```

- [ ] **Step 2: Chạy test, xác nhận FAIL**

Run: `docker run --rm -v $PWD:/src -w /src hbdb-build go test ./veritas/benchmark/ -run 'Recorder|Timeline|CSV' -v`
Expected: FAIL (`undefined: NewRecorder`)

- [ ] **Step 3: Cài `recorder.go`** — giữ toàn bộ latency thành công trong slice rồi sắp xếp khi `Summary()` (≤ vài triệu phần tử, đủ RAM); p50/p99 theo chỉ số `ceil(q*n)-1`.

- [ ] **Step 4: Sửa `ycsbbench/main.go`** — giữ phần load dữ liệu; vòng chạy lặp lại file run (quay vòng) cho tới khi hết `warmup+duration`; mỗi kết quả gọi `Observe`/`ObserveGet`; cuối cùng gọi `AppendCSVRow` và ghi timeline (mỗi dòng một số). `--mode=tm` dùng `driver.Open(addr, "", sig)`. Sửa `driver.go` theo Interfaces.

- [ ] **Step 5: Chạy test, xác nhận PASS**

Run: lệnh ở Step 2. Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add veritas/benchmark veritas/driver
git commit -m "bench: time-based runs, percentiles, CSV output"
```

### Task 3: Harness cụm Docker (thay ssh/OVS)

**Files:**
- Create: `experiments/cluster/gen_compose.py`, `experiments/cluster/test_gen_compose.py`, `experiments/cluster/up.sh`, `experiments/cluster/down.sh`, `experiments/cluster/health.sh`, `experiments/run_one.sh`, `experiments/lib.sh`

**Interfaces:**
- Produces:
  - `gen_compose.py --backend {kafka-orig,tm-orig,kafka,raft,tendermint,bft2} --n N --block-size B --commit-mode M --cpus C --mem M --out FILE` sinh compose: mạng `hbdb` subnet `172.28.0.0/16`; node i (1..N) ở `172.28.0.$((10+i))`, tên `node$i`, `cap_add: [NET_ADMIN]`, `cpus`, `mem_limit`; dịch vụ `kafka` ở `.2` (chỉ backend kafka*), `tso` ở `.3` (backend dùng TSO), `client` ở `.4` (chạy `ycsbbench`). Mặc định `--cpus 0.5 --mem 768m`.
  - `run_one.sh` nhận biến môi trường `EXP BACKEND N F CLIENTS BLOCK_SIZE WORKLOAD RTT_MS FAULT REP COMMIT_MODE DURATION`, rồi: `down.sh` → `up.sh` → `health.sh` (thất bại → exit 2, không ghi CSV) → chạy bench trong `client` → lấy mẫu `docker stats` mỗi giây trong lúc chạy → điền `host_cpu_max_pct`, `saturated`, `net_bytes_per_block` → `down.sh`. Kết quả ghi vào `results/raw/<EXP>.csv`, timeline vào `results/raw/<EXP>/<tag>.timeline`.
  - `health.sh`: mọi node trả lời gRPC `Verify("health")` và một `Set` thăm dò commit được trong 60s.
  - `down.sh`: `docker compose down -v` (xóa volume, không để dữ liệu sót giữa các lượt).

- [ ] **Step 1: Viết test cho generator**

```python
def test_kafka_compose_has_n_nodes_and_broker():
    c = gen(backend="kafka", n=4, block_size=100, commit_mode="sync", cpus=0.5, mem="768m")
    assert sorted(k for k in c["services"] if k.startswith("node")) == ["node1","node2","node3","node4"]
    assert c["services"]["node1"]["networks"]["hbdb"]["ipv4_address"] == "172.28.0.11"
    assert "kafka" in c["services"] and "tso" in c["services"]
    assert c["services"]["node2"]["cap_add"] == ["NET_ADMIN"]
def test_tendermint_compose_has_no_kafka_or_tso(): ...
def test_volumes_are_per_node_and_named(): ...  # để down -v xóa sạch
```

- [ ] **Step 2: Chạy test, xác nhận FAIL** — Run: `python3 -m pytest experiments/cluster -q` → FAIL (`ModuleNotFoundError`/`NameError`).

- [ ] **Step 3: Cài `gen_compose.py`** — lệnh khởi động từng backend lấy từ `scripts/start_veritas_kafka.sh` và `scripts/start_veritas_tendermint.sh` (cùng cờ, đổi địa chỉ sang IP mới); cấu hình Tendermint sinh bằng `tendermint testnet --v N` trong `up.sh` rồi mount vào từng node.

- [ ] **Step 4: Chạy test, xác nhận PASS** — lệnh ở Step 2.

- [ ] **Step 5: Smoke test end-to-end**

Run: `EXP=smoke BACKEND=kafka-orig N=4 CLIENTS=16 DURATION=20s experiments/run_one.sh && tail -1 results/raw/smoke.csv`
Expected: một dòng CSV có `tps` > 0. Lặp lại với `BACKEND=tm-orig`.

- [ ] **Step 6: Test cụm chưa sẵn sàng** — tạm cho `health.sh` timeout 1s (biến `HEALTH_TIMEOUT=1`) với `tm-orig`. Run: `HEALTH_TIMEOUT=1 ... run_one.sh; echo $?` → Expected: `2`, và `results/raw/smoke.csv` không thêm dòng.

- [ ] **Step 7: Commit**

```bash
git add experiments/cluster experiments/run_one.sh experiments/lib.sh
git commit -m "harness: docker-compose cluster, health check, single-run driver"
```

### Task 4: Chạy baseline P0

**Files:**
- Create: `experiments/exp/p0_baseline.sh`, `experiments/NOTES.md`, `results/p0-baseline.csv`

- [ ] **Step 1: Viết `p0_baseline.sh`** — với `BACKEND ∈ {kafka-orig, tm-orig}`, `COMMIT_MODE=async` cho kafka-orig (hành vi gốc), 3 lần chạy: (a) N=4, CLIENTS ∈ {4,16,64,256}; (b) CLIENTS = tốt nhất từ (a), N ∈ {4,8,16}.
- [ ] **Step 2: Chạy** — Run: `experiments/exp/p0_baseline.sh` (ban đêm). Expected: `results/raw/p0.csv` có 2×(4+3)×3 = 42 dòng.
- [ ] **Step 3: Kiểm tra xu hướng** — Expected: kafka-orig cao hơn tm-orig khoảng 10× tại N=4, và tm-orig giảm khi N tăng. Ghi số liệu, cấu hình máy và mọi sai lệch so với bài gốc vào `experiments/NOTES.md`. Nếu N=16 bị `saturated`, ghi quyết định giới hạn N ≤ 13.
- [ ] **Step 4: Commit và tag**

```bash
cp results/raw/p0.csv results/p0-baseline.csv
git add experiments/exp/p0_baseline.sh experiments/NOTES.md results/p0-baseline.csv
git commit -m "exp: P0 baseline on M3" && git tag p0-baseline
```

---

## P1 — Core và Kafka backend (tuần 3–5)

### Task 5: Interface `Consensus` và backend trong bộ nhớ

**Files:**
- Create: `consensus/consensus.go`, `consensus/memnet/memnet.go`, `consensus/memnet/memnet_test.go`

**Interfaces:**
- Produces:

```go
package consensus
type Consensus interface {
    Submit(ctx context.Context, blk []byte) error
    Deliver() <-chan []byte
    Close() error
}
```
- `memnet.NewHub(n int) *Hub`; `(*Hub).Node(i int) consensus.Consensus` — mọi node nhận cùng một thứ tự block; `(*Hub).Drop(i int, on bool)` để test mô phỏng node im lặng. Chỉ dùng cho test.

- [ ] **Step 1: Test** — `TestMemnetSameOrderAllNodes`: 3 node Submit đồng thời 100 block → 3 kênh Deliver ra cùng dãy byte. `TestMemnetCloseClosesDeliver`.
- [ ] **Step 2: FAIL** — Run: `go test ./consensus/... -v` (trong `hbdb-build`, như Task 2) → FAIL.
- [ ] **Step 3: Cài** — Hub giữ một mutex và một danh sách kênh; Submit gửi tuần tự tới mọi node.
- [ ] **Step 4: PASS** — lệnh ở Step 2.
- [ ] **Step 5: Commit** — `git commit -m "consensus: interface and in-memory hub"`

### Task 6: Executor tất định

**Files:**
- Create: `veritas/core/store.go`, `veritas/core/executor.go`, `veritas/core/executor_test.go`

**Interfaces:**
- Consumes: `veritas.Encode/Decode` (`veritas/entry.go`), `ledger.NewLedger/Append/AppendBlk/GetRootDigest` (`veritas/ledger/storage.go`), `pbv.Block/SharedLog/SetRequest`.
- Produces:

```go
type KVStore interface {
    Get(ctx context.Context, key string) (val string, ok bool, err error)
    Set(ctx context.Context, key, val string) error
}
func NewRedisStore(cli *redis.Client) KVStore
func NewMapStore() KVStore // test

type VersionMode int
const ( VersionClient VersionMode = iota; VersionOrdered )

type Executor struct{ /* store, ledger, mode, fault */ }
func NewExecutor(s KVStore, l *ledger.LogLedger, mode VersionMode, fp FaultPolicy) *Executor
func (e *Executor) Validate(ctx context.Context, blk *pbv.Block) bool
func (e *Executor) Apply(ctx context.Context, height int64, raw []byte, blk *pbv.Block) (committed, aborted []int64, err error)
func (e *Executor) RootDigest() []byte
```
- Luật: `Validate` trả false nếu bất kỳ Set nào có version đã lưu ≥ version của nó (đúng luật gốc trong `veritas/server.go`). `Apply` duyệt `SharedLog` theo thứ tự trong block; ở `VersionClient` bỏ qua SharedLog cũ (seq vào `aborted`); ở `VersionOrdered` version = `height<<20 | index` (index là vị trí SharedLog trong block), không bao giờ abort. Mỗi Set ghi `Encode(value, version)` vào store và `ledger.Append(key, value+"-"+version)`; cuối block `ledger.AppendBlk(raw)`. `FaultPolicy` tạm dùng `NoFault{}` (định nghĩa ở Task 15; trong task này khai báo interface rỗng tối thiểu `OnApply(key, val string) string` trong `veritas/core/fault.go`).

- [ ] **Step 1: Test**

```go
func TestApplyWritesValueAndVersion(t *testing.T)          // sau Apply: Decode(store[k]) == {Val:"v", Version:7}
func TestApplySkipsStaleClientVersion(t *testing.T)        // store có version 9, tx version 7 → aborted == []int64{7}, giá trị không đổi
func TestValidateRejectsBlockWithStaleWrite(t *testing.T)  // false
func TestOrderedModeVersionIsHeightIndex(t *testing.T)     // height 3, index 2 → Version == 3<<20|2
func TestSameKeyTwiceInBlockLastWins(t *testing.T)         // Review Focus #3: hai SharedLog cùng key → giá trị của cái sau
func TestTwoExecutorsSameBlocksSameRoot(t *testing.T)      // bytes.Equal(e1.RootDigest(), e2.RootDigest())
```
- [ ] **Step 2: FAIL** — `go test ./veritas/core/ -run Executor -v` hoặc `-run 'Apply|Validate|Ordered|SameKey|TwoExecutors'` → FAIL.
- [ ] **Step 3: Cài** theo Interfaces.
- [ ] **Step 4: PASS**.
- [ ] **Step 5: Commit** — `git commit -m "core: deterministic executor"`

### Task 7: Batcher, gRPC Server, chế độ commit, đo phân rã thời gian

**Files:**
- Create: `veritas/core/batcher.go`, `veritas/core/server.go`, `veritas/core/metrics.go`, `veritas/core/server_test.go`

**Interfaces:**
- Consumes: `consensus.Consensus`, `memnet` (test), `Executor` (Task 6).
- Produces:

```go
func NewBatcher(size int, flush time.Duration, out func(*pbv.Block) error) *Batcher // flush mặc định 1s như code gốc
func (b *Batcher) Add(sl *pbv.SharedLog)

type CommitMode int
const ( CommitAsync CommitMode = iota; CommitSync )
var ErrAborted = errors.New("transaction aborted")

type ServerConfig struct {
    NodeID int; Signature string; BlockSize int
    Mode VersionMode; Commit CommitMode
}
func NewServer(cfg ServerConfig, c consensus.Consensus, ex *Executor, l *ledger.LogLedger) *Server // implements pbv.NodeServer
func (s *Server) Breakdown() Breakdown

type Breakdown struct { Batch, Consensus, Execute, Ledger time.Duration; Blocks int64 }
```
- `Set`: ở `VersionClient` seq = `req.Version`; ở `VersionOrdered` seq = `int64(NodeID)<<40 | counter` (duy nhất, không cần sửa proto). `CommitSync` chờ trên waiter theo seq tới khi block chứa nó được Apply; seq nằm trong `aborted` → trả `ErrAborted`. `CommitAsync` trả ngay (hành vi gốc).
- Vòng deliver: `height` = số block đã nhận (bắt đầu 1); unmarshal → `Apply` → đánh thức waiter. `Get` đọc store, trả `Value.Val`. `Verify` dùng `ledger.ProveKey` như `veritas/server.go`. Khi nhận SIGTERM, `cmd/veritas` ghi `Breakdown()` ra JSON `/results/breakdown-node<i>.json`.

- [ ] **Step 1: Test** (3 node trên `memnet`, `NewMapStore`, ledger trong `t.TempDir()`)

```go
func TestSyncSetVisibleOnAllNodesAfterReturn(t *testing.T)  // Set trên node1 trả về → Get trên node2, node3 thấy giá trị
func TestAsyncSetReturnsBeforeCommit(t *testing.T)          // Hub.Drop chặn deliver → Set async vẫn trả về trong 100ms
func TestSyncSetReturnsErrAbortedOnStaleVersion(t *testing.T) // Review Focus #2: version cũ → errors.Is(err, ErrAborted), không treo quá 2s
func TestBatcherFlushesOnTimer(t *testing.T)                // 1 tx, block size 100 → out được gọi sau ~1s
func TestVerifyRootMatchesAcrossNodes(t *testing.T)         // RootDigest bằng nhau trên 3 node sau 50 Set
```
- [ ] **Step 2: FAIL** — `go test ./veritas/core/ -v` → FAIL.
- [ ] **Step 3: Cài** theo Interfaces.
- [ ] **Step 4: PASS**.
- [ ] **Step 5: Commit** — `git commit -m "core: batcher, gRPC server, sync/async commit, breakdown"`

### Task 8: Auditor và kiểm tra cùng root digest

**Files:**
- Create: `veritas/audit/audit.go`, `veritas/audit/audit_test.go`, `cmd/auditor/main.go`, `experiments/check_consistency.sh`

**Interfaces:**
- Produces:
  - `CollectRoots(ctx context.Context, addrs []string) (map[string][]byte, error)` — gọi `Verify(key="audit")` trên mọi node, lấy `RootDigest`.
  - `Divergent(roots map[string][]byte) []string` — các node khác giá trị đa số (sắp xếp tăng dần).
  - `cmd/auditor --addrs a,b,c --once` → in `CONSISTENT` (exit 0) hoặc `DIVERGENT node3` (exit 1). Chế độ định kỳ (`--interval 1s --out path`) thêm ở Task 17.
  - `check_consistency.sh <backend>`: dựng cụm N=4, chạy bench 30s YCSB-A, đợi 5s cho các block cuối, chạy `auditor --once`.

- [ ] **Step 1: Test** — `TestDivergentFindsMinority` (4 node, node3 khác → `[]string{"node3"}`), `TestDivergentEmptyWhenAllEqual`.
- [ ] **Step 2: FAIL** → **Step 3: Cài** → **Step 4: PASS** (`go test ./veritas/audit/ -v`).
- [ ] **Step 5: Commit** — `git commit -m "audit: root digest collection and divergence check"`

### Task 9: Kafka backend tất định và `cmd/veritas` mới

**Files:**
- Create: `consensus/kafka/tracker.go`, `consensus/kafka/tracker_test.go`, `consensus/kafka/kafka.go`
- Modify: `cmd/veritas/main.go`, `experiments/cluster/gen_compose.py` (backend `kafka`)
- Giữ nguyên: `veritas/server.go` (để `kafka-orig` còn chạy được cho so sánh)

**Interfaces:**
- Consumes: `kafkarole.NewConsumer/NewProducer`, `pbv.Block{Type, Signature}`, `Executor.Validate` (truyền vào dưới dạng hàm).
- Produces:

```go
type Config struct {
    Brokers, Topic, Group, Signature string
    Parties  map[string]struct{}
    Validate func(blk []byte) bool
    ShouldVote func() bool // Task 15; nil = luôn vote
}
func New(cfg Config) (*Kafka, error) // implements consensus.Consensus

// thuần logic, test được không cần Kafka:
type tracker struct{ /* parties, self, pending map[int64]*entry */ }
func newTracker(self string, parties map[string]struct{}) *tracker
// onMessage xử lý một thông điệp theo thứ tự topic; trả các block đã đủ approve, theo thứ tự hoàn tất,
// và (nếu là proposal lần đầu thấy) yêu cầu node này gửi vote.
func (t *tracker) onMessage(m *pbv.Block, raw []byte) (delivered [][]byte, needVote bool)
```
- Khác code gốc (spec quyết định #8): approve của chính node chỉ được tính khi **đọc lại từ topic**. Block có một thông điệp `Abort` bị loại, không deliver.

- [ ] **Step 1: Test tracker**

```go
func TestTrackerDeliversAfterAllApprovesReadFromTopic(t *testing.T) // 3 party; proposal(n1) + approve n2, n3 → deliver đúng 1 lần, ở thông điệp cuối
func TestTrackerSameOrderForEveryNode(t *testing.T)    // cùng dãy thông điệp, self = n1/n2/n3 → cùng dãy delivered
func TestTrackerDropsBlockOnAbort(t *testing.T)
func TestTrackerIgnoresUnknownParty(t *testing.T)
```
- [ ] **Step 2: FAIL** — `go test ./consensus/kafka/ -v` → FAIL.
- [ ] **Step 3: Cài `tracker.go`, rồi `kafka.go`** (vòng đọc consumer → `tracker.onMessage` → nếu `needVote` thì gọi `Validate` và produce Approve/Abort → đẩy `delivered` vào kênh Deliver).
- [ ] **Step 4: PASS**.
- [ ] **Step 5: Viết lại `cmd/veritas/main.go`** — thêm flag `--consensus=kafka|raft|tendermint|bft2`, `--commit-mode=sync|async`, `--version-mode=client|ordered`, `--node-id`; giữ các flag cũ. Dựng `Executor` + `Server` + backend tương ứng.
- [ ] **Step 6: Kiểm tra nhất quán** — Run: `experiments/check_consistency.sh kafka` → Expected: `CONSISTENT`.
- [ ] **Step 7: Kiểm tra hồi quy** — chạy `COMMIT_MODE=async`, BACKEND=kafka, N=4, CLIENTS ∈ {64,256}, 3 lần; so với `results/p0-baseline.csv`. Expected: chênh ≤ ±10% (theo Global Constraints). Ghi kết quả vào `experiments/NOTES.md`. Nếu vượt ±10%: dừng, dùng `Breakdown` để tìm nguyên nhân trước khi sang P2.
- [ ] **Step 8: Commit và tag**

```bash
git add consensus/kafka cmd/veritas experiments
git commit -m "consensus: deterministic kafka approve-all backend" && git tag p1-core
```

---

## P2 — Raft và Tendermint (tuần 6–7)

### Task 10: Raft backend

**Files:**
- Create: `consensus/raft/raft.go`, `consensus/raft/raft_test.go`
- Modify: `cmd/veritas/main.go`, `experiments/cluster/gen_compose.py` (backend `raft`)

**Interfaces:**
- Consumes: `github.com/hashicorp/raft` v1.3.2, `raft-boltdb` (như `raftkv/peer.go`).
- Produces:

```go
type Peer struct { ID, RaftAddr, ForwardAddr string }
type Config struct {
    ID, Bind, DataDir string
    Peers     []Peer // đầy đủ, tĩnh
    Bootstrap bool   // true chỉ ở node1: BootstrapCluster với toàn bộ Peers (không dùng Join)
    Transport raft.Transport // nil = TCP thật; test truyền InmemTransport
}
func New(cfg Config) (*Raft, error) // implements consensus.Consensus
```
- FSM `Apply(log)` đẩy `log.Data` vào kênh Deliver. `Submit` trên leader gọi `raft.Apply(blk, 10*time.Second)`; trên follower POST `http://<ForwardAddr>/submit` của leader (tra `raft.Leader()` → `RaftAddr` → `ForwardAddr` qua `Peers`); thử lại tối đa 3 lần nếu leader đổi.

- [ ] **Step 1: Test** (3 node, `raft.NewInmemTransport`, cổng forward localhost)

```go
func TestRaftSameOrderAllNodes(t *testing.T)
func TestRaftSubmitOnFollowerIsForwarded(t *testing.T)
func TestRaftSurvivesLeaderShutdown(t *testing.T) // tắt leader → Submit tiếp trên node còn lại thành công trong 10s
```
- [ ] **Step 2: FAIL** → **Step 3: Cài** → **Step 4: PASS** (`go test ./consensus/raft/ -v`).
- [ ] **Step 5: Nhất quán** — `experiments/check_consistency.sh raft` → `CONSISTENT`.
- [ ] **Step 6: Commit** — `git commit -m "consensus: raft backend"`

### Task 11: Tendermint backend

**Files:**
- Create: `consensus/tendermint/app.go`, `consensus/tendermint/app_test.go`, `consensus/tendermint/tendermint.go`
- Modify: `cmd/veritas/main.go`, `experiments/cluster/gen_compose.py` (backend `tendermint`), template `config.toml`

**Interfaces:**
- Consumes: ABCI của Tendermint v0.35.0 (như `veritastm/ledgerapp.go`), `rpchttp`.
- Produces:
  - `type App struct` (implements `abcitypes.Application`); `DeliverTx(req)` đẩy `req.Tx` (một block Veritas đã marshal) vào kênh Deliver và trả Code 0; `CheckTx` trả Code 0.
  - `New(cfg Config{ABCISocket, RPCAddr string}) (*Tendermint, error)` — implements `consensus.Consensus`; `Submit` gọi `BroadcastTxSync` (core tự chờ qua Deliver).
  - `config.toml`: `mempool.max_tx_bytes = 4194304` (đủ cho block 1000 × 1 KB); các timeout giữ như `scripts/start_veritas_tendermint.sh`.
  - Chạy với `--version-mode=ordered`.

- [ ] **Step 1: Test** — `TestDeliverTxForwardsBlockBytes` (gọi `App.DeliverTx` 3 lần → Deliver ra đúng 3 block theo thứ tự), `TestCheckTxAcceptsAll`.
- [ ] **Step 2: FAIL** → **Step 3: Cài** → **Step 4: PASS** (`go test ./consensus/tendermint/ -v`).
- [ ] **Step 5: Nhất quán** — `experiments/check_consistency.sh tendermint` → `CONSISTENT`.
- [ ] **Step 6: Commit và tag** — `git commit -m "consensus: tendermint backend" && git tag p2-backends`

### Task 12: Spike HotStuff và quyết định ở mốc tuần 8 (code bỏ đi)

**Files:**
- Create (trên nhánh `spike/hotstuff`, không merge): thư mục thử nghiệm tùy ý
- Modify (trên nhánh chính): `experiments/NOTES.md`

- [ ] **Step 1:** Build một thư viện HotStuff Go (ứng viên đầu tiên: `github.com/relab/hotstuff`) trong `hbdb-build` cho linux/arm64.
- [ ] **Step 2:** Chạy cụm 4 replica theo ví dụ của thư viện trong 60s.
- [ ] **Step 3: Quyết định** theo ba tiêu chí: (a) build được trên arm64, (b) 4 replica commit ≥ 1000 lệnh trong 60s, (c) có API Go để nhúng: đẩy lệnh vào và nhận lệnh đã commit theo thứ tự. Đạt cả 3 → `BFT2=hotstuff`. Không đạt → `BFT2=cometbft` (v0.38). Nếu thời gian chỉ còn đủ cho đường 13 tuần → `BFT2=none`. Ghi quyết định và lý do vào `experiments/NOTES.md`.
- [ ] **Step 4: Commit** — `git commit -m "docs: HotStuff gate decision"` (chỉ NOTES.md).

---

## P3 — Backend BFT thứ hai (tuần 9–10; bỏ qua nếu `BFT2=none`)

### Task 13: `consensus/bft2`

**Files:**
- Create: `consensus/bft2/bft2.go`, `consensus/bft2/bft2_test.go`
- Modify: `cmd/veritas/main.go`, `experiments/cluster/gen_compose.py` (backend `bft2`)

**Interfaces:**
- Produces: `New(cfg Config) (*BFT2, error)` implements `consensus.Consensus`; chạy với `--version-mode=ordered`.
  - Nếu `hotstuff`: `Submit` đẩy block vào nguồn lệnh của thư viện; callback thực thi của thư viện đẩy lệnh đã commit vào Deliver theo thứ tự.
  - Nếu `cometbft`: sao chép Task 11, đổi import sang CometBFT v0.38; ABCI++ `FinalizeBlock` thay `DeliverTx` (duyệt `req.Txs` theo thứ tự).

- [ ] **Step 1: Test** — `TestBFT2SameOrderAllNodes` (4 replica trong tiến trình hoặc qua harness), `TestBFT2SurvivesOneCrash` (f=1).
- [ ] **Step 2: FAIL** → **Step 3: Cài** → **Step 4: PASS**.
- [ ] **Step 5: Nhất quán** — `experiments/check_consistency.sh bft2` → `CONSISTENT`.
- [ ] **Step 6: Commit và tag** — `git tag p3-bft2`

---

## P4 — Tiêm lỗi (tuần 10–12)

### Task 14: Script tiêm lỗi ở tầng container và mạng

**Files:**
- Create: `experiments/faults.sh`, `experiments/net.sh`

**Interfaces:**
- Produces (các hàm bash, được `run_one.sh` gọi khi `FAULT` khác `none`):
  - `fault_crash NODES AT` — `docker stop` các node ở giây AT (F1).
  - `fault_slow NODE AT MS` — `tc qdisc add dev eth0 root netem delay ${MS}ms` trong container (F3, MS=200).
  - `fault_silent_net NODE AT PORTS` — `iptables -A OUTPUT -p tcp --dport $PORT -j DROP` cho cổng đồng thuận (F2 cho raft/tendermint/bft2).
  - `net_set_rtt RTT_MS` — netem delay `RTT_MS/2` trên mọi node (E5).
  - `FAULT` dạng `F1:node2,node3@60`, `F3:node1@60`, …; mã F2/F4 cho kafka và F4 cho mọi backend chuyển thành flag `--fault` (Task 15).

- [ ] **Step 1:** Cài các hàm; chọn node lỗi theo quy ước: node cuối cùng (`node$N`, `node$((N-1))`, …) để không trùng node1 (leader bootstrap Raft), trừ kịch bản "leader chậm" chỉ định rõ `node1`.
- [ ] **Step 2: Kiểm tra** — Run: `EXP=fcheck BACKEND=raft N=4 FAULT=F1:node4@20 DURATION=40s experiments/run_one.sh`; Expected: timeline có throughput > 0 sau giây 20 (Raft còn đa số). Lặp lại với `BACKEND=kafka` → Expected: throughput = 0 sau giây 20 (giả thuyết F1 của spec).
- [ ] **Step 3: Commit** — `git commit -m "faults: crash, slow, network-silent injection"`

### Task 15: FaultPolicy trong core

**Files:**
- Modify: `veritas/core/fault.go`, `veritas/core/executor.go`, `consensus/kafka/kafka.go`, `cmd/veritas/main.go`
- Create: `veritas/core/fault_test.go`

**Interfaces:**
- Produces:

```go
type FaultPolicy interface {
    OnVote() bool                         // false = không gửi approve (F2, Kafka)
    OnApply(key, val string) string       // F4: trả giá trị bị làm hỏng
}
type NoFault struct{}
func ParseFault(spec string, start time.Time, after time.Duration) (FaultPolicy, error)
// spec ∈ {"none","silent","corrupt"}; chỉ có hiệu lực sau start+after.
```
- `corrupt`: `OnApply` trả `val + benchmark.CorruptMarker` (`"-CORRUPT"`) — đi vào cả store lẫn ledger, nên root digest lệch và `Get` trả giá trị sai. Flag: `--fault=none|silent|corrupt --fault-after=60s`. `kafka.Config.ShouldVote` = `fp.OnVote`.

- [ ] **Step 1: Test** — `TestNoFaultIsIdentity`, `TestCorruptOnlyAfterDelay` (trước mốc: giữ nguyên; sau: có hậu tố), `TestSilentStopsVoting`, `TestCorruptNodeDivergesRoot` (3 executor, 1 corrupt → `audit.Divergent` trả node đó).
- [ ] **Step 2: FAIL** → **Step 3: Cài** → **Step 4: PASS** (`go test ./veritas/core/ -run Fault -v`).
- [ ] **Step 5: Commit** — `git commit -m "core: fault policy (silent, corrupt)"`

### Task 16: Auditor định kỳ và chỉ số RQ3

**Files:**
- Modify: `cmd/auditor/main.go`, `experiments/run_one.sh`
- Create: `experiments/plot/rq3.py`, `experiments/plot/test_rq3.py`

**Interfaces:**
- Produces:
  - `auditor --addrs ... --interval 1s --out path`: mỗi giây một dòng `unix_ts,divergent_nodes`; `run_one.sh` chạy nó trong container `client` song song bench khi `FAULT` khác `none`.
  - `rq3.py`: `recovery_time(timeline, fault_at, pre_mean) -> float|None` (giây đầu tiên sau lỗi mà throughput ≥ 0.9 × trung bình trước lỗi; None nếu không hồi phục), `stall_seconds(timeline, fault_at) -> int` (số giây liên tiếp throughput = 0 kể từ lúc lỗi), `detect_latency(audit_rows, fault_at) -> float|None`.

- [ ] **Step 1: Test**

```python
def test_recovery_time():  assert recovery_time([10]*60 + [0,0,5,9,10], 60, 10) == 3
def test_never_recovers(): assert recovery_time([10]*60 + [0]*10, 60, 10) is None
def test_stall_seconds():  assert stall_seconds([10]*60 + [0,0,0,4], 60) == 3
def test_detect_latency(): assert detect_latency([(61,""),(62,"node4")], 60) == 2
```
- [ ] **Step 2: FAIL** → **Step 3: Cài** → **Step 4: PASS** (`python3 -m pytest experiments/plot -q`).
- [ ] **Step 5: Kiểm tra end-to-end** — `BACKEND=tendermint N=4 FAULT=F4:node4@20 DURATION=40s run_one.sh` → file audit có `node4` sau giây 20; dòng CSV có `wrong_gets` > 0.
- [ ] **Step 6: Commit và tag** — `git tag p4-faults`

---

## P5 — Chạy ma trận thí nghiệm (tuần 8–14, ban đêm)

### Task 17: Script E1–E6

**Files:**
- Create: `experiments/exp/e1_clients.sh` … `experiments/exp/e6_faults.sh`, `experiments/exp/backends.sh` (danh sách backend đang có: `kafka raft tendermint` + `bft2` nếu có)

**Interfaces:**
- Consumes: `run_one.sh` (Task 3), `faults.sh` (Task 14).
- Giá trị tham số lấy đúng ma trận của spec mục 5:
  - E1: CLIENTS ∈ {4,16,64,256}, N=4
  - E2: N ∈ {4,7,10,13,16}, CLIENTS = tốt nhất của từng backend từ E1 (đọc từ `results/raw/e1.csv`)
  - E3: BLOCK_SIZE ∈ {10,100,1000}
  - E4: WORKLOAD ∈ {workloada,workloadb,workloadc}
  - E5: RTT_MS ∈ {0,20,60}
  - E6: F1–F4 × (N,f) ∈ {(4,1),(7,2)}, thêm F5 với f+1 node cho `tendermint`/`bft2`; DURATION=180s, lỗi ở giây 60
- Mọi script: 3 lần chạy (REP=1..3), `COMMIT_MODE=sync`; nếu một backend chưa có (ví dụ `bft2`) thì bỏ qua và ghi chú.
- E2 với backend `kafka`: trước `down.sh`, gọi logic của `scripts/get_kafka_ops.sh` (đọc CURRENT-OFFSET và LOG-END-OFFSET) và ghi số thao tác đọc/ghi Kafka vào `results/raw/e2-kafka-ops.csv` (tái hiện Hình 8).

- [ ] **Step 1:** Viết script; mỗi script có cờ `--dry-run` in ra danh sách cấu hình.
- [ ] **Step 2: Kiểm tra số cấu hình** — Run: `for e in 1 2 3 4 5 6; do experiments/exp/e${e}_*.sh --dry-run | wc -l; done` → Expected (4 backend): 48, 60, 36, 36, 36, khoảng 108 (tức 16/20/12/12/12/~36 cấu hình × 3).
- [ ] **Step 3:** Chạy dần theo backend đã sẵn sàng (Kafka/Raft/TM từ tuần 8).
- [ ] **Step 4: Commit** — `git commit -m "exp: E1-E6 drivers"`

### Task 18: Tổng hợp và vẽ biểu đồ

**Files:**
- Create: `experiments/plot/plot.py`, `experiments/plot/test_plot.py`, `experiments/plot/fixtures/small.csv`

**Interfaces:**
- Produces:
  - `aggregate(df) -> DataFrame` — gom theo mọi cột cấu hình (trừ `rep`, `ts`, `commit`), tính mean/std của `tps`, `lat_p50_ms`, `lat_p99_ms`; thêm cột `cv = std/mean`, `saturated = any(saturated)`.
  - `rerun_list(agg) -> DataFrame` — các cấu hình có `cv > 0.10` hoặc ít hơn 3 lần chạy; ghi ra `results/rerun.txt`.
  - `plot_exp(agg, exp, x, out_png)` — trục y log cho throughput như bài gốc; điểm `saturated` vẽ marker rỗng và chú thích.
  - CLI: `python3 experiments/plot/plot.py results/raw/*.csv --out results/figs`.

- [ ] **Step 1: Test**

```python
def test_aggregate_mean_std(): ...         # 3 rep tps 90,100,110 → mean 100, std 10
def test_rerun_flags_high_cv(): ...        # cv 0.2 → có trong rerun_list
def test_rerun_flags_missing_reps(): ...   # 2 rep → có trong rerun_list
def test_saturated_propagates(): ...       # Review Focus #5: một rep saturated=1 → agg.saturated True
```
- [ ] **Step 2: FAIL** → **Step 3: Cài** → **Step 4: PASS** (`python3 -m pytest experiments/plot -q`).
- [ ] **Step 5: Commit** — `git commit -m "plot: aggregation, rerun list, figures"`

---

## P6 — Phân tích và dự phòng (tuần 15–16)

### Task 19: Chạy lại và chốt số liệu

- [ ] **Step 1:** Chạy lại mọi cấu hình trong `results/rerun.txt`, rồi chạy lại `plot.py` cho tới khi file rỗng hoặc chỉ còn điểm `saturated` (ghi lý do vào `experiments/NOTES.md`).
- [ ] **Step 2:** Sao chép CSV cuối vào `results/final/`, hình vào `results/figs/`; ghi commit hash dùng cho từng thí nghiệm vào `experiments/NOTES.md`.
- [ ] **Step 3:** Đối chiếu bảng giả thuyết F1–F5 (spec mục 4) với kết quả E6; ghi giả thuyết nào đúng, sai, kèm số liệu.
- [ ] **Step 4: Commit** — `git commit -m "results: final data and figures"`
